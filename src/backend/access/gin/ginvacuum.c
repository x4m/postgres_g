/*-------------------------------------------------------------------------
 *
 * ginvacuum.c
 *	  delete & vacuum routines for the postgres GIN
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/gin/ginvacuum.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/gin_private.h"
#include "access/ginxlog.h"
#include "access/xloginsert.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/read_stream.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

struct GinVacuumState
{
	Relation	index;
	IndexBulkDeleteResult *result;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	GinState	ginstate;
	BufferAccessStrategy strategy;
	MemoryContext tmpCxt;
};

/*
 * Vacuums an uncompressed posting list. The size of the must can be specified
 * in number of items (nitems).
 *
 * If none of the items need to be removed, returns NULL. Otherwise returns
 * a new palloc'd array with the remaining items. The number of remaining
 * items is returned in *nremaining.
 */
ItemPointer
ginVacuumItemPointers(GinVacuumState *gvs, ItemPointerData *items,
					  int nitem, int *nremaining)
{
	int			i,
				remaining = 0;
	ItemPointer tmpitems = NULL;

	/*
	 * Iterate over TIDs array
	 */
	for (i = 0; i < nitem; i++)
	{
		if (gvs->callback(items + i, gvs->callback_state))
		{
			gvs->result->tuples_removed += 1;
			if (!tmpitems)
			{
				/*
				 * First TID to be deleted: allocate memory to hold the
				 * remaining items.
				 */
				tmpitems = palloc_array(ItemPointerData, nitem);
				memcpy(tmpitems, items, sizeof(ItemPointerData) * i);
			}
		}
		else
		{
			gvs->result->num_index_tuples += 1;
			if (tmpitems)
				tmpitems[remaining] = items[i];
			remaining++;
		}
	}

	*nremaining = remaining;
	return tmpitems;
}

/*
 * Create a WAL record for vacuuming entry tree leaf page.
 */
static void
xlogVacuumPage(Relation index, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	XLogRecPtr	recptr;

	/* This is only used for entry tree leaf pages. */
	Assert(!GinPageIsData(page));
	Assert(GinPageIsLeaf(page));

	if (!RelationNeedsWAL(index))
		return;

	/*
	 * Always create a full image, we don't track the changes on the page at
	 * any more fine-grained level. This could obviously be improved...
	 */
	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_VACUUM_PAGE);
	PageSetLSN(page, recptr);
}


/*
 * Delete a posting tree page.
 *
 * Removes the page identified by dBuffer from the posting tree by updating
 * the left sibling's rightlink (in lBuffer) to skip over the deleted page,
 * and removing the downlink from the parent page (in pBuffer).
 *
 * The caller must hold exclusive locks on all three buffers, and no other
 * backend may hold a pin on dBuffer (IsBufferCleanupOK).  Concurrent
 * inserters and searchers that read the downlink or the left sibling's
 * rightlink before we remove them recover by observing the deleted flag
 * and moving right; the page is stamped with an XID and cannot be recycled
 * until they are all gone (see README).
 *
 * The buffers are NOT released nor unlocked here; the caller is responsible
 * for this.
 */
static void
ginDeletePostingPage(GinVacuumState *gvs, Buffer dBuffer, Buffer lBuffer,
					 Buffer pBuffer, OffsetNumber myoff)
{
	Page		page,
				parentPage;
	BlockNumber rightlink;
	BlockNumber deleteBlkno = BufferGetBlockNumber(dBuffer);

	page = BufferGetPage(dBuffer);
	rightlink = GinPageGetOpaque(page)->rightlink;

	/*
	 * Any insert which would have gone on the leaf block will now go to its
	 * right sibling.
	 */
	PredicateLockPageCombine(gvs->index, deleteBlkno, rightlink);

	START_CRIT_SECTION();

	/* Unlink the page by changing left sibling's rightlink */
	page = BufferGetPage(lBuffer);
	GinPageGetOpaque(page)->rightlink = rightlink;

	/* Delete downlink from parent */
	parentPage = BufferGetPage(pBuffer);
#ifdef USE_ASSERT_CHECKING
	do
	{
		PostingItem *tod = GinDataPageGetPostingItem(parentPage, myoff);

		Assert(PostingItemGetBlockNumber(tod) == deleteBlkno);
	} while (0);
#endif
	GinPageDeletePostingItem(parentPage, myoff);

	page = BufferGetPage(dBuffer);

	/*
	 * we shouldn't change rightlink field to save workability of running
	 * search scan
	 */

	/*
	 * Mark page as deleted, and remember last xid which could know its
	 * address.
	 */
	GinPageSetDeleted(page);
	GinPageSetDeleteXid(page, ReadNextTransactionId());

	MarkBufferDirty(pBuffer);
	MarkBufferDirty(lBuffer);
	MarkBufferDirty(dBuffer);

	if (RelationNeedsWAL(gvs->index))
	{
		XLogRecPtr	recptr;
		ginxlogDeletePage data;

		/*
		 * We can't pass REGBUF_STANDARD for the deleted page, because we
		 * didn't set pd_lower on pre-9.4 versions. The page might've been
		 * binary-upgraded from an older version, and hence not have pd_lower
		 * set correctly. Ditto for the left page, but removing the item from
		 * the parent updated its pd_lower, so we know that's OK at this
		 * point.
		 */
		XLogBeginInsert();
		XLogRegisterBuffer(0, dBuffer, 0);
		XLogRegisterBuffer(1, pBuffer, REGBUF_STANDARD);
		XLogRegisterBuffer(2, lBuffer, 0);

		data.parentOffset = myoff;
		data.rightLink = GinPageGetOpaque(page)->rightlink;
		data.deleteXid = GinPageGetDeleteXid(page);

		XLogRegisterData(&data, sizeof(ginxlogDeletePage));

		recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_PAGE);
		PageSetLSN(page, recptr);
		PageSetLSN(parentPage, recptr);
		PageSetLSN(BufferGetPage(lBuffer), recptr);
	}

	END_CRIT_SECTION();

	gvs->result->pages_newly_deleted++;
	gvs->result->pages_deleted++;
}


/*
 * Descend from the posting tree root to its leftmost leaf page.
 *
 * On return, the leftmost leaf is pinned and exclusively locked, and
 * *parentBlkno is set to the block number of its parent (the leftmost
 * internal page one level above the leaves), or InvalidBlockNumber if the
 * root itself is a leaf.
 *
 * The leftmost path is stable: page splits move keyspace to the right, and
 * the leftmost page of each level is never deleted (see README), so the
 * downlinks we follow cannot go away under us.
 */
static Buffer
ginStepToLeftmostLeaf(GinVacuumState *gvs, BlockNumber rootBlkno,
					  BlockNumber *parentBlkno)
{
	BlockNumber blkno = rootBlkno;

	*parentBlkno = InvalidBlockNumber;

	while (true)
	{
		Buffer		buffer;
		Page		page;
		PostingItem *pitem;

		buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, gvs->strategy);
		LockBuffer(buffer, GIN_SHARE);
		page = BufferGetPage(buffer);

		Assert(GinPageIsData(page));

		if (GinPageIsLeaf(page))
		{
			LockBuffer(buffer, GIN_UNLOCK);
			LockBuffer(buffer, GIN_EXCLUSIVE);

			/*
			 * While the page was unlocked, a concurrent insert could have
			 * turned the root into an internal page.  Retry from the same
			 * block if so.  (Non-root pages never change their leaf-ness.)
			 */
			if (GinPageIsLeaf(page))
				return buffer;

			UnlockReleaseBuffer(buffer);
			continue;
		}

		Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

		*parentBlkno = blkno;
		pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber);
		blkno = PostingItemGetBlockNumber(pitem);
		Assert(blkno != InvalidBlockNumber);

		UnlockReleaseBuffer(buffer);
	}
}

/*
 * Find and exclusively lock the parent of leaf page leafBlkno, in
 * preparation for deleting the leaf.
 *
 * *parentBlkno is used as a search hint, pointing to some page of the
 * internal level immediately above the leaves, at or to the left of the
 * parent we are looking for.  Because downlinks only ever move right (when
 * internal pages split) and our caller processes the leaves in
 * left-to-right order, the hint page from a previous call remains valid.
 * The search walks right from the hint until the downlink is found.
 *
 * On success, returns the pinned and exclusively locked parent buffer,
 * sets *off to the downlink's offset, and advances *parentBlkno to the
 * parent's block number.  Returns InvalidBuffer (with *parentBlkno
 * unchanged, so that searches for subsequent leaves can still succeed) if:
 *
 * - the downlink was not found (e.g. the leaf's split was never completed,
 *   so the downlink was never inserted); or
 *
 * - the downlink is the last one on the parent page.  Posting tree internal
 *   pages have no high keys; dataLocateItem() treats the last downlink as
 *   having no upper bound ("right infinity").  Removing it would cut the
 *   parent's keyspace short, sending insertions of keys between the
 *   remaining downlinks and the parent's right bound to pages under the
 *   parent's right sibling, corrupting the key order of the tree.  So the
 *   last downlink, and thereby the page it points to, must stay.
 */
static Buffer
ginLockLeafParent(GinVacuumState *gvs, BlockNumber *parentBlkno,
				  BlockNumber leafBlkno, OffsetNumber *off)
{
	BlockNumber blkno = *parentBlkno;

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;

		buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, gvs->strategy);
		LockBuffer(buffer, GIN_EXCLUSIVE);
		page = BufferGetPage(buffer);

		Assert(GinPageIsData(page));
		Assert(!GinPageIsLeaf(page));

		maxoff = GinPageGetOpaque(page)->maxoff;
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			PostingItem *pitem = GinDataPageGetPostingItem(page, i);

			if (PostingItemGetBlockNumber(pitem) == leafBlkno)
			{
				/* never delete the last downlink of an internal page */
				if (i == maxoff)
				{
					UnlockReleaseBuffer(buffer);
					return InvalidBuffer;
				}

				*parentBlkno = blkno;
				*off = i;
				return buffer;
			}
		}

		blkno = GinPageGetOpaque(page)->rightlink;
		UnlockReleaseBuffer(buffer);
	}

	return InvalidBuffer;
}

/*
 * Vacuum a posting tree: remove deletable TIDs from its leaf pages, and
 * delete leaf pages that become completely empty.
 *
 * The leaves are processed in a single left-to-right sweep by following
 * rightlinks, holding exclusive locks on the current page and its right
 * sibling at once (lock coupling).  Keeping the left sibling locked while
 * acquiring the right one both pins down the sibling link needed for the
 * deletion and follows the left-to-right page locking order used everywhere
 * else in GIN, so it cannot deadlock with concurrent insertions or searches.
 *
 * A page is deleted only if, in addition to being empty, no other backend
 * holds a pin on it (IsBufferCleanupOK), so nobody is about to insert into
 * or read from it.  If somebody does, we just leave the page in place; a
 * future vacuum can delete it.
 *
 * Empty leaves are deleted right away, except for pages that must survive
 * for the tree to stay navigable: the leftmost leaf (fullScan descents
 * cannot recover from stepping onto a deleted page), the rightmost leaf,
 * and leaves referenced by the last downlink of their parent (see
 * ginLockLeafParent).  Internal pages are never deleted; without high keys
 * their empty siblings cannot be spliced out safely.  Concurrent inserters
 * and searchers that run into a page we deleted recover by moving right,
 * as deleted pages keep their rightlink (see README).
 */
static void
ginVacuumPostingTree(GinVacuumState *gvs, BlockNumber rootBlkno)
{
	BlockNumber parentBlkno;
	Buffer		prevBuffer;
	Page		prevPage;
	MemoryContext oldCxt;

	prevBuffer = ginStepToLeftmostLeaf(gvs, rootBlkno, &parentBlkno);
	prevPage = BufferGetPage(prevBuffer);

	/* The leftmost leaf is never deleted, just vacuum its tuples */
	oldCxt = MemoryContextSwitchTo(gvs->tmpCxt);
	ginVacuumPostingTreeLeaf(gvs->index, prevBuffer, gvs);
	MemoryContextSwitchTo(oldCxt);
	MemoryContextReset(gvs->tmpCxt);

	while (!GinPageRightMost(prevPage))
	{
		BlockNumber prevBlkno = BufferGetBlockNumber(prevBuffer);
		BlockNumber blkno;
		Buffer		buffer;
		Page		page;

		/*
		 * Come up for air: release all locks and pins before the delay
		 * point, both to let it process interrupts (which is impossible
		 * while a buffer lock is held) and to avoid blocking readers of
		 * this page while cost-based vacuum delay makes us sleep.
		 *
		 * Only vacuum deletes posting tree pages, and there is at most one
		 * vacuum per index, so the page is still there when we re-lock it.
		 * If it was concurrently split, the pages that were split off to
		 * the right contain only tuples that we have already vacuumed, so
		 * it is fine to continue from the page's new rightlink.
		 */
		UnlockReleaseBuffer(prevBuffer);

		vacuum_delay_point(false);

		/*
		 * A wait here can be used to pause the sweep between two leaf
		 * pages, while no buffer locks or pins are held, letting a
		 * concurrent session mutate the part of the tree that the sweep
		 * has not reached yet.
		 */
		INJECTION_POINT("gin-vacuum-posting-tree-resume", NULL);

		prevBuffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, prevBlkno,
										RBM_NORMAL, gvs->strategy);
		LockBuffer(prevBuffer, GIN_EXCLUSIVE);
		prevPage = BufferGetPage(prevBuffer);

		if (GinPageRightMost(prevPage))
			break;
		blkno = GinPageGetOpaque(prevPage)->rightlink;

		/*
		 * Lock the right sibling before releasing the current page, so that
		 * we can delete it if it turns out to be empty.
		 */
		buffer = ReadBufferExtended(gvs->index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, gvs->strategy);
		LockBuffer(buffer, GIN_EXCLUSIVE);
		page = BufferGetPage(buffer);

		Assert(GinPageIsData(page));
		Assert(GinPageIsLeaf(page));
		Assert(!GinPageIsDeleted(page));

		oldCxt = MemoryContextSwitchTo(gvs->tmpCxt);
		ginVacuumPostingTreeLeaf(gvs->index, buffer, gvs);
		MemoryContextSwitchTo(oldCxt);
		MemoryContextReset(gvs->tmpCxt);

		/*
		 * A page with an incomplete split must stay: its right sibling has
		 * no downlink yet, so deleting this page (and thereby its downlink)
		 * would leave the right sibling unreachable through the parent
		 * until the split is finished.
		 */
		if (GinDataLeafPageIsEmpty(page) &&
			!GinPageRightMost(page) &&
			!GinPageIsIncompleteSplit(page) &&
			BlockNumberIsValid(parentBlkno) &&
			IsBufferCleanupOK(buffer))
		{
			OffsetNumber off;
			Buffer		parentBuffer;

			parentBuffer = ginLockLeafParent(gvs, &parentBlkno, blkno, &off);
			if (BufferIsValid(parentBuffer))
			{
				/*
				 * This fires with the two leaf pages and the parent page
				 * all exclusively locked.  A 'wait' attached here pauses
				 * vacuum at its maximum lock footprint, blocking any
				 * concurrent descent through the parent page.
				 */
				INJECTION_POINT("gin-vacuum-delete-posting-page", NULL);

				ginDeletePostingPage(gvs, buffer, prevBuffer, parentBuffer,
									 off);
				UnlockReleaseBuffer(parentBuffer);
				UnlockReleaseBuffer(buffer);

				/*
				 * The current page's rightlink now points past the deleted
				 * page; continue the sweep from it.
				 */
				continue;
			}
		}

		/* Advance: the right sibling becomes the new current page */
		UnlockReleaseBuffer(prevBuffer);
		prevBuffer = buffer;
		prevPage = page;
	}

	UnlockReleaseBuffer(prevBuffer);
}

/*
 * returns modified page or NULL if page isn't modified.
 * Function works with original page until first change is occurred,
 * then page is copied into temporary one.
 */
static Page
ginVacuumEntryPage(GinVacuumState *gvs, Buffer buffer, BlockNumber *roots, uint32 *nroot)
{
	Page		origpage = BufferGetPage(buffer),
				tmppage;
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(origpage);

	tmppage = origpage;

	*nroot = 0;

	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));

		if (GinIsPostingTree(itup))
		{
			/*
			 * store posting tree's roots for further processing, we can't
			 * vacuum it just now due to risk of deadlocks with scans/inserts
			 */
			roots[*nroot] = GinGetDownlink(itup);
			(*nroot)++;
		}
		else if (GinGetNPosting(itup) > 0)
		{
			int			nitems;
			ItemPointer items_orig;
			bool		free_items_orig;
			ItemPointer items;

			/* Get list of item pointers from the tuple. */
			if (GinItupIsCompressed(itup))
			{
				items_orig = ginPostingListDecode((GinPostingList *) GinGetPosting(itup), &nitems);
				free_items_orig = true;
			}
			else
			{
				items_orig = (ItemPointer) GinGetPosting(itup);
				nitems = GinGetNPosting(itup);
				free_items_orig = false;
			}

			/* Remove any items from the list that need to be vacuumed. */
			items = ginVacuumItemPointers(gvs, items_orig, nitems, &nitems);

			if (free_items_orig)
				pfree(items_orig);

			/* If any item pointers were removed, recreate the tuple. */
			if (items)
			{
				OffsetNumber attnum;
				Datum		key;
				GinNullCategory category;
				GinPostingList *plist;
				int			plistsize;

				if (nitems > 0)
				{
					plist = ginCompressPostingList(items, nitems, GinMaxItemSize, NULL);
					plistsize = SizeOfGinPostingList(plist);
				}
				else
				{
					plist = NULL;
					plistsize = 0;
				}

				/*
				 * if we already created a temporary page, make changes in
				 * place
				 */
				if (tmppage == origpage)
				{
					/*
					 * On first difference, create a temporary copy of the
					 * page and copy the tuple's posting list to it.
					 */
					tmppage = PageGetTempPageCopy(origpage);

					/* set itup pointer to new page */
					itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));
				}

				attnum = gintuple_get_attrnum(&gvs->ginstate, itup);
				key = gintuple_get_key(&gvs->ginstate, itup, &category);
				itup = GinFormTuple(&gvs->ginstate, attnum, key, category,
									(char *) plist, plistsize,
									nitems, true);
				if (plist)
					pfree(plist);
				PageIndexTupleDelete(tmppage, i);

				if (PageAddItem(tmppage, itup, IndexTupleSize(itup), i, false, false) != i)
					elog(ERROR, "failed to add item to index page in \"%s\"",
						 RelationGetRelationName(gvs->index));

				pfree(itup);
				pfree(items);
			}
		}
	}

	return (tmppage == origpage) ? NULL : tmppage;
}

IndexBulkDeleteResult *
ginbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			  IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BlockNumber blkno = GIN_ROOT_BLKNO;
	GinVacuumState gvs;
	Buffer		buffer;
	BlockNumber rootOfPostingTree[BLCKSZ / (sizeof(IndexTupleData) + sizeof(ItemId))];
	uint32		nRoot;

	gvs.tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
									   "Gin vacuum temporary context",
									   ALLOCSET_DEFAULT_SIZES);
	gvs.index = index;
	gvs.callback = callback;
	gvs.callback_state = callback_state;
	gvs.strategy = info->strategy;
	initGinState(&gvs.ginstate, index);

	/* first time through? */
	if (stats == NULL)
	{
		/* Yes, so initialize stats to zeroes */
		stats = palloc0_object(IndexBulkDeleteResult);

		/*
		 * and cleanup any pending inserts
		 */
		ginInsertCleanup(&gvs.ginstate, !AmAutoVacuumWorkerProcess(),
						 false, true, stats);
	}

	/* we'll re-count the tuples each time */
	stats->num_index_tuples = 0;
	gvs.result = stats;

	buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
								RBM_NORMAL, info->strategy);

	/* find leaf page */
	for (;;)
	{
		Page		page = BufferGetPage(buffer);
		IndexTuple	itup;

		LockBuffer(buffer, GIN_SHARE);

		Assert(!GinPageIsData(page));

		if (GinPageIsLeaf(page))
		{
			LockBuffer(buffer, GIN_UNLOCK);
			LockBuffer(buffer, GIN_EXCLUSIVE);

			if (blkno == GIN_ROOT_BLKNO && !GinPageIsLeaf(page))
			{
				LockBuffer(buffer, GIN_UNLOCK);
				continue;		/* check it one more */
			}
			break;
		}

		Assert(PageGetMaxOffsetNumber(page) >= FirstOffsetNumber);

		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
		blkno = GinGetDownlink(itup);
		Assert(blkno != InvalidBlockNumber);

		UnlockReleaseBuffer(buffer);
		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
	}

	/* right now we found leftmost page in entry's BTree */

	for (;;)
	{
		Page		page = BufferGetPage(buffer);
		Page		resPage;
		uint32		i;

		Assert(!GinPageIsData(page));

		resPage = ginVacuumEntryPage(&gvs, buffer, rootOfPostingTree, &nRoot);

		blkno = GinPageGetOpaque(page)->rightlink;

		if (resPage)
		{
			START_CRIT_SECTION();
			PageRestoreTempPage(resPage, page);
			MarkBufferDirty(buffer);
			xlogVacuumPage(gvs.index, buffer);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(buffer);
		}
		else
		{
			UnlockReleaseBuffer(buffer);
		}

		vacuum_delay_point(false);

		for (i = 0; i < nRoot; i++)
		{
			ginVacuumPostingTree(&gvs, rootOfPostingTree[i]);
			vacuum_delay_point(false);
		}

		if (blkno == InvalidBlockNumber)	/* rightmost page */
			break;

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIN_EXCLUSIVE);
	}

	MemoryContextDelete(gvs.tmpCxt);

	return gvs.result;
}

IndexBulkDeleteResult *
ginvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;
	bool		needLock;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	GinState	ginstate;
	GinStatsData idxStat;
	BlockRangeReadStreamPrivate p;
	ReadStream *stream;

	/*
	 * In an autovacuum analyze, we want to clean up pending insertions.
	 * Otherwise, an ANALYZE-only call is a no-op.
	 */
	if (info->analyze_only)
	{
		if (AmAutoVacuumWorkerProcess())
		{
			initGinState(&ginstate, index);
			ginInsertCleanup(&ginstate, false, true, true, stats);
		}
		return stats;
	}

	/*
	 * Set up all-zero stats and cleanup pending inserts if ginbulkdelete
	 * wasn't called
	 */
	if (stats == NULL)
	{
		stats = palloc0_object(IndexBulkDeleteResult);
		initGinState(&ginstate, index);
		ginInsertCleanup(&ginstate, !AmAutoVacuumWorkerProcess(),
						 false, true, stats);
	}

	memset(&idxStat, 0, sizeof(idxStat));

	/*
	 * XXX we always report the heap tuple count as the number of index
	 * entries.  This is bogus if the index is partial, but it's real hard to
	 * tell how many distinct heap entries are referenced by a GIN index.
	 */
	stats->num_index_tuples = Max(info->num_heap_tuples, 0);
	stats->estimated_count = info->estimated_count;

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	totFreePages = 0;

	/* Scan all blocks starting from the root using streaming reads */
	p.current_blocknum = GIN_ROOT_BLKNO;
	p.last_exclusive = npages;

	/*
	 * It is safe to use batchmode as block_range_read_stream_cb takes no
	 * locks.
	 */
	stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
										READ_STREAM_FULL |
										READ_STREAM_USE_BATCHING,
										info->strategy,
										index,
										MAIN_FORKNUM,
										block_range_read_stream_cb,
										&p,
										0);

	for (blkno = GIN_ROOT_BLKNO; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point(false);

		buffer = read_stream_next_buffer(stream, NULL);

		LockBuffer(buffer, GIN_SHARE);
		page = BufferGetPage(buffer);

		if (GinPageIsRecyclable(page))
		{
			Assert(blkno != GIN_ROOT_BLKNO);
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else if (GinPageIsData(page))
		{
			idxStat.nDataPages++;
		}
		else if (!GinPageIsList(page))
		{
			idxStat.nEntryPages++;

			if (GinPageIsLeaf(page))
				idxStat.nEntries += PageGetMaxOffsetNumber(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	Assert(read_stream_next_buffer(stream, NULL) == InvalidBuffer);
	read_stream_end(stream);

	/* Update the metapage with accurate page and entry counts */
	idxStat.nTotalPages = npages;
	ginUpdateStats(info->index, &idxStat, false);

	/* Finally, vacuum the FSM */
	IndexFreeSpaceMapVacuum(info->index);

	stats->pages_free = totFreePages;

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	return stats;
}

/*
 * Return whether Page can safely be recycled.
 */
bool
GinPageIsRecyclable(Page page)
{
	TransactionId delete_xid;

	if (PageIsNew(page))
		return true;

	if (!GinPageIsDeleted(page))
		return false;

	delete_xid = GinPageGetDeleteXid(page);

	if (!TransactionIdIsValid(delete_xid))
		return true;

	/*
	 * If no backend still could view delete_xid as in running, all scans
	 * concurrent with ginDeletePostingPage() must have finished.
	 */
	return GlobalVisCheckRemovableXid(NULL, delete_xid);
}
