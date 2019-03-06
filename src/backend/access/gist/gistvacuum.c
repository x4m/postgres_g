/*-------------------------------------------------------------------------
 *
 * gistvacuum.c
 *	  vacuuming routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/gist_private.h"
#include "access/transam.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

/* Lowest level of radix tree is represented by bitmap */
typedef struct
{
	char data[256/8];
} BlockSetLevel4Data;

typedef BlockSetLevel4Data *BlockSetLevel4;

/* statically typed inner level chunks points to ground level */
typedef struct
{
	/* null references denote empty subtree */
	BlockSetLevel4 next[256];
} BlockSetLevel3Data;

typedef BlockSetLevel3Data *BlockSetLevel3;

/* inner level points to another inner level */
typedef struct
{
	BlockSetLevel3 next[256];
} BlockSetLevel2Data;

typedef BlockSetLevel2Data *BlockSetLevel2;

/* Radix tree root */
typedef struct
{
	BlockSetLevel2 next[256];
} BlockSetData;

typedef BlockSetData *BlockSet;

/* multiplex block number into indexes of radix tree */
#define BLOCKSET_SPLIT_BLKNO				\
	int i1, i2, i3, i4, byte_no, byte_mask;	\
	i4 = blkno % 256;						\
	blkno /= 256;							\
	i3 = blkno % 256;						\
	blkno /= 256;							\
	i2 = blkno % 256;						\
	blkno /= 256;							\
	i1 = blkno;								\
	byte_no = i4 / 8;						\
	byte_mask = 1 << (i4 % 8)				\

/* indicate presence of block number in set */
static
BlockSet blockset_set(BlockSet bs, BlockNumber blkno)
{
	BLOCKSET_SPLIT_BLKNO;
	if (bs == NULL)
	{
		bs = palloc0(sizeof(BlockSetData));
	}
	BlockSetLevel2 bs2 = bs->next[i1];
	if (bs2 == NULL)
	{
		bs2 = palloc0(sizeof(BlockSetLevel2Data));
		bs->next[i1] = bs2;
	}
	BlockSetLevel3 bs3 = bs2->next[i2];
	if (bs3 == NULL)
	{
		bs3 = palloc0(sizeof(BlockSetLevel3Data));
		bs2->next[i2] = bs3;
	}
	BlockSetLevel4 bs4 = bs3->next[i3];
	if (bs4 == NULL)
	{
		bs4 = palloc0(sizeof(BlockSetLevel4Data));
		bs3->next[i3] = bs4;
	}
	bs4->data[byte_no] = byte_mask | bs4->data[byte_no];
	return bs;
}

/* Test presence of block in set */
static
bool blockset_get(BlockNumber blkno, BlockSet bs)
{
	BLOCKSET_SPLIT_BLKNO;
	if (bs == NULL)
		return false;
	BlockSetLevel2 bs2 = bs->next[i1];
	if (bs2 == NULL)
		return false;
	BlockSetLevel3 bs3 = bs2->next[i2];
	if (bs3 == NULL)
		return false;
	BlockSetLevel4 bs4 = bs3->next[i3];
	if (bs4 == NULL)
		return false;
	return (bs4->data[byte_no] & byte_mask);
}

/* 
 * Find nearest block number in set no less than blkno
 * Return InvalidBlockNumber if nothing to return
 * If given InvalidBlockNumber - returns minimal element in set
 */
static
BlockNumber blockset_next(BlockSet bs, BlockNumber blkno)
{
	if (blkno == InvalidBlockNumber)
		blkno = 0; /* equvalent to ++, left for clear code */
	else
		blkno++;

	BLOCKSET_SPLIT_BLKNO;

	if (bs == NULL)
		return InvalidBlockNumber;
	for (; i1 < 256; i1++)
	{
		BlockSetLevel2 bs2 = bs->next[i1];
		if (!bs2)
			continue;
		for (; i2 < 256; i2++)
		{
			BlockSetLevel3 bs3 = bs2->next[i2];
			if (!bs3)
				continue;
			for (; i3 < 256; i3++)
			{
				BlockSetLevel4 bs4 = bs3->next[i3];
				if (!bs4)
					continue;
				for (; byte_no < 256 / 8; byte_no++)
				{
					if (!bs4->data[byte_no])
						continue;
					while (byte_mask <= 0x70)
					{
						if ((byte_mask & bs4->data[byte_no]) == byte_mask)
						{
							i4 = byte_no * 8;
							while (byte_mask >>= 1) i4++;
							return i4 + 256 * (i3 + 256 * (i2 + 256 * i1));
						}
						byte_mask <<= 1;
					}
					byte_mask = 1;
				}
				byte_no = 0;
			}
			i3 = 0;
		}
		i2 = 0;
	}
	return InvalidBlockNumber;
}

/* free anything palloced */
static
void blockset_free(BlockSet bs)
{
	BlockNumber blkno = 0;
	BLOCKSET_SPLIT_BLKNO;
	if (bs == NULL)
		return;
	for (; i1 < 256; i1++)
	{
		BlockSetLevel2 bs2 = bs->next[i1];
		if (!bs2)
			continue;
		for (; i2 < 256; i2++)
		{
			BlockSetLevel3 bs3 = bs2->next[i2];
			if (!bs3)
				continue;
			for (; i3 < 256; i3++)
			{
				BlockSetLevel4 bs4 = bs3->next[i3];
				if (bs4)
					pfree(bs4);
			}
			pfree(bs3);
		}
		pfree(bs2);
	}
	pfree(bs);
}

/* Working state needed by gistbulkdelete */
typedef struct
{
	IndexVacuumInfo *info;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	GistNSN		startNSN;
	BlockNumber totFreePages;	/* true total # of free pages */
	BlockNumber emptyPages;

	BlockSet	internalPagesMap;
	BlockSet	emptyLeafPagesMap;
} GistVacState;

static void gistvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state);
static void gistvacuumpage(GistVacState *vstate, BlockNumber blkno,
			   BlockNumber orig_blkno);

/*
 * VACUUM bulkdelete stage: remove index entries.
 */
IndexBulkDeleteResult *
gistbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	gistvacuumscan(info, stats, callback, callback_state);


	return stats;
}

/*
 * VACUUM cleanup stage: update index statistics.
 */
IndexBulkDeleteResult *
gistvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		return stats;

	/*
	 * If gistbulkdelete was called, we need not do anything, just return the
	 * stats from the latest gistbulkdelete call.  If it wasn't called, we
	 * still need to do a pass over the index, to obtain index statistics.
	 */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		gistvacuumscan(info, stats, NULL, NULL);
	}

	/*
	 * It's quite possible for us to be fooled by concurrent page splits into
	 * double-counting some index tuples, so disbelieve any total that exceeds
	 * the underlying heap's count ... if we know that accurately.  Otherwise
	 * this might just make matters worse.
	 */
	if (!info->estimated_count)
	{
		if (stats->num_index_tuples > info->num_heap_tuples)
			stats->num_index_tuples = info->num_heap_tuples;
	}

	return stats;
}

/*
 * gistvacuumscan --- scan the index for VACUUMing purposes
 *
 * This scans the index for leaf tuples that are deletable according to the
 * vacuum callback, and updates the stats.  Both btbulkdelete and
 * btvacuumcleanup invoke this (the latter only if no btbulkdelete call
 * occurred).
 *
 * This also adds unused/delete pages to the free space map, although that
 * is currently not very useful.  There is currently no support for deleting
 * empty pages, so recycleable pages can only be found if an error occurs
 * while the index is being expanded, leaving an all-zeros page behind.
 *
 * The caller is responsible for initially allocating/zeroing a stats struct.
 * 
 * Bulk deletion of all index entries pointing to a set of heap tuples and
 * check invalid tuples left after upgrade.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 */
static void
gistvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	rel = info->index;
	GistVacState vstate;
	BlockNumber num_pages;
	bool		needLock;
	BlockNumber blkno;

	/*
	 * Reset counts that will be incremented during the scan; needed in case
	 * of multiple scans during a single VACUUM command.
	 */
	stats->estimated_count = false;
	stats->num_index_tuples = 0;
	stats->pages_deleted = 0;

	/* Set up info to pass down to gistvacuumpage */
	vstate.info = info;
	vstate.stats = stats;
	vstate.callback = callback;
	vstate.callback_state = callback_state;
	if (RelationNeedsWAL(rel))
		vstate.startNSN = GetInsertRecPtr();
	else
		vstate.startNSN = gistGetFakeLSN(rel);
	vstate.totFreePages = 0;
	vstate.emptyPages = 0;
	vstate.internalPagesMap = NULL;
	vstate.emptyLeafPagesMap = NULL;

	/*
	 * The outer loop iterates over all index pages, in physical order (we
	 * hope the kernel will cooperate in providing read-ahead for speed).  It
	 * is critical that we visit all leaf pages, including ones added after we
	 * start the scan, else we might fail to delete some deletable tuples.
	 * Hence, we must repeatedly check the relation length.  We must acquire
	 * the relation-extension lock while doing so to avoid a race condition:
	 * if someone else is extending the relation, there is a window where
	 * bufmgr/smgr have created a new all-zero page but it hasn't yet been
	 * write-locked by gistNewBuffer().  If we manage to scan such a page
	 * here, we'll improperly assume it can be recycled.  Taking the lock
	 * synchronizes things enough to prevent a problem: either num_pages won't
	 * include the new page, or gistNewBuffer already has write lock on the
	 * buffer and it will be fully initialized before we can examine it.  (See
	 * also vacuumlazy.c, which has the same issue.)  Also, we need not worry
	 * if a page is added immediately after we look; the page splitting code
	 * already has write-lock on the left page before it adds a right page, so
	 * we must already have processed any tuples due to be moved into such a
	 * page.
	 *
	 * We can skip locking for new or temp relations, however, since no one
	 * else could be accessing them.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	blkno = GIST_ROOT_BLKNO;
	for (;;)
	{
		/* Get the current relation length */
		if (needLock)
			LockRelationForExtension(rel, ExclusiveLock);
		num_pages = RelationGetNumberOfBlocks(rel);
		if (needLock)
			UnlockRelationForExtension(rel, ExclusiveLock);

		/* Quit if we've scanned the whole relation */
		if (blkno >= num_pages)
			break;

		/* Iterate over pages, then loop back to recheck length */
		for (; blkno < num_pages; blkno++)
			gistvacuumpage(&vstate, blkno, blkno);
	}

	/*
	 * If we found any recyclable pages (and recorded them in the FSM), then
	 * forcibly update the upper-level FSM pages to ensure that searchers can
	 * find them.  It's possible that the pages were also found during
	 * previous scans and so this is a waste of time, but it's cheap enough
	 * relative to scanning the index that it shouldn't matter much, and
	 * making sure that free pages are available sooner not later seems
	 * worthwhile.
	 *
	 * Note that if no recyclable pages exist, we don't bother vacuuming the
	 * FSM at all.
	 */
	if (vstate.totFreePages > 0)
		IndexFreeSpaceMapVacuum(rel);

	/* update statistics */
	stats->num_pages = num_pages;
	stats->pages_free = vstate.totFreePages;

	/* rescan all inner pages to find those that has empty child pages */
	if (vstate.emptyPages > 0)
	{
		BlockNumber			x;

		x = InvalidBlockNumber;
		while (vstate.emptyPages > 0 &&
			   (x = blockset_next(vstate.internalPagesMap, x)) != InvalidBlockNumber)
		{
			Buffer		buffer;
			Page		page;
			OffsetNumber off,
				maxoff;
			IndexTuple  idxtuple;
			ItemId	    iid;
			OffsetNumber todelete[MaxOffsetNumber];
			Buffer		buftodelete[MaxOffsetNumber];
			int			ntodelete = 0;

			/* FIXME: 'x' is signed, so this will not work with indexes larger than 2^31 blocks */
			blkno = (BlockNumber) x;

			buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
										info->strategy);

			LockBuffer(buffer, GIST_EXCLUSIVE);
			page = (Page) BufferGetPage(buffer);
			if (PageIsNew(page) || GistPageIsDeleted(page) || GistPageIsLeaf(page))
			{
				UnlockReleaseBuffer(buffer);
				continue;
			}

			maxoff = PageGetMaxOffsetNumber(page);
			/* Check that leafs are still empty and decide what to delete */
			for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
			{
				Buffer		leafBuffer;
				Page		leafPage;
				BlockNumber leafBlockNo;

				iid = PageGetItemId(page, off);
				idxtuple = (IndexTuple) PageGetItem(page, iid);
				/* if this page was not empty in previous scan - we do not consider it */
				leafBlockNo = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				if (!blockset_get(leafBlockNo, vstate.emptyLeafPagesMap))
					continue;

				leafBuffer = ReadBufferExtended(rel, MAIN_FORKNUM, leafBlockNo,
												RBM_NORMAL, info->strategy);
				LockBuffer(leafBuffer, GIST_EXCLUSIVE);
				gistcheckpage(rel, leafBuffer);
				leafPage = (Page) BufferGetPage(leafBuffer);
				if (!GistPageIsLeaf(leafPage))
				{
					UnlockReleaseBuffer(leafBuffer);
					continue;
				}

				if (PageGetMaxOffsetNumber(leafPage) == InvalidOffsetNumber /* Nothing left to split */
					&& !(GistFollowRight(leafPage) || GistPageGetNSN(page) < GistPageGetNSN(leafPage)) /* No follow-right */
					&& ntodelete < maxoff-1) /* We must keep at least one leaf page per each */
				{
					buftodelete[ntodelete] = leafBuffer;
					todelete[ntodelete++] = off;
				}
				else
					UnlockReleaseBuffer(leafBuffer);
			}

			if (ntodelete)
			{
				/*
				 * Like in _bt_unlink_halfdead_page we need an upper bound on xid
				 * that could hold downlinks to this page. We use
				 * ReadNewTransactionId() to instead of GetCurrentTransactionId
				 * since we are in a VACUUM.
				 */
				TransactionId txid = ReadNewTransactionId();
				int			i;

				START_CRIT_SECTION();

				/* Mark pages as deleted dropping references from internal pages */
				for (i = 0; i < ntodelete; i++)
				{
					Page		leafPage = (Page) BufferGetPage(buftodelete[i]);
					XLogRecPtr	recptr;

					/* Remember xid of last transaction that could see this page */
					GistPageSetDeleteXid(leafPage,txid);

					GistPageSetDeleted(leafPage);
					MarkBufferDirty(buftodelete[i]);
					stats->pages_deleted++;
					vstate.emptyPages--;

					MarkBufferDirty(buffer);
					/* Offsets are changed as long as we delete tuples from internal page */
					PageIndexTupleDelete(page, todelete[i] - i);

					if (RelationNeedsWAL(rel))
						recptr 	= gistXLogSetDeleted(rel->rd_node, buftodelete[i],
													 txid, buffer, todelete[i] - i);
					else
						recptr = gistGetFakeLSN(rel);
					PageSetLSN(page, recptr);
					PageSetLSN(leafPage, recptr);

					UnlockReleaseBuffer(buftodelete[i]);
				}
				END_CRIT_SECTION();
			}

			UnlockReleaseBuffer(buffer);
		}
	}

	blockset_free(vstate.emptyLeafPagesMap);
	blockset_free(vstate.internalPagesMap);
}

/*
 * gistvacuumpage --- VACUUM one page
 *
 * This processes a single page for gistbulkdelete().  In some cases we
 * must go back and re-examine previously-scanned pages; this routine
 * recurses when necessary to handle that case.
 *
 * blkno is the page to process.  orig_blkno is the highest block number
 * reached by the outer gistvacuumscan loop (the same as blkno, unless we
 * are recursing to re-examine a previous page).
 */
static void
gistvacuumpage(GistVacState *vstate, BlockNumber blkno, BlockNumber orig_blkno)
{
	IndexVacuumInfo *info = vstate->info;
	IndexBulkDeleteResult *stats = vstate->stats;
	IndexBulkDeleteCallback callback = vstate->callback;
	void	   *callback_state = vstate->callback_state;
	Relation	rel = info->index;
	Buffer		buffer;
	Page		page;
	BlockNumber recurse_to;

restart:
	recurse_to = InvalidBlockNumber;

	/* call vacuum_delay_point while not holding any buffer lock */
	vacuum_delay_point();

	buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
								info->strategy);

	/*
	 * We are not going to stay here for a long time, aggressively grab an
	 * exclusive lock.
	 */
	LockBuffer(buffer, GIST_EXCLUSIVE);
	page = (Page) BufferGetPage(buffer);

	if (PageIsNew(page) || GistPageIsDeleted(page))
	{
		/* Okay to recycle this page */
		RecordFreeIndexPage(rel, blkno);
		vstate->totFreePages++;
		stats->pages_deleted++;
	}
	else if (GistPageIsLeaf(page))
	{
		OffsetNumber todelete[MaxOffsetNumber];
		int			ntodelete = 0;
		int			nremain;
		GISTPageOpaque opaque = GistPageGetOpaque(page);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * Check whether we need to recurse back to earlier pages.  What we
		 * are concerned about is a page split that happened since we started
		 * the vacuum scan.  If the split moved some tuples to a lower page
		 * then we might have missed 'em.  If so, set up for tail recursion.
		 *
		 * This is similar to the checks we do during searches, when following
		 * a downlink, but we don't need to jump to higher-numbered pages,
		 * because we will process them later, anyway.
		 */
		if ((GistFollowRight(page) ||
			 vstate->startNSN < GistPageGetNSN(page)) &&
			(opaque->rightlink != InvalidBlockNumber) &&
			(opaque->rightlink < orig_blkno))
		{
			recurse_to = opaque->rightlink;
		}

		/*
		 * Scan over all items to see which ones need to be deleted according
		 * to the callback function.
		 */
		if (callback)
		{
			OffsetNumber off;

			for (off = FirstOffsetNumber;
				 off <= maxoff;
				 off = OffsetNumberNext(off))
			{
				ItemId		iid = PageGetItemId(page, off);
				IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state))
					todelete[ntodelete++] = off;
			}
		}

		/*
		 * Apply any needed deletes.  We issue just one WAL record per page,
		 * so as to minimize WAL traffic.
		 */
		if (ntodelete > 0)
		{
			START_CRIT_SECTION();

			MarkBufferDirty(buffer);

			PageIndexMultiDelete(page, todelete, ntodelete);
			GistMarkTuplesDeleted(page);

			if (RelationNeedsWAL(rel))
			{
				XLogRecPtr	recptr;

				recptr = gistXLogUpdate(buffer,
										todelete, ntodelete,
										NULL, 0, InvalidBuffer);
				PageSetLSN(page, recptr);
			}
			else
				PageSetLSN(page, gistGetFakeLSN(rel));

			END_CRIT_SECTION();

			stats->tuples_removed += ntodelete;
			/* must recompute maxoff */
			maxoff = PageGetMaxOffsetNumber(page);
		}

		nremain = maxoff - FirstOffsetNumber + 1;
		if (nremain == 0)
		{
			vstate->emptyLeafPagesMap = blockset_set(vstate->emptyLeafPagesMap, blkno);
			vstate->emptyPages++;
		}
		else
			stats->num_index_tuples += nremain;
	}
	else
	{
		vstate->internalPagesMap = blockset_set(vstate->internalPagesMap, blkno);

		/*
		 * On an internal page, check for "invalid tuples", left behind by an
		 * incomplete page split on PostgreSQL 9.0 or below.  These are not
		 * created by newer PostgreSQL versions, but unfortunately, there is
		 * no version number anywhere in a GiST index, so we don't know
		 * whether this index might still contain invalid tuples or not.
		 */
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
		OffsetNumber off;

		for (off = FirstOffsetNumber;
			 off <= maxoff;
			 off = OffsetNumberNext(off))
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (GistTupleIsInvalid(idxtuple))
				ereport(LOG,
						(errmsg("index \"%s\" contains an inner tuple marked as invalid",
								RelationGetRelationName(rel)),
						 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
						 errhint("Please REINDEX it.")));
		}
	}

	UnlockReleaseBuffer(buffer);

	/*
	 * This is really tail recursion, but if the compiler is too stupid to
	 * optimize it as such, we'd eat an uncomfortably large amount of stack
	 * space per recursion level (due to the deletable[] array).  A failure is
	 * improbable since the number of levels isn't likely to be large ... but
	 * just in case, let's hand-optimize into a loop.
	 */
	if (recurse_to != InvalidBlockNumber)
	{
		blkno = recurse_to;
		goto restart;
	}
}
