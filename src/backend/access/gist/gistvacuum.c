/*-------------------------------------------------------------------------
 *
 * gistvacuum.c
 *	  vacuuming routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
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
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/snapmgr.h"
#include "access/xact.h"


/*
 * VACUUM cleanup: update FSM
 */
IndexBulkDeleteResult *
gistvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	bool		needLock;

	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		return stats;

	/* Set up all-zero stats if gistbulkdelete wasn't called */
	if (stats == NULL)
	{
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
		/* use heap's tuple count */
		stats->num_index_tuples = info->num_heap_tuples;
		stats->estimated_count = info->estimated_count;

		/*
		 * XXX the above is wrong if index is partial.  Would it be OK to just
		 * return NULL, or is there work we must do below?
		 */
	}

	/*
	 * Need lock unless it's local to this backend.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	/* try to find deleted pages */
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	totFreePages = 0;
	for (blkno = GIST_ROOT_BLKNO + 1; blkno < npages; blkno++)
	{
		Buffer		buffer;
		Page		page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
									info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);

		if (PageIsNew(page) || GistPageIsDeleted(page))
		{
			totFreePages++;
			RecordFreeIndexPage(rel, blkno);
		}
		UnlockReleaseBuffer(buffer);
	}

	/* Finally, vacuum the FSM */
	IndexFreeSpaceMapVacuum(info->index);

	/* return statistics */
	stats->pages_free = totFreePages;
	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(rel);
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	return stats;
}

typedef struct GistBDItem
{
	GistNSN		parentlsn;
	BlockNumber blkno;
	struct GistBDItem *next;
} GistBDItem;

static void
pushStackIfSplited(Page page, GistBDItem *stack)
{
	GISTPageOpaque opaque = GistPageGetOpaque(page);

	if (stack->blkno != GIST_ROOT_BLKNO && !XLogRecPtrIsInvalid(stack->parentlsn) &&
		(GistFollowRight(page) || stack->parentlsn < GistPageGetNSN(page)) &&
		opaque->rightlink != InvalidBlockNumber /* sanity check */ )
	{
		/* split page detected, install right link to the stack */

		GistBDItem *ptr = (GistBDItem *) palloc(sizeof(GistBDItem));

		ptr->blkno = opaque->rightlink;
		ptr->parentlsn = stack->parentlsn;
		ptr->next = stack->next;
		stack->next = ptr;
	}
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples and
 * check invalid tuples left after upgrade.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
gistbulkdelete(IndexVacuumInfo * info, IndexBulkDeleteResult * stats, IndexBulkDeleteCallback callback, void* callback_state)
{
	Relation	rel = info->index;
	GistBDItem *stack,
			   *ptr;
	BlockNumber recentParent = InvalidBlockNumber;
	List	   *rescanList = NULL;
	ListCell   *cell;

	/* first time through? */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	/* we'll re-count the tuples each time */
	stats->estimated_count = false;
	stats->num_index_tuples = 0;

	stack = (GistBDItem *) palloc0(sizeof(GistBDItem));
	stack->blkno = GIST_ROOT_BLKNO;

	while (stack)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		if (GistPageIsLeaf(page))
		{
			OffsetNumber todelete[MaxOffsetNumber];
			int			ntodelete = 0;

			LockBuffer(buffer, GIST_UNLOCK);
			LockBuffer(buffer, GIST_EXCLUSIVE);

			page = (Page) BufferGetPage(buffer);
			if (stack->blkno == GIST_ROOT_BLKNO && !GistPageIsLeaf(page))
			{
				/* only the root can become non-leaf during relock */
				UnlockReleaseBuffer(buffer);
				/* one more check */
				continue;
			}

			/*
			 * check for split proceeded after look at parent, we should check
			 * it after relock
			 */
			pushStackIfSplited(page, stack);

			/*
			 * Remove deletable tuples from page
			 */

			maxoff = PageGetMaxOffsetNumber(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				if (callback(&(idxtuple->t_tid), callback_state))
					todelete[ntodelete++] = i;
				else
					stats->num_index_tuples += 1;
			}

			stats->tuples_removed += ntodelete;

			if (ntodelete)
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
			}

			if (ntodelete == maxoff && recentParent!=InvalidBlockNumber &&
				(rescanList == NULL || (BlockNumber)llast_int(rescanList) != recentParent))
			{
				/* This page is a candidate to be deleted. Remember it's parent to rescan it later with xlock */
				rescanList = lappend_int(rescanList, recentParent);
			}
		}
		else
		{
			recentParent = stack->blkno;
			/* check for split proceeded after look at parent */
			pushStackIfSplited(page, stack);

			maxoff = PageGetMaxOffsetNumber(page);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);

				ptr = (GistBDItem *) palloc(sizeof(GistBDItem));
				ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				ptr->parentlsn = PageGetLSN(page);
				ptr->next = stack->next;
				stack->next = ptr;

				if (GistTupleIsInvalid(idxtuple))
					ereport(LOG,
							(errmsg("index \"%s\" contains an inner tuple marked as invalid",
									RelationGetRelationName(rel)),
							 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
							 errhint("Please REINDEX it.")));
			}
		}

		UnlockReleaseBuffer(buffer);

		ptr = stack->next;
		pfree(stack);
		stack = ptr;

		vacuum_delay_point();
	}

	/* rescan inner pages that had empty child pages */
	foreach(cell,rescanList)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;
		OffsetNumber todelete[MaxOffsetNumber];
		Buffer		buftodelete[MaxOffsetNumber];
		int			ntodelete = 0;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, (BlockNumber)lfirst_int(cell),
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, GIST_EXCLUSIVE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		Assert(!GistPageIsLeaf(page));

		maxoff = PageGetMaxOffsetNumber(page);

		for (i = OffsetNumberNext(FirstOffsetNumber); i <= maxoff; i = OffsetNumberNext(i))
		{
			Buffer		leafBuffer;
			Page		leafPage;

			iid = PageGetItemId(page, i);
			idxtuple = (IndexTuple) PageGetItem(page, iid);

			leafBuffer = ReadBufferExtended(rel, MAIN_FORKNUM, ItemPointerGetBlockNumber(&(idxtuple->t_tid)),
								RBM_NORMAL, info->strategy);
			LockBuffer(leafBuffer, GIST_EXCLUSIVE);
			gistcheckpage(rel, leafBuffer);
			leafPage = (Page) BufferGetPage(leafBuffer);
			Assert(GistPageIsLeaf(leafPage));

			if (PageGetMaxOffsetNumber(leafPage) == InvalidOffsetNumber /* Nothing left to split */
				&& !(GistFollowRight(leafPage) || GistPageGetNSN(page) < GistPageGetNSN(leafPage)) /* No follow-right */
				&& ntodelete < maxoff-1) /* We must keep at least one leaf page per each */
			{
				buftodelete[ntodelete] = leafBuffer;
				todelete[ntodelete++] = i;
			}
			else
				UnlockReleaseBuffer(leafBuffer);
		}


		if (ntodelete)
		{
			/* Drop references from internal page */
			TransactionId txid = GetCurrentTransactionId();
			START_CRIT_SECTION();

			MarkBufferDirty(buffer);
				PageIndexMultiDelete(page, todelete, ntodelete);

			if (RelationNeedsWAL(rel))
			{
				XLogRecPtr	recptr;

				recptr = gistXLogUpdate(buffer, todelete, ntodelete, NULL, 0, InvalidBuffer);
					PageSetLSN(page, recptr);
			}
			else
				PageSetLSN(page, gistGetFakeLSN(rel));

			/* Mark pages as deleted */
			for (i = 0; i < ntodelete; i++)
			{
				Page		leafPage = (Page)BufferGetPage(buftodelete[i]);
				PageHeader	header = (PageHeader)leafPage;

				header->pd_prune_xid = txid;

				GistPageSetDeleted(leafPage);
				MarkBufferDirty(buftodelete[i]);
				stats->pages_deleted++;

				if (RelationNeedsWAL(rel))
				{
					XLogRecPtr recptr = gistXLogSetDeleted(rel->rd_node, buftodelete[i], header->pd_prune_xid);
					PageSetLSN(leafPage, recptr);
				}
				else
					PageSetLSN(leafPage, gistGetFakeLSN(rel));

				UnlockReleaseBuffer(buftodelete[i]);
			}
			END_CRIT_SECTION();
		}

		UnlockReleaseBuffer(buffer);

		vacuum_delay_point();
	}

	list_free(rescanList);

	return stats;
}