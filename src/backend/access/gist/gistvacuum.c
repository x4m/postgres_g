/*-------------------------------------------------------------------------
 *
 * gistvacuum.c
 *	  vacuuming routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
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
#include "storage/indexfsm.h"
#include "storage/lmgr.h"


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
	double		tuplesCount;
	bool		needLock;

	/* No-op in ANALYZE ONLY mode */
	if (info->analyze_only)
		return stats;

	/* Set up all-zero stats if gistbulkdelete wasn't called */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

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
	tuplesCount = 0;
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
		else if (GistPageIsLeaf(page))
		{
			/* count tuples in index (considering only leaf tuples) */
			tuplesCount += PageGetMaxOffsetNumber(page);
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
	stats->num_index_tuples = tuplesCount;
	stats->estimated_count = false;

	return stats;
}

typedef struct GistBDItem
{
	Buffer	buffer;
	struct GistBDItem *next;
} GistBDItem;

static void gistmapset(char *map, BlockNumber blkno)
{
	map[blkno / 8] |= 1 << (blkno % 8);
}
static bool gistmapget(char *map, BlockNumber blkno)
{
	return (map[blkno / 8] & 1 << (blkno % 8)) != 0;
}

/*
 * This function is used to sort offsets
 * When employing physical scan rescan offsets are not ordered.
 */
static int
compare_offsetnumber(const void *x, const void *y)
{
	OffsetNumber a = *((OffsetNumber *)x);
	OffsetNumber b = *((OffsetNumber *)y);
	return a - b;
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
gistbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation		 rel = info->index;
	BlockNumber		 npages;
	bool			 needLock;
	BlockNumber      blkno;
	GistNSN			 startNSN = GetInsertRecPtr();
	void			*internals;
	void			*emptyLeafs;

	/* first time through? */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	/* we'll re-count the tuples each time */
	stats->estimated_count = false;
	stats->num_index_tuples = 0;

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
	
	internals = palloc0(npages / 8 + 1);
	emptyLeafs = palloc0(npages / 8 + 1);

	for (blkno = GIST_ROOT_BLKNO; blkno < npages; blkno++)
	{
		/*
		 * In case of concurrent splits we may need to jump back
		 * and vacuum pages again to prevent left dead tuples.
		 * These fields are here to implement tail recursion of
		 * these jumps.
		 */
		BlockNumber nextBlock = blkno;
		bool		needScan = true;
		GistBDItem *bufferStack = NULL;

		vacuum_delay_point();

		while (needScan)
		{
			Buffer		 buffer;
			Page		 page;
			OffsetNumber i,
						 maxoff;
			IndexTuple   idxtuple;
			ItemId	     iid;

			needScan = false;

			buffer = ReadBufferExtended(rel, MAIN_FORKNUM, nextBlock, RBM_NORMAL,
										info->strategy);
			/*
			 * We are not going to stay here for a long time, calling recursive algorithms.
			 * Especially for an internal page. So, agressivly grab an exclusive lock.
			 */
			LockBuffer(buffer, GIST_EXCLUSIVE);
			page = (Page) BufferGetPage(buffer);

			if (PageIsNew(page) || GistPageIsDeleted(page))
			{
				UnlockReleaseBuffer(buffer);
				/* TODO: Should not we record free page here? */
				continue;
			}

			maxoff = PageGetMaxOffsetNumber(page);

			if (GistPageIsLeaf(page))
			{
				OffsetNumber todelete[MaxOffsetNumber];
				int			ntodelete = 0;
				GISTPageOpaque opaque = GistPageGetOpaque(page);

				/*
				 * If this page was splitted after start of the VACUUM we have to
				 * revisit rightlink, if it points to block we already scanned.
				 * This is recursive revisit, should not be deep, but we check
				 * the possibility of stack overflow anyway.
				 */
				if ((GistFollowRight(page) || startNSN < GistPageGetNSN(page)) &&
					(opaque->rightlink != InvalidBlockNumber) && (opaque->rightlink < nextBlock))
					{
						/*
						 * we will aquire locks by following rightlinks
						 * this lock coupling is allowed in GiST
						 */
						nextBlock = opaque->rightlink;
						needScan = true;
					}

				/*
				 * Remove deletable tuples from page
				 */

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

				/* We have dead tuples on the page */
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
				/* The page is completely empty */
				if (ntodelete == maxoff)
				{
					gistmapset(emptyLeafs, nextBlock);
				}
			}
			else
				gistmapset(internals, nextBlock);

			/* We should not unlock buffer if we are going to jump left */
			if (needScan)
			{
				GistBDItem *ptr = (GistBDItem *) palloc(sizeof(GistBDItem));
				ptr->buffer = buffer;
				ptr->next = bufferStack;
				bufferStack = ptr;
			}
			else
				UnlockReleaseBuffer(buffer);
		}
		/* unlock stacked buffers */
		while (bufferStack)
		{
			UnlockReleaseBuffer(bufferStack->buffer);
			bufferStack = bufferStack->next;
		}
	}

	for (blkno = GIST_ROOT_BLKNO; blkno < npages; blkno++)
	{
		Buffer		 buffer;
		Page		 page;
		OffsetNumber i,
					 maxoff;
		IndexTuple   idxtuple;
		ItemId	     iid;
		OffsetNumber 	 todelete[MaxOffsetNumber];
		Buffer			 buftodelete[MaxOffsetNumber];
		int				 ntodelete = 0;

		if (!gistmapget(internals, blkno))
			continue; /* second scan is for internal pages */


		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
									info->strategy);

		LockBuffer(buffer, GIST_EXCLUSIVE);
		page = (Page) BufferGetPage(buffer);
		/* Currently block of internal page cannot become leaf */
		Assert(!GistPageIsLeaf(page));

		if (PageIsNew(page) || GistPageIsDeleted(page))
		{
			UnlockReleaseBuffer(buffer);
			/* TODO: Should not we record free page here? */
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);
		/* Check that leafs are still empty and decide what to delete */
		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			Buffer		leafBuffer;
			Page		leafPage;
			/* if this page was not empty in previous scan - we do not consider it */
			if(!gistmapget(emptyLeafs, i))
			{
				continue;
			}

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
			/* Prepare possibly onurdered offsets */
			qsort(todelete, ntodelete, sizeof(OffsetNumber), compare_offsetnumber);

			/* 
			 * Like in _bt_unlink_halfdead_page we need a upper bound on xid
			 * that could hold downlinks to this page. We use
			 * ReadNewTransactionId() to instead of GetCurrentTransactionId
			 * since we are in a VACUUM.
			 */
			TransactionId txid = ReadNewTransactionId();

			START_CRIT_SECTION();

			/* Mark pages as deleted dropping references from internal pages */
			for (i = 0; i < ntodelete; i++)
			{
				Page		leafPage = (Page)BufferGetPage(buftodelete[i]);

				GistPageSetDeleteXid(leafPage,txid);

				GistPageSetDeleted(leafPage);
				MarkBufferDirty(buftodelete[i]);
				stats->pages_deleted++;

				MarkBufferDirty(buffer);
				/* Offsets are changed as long as we delete tuples from internal page */
				PageIndexTupleDelete(page, todelete[i] - i);

				if (RelationNeedsWAL(rel))
				{
					XLogRecPtr recptr 	=
						gistXLogSetDeleted(rel->rd_node, buftodelete[i],
											txid, buffer, todelete[i] - i);
					PageSetLSN(page, recptr);
					PageSetLSN(leafPage, recptr);
				}
				else
				{
					PageSetLSN(page, gistGetFakeLSN(rel));
					PageSetLSN(leafPage, gistGetFakeLSN(rel));
				}

				UnlockReleaseBuffer(buftodelete[i]);
			}
			END_CRIT_SECTION();
		}

		UnlockReleaseBuffer(buffer);
	}

	pfree(internals);
	pfree(emptyLeafs);

	return stats;
}
