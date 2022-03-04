/*-------------------------------------------------------------------------
 *
 * pruneheap.c
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/pruneheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Working data for heap_page_prune and subroutines */
typedef struct
{
	Relation	rel;

	/* tuple visibility test, initialized for the relation */
	GlobalVisState *vistest;

	/*
	 * Thresholds set by TransactionIdLimitedForOldSnapshots() if they have
	 * been computed (done on demand, and only if
	 * OldSnapshotThresholdActive()). The first time a tuple is about to be
	 * removed based on the limited horizon, old_snap_used is set to true, and
	 * SetOldSnapshotThresholdTimestamp() is called. See
	 * heap_prune_satisfies_vacuum().
	 */
	TimestampTz old_snap_ts;
	TransactionId old_snap_xmin;
	bool		old_snap_used;

	TransactionId new_prune_xid;	/* new prune hint value for page */
	TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
	int			nredirected;	/* numbers of entries in arrays below */
	int			ndead;
	int			nunused;
	/* arrays that accumulate indexes of items to be changed */
	OffsetNumber redirected[MaxHeapTuplesPerPage * 2];
	OffsetNumber nowdead[MaxHeapTuplesPerPage];
	OffsetNumber nowunused[MaxHeapTuplesPerPage];

	/*
	 * Tuple visibility is only computed once for each tuple, for efficiency
	 * reasons.  This is of type int8[,] instead of HTSV_Result[], so we can
	 * use -1 to indicate that no visibility needs to be computed (e.g. for
	 * LP_DEAD items).
	 *
	 * This needs to be MaxHeapTuplesPerPage + 1 long as FirstOffsetNumber is
	 * 1. Otherwise every access would need to subtract 1.
	 */
	int8		htsv[MaxHeapTuplesPerPage + 1];

	/*
<<<<<<< HEAD
	 * Tuple visibility is only computed once for each tuple, for correctness
	 * and efficiency reasons; see comment in heap_page_prune() for details.
	 * This is of type int8[], instead of HTSV_Result[], so we can use -1 to
	 * indicate no visibility has been computed, e.g. for LP_DEAD items.
=======
	 * visited[i] is true if item i was already visited by second pass over
	 * page (when we decide which tuples constitute each HOT chain).
>>>>>>> Make heap pruning more robust.
	 *
	 * Same indexing as ->htsv.
	 */
	bool		visited[MaxHeapTuplesPerPage + 1];

	/*
	 * heaponly[i] is true if item i is a heap-only tuple (during second and
	 * third pass over the page)
	 *
	 * Same indexing as ->htsv.
	 */
	bool		heaponly[MaxHeapTuplesPerPage + 1];
} PruneState;

/* Local functions */
static HTSV_Result heap_prune_satisfies_vacuum(PruneState *prstate,
											   HeapTuple tup,
											   Buffer buffer);
static int	heap_prune_from_root(Page page, OffsetNumber maxoff,
								 OffsetNumber rootoffnum,
								 PruneState *prstate);
static inline int heap_prune_orphan(OffsetNumber offnum, PruneState *prstate);
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect(PruneState *prstate,
									   OffsetNumber offnum, OffsetNumber rdoffnum);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum);
static void page_verify_redirects(Page page);


/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 */
void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	TransactionId limited_xmin = InvalidTransactionId;
	TimestampTz limited_ts = 0;
	Size		minfree;

	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record
	 * soon anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

	/*
	 * XXX: Magic to keep old_snapshot_threshold tests appear "working". They
	 * currently are broken, and discussion of what to do about them is
	 * ongoing. See
	 * https://www.postgresql.org/message-id/20200403001235.e6jfdll3gh2ygbuc%40alap3.anarazel.de
	 */
	if (old_snapshot_threshold == 0)
		SnapshotTooOldMagicForTest();

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 *
	 * It is OK to check the old snapshot limit before acquiring the cleanup
	 * lock because the worst that can happen is that we are not quite as
	 * aggressive about the cleanup (by however many transaction IDs are
	 * consumed between this point and acquiring the lock).  This allows us to
	 * save significant overhead in the case where the page is found not to be
	 * prunable.
	 *
	 * Even if old_snapshot_threshold is set, we first check whether the page
	 * can be pruned without. Both because
	 * TransactionIdLimitedForOldSnapshots() is not cheap, and because not
	 * unnecessarily relying on old_snapshot_threshold avoids causing
	 * conflicts.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
	{
		if (!OldSnapshotThresholdActive())
			return;

		if (!TransactionIdLimitedForOldSnapshots(GlobalVisTestNonRemovableHorizon(vistest),
												 relation,
												 &limited_xmin, &limited_ts))
			return;

		if (!TransactionIdPrecedes(prune_xid, limited_xmin))
			return;
	}

	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 HEAP_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
	{
		/* OK, try to get exclusive buffer lock */
		if (!ConditionalLockBufferForCleanup(buffer))
			return;

		/*
		 * Now that we have buffer lock, get accurate information about the
		 * page's free space, and recheck the heuristic about whether to
		 * prune. (We needn't recheck PageIsPrunable, since no one else could
		 * have pruned while we hold pin.)
		 */
		if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		{
			int			ndeleted,
						nnewlpdead;

			ndeleted = heap_page_prune(relation, buffer, vistest, limited_xmin,
									   limited_ts, &nnewlpdead, NULL);

			/*
			 * Report the number of tuples reclaimed to pgstats.  This is
			 * ndeleted minus the number of newly-LP_DEAD-set items.
			 *
			 * We derive the number of dead tuples like this to avoid totally
			 * forgetting about items that were set to LP_DEAD, since they
			 * still need to be cleaned up by VACUUM.  We only want to count
			 * heap-only tuples that just became LP_UNUSED in our report,
			 * which don't.
			 *
			 * VACUUM doesn't have to compensate in the same way when it
			 * tracks ndeleted, since it will set the same LP_DEAD items to
			 * LP_UNUSED separately.
			 */
			if (ndeleted > nnewlpdead)
				pgstat_update_heap_dead_tuples(relation,
											   ndeleted - nnewlpdead);
		}

		/* And release buffer lock */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * We avoid reuse of any free space created on the page by unrelated
		 * UPDATEs/INSERTs by opting to not update the FSM at this point.  The
		 * free space should be reused by UPDATEs to *this* page.
		 */
	}
}


/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.  Note that we
 * don't update the FSM information for page on caller's behalf.  Caller might
 * also need to account for a reduction in the length of the line pointer
 * array following array truncation by us.
 *
 * vistest is used to distinguish whether tuples are DEAD or RECENTLY_DEAD
 * (see heap_prune_satisfies_vacuum and
 * HeapTupleSatisfiesVacuum). old_snap_xmin / old_snap_ts need to
 * either have been set by TransactionIdLimitedForOldSnapshots, or
 * InvalidTransactionId/0 respectively.
 *
 * Sets *nnewlpdead for caller, indicating the number of items that were
 * newly set LP_DEAD during prune operation.
 *
 * off_loc is the offset location required by the caller to use in error
 * callback.
 *
 * Returns the number of tuples deleted from the page during this call.
 */
int
heap_page_prune(Relation relation, Buffer buffer,
				GlobalVisState *vistest,
				TransactionId old_snap_xmin,
				TimestampTz old_snap_ts,
				int *nnewlpdead,
				OffsetNumber *off_loc)
{
	int			ndeleted = 0;
	Page		page = BufferGetPage(buffer);
	BlockNumber blockno = BufferGetBlockNumber(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;
	HeapTupleData tup;

	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.rel = relation;
	prstate.vistest = vistest;
	prstate.old_snap_xmin = old_snap_xmin;
	prstate.old_snap_ts = old_snap_ts;
	prstate.old_snap_used = false;
	prstate.latestRemovedXid = InvalidTransactionId;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	memset(prstate.visited, 0, sizeof(prstate.visited));
	memset(prstate.heaponly, 0, sizeof(prstate.heaponly));

	maxoff = PageGetMaxOffsetNumber(page);
	tup.t_tableOid = RelationGetRelid(prstate.rel);

	/*
	 * Determine HTSV for all tuples in first pass over page, and save it in
	 * prstate for later passes.  Scan the page backwards (in reverse item
	 * offset number order).
	 *
	 * This approach is good for performance.  Most commonly tuples within a
	 * page are stored at decreasing offsets (while the items are stored at
	 * increasing offsets).  When processing all tuples on a page this leads
	 * to reading memory at decreasing offsets within a page, with a variable
	 * stride.  That's hard for CPU prefetchers to deal with. Processing the
	 * items in reverse order (and thus the tuples in increasing order)
	 * increases prefetching efficiency significantly / decreases the number
	 * of cache misses.
	 */
	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		HeapTupleHeader htup;

		/*
		 * LP_DEAD/LP_UNUSED items can be eliminated up front by marking them
		 * "visited".  heap_prune_from_root can't deal with them later on.
		 */
		if (!ItemIdIsNormal(itemid))
		{
			prstate.htsv[offnum] = -1;
			if (!ItemIdIsRedirected(itemid))
				prstate.visited[offnum] = true;

			continue;
		}

		/*
		 * heap_prune_from_root can't deal with heap-only tuple "root items",
		 * either.  Remember if this is a heap-only tuple to help with that.
		 */
		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		if (HeapTupleHeaderIsHeapOnly(htup))
			prstate.heaponly[offnum] = true;

		Assert(!HeapTupleHeaderIsHotUpdated(htup) ||
			   ItemPointerGetBlockNumber(&htup->t_ctid) == blockno);
		tup.t_data = htup;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&(tup.t_self), blockno, offnum);

		/*
		 * Set the offset number so that we can display it along with any
		 * error that occurred while processing this tuple.
		 */
		if (off_loc)
			*off_loc = offnum;

		prstate.htsv[offnum] = heap_prune_satisfies_vacuum(&prstate, &tup,
														   buffer);
	}

	/* Now scan the page a second time to process each root item */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		/* Heap-only tuples cannot be root items */
		if (prstate.heaponly[offnum])
			continue;

		/* Ignore items already visited as part of an earlier HOT chain */
		if (prstate.visited[offnum])
			continue;

		/* see first scan/loop */
		if (off_loc)
			*off_loc = offnum;

		/* Process this root item, plus any child heap-only tuples */
		ndeleted += heap_prune_from_root(page, maxoff, offnum, &prstate);
	}

	/*
	 * Now scan the page a third and final time (actually, we only use cached
	 * state from the first two scans for this).  Any heap-only tuples not
	 * found through a root item (parent) are processed here instead.
	 */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		if (prstate.visited[offnum])
			continue;

		/* Process orphaned heap-only tuple */
		ndeleted += heap_prune_orphan(offnum, &prstate);
	}

	/* Clear the offset information once we have processed the given page. */
	if (off_loc)
		*off_loc = InvalidOffsetNumber;

	/* Any error while applying the changes is critical */
	START_CRIT_SECTION();

	/* Have we found any prunable items? */
	if (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0)
	{
		/*
		 * Apply the planned item changes, then repair page fragmentation, and
		 * update the page's hint bit about whether it has free line pointers.
		 */
		heap_page_prune_execute(buffer,
								prstate.redirected, prstate.nredirected,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused);

		/*
		 * Update the page's pd_prune_xid field to either zero, or the lowest
		 * XID of any soon-prunable tuple.
		 */
		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

		/*
		 * Also clear the "page is full" flag, since there's no point in
		 * repeating the prune/defrag process until something else happens to
		 * the page.
		 */
		PageClearFull(page);

		MarkBufferDirty(buffer);

		/*
		 * Emit a WAL XLOG_HEAP2_PRUNE record showing what we did
		 */
		if (RelationNeedsWAL(relation))
		{
			xl_heap_prune xlrec;
			XLogRecPtr	recptr;

			xlrec.latestRemovedXid = prstate.latestRemovedXid;
			xlrec.nredirected = prstate.nredirected;
			xlrec.ndead = prstate.ndead;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);

			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

			/*
			 * The OffsetNumber arrays are not actually in the buffer, but we
			 * pretend that they are.  When XLogInsert stores the whole
			 * buffer, the offset arrays need not be stored too.
			 */
			if (prstate.nredirected > 0)
				XLogRegisterBufData(0, (char *) prstate.redirected,
									prstate.nredirected *
									sizeof(OffsetNumber) * 2);

			if (prstate.ndead > 0)
				XLogRegisterBufData(0, (char *) prstate.nowdead,
									prstate.ndead * sizeof(OffsetNumber));

			if (prstate.nunused > 0)
				XLogRegisterBufData(0, (char *) prstate.nowunused,
									prstate.nunused * sizeof(OffsetNumber));

			recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PRUNE);

			PageSetLSN(BufferGetPage(buffer), recptr);
		}
	}
	else
	{
		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}
	}

	END_CRIT_SECTION();

	/* Record number of newly-set-LP_DEAD items for caller */
	*nnewlpdead = prstate.ndead;

	return ndeleted;
}


/*
 * Perform visibility checks for heap pruning.
 *
 * This is more complicated than just using GlobalVisTestIsRemovableXid()
 * because of old_snapshot_threshold. We only want to increase the threshold
 * that triggers errors for old snapshots when we actually decide to remove a
 * row based on the limited horizon.
 *
 * Due to its cost we also only want to call
 * TransactionIdLimitedForOldSnapshots() if necessary, i.e. we might not have
 * done so in heap_hot_prune_opt() if pd_prune_xid was old enough. But we
 * still want to be able to remove rows that are too new to be removed
 * according to prstate->vistest, but that can be removed based on
 * old_snapshot_threshold. So we call TransactionIdLimitedForOldSnapshots() on
 * demand in here, if appropriate.
 */
static HTSV_Result
heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)
{
	HTSV_Result res;
	TransactionId dead_after;

	res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

	if (res != HEAPTUPLE_RECENTLY_DEAD)
		return res;

	/*
	 * If we are already relying on the limited xmin, there is no need to
	 * delay doing so anymore.
	 */
	if (prstate->old_snap_used)
	{
		Assert(TransactionIdIsValid(prstate->old_snap_xmin));

		if (TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
			res = HEAPTUPLE_DEAD;
		return res;
	}

	/*
	 * First check if GlobalVisTestIsRemovableXid() is sufficient to find the
	 * row dead. If not, and old_snapshot_threshold is enabled, try to use the
	 * lowered horizon.
	 */
	if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
		res = HEAPTUPLE_DEAD;
	else if (OldSnapshotThresholdActive())
	{
		/* haven't determined limited horizon yet, requests */
		if (!TransactionIdIsValid(prstate->old_snap_xmin))
		{
			TransactionId horizon =
			GlobalVisTestNonRemovableHorizon(prstate->vistest);

			TransactionIdLimitedForOldSnapshots(horizon, prstate->rel,
												&prstate->old_snap_xmin,
												&prstate->old_snap_ts);
		}

		if (TransactionIdIsValid(prstate->old_snap_xmin) &&
			TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
		{
			/*
			 * About to remove row based on snapshot_too_old. Need to raise
			 * the threshold so problematic accesses would error.
			 */
			Assert(!prstate->old_snap_used);
			SetOldSnapshotThresholdTimestamp(prstate->old_snap_ts,
											 prstate->old_snap_xmin);
			prstate->old_snap_used = true;
			res = HEAPTUPLE_DEAD;
		}
	}

	return res;
}


/*
 * Prune HOT chain (or simple tuple) originating at specified root item.
 *
 * Used during second pass over the heap page (the root item pass).  Caller
 * must only pass item offsets that are known to be for LP_REDIRECT items or
 * plain heap tuples (not heap-only tuples).
 *
 * In general, pruning must never leave behind a DEAD tuple that still has
 * tuple storage.  VACUUM isn't prepared to deal with that case.  That's why
 * VACUUM prunes the same heap page a second time (without dropping its lock
 * in the interim) when it sees a newly DEAD tuple that we initially saw as
 * in-progress.  Retrying pruning like this can only happen due to certain
 * edge-cases, like the case where an inserting transaction concurrently
 * aborts.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * We don't actually change the page here. We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
static int
heap_prune_from_root(Page page, OffsetNumber maxoff, OffsetNumber rootoffnum,
					 PruneState *prstate)
{
	TransactionId priorXmax = InvalidTransactionId;
	OffsetNumber offnum = rootoffnum,
				latestdead = InvalidOffsetNumber;
	bool		redirectroot = false,
				pastlatestdead = false;
	bool		orphaned PG_USED_FOR_ASSERTS_ONLY = false;
	OffsetNumber chainitems[MaxHeapTuplesPerPage];
	int			nchain = 0;

	Assert(!prstate->visited[offnum] && !prstate->heaponly[offnum]);
	for (;;)
	{
		ItemId		lp;
		HeapTupleHeader htup;

		/* Sanity check (pure paranoia) */
		if (offnum < FirstOffsetNumber)
			break;

		/*
		 * An offset past the end of page's line pointer array is possible
		 * when the array was truncated (original item must have been unused)
		 */
		if (offnum > maxoff)
			break;

		/*
		 * If item was already processed earlier or if it's a non-root item
		 * that isn't a heap-only tuple, stop -- must not be from same chain
		 */
		if (prstate->visited[offnum] ||
			(nchain > 0 && !prstate->heaponly[offnum]))
			break;

		lp = PageGetItemId(page, offnum);

		/*
		 * If we are looking at an LP_REDIRECT, it must be caller's root item.
		 * Jump to the first heap-only tuple in the chain that follows.
		 */
		if (ItemIdIsRedirected(lp))
		{
			Assert(prstate->htsv[offnum] == -1);
			Assert(nchain == 0);

			chainitems[nchain++] = offnum;
			prstate->visited[offnum] = true;
			redirectroot = true;
			offnum = ItemIdGetRedirect(lp);
			continue;
		}

		Assert(ItemIdIsNormal(lp));
		Assert(prstate->htsv[offnum] != -1);
		htup = (HeapTupleHeader) PageGetItem(page, lp);

		/*
		 * Tuple with storage, which is either a standalone root item heap
		 * tuple, or a member of the HOT chain that starts at caller's root
		 * item.
		 *
		 * Check heap-only tuple's XMIN against prior XMAX if necessary.
		 */
		if (nchain > 0 && TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
			break;

		/*
		 * Check tuple's visibility status, and determine if tuple should be
		 * deemed part of the chain that starts at caller's root item.  We
		 * need to delay making a final decision about whether this tuple is
		 * part of caller's HOT chain until here to deal with corner cases
		 * involving DEAD tuples.
		 *
		 * This routine only removes contiguous groups of DEAD tuples from the
		 * start of the HOT chain.  DEAD tuples at the end of the HOT chain
		 * (left behind by aborted HOT updates) need to be left unvisited so
		 * that they'll be dealt with by heap_prune_orphan instead.
		 */
		switch ((HTSV_Result) prstate->htsv[offnum])
		{
			case HEAPTUPLE_DEAD:

				if (!pastlatestdead)
				{
					/*
					 * Still deleting deleted DEAD tuples from beginning of
					 * the chain
					 */
					Assert(!orphaned);
					latestdead = offnum;
					HeapTupleHeaderAdvanceLatestRemovedXid(htup,
														   &prstate->latestRemovedXid);
					prstate->visited[offnum] = true;
					chainitems[nchain++] = offnum;
				}
				else
				{
					/* Deal with this tuple in heap_prune_orphan instead */
					Assert(prstate->heaponly[offnum] &&
						   !prstate->visited[offnum]);
					orphaned = true;
				}

				break;

			case HEAPTUPLE_RECENTLY_DEAD:
			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				/* FALL THRU */
			case HEAPTUPLE_LIVE:
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * Once we reach here we won't delete anymore tuples for this
				 * HOT chain during current call.
				 *
				 * We don't really need to do anything else with this HOT
				 * chain here.  We must continue traversing it all the same,
				 * so that pruning has a clear and self-consistent picture of
				 * the structure of HOT chains on the page (anything that's
				 * left behind is an orphaned heap-only tuple).
				 */
				Assert(!orphaned);
				pastlatestdead = true;
				prstate->visited[offnum] = true;
				chainitems[nchain++] = offnum;

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in heapam.c.
				 */
				break;

			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}

		/*
		 * If the tuple is not HOT-updated, then we are at the end of this
		 * HOT-update chain.
		 *
		 * There might actually be more tuples that were considered part of
		 * the same HOT chain in the past, before the updater's xact aborted.
		 * They'll be processed in heap_prune_orphan later on.  No call here
		 * need recognize these tuples as orphaned.
		 */
		if (!HeapTupleHeaderIsHotUpdated(htup))
			break;

		/* HOT implies it can't have moved to different partition */
		Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

		/*
		 * Advance to next HOT chain member
		 */
		offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
		priorXmax = HeapTupleHeaderGetUpdateXid(htup);
	}

	Assert(nchain >= (redirectroot ? 2 : 1));
	Assert(prstate->visited[rootoffnum]);
	if (OffsetNumberIsValid(latestdead))
	{
		int			ndeleted = 0,
					i;

		/*
		 * Okay, at least one tuple from the beginning of the chain (or a
		 * single plain heap tuple) is considered DEAD.  Record what to do
		 * with items in the chain now.
		 *
		 * First deal with the non-root items from HOT chain.  Mark earlier
		 * items we consider DEAD as LP_UNUSED (since they're heap-only
		 * tuples).
		 *
		 * When the previous item is the last DEAD tuple seen, we are at the
		 * right candidate for redirection.
		 */
		for (i = 1; (i < nchain) && (chainitems[i - 1] != latestdead); i++)
		{
			heap_prune_record_unused(prstate, chainitems[i]);
			ndeleted++;
		}

		/*
		 * If the root item is a normal tuple, we are logically deleting it,
		 * so count it in the result.  But changing an LP_REDIRECT (even to
		 * make it LP_DEAD) doesn't get counted in ndeleted -- that would
		 * amount to double-counting DEAD tuples (with tuple storage) in
		 * ndeleted.
		 */
		if (!redirectroot)
			ndeleted++;

		/*
		 * Finally, consider what to do with the root item itself.
		 *
		 * If the DEAD tuple is at the end of the HOT chain, the entire chain
		 * is considered DEAD.  The root item must therefore become LP_DEAD.
		 * Otherwise just redirect the root to the correct chain member.
		 */
		if (i >= nchain)
			heap_prune_record_dead(prstate, rootoffnum);
		else
			heap_prune_record_redirect(prstate, rootoffnum, chainitems[i]);

		return ndeleted;
	}

	return 0;
}

/*
 * Handle orphaned heap-only tuples during third and final pass over page.
 * Process these tuples as DEAD tuples here.
 *
 * This is how we handle aborted heap-only tuples that were not visited in our
 * second pass (via HOT chain traversal with the usual cross-checks).  These
 * tuples occur when a parent tuple is updated, the updater aborts, and some
 * unrelated updater re-updates the original parent tuple again.  The parent's
 * t_ctid link won't continue to point to the aborted tuple.  (Even when it
 * does, we won't consider the parent to have been HOT updated, just because
 * its XMAX aborted -- so we still end up here for the aborted tuple).
 *
 * Returns the number of tuples (to be) deleted from the page, though this
 * should always be 1 in practice.
*/
static inline int
heap_prune_orphan(OffsetNumber offnum, PruneState *prstate)
{
	Assert(!prstate->visited[offnum] && prstate->heaponly[offnum]);

	/*
	 * We expect that orphaned heap-only tuples must be from aborted
	 * transactions.  They must already be DEAD, or something is amiss.
	 */
	if (likely((HTSV_Result) prstate->htsv[offnum] == HEAPTUPLE_DEAD))
	{
		/* HeapTupleHeaderAdvanceLatestRemovedXid unnecessary here */
		heap_prune_record_unused(prstate, offnum);
		return 1;
	}

	/*
	 * Should always be DEAD.  A DEAD heap-only tuple is always counted in
	 * top-level ndeleted counter for pruning operation.
	 */
	Assert(false);
	return 0;
}

/* Record lowest soon-prunable XID */
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/* Record line pointer to be redirected */
static void
heap_prune_record_redirect(PruneState *prstate,
						   OffsetNumber offnum, OffsetNumber rdoffnum)
{
	Assert(prstate->nredirected < MaxHeapTuplesPerPage);
	Assert(!prstate->heaponly[offnum]);
	Assert(prstate->heaponly[rdoffnum]);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
	prstate->nredirected++;
}

/* Record line pointer to be marked dead */
static void
heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->ndead < MaxHeapTuplesPerPage);
	Assert(!prstate->heaponly[offnum]);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;
}

/* Record line pointer to be marked unused */
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->nunused < MaxHeapTuplesPerPage);
	Assert(prstate->htsv[offnum] != -1);
	Assert(prstate->heaponly[offnum]);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;
}


/*
 * Perform the actual page changes needed by heap_page_prune.
 * It is expected that the caller has a full cleanup lock on the
 * buffer.
 */
void
heap_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	HeapTupleHeader htup PG_USED_FOR_ASSERTS_ONLY;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* Update all redirected line pointers */
	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		ItemId		tolp PG_USED_FOR_ASSERTS_ONLY;

#ifdef USE_ASSERT_CHECKING

		/*
		 * Any existing item that we set as an LP_REDIRECT (any 'from' item)
		 * must be the first item from a HOT chain.  If the item has tuple
		 * storage then it can't be a heap-only tuple.  Otherwise we are just
		 * maintaining an existing LP_REDIRECT from an existing HOT chain that
		 * has been pruned at least once before now.
		 */
		if (!ItemIdIsRedirected(fromlp))
		{
			Assert(ItemIdHasStorage(fromlp) && ItemIdIsNormal(fromlp));

			htup = (HeapTupleHeader) PageGetItem(page, fromlp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* We shouldn't need to redundantly set the redirect */
			Assert(ItemIdGetRedirect(fromlp) != tooff);
		}

		/*
		 * The item that we're about to set as an LP_REDIRECT (the 'from'
		 * item) will point to an existing item (the 'to' item) that is
		 * already a heap-only tuple.  There can be at most one LP_REDIRECT
		 * item per HOT chain.
		 *
		 * We need to keep around an LP_REDIRECT item (after original
		 * non-heap-only root tuple gets pruned away) so that it's always
		 * possible for VACUUM to easily figure out what TID to delete from
		 * indexes when an entire HOT chain becomes dead.  A heap-only tuple
		 * can never become LP_DEAD; an LP_REDIRECT item or a regular heap
		 * tuple can.
		 *
		 * This check may miss problems, e.g. the target of a redirect could
		 * be marked as unused subsequently. The page_verify_redirects() check
		 * below will catch such problems.
		 */
		tolp = PageGetItemId(page, tooff);
		Assert(ItemIdHasStorage(tolp) && ItemIdIsNormal(tolp));
		htup = (HeapTupleHeader) PageGetItem(page, tolp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * An LP_DEAD line pointer must be left behind when the original item
		 * (which is dead to everybody) could still be referenced by a TID in
		 * an index.  This should never be necessary with any individual
		 * heap-only tuple item, though. (It's not clear how much of a problem
		 * that would be, but there is no reason to allow it.)
		 */
		if (ItemIdHasStorage(lp))
		{
			Assert(ItemIdIsNormal(lp));
			htup = (HeapTupleHeader) PageGetItem(page, lp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* Whole HOT chain becomes dead */
			Assert(ItemIdIsRedirected(lp));
		}
#endif

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * Only heap-only tuples can become LP_UNUSED during pruning.  They
		 * don't need to be left in place as LP_DEAD items until VACUUM gets
		 * around to doing index vacuuming.
		 */
		Assert(ItemIdHasStorage(lp) && ItemIdIsNormal(lp));
		htup = (HeapTupleHeader) PageGetItem(page, lp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);

	/*
	 * Now that the page has been modified, assert that redirect items still
	 * point to valid targets.
	 */
	page_verify_redirects(page);
}


/*
 * If built with assertions, verify that all LP_REDIRECT items point to a
 * valid item.
 *
 * One way that bugs related to HOT pruning show is redirect items pointing to
 * removed tuples. It's not trivial to reliably check that marking an item
 * unused will not orphan a redirect item during heap_prune_from_root() /
 * heap_page_prune_execute(), so we additionally check the whole page after
 * pruning. Without this check such bugs would typically only cause asserts
 * later, potentially well after the corruption has been introduced.
 *
 * Also check comments in heap_page_prune_execute()'s redirection loop.
 */
static void
page_verify_redirects(Page page)
{
#ifdef USE_ASSERT_CHECKING
	OffsetNumber offnum;
	OffsetNumber maxoff;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		OffsetNumber targoff;
		ItemId		targitem;
		HeapTupleHeader htup;

		if (!ItemIdIsRedirected(itemid))
			continue;

		targoff = ItemIdGetRedirect(itemid);
		targitem = PageGetItemId(page, targoff);

		Assert(ItemIdIsUsed(targitem));
		Assert(ItemIdIsNormal(targitem));
		Assert(ItemIdHasStorage(targitem));
		htup = (HeapTupleHeader) PageGetItem(page, targitem);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
	}
#endif
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxHeapTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		HeapTupleHeader htup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (HeapTupleHeaderIsHeapOnly(htup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			/* Sanity check (pure paranoia) */
			if (offnum < FirstOffsetNumber)
				break;

			/*
			 * An offset past the end of page's line pointer array is possible
			 * when the array was truncated
			 */
			if (offnum > maxoff)
				break;

			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
	}
}
