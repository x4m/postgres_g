/*-------------------------------------------------------------------------
 *
 * pg_btree_compact.c
 *	  Compact bloated B-tree indexes by merging underutilized pages
 *
 * This module provides functions to reduce B-tree index bloat by merging
 * sparsely populated leaf pages. The compaction process uses a two-phase
 * locking strategy to minimize the time spent holding an exclusive lock.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_btree_compact/pg_btree_compact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/* Function declarations */
PG_FUNCTION_INFO_V1(btree_compact);
PG_FUNCTION_INFO_V1(btree_compact_estimate);

/*
 * Structure to track a potential page merge
 */
typedef struct MergePlan
{
	BlockNumber left_blkno;		/* Page to merge into */
	BlockNumber right_blkno;	/* Page to be emptied */
	int			right_nitems;	/* Number of items on right page */
	int			left_nitems;	/* Number of items on left page */
} MergePlan;

/*
 * Statistics collected during compaction
 */
typedef struct CompactionStats
{
	int64		pages_visited;
	int64		pages_merged;
	int64		pages_deleted;
	int64		tuples_moved;
	int64		lock_time_ms;
	int64		total_time_ms;
} CompactionStats;

/*
 * Forward declarations
 */
static List *build_merge_plan(Relation index, int min_items, int max_items,
							   CompactionStats *stats);
static void execute_merge_plan(Relation index, List *merge_plan,
								CompactionStats *stats);
static bool validate_and_merge_pages(Relation index, MergePlan *plan,
									  CompactionStats *stats);
static int get_max_items_for_page(Relation index);

/*
 * btree_compact - Main entry point for index compaction
 *
 * Takes an index name and compaction parameters, performs two-phase
 * compaction, and returns statistics about the operation.
 */
Datum
btree_compact(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	int			min_items = PG_GETARG_INT32(1);
	int			max_items = PG_ARGISNULL(2) ? -1 : PG_GETARG_INT32(2);
	Relation	index;
	List	   *merge_plan;
	CompactionStats stats;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[6];
	bool		nulls[6];
	HeapTuple	tuple;
	instr_time	start_time,
				end_time;

	/* Initialize statistics */
	memset(&stats, 0, sizeof(CompactionStats));
	INSTR_TIME_SET_CURRENT(start_time);

	/* Validate parameters */
	if (min_items < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("min_items_per_page must be at least 1")));

	if (max_items != -1 && max_items < min_items)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_items_per_page must be greater than min_items_per_page")));

	/* Check result type */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/* Open the index with AccessShareLock - allows concurrent reads and writes */
	index = index_open(indexoid, AccessShareLock);

	/* Verify it's a B-tree index */
	if (index->rd_rel->relam != BTREE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a B-tree index",
						RelationGetRelationName(index))));

	/* Check permissions - must be owner or superuser */
	if (!object_ownercheck(RelationRelationId, indexoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX,
					   RelationGetRelationName(index));

	/* Determine max_items if not specified */
	if (max_items == -1)
		max_items = get_max_items_for_page(index);

	ereport(NOTICE,
			(errmsg("compacting index \"%s\"",
					RelationGetRelationName(index)),
			 errdetail("min_items=%d, max_items=%d", min_items, max_items)));

	/*
	 * Phase 1: Build merge plan under AccessShareLock
	 * This allows concurrent reads AND writes during planning
	 */
	merge_plan = build_merge_plan(index, min_items, max_items, &stats);

	ereport(NOTICE,
			(errmsg("found %d page pairs to merge", list_length(merge_plan))));

	/* Release AccessShareLock before acquiring exclusive lock */
	index_close(index, AccessShareLock);

	if (list_length(merge_plan) > 0)
	{
		/*
		 * Phase 2: Execute merge plan under AccessExclusiveLock
		 * This is the brief critical section
		 */
		index = index_open(indexoid, AccessExclusiveLock);
		
		execute_merge_plan(index, merge_plan, &stats);
		
		index_close(index, AccessExclusiveLock);
	}

	/* Calculate total time */
	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, start_time);
	stats.total_time_ms = INSTR_TIME_GET_MILLISEC(end_time);

	/* Build result tuple */
	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(stats.pages_visited);
	values[1] = Int64GetDatum(stats.pages_merged);
	values[2] = Int64GetDatum(stats.pages_deleted);
	values[3] = Int64GetDatum(stats.tuples_moved);
	values[4] = Int64GetDatum(stats.lock_time_ms);
	values[5] = Int64GetDatum(stats.total_time_ms);

	tuple = heap_form_tuple(rsinfo->expectedDesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * build_merge_plan - Scan index and identify pages to merge
 *
 * Returns a list of MergePlan structures describing page pairs that
 * can be merged to reduce bloat.
 */
static List *
build_merge_plan(Relation index, int min_items, int max_items,
				 CompactionStats *stats)
{
	List	   *merge_plan = NIL;
	BlockNumber nblocks;
	BlockNumber blkno;
	Buffer		buf = InvalidBuffer;
	Page		page;
	BTPageOpaque opaque;

	nblocks = RelationGetNumberOfBlocks(index);

	/* Scan all pages */
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		int			nitems;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BT_READ);

		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);

		stats->pages_visited++;

		/* Only interested in live leaf pages */
		if (!P_ISLEAF(opaque) || P_ISDELETED(opaque))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		nitems = PageGetMaxOffsetNumber(page);

		/*
		 * If this page is sparse, check if we can merge it with left sibling
		 */
		if (nitems < min_items && nitems > 0)
		{
			BlockNumber left_blkno = opaque->btpo_prev;

			/* Must have a valid left sibling */
			if (left_blkno != P_NONE && left_blkno < blkno)
			{
				Buffer		left_buf;
				Page		left_page;
				BTPageOpaque left_opaque;
				int			left_nitems;

				/* Read left sibling */
				left_buf = ReadBuffer(index, left_blkno);
				LockBuffer(left_buf, BT_READ);

				left_page = BufferGetPage(left_buf);
				left_opaque = BTPageGetOpaque(left_page);
				left_nitems = PageGetMaxOffsetNumber(left_page);

				/*
				 * Check if left page is suitable:
				 * - Must be a live leaf page
				 * - Must have space for items from right page
				 * - Right pointer must point to our page (consistency check)
				 */
				if (P_ISLEAF(left_opaque) &&
					!P_ISDELETED(left_opaque) &&
					left_nitems + nitems <= max_items &&
					left_opaque->btpo_next == blkno)
				{
					MergePlan  *plan = palloc(sizeof(MergePlan));

					plan->left_blkno = left_blkno;
					plan->right_blkno = blkno;
					plan->right_nitems = nitems;
					plan->left_nitems = left_nitems;

					merge_plan = lappend(merge_plan, plan);
				}

				UnlockReleaseBuffer(left_buf);
			}
		}

		UnlockReleaseBuffer(buf);
	}

	return merge_plan;
}

/*
 * execute_merge_plan - Execute the merge plan under exclusive lock
 */
static void
execute_merge_plan(Relation index, List *merge_plan, CompactionStats *stats)
{
	ListCell   *lc;
	instr_time	start_time,
				end_time;

	INSTR_TIME_SET_CURRENT(start_time);

	foreach(lc, merge_plan)
	{
		MergePlan  *plan = (MergePlan *) lfirst(lc);

		CHECK_FOR_INTERRUPTS();

		/* Validate conditions and perform merge */
		if (validate_and_merge_pages(index, plan, stats))
		{
			stats->pages_merged++;
			stats->pages_deleted++;
			stats->tuples_moved += plan->right_nitems;
		}
	}

	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, start_time);
	stats->lock_time_ms = INSTR_TIME_GET_MILLISEC(end_time);
}

/*
 * validate_and_merge_pages - Validate and merge a page pair
 *
 * Re-checks conditions (TOCTOU protection) and performs the actual merge.
 * Returns true if merge was successful.
 */
static bool
validate_and_merge_pages(Relation index, MergePlan *plan,
						  CompactionStats *stats)
{
	Buffer		left_buf,
				right_buf;
	Page		left_page,
				right_page;
	BTPageOpaque left_opaque,
				right_opaque;
	OffsetNumber maxoff,
				offnum;
	int			left_nitems,
				right_nitems;
	BlockNumber right_next;
	bool		result = false;

	/* Lock both pages */
	left_buf = ReadBuffer(index, plan->left_blkno);
	right_buf = ReadBuffer(index, plan->right_blkno);

	LockBuffer(left_buf, BT_WRITE);
	LockBuffer(right_buf, BT_WRITE);

	left_page = BufferGetPage(left_buf);
	right_page = BufferGetPage(right_buf);
	left_opaque = BTPageGetOpaque(left_page);
	right_opaque = BTPageGetOpaque(right_page);

	/* Re-validate conditions */
	if (P_ISDELETED(left_opaque) || P_ISDELETED(right_opaque) ||
		!P_ISLEAF(left_opaque) || !P_ISLEAF(right_opaque) ||
		left_opaque->btpo_next != plan->right_blkno ||
		right_opaque->btpo_prev != plan->left_blkno)
	{
		/* Conditions changed, skip this merge */
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/* Re-check that we still have space */
	left_nitems = PageGetMaxOffsetNumber(left_page);
	right_nitems = PageGetMaxOffsetNumber(right_page);
	maxoff = PageGetMaxOffsetNumber(right_page);

	/*
	 * Verify we have enough space on left page.
	 * For simplicity, check if left page has enough free space for all items.
	 */
	if (maxoff > 0)
	{
		Size		total_size = 0;
		OffsetNumber i;

		for (i = P_FIRSTDATAKEY(right_opaque); i <= maxoff; i++)
		{
			IndexTuple	itup = (IndexTuple) PageGetItem(right_page, PageGetItemId(right_page, i));

			total_size += IndexTupleSize(itup) + sizeof(ItemIdData);
		}

		if (PageGetFreeSpace(left_page) < total_size)
		{
			/* Not enough space after all, skip */
			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);
			return false;
		}
	}

	/* Remember right sibling for later link update */
	right_next = right_opaque->btpo_next;

	/*
	 * If there's a right sibling, lock it now so we can update it atomically
	 */
	if (BlockNumberIsValid(right_next) && right_next != P_NONE)
	{
		Buffer		next_buf;
		Page		next_page;
		BTPageOpaque next_opaque;

		next_buf = ReadBuffer(index, right_next);
		LockBuffer(next_buf, BT_WRITE);
		next_page = BufferGetPage(next_buf);
		next_opaque = BTPageGetOpaque(next_page);

		/*
		 * Start critical section - from here, we must complete or PANIC
		 */
		START_CRIT_SECTION();

		/*
		 * Copy all tuples from right page to left page
		 */
		for (offnum = P_FIRSTDATAKEY(right_opaque); offnum <= maxoff; offnum++)
		{
			IndexTuple	itup;
			Size		itemsz;
			OffsetNumber newoff;

			itup = (IndexTuple) PageGetItem(right_page, PageGetItemId(right_page, offnum));
			itemsz = IndexTupleSize(itup);

			/*
			 * Add item to left page. We add at end since items should already
			 * be in sorted order.
			 */
			newoff = PageAddItem(left_page, (Pointer) itup, itemsz,
								 InvalidOffsetNumber, false, false);

			if (newoff == InvalidOffsetNumber)
				elog(PANIC, "failed to add index item to page %u during compaction",
					 plan->left_blkno);
		}

		/*
		 * Update left page's right link to skip over the right page
		 */
		left_opaque->btpo_next = right_next;

		/*
		 * Mark right page as deleted.
		 */
		right_opaque->btpo_flags |= BTP_DELETED;
		right_opaque->btpo_prev = plan->left_blkno;	/* remember who deleted us */

		/*
		 * Update right sibling's left link
		 */
		next_opaque->btpo_prev = plan->left_blkno;

		/* Mark buffers dirty */
		MarkBufferDirty(left_buf);
		MarkBufferDirty(right_buf);
		MarkBufferDirty(next_buf);

		/*
		 * Write WAL record.
		 * We use REGBUF_FORCE_IMAGE to log full page images of all modified
		 * pages. This is simpler and safer than creating a custom WAL record
		 * for this complex operation.
		 */
		if (RelationNeedsWAL(index))
		{
			XLogRecPtr	recptr;

			XLogBeginInsert();
			XLogRegisterBuffer(0, left_buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			XLogRegisterBuffer(1, right_buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			XLogRegisterBuffer(2, next_buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

			/*
			 * We reuse XLOG_BTREE_VACUUM since it's a maintenance operation
			 * and the FPIs contain all the information needed for recovery.
			 */
			recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_VACUUM);

			PageSetLSN(left_page, recptr);
			PageSetLSN(right_page, recptr);
			PageSetLSN(next_page, recptr);
		}

		END_CRIT_SECTION();

		UnlockReleaseBuffer(next_buf);
	}
	else
	{
		/*
		 * No right sibling, simpler case
		 */
		START_CRIT_SECTION();

		/*
		 * Copy all tuples from right page to left page
		 */
		for (offnum = P_FIRSTDATAKEY(right_opaque); offnum <= maxoff; offnum++)
		{
			IndexTuple	itup;
			Size		itemsz;
			OffsetNumber newoff;

			itup = (IndexTuple) PageGetItem(right_page, PageGetItemId(right_page, offnum));
			itemsz = IndexTupleSize(itup);

			newoff = PageAddItem(left_page, (Pointer) itup, itemsz,
								 InvalidOffsetNumber, false, false);

			if (newoff == InvalidOffsetNumber)
				elog(PANIC, "failed to add index item to page %u during compaction",
					 plan->left_blkno);
		}

		/*
		 * Update left page's right link to skip over the right page
		 */
		left_opaque->btpo_next = right_next;

		/*
		 * Mark right page as deleted
		 */
		right_opaque->btpo_flags |= BTP_DELETED;
		right_opaque->btpo_prev = plan->left_blkno;

		/* Mark buffers dirty */
		MarkBufferDirty(left_buf);
		MarkBufferDirty(right_buf);

		/*
		 * Write WAL record with FPIs
		 */
		if (RelationNeedsWAL(index))
		{
			XLogRecPtr	recptr;

			XLogBeginInsert();
			XLogRegisterBuffer(0, left_buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			XLogRegisterBuffer(1, right_buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

			recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_VACUUM);

			PageSetLSN(left_page, recptr);
			PageSetLSN(right_page, recptr);
		}

		END_CRIT_SECTION();
	}

	ereport(DEBUG1,
			(errmsg("merged pages %u -> %u (%d tuples)",
					plan->right_blkno, plan->left_blkno, right_nitems)));

	result = true;

	UnlockReleaseBuffer(right_buf);
	UnlockReleaseBuffer(left_buf);

	return result;
}

/*
 * get_max_items_for_page - Calculate max items based on fillfactor
 */
static int
get_max_items_for_page(Relation index)
{
	/*
	 * Simple heuristic: assume average B-tree page can hold ~300 items
	 * at 100% fillfactor. Adjust based on actual fillfactor.
	 */
	int			fillfactor = RelationGetFillFactor(index, BTREE_DEFAULT_FILLFACTOR);
	int			max_items = (300 * fillfactor) / 100;

	/* Sanity bounds */
	if (max_items < 10)
		max_items = 10;
	if (max_items > 1000)
		max_items = 1000;

	return max_items;
}

/*
 * btree_compact_estimate - Estimate potential space savings
 *
 * Scans the index to estimate how much space could be saved by compaction.
 */
Datum
btree_compact_estimate(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	int			min_items = PG_GETARG_INT32(1);
	Relation	index;
	BlockNumber nblocks;
	BlockNumber blkno;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[5];
	bool		nulls[5];
	HeapTuple	tuple;
	int64		total_pages = 0;
	int64		leaf_pages = 0;
	int64		sparse_pages = 0;
	int64		block_size;

	/* Check result type */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/* Open the index with AccessShareLock (lightest lock) */
	index = index_open(indexoid, AccessShareLock);

	/* Verify it's a B-tree index */
	if (index->rd_rel->relam != BTREE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a B-tree index",
						RelationGetRelationName(index))));

	nblocks = RelationGetNumberOfBlocks(index);
	block_size = BLCKSZ;

	/* Scan all pages to count sparse pages */
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;
		int			nitems;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BT_READ);

		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);

		total_pages++;

		if (P_ISLEAF(opaque) && !P_ISDELETED(opaque))
		{
			leaf_pages++;
			nitems = PageGetMaxOffsetNumber(page);

			if (nitems < min_items && nitems > 0)
				sparse_pages++;
		}

		UnlockReleaseBuffer(buf);
	}

	index_close(index, AccessShareLock);

	/* Build result tuple */
	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(total_pages);
	values[1] = Int64GetDatum(leaf_pages);
	values[2] = Int64GetDatum(sparse_pages);
	values[3] = Int64GetDatum(sparse_pages * block_size);
	
	if (leaf_pages > 0)
		values[4] = DirectFunctionCall2(numeric_div,
										DirectFunctionCall1(int8_numeric,
															Int64GetDatum(sparse_pages * 100)),
										DirectFunctionCall1(int8_numeric,
															Int64GetDatum(leaf_pages)));
	else
		values[4] = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));

	tuple = heap_form_tuple(rsinfo->expectedDesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

