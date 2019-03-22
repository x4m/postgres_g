/*-------------------------------------------------------------------------
 *
 * verify_nbtree.c
 *		Verifies the integrity of GiST indexes based on invariants.
 *
 * Verification checks that all paths in GiST graph are contatining
 * consisnent keys: tuples on parent pages consistently include tuples
 * from children pages. Also, verification checks graph invariants:
 * internal page must have at least one downlinks, internal page can
 * reference either only leaf pages or only internal pages.
 *
 *
 * Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/verify_gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "amcheck.h"

#include "access/gist_private.h"


typedef struct GistScanItem
{
	GistNSN		parentlsn;
	BlockNumber blkno;
	struct GistScanItem *next;
} GistScanItem;

static inline void
check_index_tuple(IndexTuple idxtuple, Relation rel, ItemId iid);

static inline void
check_index_page(Relation rel, Page page, Buffer buffer);

static inline bool
gist_check_internal_page(Relation rel, Page page_copy, Buffer buffer,
						 BufferAccessStrategy strategy, GISTSTATE *state);

static inline void
gist_check_parent_keys_consistency(Relation rel);

static inline void
gist_check_page_keys(Relation rel, Buffer parentbuffer, Page page,
					 BlockNumber blkno, IndexTuple parent, GISTSTATE *state);

static void
pushStackIfSplited(Page page, GistScanItem *stack);

static inline void
gist_index_checkable(Relation rel);

static inline void
check_index_tuple(IndexTuple idxtuple, Relation rel, ItemId iid)
{
	/*
	 * Check that it's not a leftover invalid tuple from pre-9.1
	 * See also gistdoinsert() and gistbulkdelete() handlding of such tuples.
	 * We do consider it error here.
	 */
	if (GistTupleIsInvalid(idxtuple))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("index \"%s\" contains an inner tuple marked as"
						" invalid",RelationGetRelationName(rel)),
				 errdetail("This is caused by an incomplete page split at "
				 "crash recovery before upgrading to PostgreSQL 9.1."),
				 errhint("Please REINDEX it.")));

	if (MAXALIGN(ItemIdGetLength(iid)) != MAXALIGN(IndexTupleSize(idxtuple)))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" has tuple sizes",
						RelationGetRelationName(rel))));
}

static inline void
check_index_page(Relation rel, Page page, Buffer buffer)
{
	gistcheckpage(rel, buffer);
	if (GistPageGetOpaque(page)->gist_page_id != GIST_PAGE_ID)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" has corrupted pages",
						RelationGetRelationName(rel))));
	if (GistPageIsDeleted(page))
	{
		elog(ERROR,"boom");
		if (!GistPageIsLeaf(page))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					errmsg("index \"%s\" has deleted internal page",
							RelationGetRelationName(rel))));
		if (PageGetMaxOffsetNumber(page) > InvalidOffsetNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					errmsg("index \"%s\" has deleted page with tuples",
							RelationGetRelationName(rel))));
	}
}

/*
 * For every tuple on page check if it is contained by tuple on parent page
 */
static inline void
gist_check_page_keys(Relation rel, Buffer parentbuffer, Page page,
					 BlockNumber blkno, IndexTuple parent, GISTSTATE *state)
{
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(page);

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId iid = PageGetItemId(page, i);
		IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);

		check_index_tuple(idxtuple, rel, iid);

		/*
		 * Tree is inconsistent if adjustement is necessary for any parent
		 * tuple
		 */
		if (gistgetadjusted(rel, parent, idxtuple, state))
		{
			/*
			 * OK, we found a discrepency between parent and child tuples.
			 * We need to verify it is not a result of concurrent call
			 * of gistplacetopage(). So, lock parent and try to find downlink
			 * for current page. It may be missing due to concurrent page
			 * split, this is OK.
			 */
			LockBuffer(parentbuffer, GIST_SHARE);
			Page parentpage = (Page) BufferGetPage(parentbuffer);
			OffsetNumber o,
				parent_maxoff = PageGetMaxOffsetNumber(parentpage);

			for (o = FirstOffsetNumber; o <= parent_maxoff; o = OffsetNumberNext(o))
			{
				ItemId p_iid = PageGetItemId(parentpage, o);
				parent = (IndexTuple) PageGetItem(parentpage, p_iid);
				BlockNumber child_blkno = ItemPointerGetBlockNumber(&(parent->t_tid));
				if (child_blkno == blkno)
				{
					/* We found it - make a final check before failing */
					if (gistgetadjusted(rel, parent, idxtuple, state))
					{
						ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							errmsg("index \"%s\" has inconsistent records",
									RelationGetRelationName(rel))));
					}
					else
					{
						/*
						 * But now it is properly adjusted - nothing to do here.
						 */
						break;
					}
				}
			}

			LockBuffer(parentbuffer, GIST_UNLOCK);
		}
	}
}

/*
 * Check of an internal page.
 * Return true if further descent is necessary.
 * Hold pins on two pages at a time (parent+child).
 * But coupled lock on parent is taken iif parent-child discrepency found.
 * Locks is taken on every leaf page, and only then, if neccesary, on leaf
 * inside gist_check_page_keys() call.
 */
static inline bool
gist_check_internal_page(Relation rel, Page page_copy, Buffer buffer,
						 BufferAccessStrategy strategy, GISTSTATE *state)
{
	bool		 has_leafs = false;
	bool		 has_internals = false;
	OffsetNumber i,
				 maxoff = PageGetMaxOffsetNumber(page_copy);

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId		iid = PageGetItemId(page_copy, i);
		IndexTuple	idxtuple = (IndexTuple) PageGetItem(page_copy, iid);

		BlockNumber child_blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
		Buffer		buffer;
		Page		child_page;

		check_index_tuple(idxtuple, rel, iid);

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, child_blkno,
									RBM_NORMAL, strategy);

		LockBuffer(buffer, GIST_SHARE);
		child_page = (Page) BufferGetPage(buffer);
		check_index_page(rel, child_page, buffer);

		has_leafs = has_leafs || GistPageIsLeaf(child_page);
		has_internals = has_internals || !GistPageIsLeaf(child_page);
		gist_check_page_keys(rel, buffer, child_page, child_blkno, idxtuple, state);

		UnlockReleaseBuffer(buffer);
	}

	if (!(has_leafs || has_internals))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" internal page has no downlink references",
						RelationGetRelationName(rel))));

	if (has_leafs == has_internals)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" page references both internal and leaf pages",
						RelationGetRelationName(rel))));

	return has_internals;
}

/* add pages with unfinished split to scan */
static void
pushStackIfSplited(Page page, GistScanItem *stack)
{
	GISTPageOpaque opaque = GistPageGetOpaque(page);

	if (stack->blkno != GIST_ROOT_BLKNO && !XLogRecPtrIsInvalid(stack->parentlsn) &&
		(GistFollowRight(page) || stack->parentlsn < GistPageGetNSN(page)) &&
		opaque->rightlink != InvalidBlockNumber /* sanity check */ )
	{
		/* split page detected, install right link to the stack */

		GistScanItem *ptr = (GistScanItem *) palloc(sizeof(GistScanItem));

		ptr->blkno = opaque->rightlink;
		ptr->parentlsn = stack->parentlsn;
		ptr->next = stack->next;
		stack->next = ptr;
	}
}

/*
 * Main entry point for GiST check. Allocates memory context and scans
 * through GiST graph.
 * This function verifies that tuples of internal pages cover all the key
 * space of each tuple on leaf page. To do this we invoke
 * gist_check_internal_page() for every internal page.
 *
 * gist_check_internal_page() in it's turn takes every tuple and tries
 * to adjust it by tuples on referenced child page. Parent gist tuple should
 * never requre an adjustement.
 */
static inline void
gist_check_parent_keys_consistency(Relation rel)
{
	GistScanItem *stack,
			   *ptr;

	BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);

	MemoryContext mctx = AllocSetContextCreate(CurrentMemoryContext,
												 "amcheck context",
												 ALLOCSET_DEFAULT_SIZES);

	MemoryContext oldcontext = MemoryContextSwitchTo(mctx);
	GISTSTATE *state = initGISTstate(rel);

	stack = (GistScanItem *) palloc0(sizeof(GistScanItem));
	stack->blkno = GIST_ROOT_BLKNO;

	while (stack)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, strategy);
		LockBuffer(buffer, GIST_SHARE);
		page = (Page) BufferGetPage(buffer);
		check_index_page(rel, page, buffer);
		maxoff = PageGetMaxOffsetNumber(page);

		if (GistPageIsLeaf(page))
		{
			/* should never happen unless it is root */
			if (stack->blkno != GIST_ROOT_BLKNO)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						errmsg("index \"%s\": internal pages traversal "
						"encountered leaf page unexpectedly",
								RelationGetRelationName(rel))));
			}
			check_index_page(rel, page, buffer);

			for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
			{
				iid = PageGetItemId(page, i);
				idxtuple = (IndexTuple) PageGetItem(page, iid);
				check_index_tuple(idxtuple, rel, iid);
			}
			LockBuffer(buffer, GIST_UNLOCK);
		}
		else
		{
			/* we need to copy only internal pages */
			Page page_copy = palloc(BLCKSZ);
			memcpy(page_copy, page, BLCKSZ);
			LockBuffer(buffer, GIST_UNLOCK);

			/* check for split proceeded after look at parent */
			pushStackIfSplited(page_copy, stack);

			if (gist_check_internal_page(rel, page_copy, buffer, strategy, state))
			{
				for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
				{
					iid = PageGetItemId(page_copy, i);
					idxtuple = (IndexTuple) PageGetItem(page_copy, iid);

					ptr = (GistScanItem *) palloc(sizeof(GistScanItem));
					ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
					ptr->parentlsn = BufferGetLSNAtomic(buffer);
					ptr->next = stack->next;
					stack->next = ptr;
				}
			}

			pfree(page_copy);
		}

		ReleaseBuffer(buffer);

		ptr = stack->next;
		pfree(stack);
		stack = ptr;
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(mctx);
}

/* Check that relation is eligible for GiST verification */
static inline void
gist_index_checkable(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != GIST_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only GiST indexes are supported as targets for this"
						 " verification"),
				 errdetail("Relation \"%s\" is not a GiST index.",
						   RelationGetRelationName(rel))));

	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions"),
				 errdetail("Index \"%s\" is associated with temporary relation.",
						   RelationGetRelationName(rel))));

	if (!rel->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot check index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Index is not valid")));
}

PG_FUNCTION_INFO_V1(gist_index_parent_check);

Datum
gist_index_parent_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	Relation	indrel;
	Relation	heaprel;
	LOCKMODE	lockmode;

	/* lock table and index with neccesary level */
	amcheck_lock_relation(indrelid, true, &indrel, &heaprel, &lockmode);

	/* verify that this is GiST eligible for check */
	gist_index_checkable(indrel);
	gist_check_parent_keys_consistency(indrel);

	/* Unlock index and table */
	amcheck_unlock_relation(indrelid, indrel, heaprel, lockmode);

	PG_RETURN_VOID();
}
