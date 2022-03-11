/*-------------------------------------------------------------------------
 *
 * heap_convert.c
 *	  Heap page converter from 32bit to 64bit xid format
 *
 *	Copyright (c) 2015-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/heap_convert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"

static void convert_heap(Relation rel, Page page, Buffer buf, BlockNumber blkno);
static void repack_heap_tuples(Relation rel, Page page, Buffer buf,
							   BlockNumber blkno, bool double_xmax);

/*
 * Initialize special heap page area.
 */
static void
init_heap_page_special(PageHeader hdr, TransactionId xid_base,
					   MultiXactId multi_base, TransactionId prune_xid)
{
	HeapPageSpecial special;

	/* Page in 32-bit xid format should not have PageSpecial. */
	Assert(PageGetSpecialSize(hdr) == 0);

	hdr->pd_special = BLCKSZ - MAXALIGN(sizeof(HeapPageSpecialData));

	special = (HeapPageSpecial) ((char *) hdr + hdr->pd_special);
	special->pd_xid_base = xid_base;
	special->pd_multi_base = multi_base;
	HeapPageSetPruneXid(hdr, prune_xid);
}

/*
 * itemoffcompare
 *		Sorting support for repack_tuples()
 */
int
itemoffcompare(const void *item1, const void *item2)
{
	/* Sort in decreasing itemoff order */
	return ((ItemIdCompactData *) item2)->itemoff -
		   ((ItemIdCompactData *) item1)->itemoff;
}

/*
 * Lazy page conversion from 32-bit to 64-bit XID at first read.
 */
void
convert_page(Relation rel, Page page, Buffer buf, BlockNumber blkno)
{
	static unsigned		logcnt = 0;
	bool				logit;
	PageHeader			hdr = (PageHeader) page;
	GenericXLogState   *state = NULL;
	uint16				checksum;

	/* Not during XLog replaying */
	Assert(rel != NULL);

	/* Verify checksum */
	if (hdr->pd_checksum)
	{
		checksum = pg_checksum_page((char *) page, blkno);
		if (checksum != hdr->pd_checksum)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("page verification failed, calculated checksum %u but expected %u",
							checksum, hdr->pd_checksum)));
	}

	/*
	 * We occasionally force logging of page conversion, so never-changed
	 * pages are converted in the end. FORCE_LOG_EVERY is chosen arbitrarily
	 * to log neither too much nor too little.
	 */
#define FORCE_LOG_EVERY 128
	logit = !RecoveryInProgress() && XLogIsNeeded() && RelationNeedsWAL(rel);
	logit = logit && (++logcnt % FORCE_LOG_EVERY) == 0;
	if (logit)
	{
		state = GenericXLogStart(rel);
		page = GenericXLogRegisterBuffer(state, buf,
										 GENERIC_XLOG_FULL_IMAGE);
		hdr = (PageHeader) page;
	}

	/* Not already converted */
	Assert(PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION);

	switch (rel->rd_rel->relkind)
	{
		case 'r':
		case 'p':
		case 't':
		case 'm':
			convert_heap(rel, page, buf, blkno);
			break;
		case 'i':
			/* no need to convert index */
		case 'S':
			/* no real need to convert sequences */
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("conversion for relation \"%s\" cannot be done",
							RelationGetRelationName(rel)),
					 errdetail_relkind_not_supported(rel->rd_rel->relkind)));
	}

	hdr->pd_checksum = pg_checksum_page((char *) page, blkno);

	PageSetPageSizeAndVersion((hdr), PageGetPageSize(hdr),
							  PG_PAGE_LAYOUT_VERSION);

	if (logit)
	{
		/*
		 * Finish logging buffer conversion and mark buffer as dirty.
		 */
		Assert(state != NULL);
		MarkBufferDirty(buf);
		GenericXLogFinish(state);
	}
	else
	{
		/*
		 * Otherwise, it will be logged with full-page-write record on first
		 * actual change.
		 */
		MarkBufferConverted(buf, true);
	}
}

static void
convert_heap(Relation rel, Page page, Buffer buf, BlockNumber blkno)
{
	PageHeader	page_hdr = (PageHeader) page;
	bool		try_double_xmax;

	/*
	 * If page has enough space for PageSpecial no need to use double xmax.
	 */
	try_double_xmax = page_hdr->pd_upper - page_hdr->pd_lower <
							MAXALIGN(sizeof(HeapPageSpecialData));
	repack_heap_tuples(rel, page, buf, blkno, try_double_xmax);
}

/*
 * Convert xmin and xmax in a tuple.
 * This also considers special cases: "double xmax" page format and multixact
 * in xmax.
 */
static void
convert_heap_tuple_xids(HeapTupleHeader tuple, TransactionId xid_base,
						MultiXactId mxid_base, bool double_xmax)
{
	/* Convert xmin */
	if (double_xmax)
	{
		/* Prepare tuple for "double xmax" page format */
		tuple->t_infomask |= HEAP_XMIN_FROZEN;
		tuple->t_choice.t_heap.t_xmin = 0;
	}
	else
	{
		TransactionId xmin = tuple->t_choice.t_heap.t_xmin;

		if (TransactionIdIsNormal(xmin))
		{
			if (HeapTupleHeaderXminFrozen(tuple))
				tuple->t_choice.t_heap.t_xmin = FrozenTransactionId;
			else if (HeapTupleHeaderXminInvalid(tuple))
				tuple->t_choice.t_heap.t_xmin = InvalidTransactionId;
			else
			{
				Assert(xmin >= xid_base + FirstNormalTransactionId);
				/* Subtract xid_base from normal xmin */
				tuple->t_choice.t_heap.t_xmin = xmin - xid_base;
			}
		}
	}

	/* If tuple has multixact flag, handle mxid wraparound */
	if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) &&
		!(tuple->t_infomask & HEAP_XMAX_INVALID))
	{
		MultiXactId mxid = tuple->t_choice.t_heap.t_xmax;

		/* Handle mxid wraparound */
		if (mxid < mxid_base)
		{
			mxid += ((MultiXactId) 1 << 32) - FirstMultiXactId;
			Assert(mxid >= mxid_base);
		}

		if (double_xmax)
		{
			/* Save converted mxid into "double xmax" format */
			HeapTupleHeaderSetDoubleXmax(tuple, mxid);
		}
		else
		{
			/*
			 * Save converted mxid offset relative to (minmxid - 1), which
			 * will be page's mxid base.
			 */
			Assert(mxid - mxid_base + FirstMultiXactId <= PG_UINT32_MAX);
			tuple->t_choice.t_heap.t_xmax =
				(uint32) (mxid - mxid_base + FirstMultiXactId);
		}
	}
	/* Convert xmax */
	else if (!(tuple->t_infomask & HEAP_XMAX_INVALID))
	{
		TransactionId xmax = tuple->t_choice.t_heap.t_xmax;

		if (double_xmax)
		{
			/* Save converted xmax into "double xmax" format */
			HeapTupleHeaderSetDoubleXmax(tuple, xmax);
		}
		else if (TransactionIdIsNormal(xmax))
		{
			/* Subtract xid_base from normal xmax */
			Assert(xmax >= xid_base + FirstNormalTransactionId);
			tuple->t_choice.t_heap.t_xmax = xmax - xid_base;
		}
	}
	else
	{
		if (double_xmax)
			HeapTupleHeaderSetDoubleXmax(tuple, InvalidTransactionId);
		else
			tuple->t_choice.t_heap.t_xmax = InvalidTransactionId;
	}
}

/*
 * Correct page xmin/xmax based on tuple xmin/xmax values.
 */
static void
compute_xid_min_max(HeapTuple tuple, MultiXactId mxid_base,
					TransactionId *xid_min, TransactionId *xid_max,
					MultiXactId *mxid_min, MultiXactId *mxid_max)
{
	/* xmin */
	if (!HeapTupleHeaderXminInvalid(tuple->t_data) &&
		!HeapTupleHeaderXminFrozen(tuple->t_data))
	{
		TransactionId xid = HeapTupleGetRawXmin(tuple);

		if (TransactionIdIsNormal(xid))
		{
			*xid_max = Max(*xid_max, xid);
			*xid_min = Min(*xid_min, xid);
		}
	}

	/* xmax */
	if (!(tuple->t_data->t_infomask & HEAP_XMAX_INVALID))
	{
		TransactionId xid;

		if (tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			MultiXactId mxid = HeapTupleGetRawXmax(tuple);

			Assert(MultiXactIdIsValid(mxid));

			/* Handle mxid wraparound */
			if (mxid < mxid_base)
			{
				mxid += ((MultiXactId) 1 << 32) - FirstMultiXactId;
				Assert(mxid >= mxid_base);
			}

			*mxid_max = Max(*mxid_max, mxid);
			*mxid_min = Min(*mxid_min, mxid);

			/*
			 * Also take into account hidden update xid, which can be
			 * extracted by the vacuum.
			 */
			if (tuple->t_data->t_infomask & HEAP_XMAX_LOCK_ONLY)
				xid = InvalidTransactionId;
			else
				xid = HeapTupleGetUpdateXid(tuple);
		}
		else
		{
			xid = HeapTupleGetRawXmax(tuple);
		}

		if (TransactionIdIsNormal(xid))
		{
			*xid_max = Max(*xid_max, xid);
			*xid_min = Min(*xid_min, xid);
		}
	}
}

/*
 * Create PageHeader for page converted from 32-bit to 64-bit XID format.
 *
 * Return true, if "double xmax" format used.
 */
static bool
init_heap_page_header(Relation rel, BlockNumber blkno, PageHeader new_hdr,
					  TransactionId prune_xid, bool header_fits,
					  TransactionId xid_min, TransactionId xid_max,
					  MultiXactId mxid_min, MultiXactId mxid_max,
					  TransactionId *xid_base, MultiXactId *mxid_base)
{
	bool	xid_max_invalid = xid_max == InvalidTransactionId;
	bool	mxid_max_invalid = mxid_max == InvalidMultiXactId;
	bool	xid_max_fits = xid_max_invalid || xid_max - xid_min <=
								MaxShortTransactionId - FirstNormalTransactionId;
	bool	mxid_max_fits = mxid_max_invalid || mxid_max - mxid_min <=
								MaxShortTransactionId - FirstMultiXactId;

	if (header_fits && xid_max_fits && mxid_max_fits)
	{
		Assert(xid_max_invalid || xid_max >= xid_min);
		Assert(mxid_max_invalid || mxid_max >= mxid_min);

		*xid_base = xid_max_invalid ? InvalidTransactionId :
									  xid_min - FirstNormalTransactionId;
		*mxid_base = mxid_max_invalid ? InvalidMultiXactId :
										mxid_min - FirstMultiXactId;

		init_heap_page_special(new_hdr, *xid_base, *mxid_base, prune_xid);
		return false;
	}
	else
	{
		/* No space for special area, switch to "double xmax" format */
		new_hdr->pd_special = BLCKSZ;

		*xid_base = InvalidTransactionId;
		*mxid_base = InvalidMultiXactId;

		elog(DEBUG2, "convert heap page %u of relation \"%s\" to double xmax format",
			 blkno, RelationGetRelationName(rel));
		return true;
	}
}

/*
 * repack_heap_tuples
 *		Convert heap page format reusing space of dead tuples
 */
static void
repack_heap_tuples(Relation rel, Page page, Buffer buf, BlockNumber blkno,
				   bool try_double_xmax)
{
	ItemIdCompactData	items[MaxHeapTuplesPerPage];
	ItemIdCompact		itemPtr = items;
	ItemId				lp;
	int					nitems = 0,
						maxoff = PageGetMaxOffsetNumber(page),
						idx,
						occupied_space = 0;
	Offset				upper;
	bool				double_xmax;
	PageHeader			hdr = (PageHeader) page,
						new_hdr;
	char				new_page[BLCKSZ] = {0};
	MultiXactId			mxid_base = rel->rd_rel->relminmxid,
						mxid_min = MaxMultiXactId,
						mxid_max = InvalidMultiXactId;
	TransactionId		xid_base = rel->rd_rel->relfrozenxid,
						xid_min = MaxTransactionId,
						xid_max = InvalidTransactionId;

	if (TransactionIdIsNormal(hdr->pd_prune_xid))
		xid_min = xid_max = hdr->pd_prune_xid;

	for (idx = 0; idx < maxoff; idx++)
	{
		HeapTupleData tuple;

		lp = PageGetItemId(page, idx + 1);

		/* Skip redirects and items without storage */
		if (!ItemIdHasStorage(lp))
			continue;

		/* Build in-memory tuple representation */
		tuple.t_tableOid = 1;	/* doesn't matter in this case */
		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
		HeapTupleCopyHeaderXids(&tuple);
		tuple.t_len = ItemIdGetLength(lp);
		ItemPointerSet(&(tuple.t_self), blkno, ItemIdGetOffset(lp));

		/*
		 * This is only needed to determine whether tuple is HEAPTUPLE_DEAD or
		 * HEAPTUPLE_RECENTLY_DEAD. And since this is the first time we read
		 * page after pg_upgrade, it cannot be HEAPTUPLE_RECENTLY_DEAD. See
		 * HeapTupleSatisfiesVacuum() for details
		 */
		if (try_double_xmax &&
			HeapTupleSatisfiesVacuum(&tuple,
									 (TransactionId) 1 << 32, buf) == HEAPTUPLE_DEAD)
		{
			ItemIdSetDead(lp);
		}

		if (ItemIdIsNormal(lp) && ItemIdHasStorage(lp))
		{
			itemPtr->offsetindex = idx;
			itemPtr->itemoff = ItemIdGetOffset(lp);
			if (unlikely(itemPtr->itemoff < hdr->pd_upper ||
						 itemPtr->itemoff >= hdr->pd_special))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("corrupted item pointer: %u",
								itemPtr->itemoff)));
			}

			itemPtr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
			occupied_space += itemPtr->alignedlen;
			nitems++;
			itemPtr++;
			if (try_double_xmax)
			{
				HeapTupleSetXmin(&tuple, FrozenTransactionId);
				HeapTupleHeaderSetXminFrozen(tuple.t_data);
			}

			compute_xid_min_max(&tuple, mxid_base,
								&xid_min, &xid_max,
								&mxid_min, &mxid_max);
		}
	}

	/* Write new header */
	new_hdr = (PageHeader) new_page;
	*new_hdr = *hdr;
	new_hdr->pd_lower = SizeOfPageHeaderData + maxoff * sizeof(ItemIdData);

	double_xmax = init_heap_page_header(rel, blkno, new_hdr,
										hdr->pd_prune_xid,
										BLCKSZ - new_hdr->pd_lower - occupied_space >= sizeof(HeapPageSpecialData),
										xid_min, xid_max,
										mxid_min, mxid_max,
										&xid_base, &mxid_base);
	if (!try_double_xmax && double_xmax)
		return repack_heap_tuples(rel, page, buf, blkno, true);

	/* Copy ItemIds with an offset */
	memcpy((char *) new_page + SizeOfPageHeaderData,
		   (char *) page + SizeOfPageHeaderData,
		   hdr->pd_lower - SizeOfPageHeaderData);

	/* Move live tuples */
	upper = new_hdr->pd_special;
	for (idx = 0; idx < nitems; idx++)
	{
		HeapTupleHeader tuple;

		itemPtr = &items[idx];
		lp = PageGetItemId(new_page, itemPtr->offsetindex + 1);
		upper -= itemPtr->alignedlen;

		memcpy((char *) new_page + upper,
			   (char *) page + itemPtr->itemoff,
			   itemPtr->alignedlen);

		tuple = (HeapTupleHeader) (((char *) new_page) + upper);

		convert_heap_tuple_xids(tuple, xid_base, mxid_base, double_xmax);

		lp->lp_off = upper;

		occupied_space -= itemPtr->alignedlen;
	}

	Assert(occupied_space == 0);

	new_hdr->pd_upper = upper;
	if (new_hdr->pd_lower > new_hdr->pd_upper)
		elog(ERROR, "cannot convert block %u of relation \"%s\"",
			 blkno, RelationGetRelationName(rel));

	memcpy(page, new_page, BLCKSZ);
}
