/*-------------------------------------------------------------------------
 *
 * heapcheck.c
 *	  Functions to check postgresql relations for corruption
 *
 * Copyright (c) 2016-2020, PostgreSQL Global Development Group
 *
 *	  contrib/heapcheck/heapcheck.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/toast_internals.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(heapcheck_relation);

typedef struct CorruptionInfo
{
	BlockNumber blkno;
	OffsetNumber offnum;
	int16		lp_off;
	int16		lp_flags;
	int16		lp_len;
	int32		attnum;
	int32		chunk;
	char	   *msg;
}			CorruptionInfo;

typedef struct HeapCheckContext
{
	/* Values concerning the heap relation being checked */
	Oid			relid;
	Relation	rel;
	TupleDesc	relDesc;
	TransactionId relfrozenxid;
	MultiXactId relminmxid;
	int			rel_natts;
	bool		has_toastrel;
	Relation	toastrel;
	Relation   *toast_indexes;
	Relation	valid_toast_index;
	int			num_toast_indexes;

	/* Values for iterating over pages in the relation */
	BlockNumber nblocks;
	BlockNumber blkno;
	BufferAccessStrategy bstrategy;
	Buffer		buffer;
	Page		page;

	/* Values for iterating over tuples within a page */
	OffsetNumber offnum;
	OffsetNumber maxoff;
	ItemId		itemid;
	uint16		lp_len;
	HeapTupleHeader tuphdr;
	TransactionId xmin;
	TransactionId xmax;
	uint16		infomask;
	int			natts;
	bool		hasnulls;

	/* Values for iterating over attributes within the tuple */
	uint32		offset;			/* offset in tuple data */
	AttrNumber	attnum;
	char	   *tp;				/* pointer to the tuple data */
	bits8	   *bp;				/* ptr to null bitmap in tuple */
	Form_pg_attribute thisatt;

	/* Values for iterating over toast for the attribute */
	ScanKeyData toastkey;
	SysScanDesc toastscan;
	SnapshotData SnapshotToast;
	int32		chunkno;
	HeapTuple	toasttup;
	int32		attrsize;
	int32		endchunk;
	int32		totalchunks;
	TupleDesc	toasttupDesc;
	bool		found_toasttup;

	/* List of CorruptionInfo */
	List	   *corruption;
}			HeapCheckContext;

/* Public API */
typedef struct CheckRelCtx
{
	List	   *corruption;
	int			idx;
}			CheckRelCtx;

Datum		heapcheck_relation(PG_FUNCTION_ARGS);

/* Internal implementation */
void		record_corruption(HeapCheckContext * ctx, char *msg);
TupleDesc	heapcheck_relation_tupdesc(void);

void		beginRelBlockIteration(HeapCheckContext * ctx);
bool		relBlockIteration_next(HeapCheckContext * ctx);
void		endRelBlockIteration(HeapCheckContext * ctx);

void		beginPageTupleIteration(HeapCheckContext * ctx);
bool		pageTupleIteration_next(HeapCheckContext * ctx);
void		endPageTupleIteration(HeapCheckContext * ctx);

void		beginTupleAttributeIteration(HeapCheckContext * ctx);
bool		tupleAttributeIteration_next(HeapCheckContext * ctx);
void		endTupleAttributeIteration(HeapCheckContext * ctx);

void		beginToastTupleIteration(HeapCheckContext * ctx,
									 struct varatt_external *toast_pointer);
void		endToastTupleIteration(HeapCheckContext * ctx);
bool		toastTupleIteration_next(HeapCheckContext * ctx);

bool		TransactionIdStillValid(TransactionId xid, FullTransactionId *fxid);
bool		HeapTupleIsVisible(HeapTupleHeader tuphdr, HeapCheckContext * ctx);
void		check_toast_tuple(HeapCheckContext * ctx);
bool		check_tuple_attribute(HeapCheckContext * ctx);
void		check_tuple(HeapCheckContext * ctx);

List	   *check_relation(Oid relid);
void		check_relation_relkind(Relation rel);

/*
 * record_corruption
 *
 *   Record a message about corruption, including information
 *   about where in the relation the corruption was found.
 */
void
record_corruption(HeapCheckContext * ctx, char *msg)
{
	CorruptionInfo *info = (CorruptionInfo *) palloc0(sizeof(CorruptionInfo));

	info->blkno = ctx->blkno;
	info->offnum = ctx->offnum;
	info->lp_off = ItemIdGetOffset(ctx->itemid);
	info->lp_flags = ItemIdGetFlags(ctx->itemid);
	info->lp_len = ItemIdGetLength(ctx->itemid);
	info->attnum = ctx->attnum;
	info->chunk = ctx->chunkno;
	info->msg = msg;

	ctx->corruption = lappend(ctx->corruption, info);
}

/*
 * Helper function to construct the TupleDesc needed by heapcheck_relation.
 */
TupleDesc
heapcheck_relation_tupdesc()
{
	TupleDesc	tupdesc;
	AttrNumber	maxattr = 8;
	AttrNumber	a = 0;

	tupdesc = CreateTemplateTupleDesc(maxattr);
	TupleDescInitEntry(tupdesc, ++a, "blkno", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "offnum", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "lp_off", INT2OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "lp_flags", INT2OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "lp_len", INT2OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "attnum", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "chunk", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "msg", TEXTOID, -1, 0);
	Assert(a == maxattr);

	return BlessTupleDesc(tupdesc);
}

/*
 * heapcheck_relation
 *
 *   Scan and report corruption in heap pages or in associated toast relation.
 */
Datum
heapcheck_relation(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	CheckRelCtx *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		Oid			relid = PG_GETARG_OID(0);
		MemoryContext oldcontext;

		/*
		 * Scan the entire relation, building up a list of corruption found in
		 * ctx->corruption, for returning later.  The scan must be performed
		 * in a memory context that will survive until after all rows are
		 * returned.
		 */
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->tuple_desc = heapcheck_relation_tupdesc();
		ctx = (CheckRelCtx *) palloc0(sizeof(CheckRelCtx));
		ctx->corruption = check_relation(relid);
		ctx->idx = 0;			/* start the iterator at the beginning */
		funcctx->user_fctx = (void *) ctx;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (CheckRelCtx *) funcctx->user_fctx;

	/*
	 * Return the next corruption message from the list, if any.  Our location
	 * in the list is recorded in ctx->idx.  The special value -1 is used in
	 * the list of corruptions to represent NULL; we check for negative
	 * numbers when setting the nulls[] values.
	 */
	if (ctx->idx < list_length(ctx->corruption))
	{
		Datum		values[8];
		bool		nulls[8];
		HeapTuple	tuple;
		CorruptionInfo *info = list_nth(ctx->corruption, ctx->idx);

		MemSet(values, 0, sizeof(nulls));
		MemSet(nulls, 0, sizeof(nulls));
		values[0] = Int64GetDatum(info->blkno);
		values[1] = Int32GetDatum(info->offnum);
		nulls[1] = (info->offnum < 0);
		values[2] = Int16GetDatum(info->lp_off);
		nulls[2] = (info->lp_off < 0);
		values[3] = Int16GetDatum(info->lp_flags);
		nulls[3] = (info->lp_flags < 0);
		values[4] = Int16GetDatum(info->lp_len);
		nulls[4] = (info->lp_len < 0);
		values[5] = Int32GetDatum(info->attnum);
		nulls[5] = (info->attnum < 0);
		values[6] = Int32GetDatum(info->chunk);
		nulls[6] = (info->chunk < 0);
		values[7] = CStringGetTextDatum(info->msg);
		ctx->idx++;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * beginRelBlockIteration
 *
 *   For the given heap relation being checked, as recorded in ctx, sets up
 *   variables for iterating over the heap's pages.
 *
 *   The caller should have already opened the heap relation, ctx->rel
 */
void
beginRelBlockIteration(HeapCheckContext * ctx)
{
	ctx->nblocks = RelationGetNumberOfBlocks(ctx->rel);
	ctx->blkno = InvalidBlockNumber;
	ctx->bstrategy = GetAccessStrategy(BAS_BULKREAD);
	ctx->buffer = InvalidBuffer;
	ctx->page = NULL;
}

/*
 * endRelBlockIteration
 *
 *   Releases resources that were reserved by either beginRelBlockIteration or
 *   relBlockIteration_next.
 */
void
endRelBlockIteration(HeapCheckContext * ctx)
{
	/*
	 * Clean up.  If the caller iterated to the end, the final call to
	 * relBlockIteration_next will already have released the buffer, but if
	 * the caller is bailing out early, we have to release it ourselves.
	 */
	if (InvalidBuffer != ctx->buffer)
		UnlockReleaseBuffer(ctx->buffer);
}

/*
 * relBlockIteration_next
 *
 *   Updates the state in ctx to point to the next page in the relation.
 *   Returns true if there is any such page, else false.
 *
 *   The caller should have already called beginRelBlockIteration, and should
 *   only continue calling until the false result.
 */
bool
relBlockIteration_next(HeapCheckContext * ctx)
{
	/* We must unlock the page from the prior iteration, if any */
	Assert(ctx->blkno == InvalidBlockNumber || ctx->buffer != InvalidBuffer);
	if (InvalidBuffer != ctx->buffer)
	{
		UnlockReleaseBuffer(ctx->buffer);
		ctx->buffer = InvalidBuffer;
	}

	/* We rely on this math property for the first iteration */
	StaticAssertStmt(InvalidBlockNumber + 1 == 0,
					 "InvalidBlockNumber increments to zero");
	ctx->blkno++;
	if (ctx->blkno >= ctx->nblocks)
		return false;

	/* Read and lock the next page. */
	ctx->buffer = ReadBufferExtended(ctx->rel, MAIN_FORKNUM, ctx->blkno,
									 RBM_NORMAL, ctx->bstrategy);
	LockBuffer(ctx->buffer, BUFFER_LOCK_SHARE);
	ctx->page = BufferGetPage(ctx->buffer);

	return true;
}

/*
 * beginPageTupleIteration
 *
 *   For the given page begin visited, as stored in ctx, sets up variables for
 *   iterating over the tuples on the page.
 */
void
beginPageTupleIteration(HeapCheckContext * ctx)
{
	/* We rely on this math property for the first iteration */
	StaticAssertStmt(InvalidOffsetNumber + 1 == FirstOffsetNumber,
					 "InvalidOffsetNumber increments to FirstOffsetNumber");

	ctx->offnum = InvalidOffsetNumber;
	ctx->maxoff = PageGetMaxOffsetNumber(ctx->page);
	ctx->itemid = NULL;
	ctx->lp_len = 0;
	ctx->tuphdr = NULL;
	ctx->xmin = InvalidOid;
	ctx->xmax = InvalidOid;
	ctx->infomask = 0;
	ctx->natts = 0;
	ctx->hasnulls = false;
}

/*
 * endPageTupleIteration
 *
 *   Releases resources taken by beginPageTupleIteration or
 *   pageTupleIteration_next.
 */
void
endPageTupleIteration(HeapCheckContext * ctx)
{
	/* Abuse beginPageTupleIteration to reset the tuple iteration variables */
	beginPageTupleIteration(ctx);
}

/*
 * pageTupleIteration_next
 *
 *   Advances the state tracked in ctx to the next tuple on the page.
 *
 *   Caller should have already set up the iteration via
 *   beginPageTupleIteration, and should stop calling when this function
 *   returns false.
 */
bool
pageTupleIteration_next(HeapCheckContext * ctx)
{
	/*
	 * Iterate to the next interesting line pointer, if any. Unused, dead and
	 * redirect line pointers are of no interest.
	 */
	do
	{
		ctx->offnum = OffsetNumberNext(ctx->offnum);
		if (ctx->offnum > ctx->maxoff)
			return false;
		ctx->itemid = PageGetItemId(ctx->page, ctx->offnum);
	} while (!ItemIdIsUsed(ctx->itemid) ||
			 ItemIdIsDead(ctx->itemid) ||
			 ItemIdIsRedirected(ctx->itemid));

	/* Set up context information about this next tuple */
	ctx->lp_len = ItemIdGetLength(ctx->itemid);
	ctx->tuphdr = (HeapTupleHeader) PageGetItem(ctx->page, ctx->itemid);
	ctx->xmin = HeapTupleHeaderGetXmin(ctx->tuphdr);
	ctx->xmax = HeapTupleHeaderGetRawXmax(ctx->tuphdr);
	ctx->infomask = ctx->tuphdr->t_infomask;
	ctx->natts = HeapTupleHeaderGetNatts(ctx->tuphdr);
	ctx->hasnulls = ctx->infomask & HEAP_HASNULL;

	/*
	 * Reset information about individual attributes and related toast values,
	 * so they show as NULL in the corruption report if we record a corruption
	 * before beginning to iterate over the attributes.
	 */
	ctx->attnum = -1;
	ctx->chunkno = -1;

	return true;
}

/*
 * beginTupleAttributeIteration
 *
 *   For the given tuple begin visited, as stored in ctx, sets up variables for
 *   iterating over the attributes in the tuple.
 */
void
beginTupleAttributeIteration(HeapCheckContext * ctx)
{
	ctx->offset = 0;
	ctx->attnum = -1;
	ctx->tp = (char *) ctx->tuphdr + ctx->tuphdr->t_hoff;
	ctx->bp = ctx->tuphdr->t_bits;
}

/*
 * tupleAttributeIteration_next
 *
 *   Advances the state tracked in ctx to the next attribute in the tuple.
 *
 *   Caller should have already set up the iteration via
 *   beginTupleAttributeIteration, and should stop calling when this function
 *   returns false.
 */
bool
tupleAttributeIteration_next(HeapCheckContext * ctx)
{
	ctx->attnum++;
	if (ctx->attnum >= ctx->natts)
		return false;
	ctx->thisatt = TupleDescAttr(ctx->relDesc, ctx->attnum);
	return true;
}

/*
 * endTupleAttributeIteration
 *
 *   Resets state tracked in ctx after iteration over attributes ends.
 */
void
endTupleAttributeIteration(HeapCheckContext * ctx)
{
	ctx->offset = -1;
	ctx->attnum = -1;
}

/*
 * beginToastTupleIteration
 *
 *   For the given attribute begin visited, as stored in ctx, sets up variables for
 *   iterating over the related toast value.
 */
void
beginToastTupleIteration(HeapCheckContext * ctx,
						 struct varatt_external *toast_pointer)
{
	ctx->toasttupDesc = ctx->toastrel->rd_att;
	ctx->found_toasttup = false;

	ctx->attrsize = toast_pointer->va_extsize;
	ctx->endchunk = (ctx->attrsize - 1) / TOAST_MAX_CHUNK_SIZE;
	ctx->totalchunks = ctx->endchunk + 1;

	/*
	 * Setup a scan key to find chunks in toast table with matching va_valueid
	 */
	ScanKeyInit(&ctx->toastkey,
				(AttrNumber) 1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(toast_pointer->va_valueid));

	/*
	 * Check if any chunks for this toasted object exist in the toast table,
	 * accessible via the index.
	 */
	init_toast_snapshot(&ctx->SnapshotToast);
	ctx->toastscan = systable_beginscan_ordered(ctx->toastrel,
												ctx->valid_toast_index,
												&ctx->SnapshotToast, 1,
												&ctx->toastkey);
	ctx->chunkno = 0;
}

/*
 * toastTupleIteration_next
 *
 *   Advances the state tracked in ctx to the next toast tuple for the
 *   attribute.
 *
 *   Caller should have already set up the iteration via
 *   beginToastTupleIteration, and should stop calling when this function
 *   returns false.
 */
bool
toastTupleIteration_next(HeapCheckContext * ctx)
{
	ctx->toasttup = systable_getnext_ordered(ctx->toastscan,
											 ForwardScanDirection);
	return ctx->toasttup != NULL;
}

/*
 * endToastTupleIteration
 *
 *   Releases resources taken by beginToastTupleIteration or
 *   toastTupleIteration_next.
 */
void
endToastTupleIteration(HeapCheckContext * ctx)
{
	systable_endscan_ordered(ctx->toastscan);
}

/*
 * Given a TransactionId, attempt to interpret it as a valid
 * FullTransactionId, neither in the future nor overlong in
 * the past.  Stores the inferred FullTransactionId in *fxid.
 *
 * Returns whether the xid is newer than the oldest clog xid.
 */
bool
TransactionIdStillValid(TransactionId xid, FullTransactionId *fxid)
{
	FullTransactionId fnow;
	uint32		epoch;

	/* Initialize fxid; we'll overwrite this later if needed */
	*fxid = FullTransactionIdFromEpochAndXid(0, xid);

	/* Special xids can quickly be turned into invalid fxids */
	if (!TransactionIdIsValid(xid))
		return false;
	if (!TransactionIdIsNormal(xid))
		return true;

	/*
	 * Charitably infer the full transaction id as being within one epoch ago
	 */
	fnow = ReadNextFullTransactionId();
	epoch = EpochFromFullTransactionId(fnow);
	*fxid = FullTransactionIdFromEpochAndXid(epoch, xid);
	if (!FullTransactionIdPrecedes(*fxid, fnow))
		*fxid = FullTransactionIdFromEpochAndXid(epoch - 1, xid);
	if (!FullTransactionIdPrecedes(*fxid, fnow))
		return false;

	/* The oldestClogXid is protected by CLogTruncationLock */
	Assert(LWLockHeldByMe(CLogTruncationLock));
	if (TransactionIdPrecedes(xid, ShmemVariableCache->oldestClogXid))
		return false;
	return true;
}

/*
 * HeapTupleIsVisible
 *
 *	Determine whether tuples are visible for heapcheck.  Similar to
 *  HeapTupleSatisfiesVacuum, but with critical differences.
 *
 *  1) Does not touch hint bits.  It seems imprudent to write hint bits
 *     to a table during a corruption check.
 *  2) Gracefully handles xids that are too old by calling
 *     TransactionIdStillValid before TransactionLogFetch, thus avoiding
 *     a backend abort.
 *  3) Only makes a boolean determination of whether heapcheck should
 *     see the tuple, rather than doing extra work for vacuum-related
 *     categorization.
 */
bool
HeapTupleIsVisible(HeapTupleHeader tuphdr, HeapCheckContext * ctx)
{
	FullTransactionId fxmin,
				fxmax;
	uint16		infomask = tuphdr->t_infomask;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuphdr);

	if (!HeapTupleHeaderXminCommitted(tuphdr))
	{
		if (HeapTupleHeaderXminInvalid(tuphdr))
		{
			return false;		/* HEAPTUPLE_DEAD */
		}
		/* Used by pre-9.0 binary upgrades */
		else if (infomask & HEAP_MOVED_OFF)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuphdr);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;	/* HEAPTUPLE_DELETE_IN_PROGRESS */
			if (TransactionIdIsInProgress(xvac))
				return false;	/* HEAPTUPLE_DELETE_IN_PROGRESS */
			if (TransactionIdDidCommit(xvac))
				return false;	/* HEAPTUPLE_DEAD */
		}
		/* Used by pre-9.0 binary upgrades */
		else if (infomask & HEAP_MOVED_IN)
		{
			TransactionId xvac = HeapTupleHeaderGetXvac(tuphdr);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;	/* HEAPTUPLE_INSERT_IN_PROGRESS */
			if (TransactionIdIsInProgress(xvac))
				return false;	/* HEAPTUPLE_INSERT_IN_PROGRESS */
			if (!TransactionIdDidCommit(xvac))
				return false;	/* HEAPTUPLE_DEAD */
		}
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuphdr)))
			return false;		/* insert or delete in progress */
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuphdr)))
			return false;		/* HEAPTUPLE_INSERT_IN_PROGRESS */

		/*
		 * The tuple appears to either be or to have been visible to us, but
		 * the xmin may be too far in the past to be used.  We have to check
		 * that before calling TransactionIdDidCommit to avoid an Assertion.
		 */
		LWLockAcquire(CLogTruncationLock, LW_SHARED);
		if (!TransactionIdStillValid(xmin, &fxmin))
		{
			LWLockRelease(CLogTruncationLock);
			record_corruption(ctx, psprintf("tuple xmin = %u (interpreted as "
											UINT64_FORMAT
											") not or no longer valid",
											xmin, fxmin.value));
			return false;
		}
		else if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuphdr)))
		{
			LWLockRelease(CLogTruncationLock);
			return false;		/* HEAPTUPLE_DEAD */
		}
		LWLockRelease(CLogTruncationLock);
	}

	if (!(infomask & HEAP_XMAX_INVALID) && !HEAP_XMAX_IS_LOCKED_ONLY(infomask))
	{
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			TransactionId xmax = HeapTupleGetUpdateXid(tuphdr);

			/* not LOCKED_ONLY, so it has to have an xmax */
			if (!TransactionIdIsValid(xmax))
			{
				record_corruption(ctx, _("heap tuple with XMAX_IS_MULTI is "
										 "neither LOCKED_ONLY nor has a "
										 "valid xmax"));
				return false;
			}
			if (TransactionIdIsInProgress(xmax))
				return false;	/* HEAPTUPLE_DELETE_IN_PROGRESS */

			LWLockAcquire(CLogTruncationLock, LW_SHARED);
			if (!TransactionIdStillValid(xmax, &fxmax))
			{
				LWLockRelease(CLogTruncationLock);
				record_corruption(ctx, psprintf("tuple xmax = %u (interpreted "
												"as " UINT64_FORMAT
												") not or no longer valid",
												xmax, fxmax.value));
				return false;
			}
			else if (TransactionIdDidCommit(xmax))
			{
				LWLockRelease(CLogTruncationLock);
				return false;	/* HEAPTUPLE_RECENTLY_DEAD or HEAPTUPLE_DEAD */
			}
			LWLockRelease(CLogTruncationLock);
			/* Ok, the tuple is live */
		}
		else if (!(infomask & HEAP_XMAX_COMMITTED))
		{
			if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuphdr)))
				return false;	/* HEAPTUPLE_DELETE_IN_PROGRESS */
			/* Ok, the tuple is live */
		}
		else
			return false;		/* HEAPTUPLE_RECENTLY_DEAD or HEAPTUPLE_DEAD */
	}
	return true;
}

/*
 * check_toast_tuple
 *
 *   Checks the current toast tuple as tracked in ctx for corruption.  Records
 *   any corruption found in ctx->corruption.
 *
 *   The caller should have iterated to a tuple via toastTupleIteration_next.
 */
void
check_toast_tuple(HeapCheckContext * ctx)
{
	int32		curchunk;
	Pointer		chunk;
	bool		isnull;
	char	   *chunkdata;
	int32		chunksize;
	int32		expected_size;

	ctx->found_toasttup = true;

	/*
	 * Have a chunk, extract the sequence number and the data
	 */
	curchunk = DatumGetInt32(fastgetattr(ctx->toasttup, 2,
										 ctx->toasttupDesc, &isnull));
	if (isnull)
	{
		record_corruption(ctx, _("toast chunk sequencenumber is null"));
		return;
	}
	chunk = DatumGetPointer(fastgetattr(ctx->toasttup, 3,
										ctx->toasttupDesc, &isnull));
	if (isnull)
	{
		record_corruption(ctx, _("toast chunk data is null"));
		return;
	}
	if (!VARATT_IS_EXTENDED(chunk))
	{
		chunksize = VARSIZE(chunk) - VARHDRSZ;
		chunkdata = VARDATA(chunk);
	}
	else if (VARATT_IS_SHORT(chunk))
	{
		/*
		 * could happen due to heap_form_tuple doing its thing
		 */
		chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
		chunkdata = VARDATA_SHORT(chunk);
	}
	else
	{
		/* should never happen */
		record_corruption(ctx, _("toast chunk is neither short nor extended"));
		return;
	}

	/*
	 * Some checks on the data we've found
	 */
	if (curchunk != ctx->chunkno)
	{
		record_corruption(ctx, psprintf("toast chunk sequence number %u "
										"not the expected sequence number %u",
										curchunk, ctx->chunkno));
		return;
	}
	if (curchunk > ctx->endchunk)
	{
		record_corruption(ctx, psprintf("toast chunk sequence number %u "
										"exceeds the end chunk sequence "
										"number %u",
										curchunk, ctx->endchunk));
		return;
	}

	expected_size = curchunk < ctx->totalchunks - 1 ? TOAST_MAX_CHUNK_SIZE
		: ctx->attrsize - ((ctx->totalchunks - 1) * TOAST_MAX_CHUNK_SIZE);
	if (chunksize != expected_size)
	{
		record_corruption(ctx, psprintf("chunk size %u differs from "
										"expected size %u",
										chunksize, expected_size));
		return;
	}

	ctx->chunkno++;
}

/*
 * check_tuple_attribute
 *
 *   Checks the current attribute as tracked in ctx for corruption.  Records
 *   any corruption found in ctx->corruption.
 *
 *   The caller should have iterated to a tuple via
 *   tupleAttributeIteration_next.
 */
bool
check_tuple_attribute(HeapCheckContext * ctx)
{
	Datum		attdatum;
	struct varlena *attr;

	if (ctx->tuphdr->t_hoff + ctx->offset > ctx->lp_len)
	{
		record_corruption(ctx, psprintf("t_hoff + offset > lp_len (%u + %u > %u)",
										ctx->tuphdr->t_hoff, ctx->offset,
										ctx->lp_len));
		return false;
	}

	/* Skip null values */
	if (ctx->hasnulls && att_isnull(ctx->attnum, ctx->bp))
		return true;

	/* Skip non-varlena values, but update offset first */
	if (ctx->thisatt->attlen != -1)
	{
		ctx->offset = att_align_nominal(ctx->offset, ctx->thisatt->attalign);
		ctx->offset = att_addlength_pointer(ctx->offset, ctx->thisatt->attlen,
											ctx->tp + ctx->offset);
		return true;
	}

	/* Ok, we're looking at a varlena attribute. */
	ctx->offset = att_align_pointer(ctx->offset, ctx->thisatt->attalign, -1,
									ctx->tp + ctx->offset);

	/* Get the (possibly corrupt) varlena datum */
	attdatum = fetchatt(ctx->thisatt, ctx->tp + ctx->offset);

	/*
	 * We have the datum, but we cannot decode it carelessly, as it may still
	 * be corrupt.
	 */

	/*
	 * Check that VARTAG_SIZE won't hit a TrapMacro on a corrupt va_tag before
	 * risking a call into att_addlength_pointer
	 */
	if (VARATT_IS_1B_E(ctx->tp + ctx->offset))
	{
		uint8		va_tag = va_tag = VARTAG_EXTERNAL(ctx->tp + ctx->offset);

		if (va_tag != VARTAG_ONDISK)
		{
			record_corruption(ctx, psprintf("unexpected TOAST vartag %u for "
											"attribute #%u at t_hoff = %u, "
											"offset = %u",
											va_tag, ctx->attnum,
											ctx->tuphdr->t_hoff, ctx->offset));
			return false;		/* We can't know where the next attribute
								 * begins */
		}
	}

	/* Ok, should be safe now */
	ctx->offset = att_addlength_pointer(ctx->offset, ctx->thisatt->attlen,
										ctx->tp + ctx->offset);

	/*
	 * heap_deform_tuple would be done with this attribute at this point,
	 * having stored it in values[], and would continue to the next attribute.
	 * We go further, because we need to check if the toast datum is corrupt.
	 */

	attr = (struct varlena *) DatumGetPointer(attdatum);

	/*
	 * Now we follow the logic of detoast_external_attr(), with the same
	 * caveats about being paranoid about corruption.
	 */

	/* Skip values that are not external */
	if (!VARATT_IS_EXTERNAL(attr))
		return true;

	/* It is external, and we're looking at a page on disk */
	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		record_corruption(ctx,
						  _("attribute is external but not marked as on disk"));
		return true;
	}

	/* The tuple header better claim to contain toasted values */
	if (!(ctx->infomask & HEAP_HASEXTERNAL))
	{
		record_corruption(ctx, _("attribute is external but tuple header "
								 "flag HEAP_HASEXTERNAL not set"));
		return true;
	}

	/* The relation better have a toast table */
	if (!ctx->has_toastrel)
	{
		record_corruption(ctx, _("attribute is external but relation has "
								 "no toast relation"));
		return true;
	}

	/*
	 * Must dereference indirect toast pointers before we can check them
	 */
	if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		struct varatt_indirect redirect;

		VARATT_EXTERNAL_GET_POINTER(redirect, attr);
		attr = (struct varlena *) redirect.pointer;

		/* nested indirect Datums aren't allowed */
		if (VARATT_IS_EXTERNAL_INDIRECT(attr))
		{
			record_corruption(ctx, _("attribute has nested external "
									 "indirect toast pointer"));
			return true;
		}
	}

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		/*
		 * Must copy attr into toast_pointer for alignment considerations
		 */
		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
		beginToastTupleIteration(ctx, &toast_pointer);

		while (toastTupleIteration_next(ctx))
			check_toast_tuple(ctx);

		if (ctx->chunkno != (ctx->endchunk + 1))
			record_corruption(ctx, psprintf("final chunk number differs from "
											"expected (%u vs. %u)",
											ctx->chunkno, (ctx->endchunk + 1)));
		if (!ctx->found_toasttup)
			record_corruption(ctx, _("toasted value missing from "
									 "toast table"));
		endToastTupleIteration(ctx);
	}
	return true;
}

/*
 * check_tuple
 *
 *   Checks the current tuple as tracked in ctx for corruption.  Records any
 *   corruption found in ctx->corruption.
 *
 *   The caller should have iterated to a tuple via pageTupleIteration_next.
 */
void
check_tuple(HeapCheckContext * ctx)
{
	bool		fatal = false;

	/* Check relminmxid against mxid, if any */
	if (ctx->infomask & HEAP_XMAX_IS_MULTI &&
		MultiXactIdPrecedes(ctx->xmax, ctx->relminmxid))
	{
		record_corruption(ctx, psprintf("tuple xmax = %u precedes relation "
										"relminmxid = %u",
										ctx->xmax, ctx->relminmxid));
	}

	/* Check xmin against relfrozenxid */
	if (TransactionIdIsNormal(ctx->relfrozenxid) &&
		TransactionIdIsNormal(ctx->xmin) &&
		TransactionIdPrecedes(ctx->xmin, ctx->relfrozenxid))
	{
		record_corruption(ctx, psprintf("tuple xmin = %u precedes relation "
										"relfrozenxid = %u",
										ctx->xmin, ctx->relfrozenxid));
	}

	/* Check xmax against relfrozenxid */
	if (TransactionIdIsNormal(ctx->relfrozenxid) &&
		TransactionIdIsNormal(ctx->xmax) &&
		TransactionIdPrecedes(ctx->xmax, ctx->relfrozenxid))
	{
		record_corruption(ctx, psprintf("tuple xmax = %u precedes relation "
										"relfrozenxid = %u",
										ctx->xmax, ctx->relfrozenxid));
	}

	/* Check for tuple header corruption */
	if (ctx->tuphdr->t_hoff < SizeofHeapTupleHeader)
	{
		record_corruption(ctx, psprintf("t_hoff < SizeofHeapTupleHeader (%u < %u)",
										ctx->tuphdr->t_hoff,
										(unsigned) SizeofHeapTupleHeader));
		fatal = true;
	}
	if (ctx->tuphdr->t_hoff > ctx->lp_len)
	{
		record_corruption(ctx, psprintf("t_hoff > lp_len (%u > %u)",
										ctx->tuphdr->t_hoff, ctx->lp_len));
		fatal = true;
	}
	if (ctx->tuphdr->t_hoff != MAXALIGN(ctx->tuphdr->t_hoff))
	{
		record_corruption(ctx, psprintf("t_hoff not max-aligned (%u)",
										ctx->tuphdr->t_hoff));
		fatal = true;
	}

	/*
	 * If the tuple has nulls, check that the implied length of the variable
	 * length nulls bitmap field t_bits does not overflow the allowed space.
	 * We don't know if the corruption is in the natts field or the infomask
	 * bit HEAP_HASNULL.
	 */
	if (ctx->hasnulls &&
		SizeofHeapTupleHeader + BITMAPLEN(ctx->natts) > ctx->tuphdr->t_hoff)
	{
		record_corruption(ctx, psprintf("SizeofHeapTupleHeader + "
										"BITMAPLEN(natts) > t_hoff "
										"(%u + %u > %u)",
										(unsigned) SizeofHeapTupleHeader,
										BITMAPLEN(ctx->natts),
										ctx->tuphdr->t_hoff));
		fatal = true;
	}

	/* Cannot process tuple data if tuple header was corrupt */
	if (fatal)
		return;

	/*
	 * Skip tuples that are invisible, as we cannot assume the TupleDesc we
	 * are using is appropriate.
	 */
	if (!HeapTupleIsVisible(ctx->tuphdr, ctx))
		return;

	/*
	 * If we get this far, the tuple is visible to us, so it must not be
	 * incompatible with our relDesc.  The natts field could be legitimately
	 * shorter than rel_natts, but it cannot be longer than rel_natts.
	 */
	if (ctx->rel_natts < ctx->natts)
	{
		record_corruption(ctx, psprintf("relation natts < tuple natts (%u < %u)",
										ctx->rel_natts, ctx->natts));
		return;
	}

	/*
	 * Iterate over the attributes looking for broken toast values. This
	 * roughly follows the logic of heap_deform_tuple, except that it doesn't
	 * bother building up isnull[] and values[] arrays, since nobody wants
	 * them, and it unrolls anything that might trip over an Assert when
	 * processing corrupt data.
	 */
	beginTupleAttributeIteration(ctx);
	while (tupleAttributeIteration_next(ctx) &&
		   check_tuple_attribute(ctx))
		;
	endTupleAttributeIteration(ctx);
}

/*
 * check_relation
 *
 *   Checks the relation given by relid for corruption, returning a list of all
 *   it finds.
 *
 *   The caller should set up the memory context as desired before calling.
 *   The returned list belongs to the caller.
 */
List *
check_relation(Oid relid)
{
	HeapCheckContext ctx;

	memset(&ctx, 0, sizeof(HeapCheckContext));

	/* Open the relation */
	ctx.relid = relid;
	ctx.corruption = NIL;
	ctx.rel = relation_open(relid, AccessShareLock);
	check_relation_relkind(ctx.rel);

	ctx.relDesc = RelationGetDescr(ctx.rel);
	ctx.rel_natts = RelationGetDescr(ctx.rel)->natts;
	ctx.relfrozenxid = ctx.rel->rd_rel->relfrozenxid;
	ctx.relminmxid = ctx.rel->rd_rel->relminmxid;

	/* Open the toast relation, if any */
	if (ctx.rel->rd_rel->reltoastrelid)
	{
		int			offset;

		/* Main relation has associated toast relation */
		ctx.has_toastrel = true;
		ctx.toastrel = table_open(ctx.rel->rd_rel->reltoastrelid,
								  AccessShareLock);
		offset = toast_open_indexes(ctx.toastrel,
									AccessShareLock,
									&(ctx.toast_indexes),
									&(ctx.num_toast_indexes));
		ctx.valid_toast_index = ctx.toast_indexes[offset];
	}
	else
	{
		/* Main relation has no associated toast relation */
		ctx.has_toastrel = false;
		ctx.toast_indexes = NULL;
		ctx.num_toast_indexes = 0;
	}

	/* check all blocks of the relation */
	beginRelBlockIteration(&ctx);
	while (relBlockIteration_next(&ctx))
	{
		/* Perform tuple checks */
		beginPageTupleIteration(&ctx);
		while (pageTupleIteration_next(&ctx))
			check_tuple(&ctx);
		endPageTupleIteration(&ctx);
	}
	endRelBlockIteration(&ctx);

	/* Close the associated toast table and indexes, if any. */
	if (ctx.has_toastrel)
	{
		toast_close_indexes(ctx.toast_indexes, ctx.num_toast_indexes,
							AccessShareLock);
		table_close(ctx.toastrel, AccessShareLock);
	}

	/* Close the main relation */
	relation_close(ctx.rel, AccessShareLock);

	return ctx.corruption;
}

/*
 * check_relation_relkind
 *
 *   convenience routine to check that relation is of a supported relkind.
 */
void
check_relation_relkind(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table, materialized view, "
						"or TOAST table",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relam != HEAP_TABLE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a heap AM",
						RelationGetRelationName(rel))));
}
