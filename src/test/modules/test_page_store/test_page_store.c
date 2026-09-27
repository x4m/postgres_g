/*-------------------------------------------------------------------------
 *
 * test_page_store.c
 *      Test-only page service using ordinary standby redo.
 *
 * The original raw-page oracle requires one frozen replay cut.  Physical
 * fetches can also use the bounded retained-history experiment.  Neither
 * path substitutes a newer page for an unavailable cut.  For frozen reads,
 * buffer locks protect the copy but do not freeze replay globally, so check
 * the replay position again after taking the copy.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"

#include "test_page_store.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_page_store_read);
PG_FUNCTION_INFO_V1(test_page_store_fetch);

/* No waiting here: a request has to identify the already frozen read view. */
void
test_page_store_check_cut(TimeLineID expected_tli, XLogRecPtr expected_lsn)
{
	TimeLineID	tli;
	XLogRecPtr	lsn;

	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("page service requires a standby")));

	if (GetRecoveryPauseState() != RECOVERY_PAUSED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("page service requires paused recovery")));

	lsn = GetXLogReplayRecPtr(&tli);
	if (tli != expected_tli || lsn != expected_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("requested replay cut is not available"),
				 errdetail("Requested timeline %u at %X/%X, current timeline %u at %X/%X.",
						   expected_tli, LSN_FORMAT_ARGS(expected_lsn),
						   tli, LSN_FORMAT_ARGS(lsn))));
}

Datum
test_page_store_read(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	char	   *expected_sysid = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int64		expected_tli = PG_GETARG_INT64(2);
	XLogRecPtr	expected_lsn = PG_GETARG_LSN(3);
	int64		blkno = PG_GETARG_INT64(4);
	char		sysid[32];
	Relation	rel;
	BlockNumber nblocks;
	Buffer		buf;
	bytea	   *page;
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		nulls[5] = {false};
	HeapTuple	tuple;

	/* Raw pages bypass MVCC and ordinary SQL column privileges. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use page service prototype")));

	if (expected_tli <= 0 || expected_tli > PG_UINT32_MAX ||
		XLogRecPtrIsInvalid(expected_lsn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid replay cut")));
	if (blkno < 0 || blkno > MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid block number")));

	snprintf(sysid, sizeof(sysid), UINT64_FORMAT, GetSystemIdentifier());
	if (strcmp(sysid, expected_sysid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("storage system identifier does not match")));

	test_page_store_check_cut((TimeLineID) expected_tli, expected_lsn);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	rel = relation_open(relid, AccessShareLock);
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind) ||
		rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("page service requires a permanent stored relation")));

	nblocks = RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM);
	if (blkno >= nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number is outside the relation")));

	page = (bytea *) palloc(VARHDRSZ + BLCKSZ);
	SET_VARSIZE(page, VARHDRSZ + BLCKSZ);
	buf = ReadBufferExtended(rel, MAIN_FORKNUM, (BlockNumber) blkno,
							 RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	memcpy(VARDATA(page), BufferGetPage(buf), BLCKSZ);
	UnlockReleaseBuffer(buf);

	INJECTION_POINT("test-page-store-after-read", NULL);

	/* A page's pd_lsn alone cannot establish this: unchanged pages are old. */
	test_page_store_check_cut((TimeLineID) expected_tli, expected_lsn);

	values[0] = ObjectIdGetDatum(rel->rd_locator.spcOid);
	values[1] = ObjectIdGetDatum(rel->rd_locator.dbOid);
	values[2] = ObjectIdGetDatum(rel->rd_locator.relNumber);
	values[3] = Int64GetDatum(nblocks);
	values[4] = PointerGetDatum(page);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	relation_close(rel, AccessShareLock);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * Physical, bounded reads for the SMgr experiment.  A zero block count asks
 * only for fork existence and size, including the present-but-empty case.
 * The old regclass reader remains an independent raw-page test oracle.
 */
Datum
test_page_store_fetch(PG_FUNCTION_ARGS)
{
	RelFileLocator locator;
	int32		forknum = PG_GETARG_INT32(3);
	int64		blkno = PG_GETARG_INT64(4);
	int32		count = PG_GETARG_INT32(5);
	char	   *expected_sysid = text_to_cstring(PG_GETARG_TEXT_PP(6));
	int64		tli = PG_GETARG_INT64(7);
	XLogRecPtr	lsn = PG_GETARG_LSN(8);
	char		sysid[32];
	SMgrRelation smgr;
	bool		exists;
	BlockNumber nblocks;
	bytea	   *pages;
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false};

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use page service prototype")));

	locator.spcOid = PG_GETARG_OID(0);
	locator.dbOid = PG_GETARG_OID(1);
	locator.relNumber = PG_GETARG_OID(2);
	if (!OidIsValid(locator.spcOid) ||
		!RelFileNumberIsValid(locator.relNumber) ||
		forknum < MAIN_FORKNUM || forknum > MAX_FORKNUM ||
		blkno < 0 || blkno > MaxBlockNumber ||
		count < 0 || count > TEST_PAGE_STORE_MAX_BLOCKS ||
		(uint64) blkno + count > InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid physical page request")));
	if (tli <= 0 || tli > PG_UINT32_MAX || XLogRecPtrIsInvalid(lsn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid replay cut")));

	snprintf(sysid, sizeof(sysid), UINT64_FORMAT, GetSystemIdentifier());
	if (strcmp(sysid, expected_sysid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("storage system identifier does not match")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (test_page_store_history_enabled())
	{
		pages = test_page_store_history_fetch(locator, (ForkNumber) forknum,
											  (BlockNumber) blkno, count,
											  (TimeLineID) tli, lsn, PG_GETARG_BOOL(9),
											  &exists, &nblocks);
		values[0] = BoolGetDatum(exists);
		values[1] = Int64GetDatum(nblocks);
		values[2] = PointerGetDatum(pages);
		PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
	}
	test_page_store_check_cut((TimeLineID) tli, lsn);

	smgr = smgropen(locator, INVALID_PROC_NUMBER);
	exists = smgrexists(smgr, (ForkNumber) forknum);
	nblocks = exists ? smgrnblocks(smgr, (ForkNumber) forknum) : 0;
	if (count > 0 && (!exists || (uint64) blkno + count > nblocks))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("requested blocks are outside the relation")));

	pages = (bytea *) palloc(VARHDRSZ + count * BLCKSZ);
	SET_VARSIZE(pages, VARHDRSZ + count * BLCKSZ);
	for (int i = 0; i < count; i++)
	{
		BlockNumber block = (BlockNumber) blkno + i;
		Buffer		buf;
		PGAlignedBlock copy;

		buf = ReadBufferWithoutRelcache(locator, (ForkNumber) forknum, block,
										RBM_NORMAL, NULL, true);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		memcpy(copy.data, BufferGetPage(buf), BLCKSZ);
		UnlockReleaseBuffer(buf);
		/* A shared-buffer image does not necessarily have a valid checksum. */
		PageSetChecksum(copy.data, block);
		/* bytea's payload is not aligned for PageHeader access. */
		memcpy(VARDATA(pages) + i * BLCKSZ, copy.data, BLCKSZ);
	}

	INJECTION_POINT("test-page-store-after-fetch", NULL);
	test_page_store_check_cut((TimeLineID) tli, lsn);
	values[0] = BoolGetDatum(exists);
	values[1] = Int64GetDatum(nblocks);
	values[2] = PointerGetDatum(pages);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
