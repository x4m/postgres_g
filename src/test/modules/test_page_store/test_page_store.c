/*-------------------------------------------------------------------------
 *
 * test_page_store.c
 *      Test-only page service using ordinary standby redo.
 *
 * This implements one frozen replay cut, not historical page reconstruction
 * or a compute storage manager.  It deliberately rejects a request instead
 * of substituting a page from a newer replay position.  A real service needs
 * a retained read view covering the entire operation, not an administrative
 * recovery pause.  Buffer content locks protect the copy; they do not freeze
 * replay globally.  Validate the replay position again after taking the copy.
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
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_page_store_read);

/* No waiting here: a request has to identify the already frozen read view. */
static void
check_replay_cut(TimeLineID expected_tli, XLogRecPtr expected_lsn)
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

	check_replay_cut((TimeLineID) expected_tli, expected_lsn);

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
	check_replay_cut((TimeLineID) expected_tli, expected_lsn);

	values[0] = ObjectIdGetDatum(rel->rd_locator.spcOid);
	values[1] = ObjectIdGetDatum(rel->rd_locator.dbOid);
	values[2] = ObjectIdGetDatum(rel->rd_locator.relNumber);
	values[3] = Int64GetDatum(nblocks);
	values[4] = PointerGetDatum(page);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	relation_close(rel, AccessShareLock);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
