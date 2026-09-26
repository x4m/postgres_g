/*-------------------------------------------------------------------------
 *
 * test_remote_smgr.c
 *      A frozen-view SMgr consumer of the test page service.
 *
 * This intentionally redirects just one physical relation on a paused
 * standby.  Startup redo and all other relations still use md.  It is a
 * transport/buffer-I/O experiment, not selective redo or diskless compute.
 * SQL is a temporary transport to the independent test endpoint; the final
 * physical service must also work before database connections are possible.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "storage/aio.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "test_page_store.h"

void		_PG_init(void);

PG_FUNCTION_INFO_V1(test_page_store_io_counts);
PG_FUNCTION_INFO_V1(test_page_store_read_smgr);

static char *page_conninfo;
static char *page_lsn;
static int	page_tli;
static int	page_spc;
static int	page_db;
static int	page_rel;
static int	request_timeout;
static PGconn *page_conn;
static PGresult *page_result;
static bool cleanup_registered;
static uint64 readv_count;
static uint64 startreadv_count;
static PgAioHandleCallbackID read_callback;
static ExecutorStart_hook_type previous_executor_start;
static ExecutorRun_hook_type previous_executor_run;

static bool remote_relation(SMgrRelation reln);
static bool remote_fetch(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, void **buffers,
						 BlockNumber count, BlockNumber *nblocks);
static bool remote_exists(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next);
static BlockNumber remote_nblocks(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next);
static bool remote_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							int nblocks, SmgrChainIndex next);
static uint32 remote_maxcombine(SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, SmgrChainIndex next);
static void remote_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks, SmgrChainIndex next);
static void remote_startreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, void **buffers,
							  BlockNumber nblocks, SmgrChainIndex next);
static void remote_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						  const void **buffers, BlockNumber nblocks, bool skipFsync,
						  SmgrChainIndex next);
static void remote_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							 BlockNumber nblocks, SmgrChainIndex next);
static PgAioResult remote_read_complete(PgAioHandle *ioh, PgAioResult result, uint8 flags);
static void remote_executor_start(QueryDesc *queryDesc, int eflags);
static void remote_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count);

static const f_smgr remote_smgr = {
	.name = "test_page_store",
	.chain_position = SMGR_CHAIN_MODIFIER,
	.smgr_exists = remote_exists,
	.smgr_nblocks = remote_nblocks,
	.smgr_prefetch = remote_prefetch,
	.smgr_maxcombine = remote_maxcombine,
	.smgr_readv = remote_readv,
	.smgr_startreadv = remote_startreadv,
	.smgr_writev = remote_writev,
	.smgr_writeback = remote_writeback,
};

static const PgAioHandleCallbacks remote_read_callbacks = {
	.complete_shared = remote_read_complete,
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("test_page_store.conninfo", "Page service connection.",
							   NULL, &page_conninfo, "", PGC_POSTMASTER, GUC_SUPERUSER_ONLY,
							   NULL, NULL, NULL);
	DefineCustomStringVariable("test_page_store.replay_lsn", "Frozen replay position.",
							   NULL, &page_lsn, "0/0", PGC_POSTMASTER, 0,
							   NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.replay_tli", "Frozen timeline.",
							NULL, &page_tli, 1, 1, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.tablespace", "Remote tablespace.",
							NULL, &page_spc, 0, 0, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.database", "Remote database.",
							NULL, &page_db, 0, 0, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.relfilenumber", "Remote relation.",
							NULL, &page_rel, 0, 0, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.request_timeout", "Page request timeout.",
							NULL, &request_timeout, 5000, 1, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);
	MarkGUCPrefixReserved("test_page_store");
	smgr_register(&remote_smgr, 0);
	read_callback = pgaio_io_register_callback_entry(&remote_read_callbacks,
													 "test_page_store_read");
	previous_executor_start = ExecutorStart_hook;
	ExecutorStart_hook = remote_executor_start;
	previous_executor_run = ExecutorRun_hook;
	ExecutorRun_hook = remote_executor_run;
}

/*
 * Checking just buffer misses would let cached scans continue after promotion
 * or replay advancement.  This deliberately restricts all executor queries on
 * the experimental compute to its configured, administratively frozen cut.
 * It is a guard for the experiment, not a retained-view implementation.
 */
static void
remote_executor_start(QueryDesc *queryDesc, int eflags)
{
	if (page_rel != 0)
		test_page_store_check_cut(page_tli, pg_lsn_in_safe(page_lsn, NULL));
	if (previous_executor_start)
		previous_executor_start(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
remote_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	if (page_rel != 0)
		test_page_store_check_cut(page_tli, pg_lsn_in_safe(page_lsn, NULL));
	if (previous_executor_run)
		previous_executor_run(queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
	if (page_rel != 0)
		test_page_store_check_cut(page_tli, pg_lsn_in_safe(page_lsn, NULL));
}

static bool
remote_relation(SMgrRelation reln)
{
	RelFileLocator locator = reln->smgr_rlocator.locator;

	/* Bootstrap/replay still materialize local files in this experiment. */
	return !AmStartupProcess() && !SmgrIsTemp(reln) && page_rel != 0 &&
		locator.spcOid == page_spc && locator.dbOid == page_db &&
		locator.relNumber == page_rel;
}

static void
remote_disconnect(int code, Datum arg)
{
	if (page_result)
		PQclear(page_result);
	page_result = NULL;
	if (page_conn)
		PQfinish(page_conn);
	page_conn = NULL;
}

/*
 * SMgr callers hold interrupts, and may already hold locks.  In particular,
 * processing a SMgr release barrier here would reenter the SMgr callbacks.
 * Use nonblocking libpq and a deadline, but do not process interrupts here.
 * A transport worker and a cancellable request lifetime are still needed
 * before this can be used as a general-purpose remote storage manager.
 */
static void
remote_wait(int event, TimestampTz deadline)
{
	long		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);

	if (remaining <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("page service request timed out")));
	ResetLatch(MyLatch);
	(void) WaitLatchOrSocket(MyLatch,
							 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT | event,
							 PQsocket(page_conn), remaining, PG_WAIT_EXTENSION);
}

static void
remote_connect(TimestampTz deadline)
{
	PostgresPollingStatusType status;

	if (page_conn)
		return;
	if (page_conninfo[0] == '\0')
		elog(ERROR, "test_page_store.conninfo is required");
	if (!cleanup_registered)
	{
		on_proc_exit(remote_disconnect, 0);
		cleanup_registered = true;
	}
	page_conn = PQconnectStart(page_conninfo);
	if (page_conn == NULL)
		elog(ERROR, "could not allocate page service connection");
	for (;;)
	{
		status = PQconnectPoll(page_conn);
		if (status == PGRES_POLLING_OK)
			break;
		if (status == PGRES_POLLING_FAILED)
			elog(ERROR, "could not connect to page service: %s", PQerrorMessage(page_conn));
		remote_wait(status == PGRES_POLLING_READING ? WL_SOCKET_READABLE : WL_SOCKET_WRITEABLE,
					deadline);
	}
	if (PQsetnonblocking(page_conn, 1) != 0)
		elog(ERROR, "could not set page service connection to nonblocking mode");
}

static bool
remote_fetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 void **buffers, BlockNumber count, BlockNumber *nblocks)
{
	XLogRecPtr	lsn = pg_lsn_in_safe(page_lsn, NULL);
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), request_timeout);
	char		params[9][32];
	const char *values[9];
	bool		exists = false;
	uint64		size;

	Assert(!INTERRUPTS_CAN_BE_PROCESSED());
	Assert(count <= TEST_PAGE_STORE_MAX_BLOCKS);
	test_page_store_check_cut(page_tli, lsn);
	snprintf(params[0], 32, "%u", reln->smgr_rlocator.locator.spcOid);
	snprintf(params[1], 32, "%u", reln->smgr_rlocator.locator.dbOid);
	snprintf(params[2], 32, "%u", reln->smgr_rlocator.locator.relNumber);
	snprintf(params[3], 32, "%d", forknum);
	snprintf(params[4], 32, "%u", blocknum);
	snprintf(params[5], 32, "%u", count);
	snprintf(params[6], 32, UINT64_FORMAT, GetSystemIdentifier());
	snprintf(params[7], 32, "%d", page_tli);
	snprintf(params[8], 32, "%X/%X", LSN_FORMAT_ARGS(lsn));
	for (int i = 0; i < 9; i++)
		values[i] = params[i];

	PG_TRY();
	{
		int			flushed;

		remote_connect(deadline);
		if (!PQsendQueryParams(page_conn,
							   "SELECT fork_exists, nblocks, pages FROM public.test_page_store_fetch("
							   "$1::oid, $2::oid, $3::oid, $4::int, $5::bigint, $6::int, "
							   "$7::text, $8::bigint, $9::pg_lsn)",
							   9, NULL, values, NULL, NULL, 1))
			elog(ERROR, "could not send page service request: %s", PQerrorMessage(page_conn));
		while ((flushed = PQflush(page_conn)) > 0)
			remote_wait(WL_SOCKET_WRITEABLE, deadline);
		if (flushed < 0)
			elog(ERROR, "could not flush page service request: %s", PQerrorMessage(page_conn));
		for (;;)
		{
			PGresult   *result;

			while (PQisBusy(page_conn))
			{
				remote_wait(WL_SOCKET_READABLE, deadline);
				if (!PQconsumeInput(page_conn))
					elog(ERROR, "could not receive page service response: %s", PQerrorMessage(page_conn));
			}
			result = PQgetResult(page_conn);
			if (result == NULL)
				break;
			if (page_result)
			{
				PQclear(result);
				elog(ERROR, "unexpected extra page service response");
			}
			page_result = result;
		}
		if (!page_result || PQresultStatus(page_result) != PGRES_TUPLES_OK)
			elog(ERROR, "page service request failed: %s",
				 page_result ? PQresultErrorMessage(page_result) : PQerrorMessage(page_conn));
		if (PQntuples(page_result) != 1 || PQnfields(page_result) != 3 ||
			PQgetisnull(page_result, 0, 0) || PQgetisnull(page_result, 0, 1) ||
			PQgetisnull(page_result, 0, 2) ||
			PQgetlength(page_result, 0, 0) != 1 ||
			PQgetlength(page_result, 0, 1) != sizeof(uint64) ||
			PQgetlength(page_result, 0, 2) != count * BLCKSZ)
			elog(ERROR, "invalid page service response");
		exists = PQgetvalue(page_result, 0, 0)[0] != 0;
		memcpy(&size, PQgetvalue(page_result, 0, 1), sizeof(size));
		size = pg_ntoh64(size);
		if (size > InvalidBlockNumber || (!exists && size != 0) ||
			(count > 0 && (!exists || (uint64) blocknum + count > size)))
			elog(ERROR, "invalid page service relation size");
		*nblocks = (BlockNumber) size;
		for (BlockNumber i = 0; i < count; i++)
			memcpy(buffers[i], PQgetvalue(page_result, 0, 2) + i * BLCKSZ, BLCKSZ);
		PQclear(page_result);
		page_result = NULL;
		test_page_store_check_cut(page_tli, lsn);
	}
	PG_CATCH();
	{
		remote_disconnect(0, (Datum) 0);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return exists;
}

static bool
remote_exists(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next)
{
	BlockNumber nblocks;

	if (!remote_relation(reln))
		return smgr_exists_next(reln, forknum, next + 1);
	return remote_fetch(reln, forknum, 0, NULL, 0, &nblocks);
}

static BlockNumber
remote_nblocks(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next)
{
	BlockNumber nblocks;

	if (!remote_relation(reln))
		return smgr_nblocks_next(reln, forknum, next + 1);
	if (!remote_fetch(reln, forknum, 0, NULL, 0, &nblocks))
		elog(ERROR, "remote relation fork does not exist");
	return nblocks;
}

static bool
remote_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				int nblocks, SmgrChainIndex next)
{
	if (remote_relation(reln))
		return false;
	return smgr_prefetch_next(reln, forknum, blocknum, nblocks, next + 1);
}

static uint32
remote_maxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				  SmgrChainIndex next)
{
	if (remote_relation(reln))
		return TEST_PAGE_STORE_MAX_BLOCKS;
	return smgr_maxcombine_next(reln, forknum, blocknum, next + 1);
}

static void
remote_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 void **buffers, BlockNumber nblocks, SmgrChainIndex next)
{
	if (!remote_relation(reln))
	{
		smgr_readv_next(reln, forknum, blocknum, buffers, nblocks, next + 1);
		return;
	}
	readv_count++;
	while (nblocks > 0)
	{
		BlockNumber count = Min(nblocks, TEST_PAGE_STORE_MAX_BLOCKS);
		BlockNumber size;

		remote_fetch(reln, forknum, blocknum, buffers, count, &size);
		blocknum += count;
		buffers += count;
		nblocks -= count;
	}
}

static PgAioResult
remote_read_complete(PgAioHandle *ioh, PgAioResult result, uint8 flags)
{
	Assert(result.result == pgaio_io_get_target_data(ioh)->smgr.nblocks * BLCKSZ);
	result.result /= BLCKSZ;
	return result;
}

static void
remote_startreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
				  BlockNumber blocknum, void **buffers, BlockNumber nblocks,
				  SmgrChainIndex next)
{
	struct iovec *iov;
	int			capacity;
	BlockNumber size;

	if (!remote_relation(reln))
	{
		smgr_startreadv_next(ioh, reln, forknum, blocknum, buffers, nblocks, next + 1);
		return;
	}
	startreadv_count++;
	capacity = pgaio_io_get_iovec(ioh, &iov);
	if (nblocks > capacity || nblocks > TEST_PAGE_STORE_MAX_BLOCKS)
		elog(ERROR, "remote read exceeds the batch limit");
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		iov[i].iov_base = buffers[i];
		iov[i].iov_len = BLCKSZ;
	}
	remote_fetch(reln, forknum, blocknum, buffers, nblocks, &size);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	pgaio_io_register_callbacks(ioh, read_callback, 0);
	pgaio_io_complete_readv(ioh, nblocks, nblocks * BLCKSZ);
}

/* Only disposable hint/FSM writes are possible on the frozen read-only node. */
static void
remote_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  const void **buffers, BlockNumber nblocks, bool skipFsync, SmgrChainIndex next)
{
	if (!remote_relation(reln))
		smgr_writev_next(reln, forknum, blocknum, buffers, nblocks, skipFsync, next + 1);
	else
		test_page_store_check_cut(page_tli, pg_lsn_in_safe(page_lsn, NULL));
}

static void
remote_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				 BlockNumber nblocks, SmgrChainIndex next)
{
	if (!remote_relation(reln))
		smgr_writeback_next(reln, forknum, blocknum, nblocks, next + 1);
}

Datum
test_page_store_io_counts(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = Int64GetDatum(readv_count);
	values[1] = Int64GetDatum(startreadv_count);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
test_page_store_read_smgr(PG_FUNCTION_ARGS)
{
	Relation	rel;
	bytea	   *page;
	int32		block = PG_GETARG_INT32(1);

	if (!superuser())
		elog(ERROR, "must be superuser to use page service prototype");
	if (block < 0)
		elog(ERROR, "invalid block number");
	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	page = (bytea *) palloc(VARHDRSZ + BLCKSZ);
	SET_VARSIZE(page, VARHDRSZ + BLCKSZ);
	smgrread(RelationGetSmgr(rel), MAIN_FORKNUM, block, VARDATA(page));
	relation_close(rel, AccessShareLock);
	PG_RETURN_BYTEA_P(page);
}
