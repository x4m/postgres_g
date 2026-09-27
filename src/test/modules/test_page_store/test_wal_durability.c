/*-------------------------------------------------------------------------
 *
 * test_wal_durability.c
 *      An additional WAL flush barrier for a single-storage experiment.
 *
 * The ordinary walsender ships locally flushed WAL independently of this
 * barrier.  Only flush feedback from the configured physical slot and
 * timeline satisfies it.  Unlike synchronous commit, the barrier also covers
 * page/SLRU flushes and cannot return success after query cancellation.
 *
 * This is not writer fencing, quorum durability, or a recovery protocol.
 * The configured slot must belong to the trusted storage with fsync enabled.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/slot.h"
#include "replication/walsender.h"
#include "storage/condition_variable.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "test_page_store.h"

PG_FUNCTION_INFO_V1(test_page_store_wal_status);
PG_FUNCTION_INFO_V1(test_page_store_flush_wal);
PG_FUNCTION_INFO_V1(test_page_store_wal_needs_flush);

typedef struct WalDurabilityControl
{
	pg_atomic_uint64 flushed;
	pg_atomic_uint64 requested;
	pg_atomic_uint64 calls;
	pg_atomic_uint32 waiters;
	ConditionVariable changed;
} WalDurabilityControl;

static char *durability_slot;
static int	durability_timeout;
static TimeLineID durability_tli;
static WalDurabilityControl *durability;
static wal_flush_hook_type previous_flush;
static wal_needs_flush_hook_type previous_needs_flush;
static physical_replication_flush_hook_type previous_feedback;

static void durability_request(void *arg);
static void durability_initialize(void *arg);
static void durability_flush(XLogRecPtr record, TimeLineID tli);
static bool durability_needs_flush(XLogRecPtr record);
static void durability_feedback(const char *slot, TimeLineID tli, XLogRecPtr flush);

static const ShmemCallbacks durability_callbacks = {
	.request_fn = durability_request,
	.init_fn = durability_initialize,
};

void
test_page_store_durability_init(TimeLineID tli)
{
	DefineCustomStringVariable("test_page_store.wal_durability_slot",
							   "Physical storage slot that must acknowledge WAL flushes.",
							   NULL, &durability_slot, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.wal_flush_timeout",
							"Maximum time to wait for storage WAL durability.",
							NULL, &durability_timeout, 60000, 1, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);
	if (durability_slot[0] == '\0')
		return;
	ReplicationSlotValidateName(durability_slot, false, ERROR);
	durability_tli = tli;
	RegisterShmemCallbacks(&durability_callbacks);
	previous_flush = wal_flush_hook;
	wal_flush_hook = durability_flush;
	previous_needs_flush = wal_needs_flush_hook;
	wal_needs_flush_hook = durability_needs_flush;
	previous_feedback = physical_replication_flush_hook;
	physical_replication_flush_hook = durability_feedback;
}

static void
durability_request(void *arg)
{
	ShmemRequestStruct(.name = "test_page_store WAL durability",
					   .size = sizeof(WalDurabilityControl), .ptr = (void **) &durability);
}

static void
durability_initialize(void *arg)
{
	pg_atomic_init_u64(&durability->flushed, InvalidXLogRecPtr);
	pg_atomic_init_u64(&durability->requested, InvalidXLogRecPtr);
	pg_atomic_init_u64(&durability->calls, 0);
	pg_atomic_init_u32(&durability->waiters, 0);
	ConditionVariableInit(&durability->changed);
}

static void
durability_advance(pg_atomic_uint64 *value, XLogRecPtr lsn)
{
	uint64		old = pg_atomic_read_u64(value);

	while (old < lsn && !pg_atomic_compare_exchange_u64(value, &old, lsn))
		;
}

static void
durability_feedback(const char *slot, TimeLineID tli, XLogRecPtr flush)
{
	if (previous_feedback)
		previous_feedback(slot, tli, flush);
	if (tli != durability_tli || strcmp(slot, durability_slot) != 0)
		return;
	durability_advance(&durability->flushed, flush);
	ConditionVariableBroadcast(&durability->changed);
}

/* Called under critical sections and buffer/SLRU locks: no allocation or I/O. */
static bool
durability_needs_flush(XLogRecPtr record)
{
	return (previous_needs_flush && previous_needs_flush(record)) ||
		record > pg_atomic_read_u64(&durability->flushed);
}

static void
durability_flush(XLogRecPtr record, TimeLineID tli)
{
	TimestampTz deadline;

	if (previous_flush)
		previous_flush(record, tli);
	if (tli != durability_tli)
		elog(ERROR, "test WAL durability cannot change timeline");
	pg_atomic_fetch_add_u64(&durability->calls, 1);
	durability_advance(&durability->requested, record);
	if (record <= pg_atomic_read_u64(&durability->flushed))
		return;
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), durability_timeout);
	pg_atomic_fetch_add_u32(&durability->waiters, 1);
	PG_TRY();
	{
		/* Includes the case where another process already flushed locally. */
		WalSndWakeup(true, false);
		/* Broadcast above can cancel a prepared CV wait. */
		ConditionVariablePrepareToSleep(&durability->changed);
		while (record > pg_atomic_read_u64(&durability->flushed))
		{
			long		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);

			if (remaining <= 0)
				elog(ERROR, "timed out waiting for storage WAL flush through %X/%X",
					 LSN_FORMAT_ARGS(record));

			/*
			 * Registering a custom wait event here might allocate in a
			 * critical section.
			 */
			ConditionVariableTimedSleep(&durability->changed, remaining, PG_WAIT_EXTENSION);
		}
	}
	PG_FINALLY();
	{
		ConditionVariableCancelSleep();
		pg_atomic_fetch_sub_u32(&durability->waiters, 1);
	}
	PG_END_TRY();
}

Datum
test_page_store_wal_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false};

	if (!durability)
		elog(ERROR, "test WAL durability is not configured");
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = LSNGetDatum(pg_atomic_read_u64(&durability->flushed));
	values[1] = LSNGetDatum(pg_atomic_read_u64(&durability->requested));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&durability->calls));
	values[3] = Int32GetDatum(pg_atomic_read_u32(&durability->waiters));
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/* Test driver for the local-only fast path and non-transaction flush callers. */
Datum
test_page_store_flush_wal(PG_FUNCTION_ARGS)
{
	XLogRecPtr	record = PG_GETARG_LSN(0);

	if (!superuser() || RecoveryInProgress())
		elog(ERROR, "WAL flush test requires a superuser on a primary");
	if (PG_GETARG_BOOL(1))
		XLogFlushLocal(record);
	else
		XLogFlush(record);
	PG_RETURN_VOID();
}

Datum
test_page_store_wal_needs_flush(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(XLogNeedsFlush(PG_GETARG_LSN(0)));
}
