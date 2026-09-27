/*-------------------------------------------------------------------------
 *
 * test_wal_durability.c
 *      An additional WAL flush barrier for a single-storage experiment.
 *
 * Either ordinary physical-slot feedback or the early outbound WAL sender
 * acknowledges locally flushed WAL independently of this barrier.  Unlike
 * synchronous commit, the barrier also covers page/SLRU flushes and cannot
 * return success after query cancellation.
 *
 * This is not writer fencing, quorum durability, or a recovery protocol.
 * The configured receiver or inbox must be trusted storage with fsync enabled.
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
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
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
	pg_atomic_uint64 recovery_calls;
	pg_atomic_uint32 waiters;
	pg_atomic_uint32 sender_pid;
	pg_atomic_uint32 sender_proc;
	ConditionVariable changed;
	ConditionVariable work;
} WalDurabilityControl;

static char *durability_slot;
static int	durability_timeout;
static TimeLineID durability_tli;
static bool use_sender;
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
test_page_store_durability_init(TimeLineID tli, bool require_inbox)
{
	DefineCustomStringVariable("test_page_store.wal_durability_slot",
							   "Physical storage slot that must acknowledge WAL flushes.",
							   NULL, &durability_slot, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.wal_flush_timeout",
							"Maximum time to wait for storage WAL durability.",
							NULL, &durability_timeout, 60000, 1, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);
	use_sender = test_page_store_wal_sender_init(tli);
	if (require_inbox && !use_sender)
		elog(ERROR, "recovered writer requires an independent WAL inbox");
	if (use_sender && durability_slot[0] != '\0')
		elog(ERROR, "test WAL durability needs either a slot or an inbox, not both");
	if (!use_sender && durability_slot[0] == '\0')
		return;
	if (!use_sender)
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
	pg_atomic_init_u64(&durability->recovery_calls, 0);
	pg_atomic_init_u32(&durability->waiters, 0);
	pg_atomic_init_u32(&durability->sender_pid, 0);
	pg_atomic_init_u32(&durability->sender_proc, INVALID_PROC_NUMBER);
	ConditionVariableInit(&durability->changed);
	ConditionVariableInit(&durability->work);
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
	if (use_sender || tli != durability_tli || strcmp(slot, durability_slot) != 0)
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
	/* End-of-recovery checkpoints run in checkpointer, not startup. */
	if (RecoveryInProgress())
		pg_atomic_fetch_add_u64(&durability->recovery_calls, 1);
	durability_advance(&durability->requested, record);
	if (record <= pg_atomic_read_u64(&durability->flushed))
		return;
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), durability_timeout);
	pg_atomic_fetch_add_u32(&durability->waiters, 1);
	PG_TRY();
	{
		/* Includes the case where another process already flushed locally. */
		if (use_sender)
			ConditionVariableSignal(&durability->work);
		else
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
	Datum		values[7];
	bool		nulls[7] = {false};
	uint32		proc;
	uint32		wait_event = 0;
	const char *event_name;

	if (!durability)
		elog(ERROR, "test WAL durability is not configured");
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = LSNGetDatum(pg_atomic_read_u64(&durability->flushed));
	values[1] = LSNGetDatum(pg_atomic_read_u64(&durability->requested));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&durability->calls));
	values[3] = Int32GetDatum(pg_atomic_read_u32(&durability->waiters));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&durability->recovery_calls));
	values[5] = Int32GetDatum(pg_atomic_read_u32(&durability->sender_pid));
	proc = pg_atomic_read_u32(&durability->sender_proc);
	if (proc < ProcGlobal->allProcCount &&
		ProcGlobal->allProcs[proc].pid == DatumGetInt32(values[5]))
		wait_event = ProcGlobal->allProcs[proc].wait_event_info;
	event_name = pgstat_get_wait_event(wait_event);
	if (event_name)
		values[6] = CStringGetTextDatum(event_name);
	else
		nulls[6] = true;
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

XLogRecPtr
test_page_store_wal_requested(void)
{
	return pg_atomic_read_u64(&durability->requested);
}

XLogRecPtr
test_page_store_wal_confirmed(void)
{
	return pg_atomic_read_u64(&durability->flushed);
}

void
test_page_store_wal_confirm(TimeLineID tli, XLogRecPtr lsn)
{
	Assert(use_sender);
	if (tli != durability_tli)
		elog(ERROR, "test WAL sender cannot change timeline");
	durability_advance(&durability->flushed, lsn);
	ConditionVariableBroadcast(&durability->changed);
}

void
test_page_store_wal_wait_for_work(void)
{
	ConditionVariablePrepareToSleep(&durability->work);
	if (test_page_store_wal_requested() <= test_page_store_wal_confirmed())
		ConditionVariableTimedSleep(&durability->work, 10,
									WaitEventExtensionNew("TestWalStoreSender"));
	ConditionVariableCancelSleep();
}

void
test_page_store_wal_sender_pid(int pid)
{
	pg_atomic_write_u32(&durability->sender_proc, pid ? MyProcNumber : INVALID_PROC_NUMBER);
	pg_atomic_write_u32(&durability->sender_pid, pid);
}
