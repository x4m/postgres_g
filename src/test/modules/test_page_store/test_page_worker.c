/*-------------------------------------------------------------------------
 *
 * test_page_worker.c
 *      Bounded transport queue for the read-only compute experiment.
 *
 * The worker owns the connection, never a caller's buffer or I/O handle.
 * Responses are copied out only by the owner of a completed queue slot.
 * Generation checks discard replies to requests whose owners have left.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "test_page_store.h"

PG_FUNCTION_INFO_V1(test_page_store_transport_status);
PGDLLEXPORT void test_page_store_worker_main(Datum arg);

typedef enum PageRequestState
{
	PAGE_REQUEST_FREE,
	PAGE_REQUEST_READY,
	PAGE_REQUEST_RUNNING,
	PAGE_REQUEST_DONE,
} PageRequestState;

typedef struct PageRequestSlot
{
	PageRequestState state;
	uint64		generation;
	ProcNumber	owner;
	int			owner_pid;
	TimestampTz deadline;
	int			request_len;
	int			response_len;
	int			error_code;
	char		error[256];
	char		request[TEST_PAGE_SERVICE_MAX_REQUEST];
	char		response[TEST_PAGE_SERVICE_MAX_RESPONSE];
} PageRequestSlot;

typedef struct PageTransportQueue
{
	LWLock		lock;
	ConditionVariable changed;
	bool		started;
	int			worker_pid;
	ProcNumber	worker_proc;
	uint64		submitted;
	uint64		completed;
	uint64		discarded;
} PageTransportQueue;

bool		test_page_store_transport_worker;
static int	queue_size;
static PageTransportQueue *queue;
static PageRequestSlot *slots;
static int	owned_slot = -1;
static uint64 owned_generation;

static void queue_request(void *arg);
static void queue_initialize(void *arg);
static void queue_release(int code, Datum arg);
static void worker_exit(int code, Datum arg);

static const ShmemCallbacks queue_callbacks = {
	.request_fn = queue_request,
	.init_fn = queue_initialize,
};

void
test_page_store_worker_init(bool physical_service)
{
	BackgroundWorker worker = {0};

	DefineCustomIntVariable("test_page_store.transport_slots",
							"Maximum outstanding requests to a shared transport worker.",
							NULL, &queue_size, 0, 0, 64, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	if (queue_size == 0)
		return;
	if (!physical_service)
		elog(ERROR, "page transport worker requires physical_service");
	RegisterShmemCallbacks(&queue_callbacks);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	/* Startup itself needs page requests before reaching consistency. */
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 1;
	strlcpy(worker.bgw_name, "test page transport", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "test page transport", BGW_MAXLEN);
	strlcpy(worker.bgw_library_name, "test_page_store", MAXPGPATH);
	strlcpy(worker.bgw_function_name, "test_page_store_worker_main", BGW_MAXLEN);
	RegisterBackgroundWorker(&worker);
}

bool
test_page_store_worker_enabled(void)
{
	return queue_size > 0;
}

static void
queue_request(void *arg)
{
	ShmemRequestStruct(.name = "test_page_store transport",
					   .size = sizeof(PageTransportQueue), .ptr = (void **) &queue);
	ShmemRequestStruct(.name = "test_page_store requests",
					   .size = mul_size(queue_size, sizeof(PageRequestSlot)),
					   .ptr = (void **) &slots);
}

static void
queue_initialize(void *arg)
{
	memset(queue, 0, sizeof(*queue));
	memset(slots, 0, queue_size * sizeof(PageRequestSlot));
	LWLockInitialize(&queue->lock, LWLockNewTrancheId("test_page_store transport"));
	ConditionVariableInit(&queue->changed);
	queue->worker_proc = INVALID_PROC_NUMBER;
}

/* Also used on ERROR and ordinary process exit, including startup failure. */
static void
queue_release(int code, Datum arg)
{
	ConditionVariableCancelSleep();
	if (owned_slot < 0)
		return;
	LWLockAcquire(&queue->lock, LW_EXCLUSIVE);
	if (slots[owned_slot].generation == owned_generation &&
		slots[owned_slot].owner == MyProcNumber &&
		slots[owned_slot].owner_pid == MyProcPid)
		slots[owned_slot].state = PAGE_REQUEST_FREE;
	owned_slot = -1;
	LWLockRelease(&queue->lock);
	ConditionVariableBroadcast(&queue->changed);
}

static void
queue_wait(TimestampTz deadline)
{
	long		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);

	if (remaining <= 0)
		elog(ERROR, "page transport request timed out");
	ConditionVariableTimedSleep(&queue->changed, remaining,
								WaitEventExtensionNew("TestPageStoreQueue"));
}

void
test_page_store_worker_exchange(StringInfo request, StringInfo response,
								TimestampTz deadline)
{
	Assert(owned_slot == -1 && request->len <= TEST_PAGE_SERVICE_MAX_REQUEST);
	Assert(!test_page_store_transport_worker);
	/* Nest with the caller's transient cleanup, including a first AIO read. */
	PG_ENSURE_ERROR_CLEANUP(queue_release, (Datum) 0);
	{
		PageRequestSlot *slot;

		ConditionVariablePrepareToSleep(&queue->changed);
		for (;;)
		{
			LWLockAcquire(&queue->lock, LW_EXCLUSIVE);
			if (queue->started && queue->worker_pid == 0)
			{
				LWLockRelease(&queue->lock);
				elog(ERROR, "page transport worker is not running");
			}
			if (queue->submitted == PG_UINT64_MAX)
			{
				LWLockRelease(&queue->lock);
				elog(ERROR, "page transport generation exhausted");
			}
			for (int i = 0; i < queue_size; i++)
			{
				if (slots[i].state != PAGE_REQUEST_FREE)
					continue;
				owned_slot = i;
				owned_generation = ++queue->submitted;
				slot = &slots[i];
				slot->generation = owned_generation;
				slot->owner = MyProcNumber;
				slot->owner_pid = MyProcPid;
				slot->deadline = deadline;
				slot->request_len = request->len;
				memcpy(slot->request, request->data, request->len);
				slot->error_code = 0;
				slot->response_len = 0;
				slot->state = PAGE_REQUEST_READY;
				break;
			}
			LWLockRelease(&queue->lock);
			if (owned_slot >= 0)
				break;
			queue_wait(deadline);
		}
		ConditionVariableBroadcast(&queue->changed);
		slot = &slots[owned_slot];
		for (;;)
		{
			bool		done;

			LWLockAcquire(&queue->lock, LW_SHARED);
			Assert(slot->generation == owned_generation);
			done = slot->state == PAGE_REQUEST_DONE;
			LWLockRelease(&queue->lock);
			if (done)
				break;
			queue_wait(deadline);
		}
		ConditionVariableCancelSleep();
		/* DONE remains owned and immutable until queue_release(). */
		if (slot->error_code)
			ereport(ERROR, (errcode(slot->error_code),
							errmsg("page transport request failed: %s", slot->error)));
		initStringInfo(response);
		appendBinaryStringInfo(response, slot->response, slot->response_len);
		queue_release(0, (Datum) 0);
	}
	PG_END_ENSURE_ERROR_CLEANUP(queue_release, (Datum) 0);
}

/* Do not leave callers waiting for an answer from an exited worker. */
static void
worker_exit(int code, Datum arg)
{
	LWLockAcquire(&queue->lock, LW_EXCLUSIVE);
	queue->worker_pid = 0;
	queue->worker_proc = INVALID_PROC_NUMBER;
	for (int i = 0; i < queue_size; i++)
	{
		PageRequestSlot *slot = &slots[i];

		if (slot->state == PAGE_REQUEST_READY || slot->state == PAGE_REQUEST_RUNNING)
		{
			slot->error_code = ERRCODE_CONNECTION_FAILURE;
			strlcpy(slot->error, "page transport worker exited", sizeof(slot->error));
			slot->state = PAGE_REQUEST_DONE;
		}
	}
	LWLockRelease(&queue->lock);
	ConditionVariableBroadcast(&queue->changed);
}

void
test_page_store_worker_main(Datum arg)
{
	MemoryContext request_context;

	test_page_store_transport_worker = true;
	before_shmem_exit(worker_exit, 0);
	BackgroundWorkerUnblockSignals();
	SetProcessingMode(NormalProcessing);
	request_context = AllocSetContextCreate(TopMemoryContext, "Page transport request",
											ALLOCSET_DEFAULT_SIZES);
	LWLockAcquire(&queue->lock, LW_EXCLUSIVE);
	queue->started = true;
	queue->worker_pid = MyProcPid;
	queue->worker_proc = MyProcNumber;
	LWLockRelease(&queue->lock);
	ConditionVariableBroadcast(&queue->changed);

	for (;;)
	{
		int			index = -1;
		uint64		generation = PG_UINT64_MAX;
		TimestampTz deadline;
		char		request_data[TEST_PAGE_SERVICE_MAX_REQUEST];
		StringInfoData request = {0};
		StringInfoData response = {0};
		volatile int error_code = 0;
		char		error[256];
		PageRequestSlot *slot;

		CHECK_FOR_INTERRUPTS();
		MemoryContextReset(request_context);
		MemoryContextSwitchTo(request_context);
		ConditionVariablePrepareToSleep(&queue->changed);
		LWLockAcquire(&queue->lock, LW_EXCLUSIVE);
		/* Oldest admitted request first; slot numbers do not imply age. */
		for (int i = 0; i < queue_size; i++)
			if (slots[i].state == PAGE_REQUEST_READY &&
				(index < 0 || slots[i].generation < generation))
			{
				index = i;
				generation = slots[i].generation;
			}
		if (index < 0)
		{
			LWLockRelease(&queue->lock);
			ConditionVariableSleep(&queue->changed,
								   WaitEventExtensionNew("TestPageStoreWorker"));
			continue;
		}
		slot = &slots[index];
		slot->state = PAGE_REQUEST_RUNNING;
		deadline = slot->deadline;
		request.data = request_data;
		request.len = slot->request_len;
		request.maxlen = sizeof(request_data);
		memcpy(request_data, slot->request, request.len);
		LWLockRelease(&queue->lock);
		ConditionVariableCancelSleep();
		PG_TRY();
		{
			if (GetCurrentTimestamp() >= deadline)
				elog(ERROR, "page transport request expired in the queue");
			test_page_store_client_exchange(&request, &response, deadline);
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(request_context);
			edata = CopyErrorData();
			error_code = edata->sqlerrcode;
			strlcpy(error, edata->message, sizeof(error));
			FreeErrorData(edata);
			FlushErrorState();
		}
		PG_END_TRY();

		INJECTION_POINT("test-page-store-worker-before-publish", NULL);
		LWLockAcquire(&queue->lock, LW_EXCLUSIVE);
		/* The owner may have timed out and reused this slot while we read. */
		if (slot->state == PAGE_REQUEST_RUNNING && slot->generation == generation)
		{
			slot->error_code = error_code;
			if (error_code)
				strlcpy(slot->error, error, sizeof(slot->error));
			else
			{
				slot->response_len = response.len;
				memcpy(slot->response, response.data, response.len);
			}
			slot->state = PAGE_REQUEST_DONE;
			queue->completed++;
		}
		else
			queue->discarded++;
		LWLockRelease(&queue->lock);
		ConditionVariableBroadcast(&queue->changed);
	}
}

Datum
test_page_store_transport_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[9];
	bool		nulls[9] = {false};
	int			counts[4] = {0};
	uint32		wait_event;
	const char *event_name;

	if (!queue)
		elog(ERROR, "page transport worker is not configured");
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	LWLockAcquire(&queue->lock, LW_SHARED);
	values[0] = Int32GetDatum(queue->worker_pid);
	values[1] = Int32GetDatum(queue_size);
	for (int i = 0; i < queue_size; i++)
		counts[slots[i].state]++;
	values[2] = Int32GetDatum(counts[PAGE_REQUEST_READY]);
	values[3] = Int32GetDatum(counts[PAGE_REQUEST_RUNNING]);
	values[4] = Int32GetDatum(counts[PAGE_REQUEST_DONE]);
	values[5] = Int64GetDatum(queue->submitted);
	values[6] = Int64GetDatum(queue->completed);
	values[7] = Int64GetDatum(queue->discarded);
	wait_event = queue->worker_pid ?
		ProcGlobal->allProcs[queue->worker_proc].wait_event_info : 0;
	LWLockRelease(&queue->lock);
	event_name = pgstat_get_wait_event(wait_event);
	if (event_name)
		values[8] = CStringGetTextDatum(event_name);
	else
		nulls[8] = true;
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
