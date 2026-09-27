/*-------------------------------------------------------------------------
 *
 * test_wal_sender.c
 *      Push locally flushed WAL to the independent test WAL inbox.
 *
 * The flush caller only publishes a request and waits.  This process owns
 * libpq and local WAL reads, starts before recovery finishes, and survives
 * until the shutdown checkpoint no longer needs its services.
 *
 * A pre-existing physical slot retains the complete bounded test epoch.
 * Do not advance it: after a postmaster restart, compare existing inbox
 * bytes with local WAL again before publishing any acknowledgements.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "replication/slot.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/wait_event.h"

#include "test_page_store.h"

PGDLLEXPORT void test_page_store_wal_sender_main(Datum arg);

static char *sender_conninfo;
static char *sender_start_lsn;
static char *sender_slot;
static int	sender_epoch;
static int	sender_timeout;
static TimeLineID sender_tli;
static XLogRecPtr sender_start;
static PGconn *sender_conn;
static PGresult *sender_result;
static char *sender_copy_data;

bool
test_page_store_wal_sender_init(TimeLineID tli)
{
	BackgroundWorker worker = {0};

	DefineCustomStringVariable("test_page_store.wal_store_conninfo",
							   "Connection to the independent WAL inbox.",
							   NULL, &sender_conninfo, "", PGC_POSTMASTER,
							   GUC_SUPERUSER_ONLY, NULL, NULL, NULL);
	DefineCustomStringVariable("test_page_store.wal_store_start_lsn",
							   "First retained segment of the test WAL epoch.",
							   NULL, &sender_start_lsn, "0/0", PGC_POSTMASTER, 0,
							   NULL, NULL, NULL);
	DefineCustomStringVariable("test_page_store.wal_store_slot",
							   "Pre-existing physical slot retaining the test WAL epoch.",
							   NULL, &sender_slot, "", PGC_POSTMASTER, 0,
							   NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.wal_store_epoch",
							"Writer epoch issued by the test controller.",
							NULL, &sender_epoch, 1, 1, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.wal_store_timeout",
							"Maximum time for one WAL inbox exchange.",
							NULL, &sender_timeout, 10000, 1, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);
	if (sender_conninfo[0] == '\0')
		return false;
	sender_start = pg_lsn_in_safe(sender_start_lsn, NULL);
	if (XLogRecPtrIsInvalid(sender_start) || sender_slot[0] == '\0')
		elog(ERROR, "WAL sender requires a retained start LSN and physical slot");
	ReplicationSlotValidateName(sender_slot, false, ERROR);
	sender_tli = tli;
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_SHUTDOWN_AFTER_CHECKPOINT;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 1;
	strlcpy(worker.bgw_name, "test WAL sender", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "test WAL sender", BGW_MAXLEN);
	strlcpy(worker.bgw_library_name, "test_page_store", MAXPGPATH);
	strlcpy(worker.bgw_function_name, "test_page_store_wal_sender_main", BGW_MAXLEN);
	RegisterBackgroundWorker(&worker);
	return true;
}

static void
sender_exit(int code, Datum arg)
{
	test_page_store_wal_sender_pid(0);
	if (sender_copy_data)
		PQfreemem(sender_copy_data);
	if (sender_result)
		PQclear(sender_result);
	if (sender_conn)
		PQfinish(sender_conn);
}

static void
sender_wait(int event, TimestampTz deadline)
{
	long		remaining;

	ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
	remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
	if (remaining <= 0)
		elog(ERROR, "WAL inbox request timed out");
	(void) WaitLatchOrSocket(MyLatch,
							 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT | event,
							 PQsocket(sender_conn), remaining, PG_WAIT_EXTENSION);
}

static void
sender_flush(TimestampTz deadline)
{
	int			ret;

	while ((ret = PQflush(sender_conn)) > 0)
		sender_wait(WL_SOCKET_WRITEABLE, deadline);
	if (ret < 0)
		elog(ERROR, "could not flush WAL inbox request: %s", PQerrorMessage(sender_conn));
}

static PGresult *
sender_get_result(TimestampTz deadline)
{
	for (;;)
	{
		if (!PQconsumeInput(sender_conn))
			elog(ERROR, "could not receive WAL inbox response: %s", PQerrorMessage(sender_conn));
		if (!PQisBusy(sender_conn))
			return PQgetResult(sender_conn);
		sender_wait(WL_SOCKET_READABLE, deadline);
	}
}

static void
sender_response(StringInfo response, TimestampTz deadline)
{
	int			len;

	for (;;)
	{
		if (!PQconsumeInput(sender_conn))
			elog(ERROR, "could not receive WAL inbox response: %s", PQerrorMessage(sender_conn));
		len = PQgetCopyData(sender_conn, &sender_copy_data, 1);
		if (len > 0)
			break;
		if (len < 0)
		{
			sender_result = sender_get_result(deadline);
			elog(ERROR, "WAL inbox session ended: %s",
				 sender_result ? PQresultErrorMessage(sender_result) : PQerrorMessage(sender_conn));
		}
		sender_wait(WL_SOCKET_READABLE, deadline);
	}
	if (len > 49)
		elog(ERROR, "oversized WAL inbox acknowledgement");
	initStringInfo(response);
	appendBinaryStringInfo(response, sender_copy_data, len);
	PQfreemem(sender_copy_data);
	sender_copy_data = NULL;
}

static void
sender_connect(void)
{
	const char *keywords[] = {"dbname", "replication", "application_name", NULL};
	const char *values[] = {sender_conninfo, "true", "test WAL sender", NULL};
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), sender_timeout);
	StringInfoData greeting;

	sender_conn = PQconnectStartParams(keywords, values, 1);
	if (!sender_conn)
		elog(ERROR, "could not allocate WAL inbox connection");
	for (;;)
	{
		PostgresPollingStatusType status = PQconnectPoll(sender_conn);

		if (status == PGRES_POLLING_OK)
			break;
		if (status == PGRES_POLLING_FAILED)
			elog(ERROR, "could not connect to WAL inbox: %s", PQerrorMessage(sender_conn));
		sender_wait(status == PGRES_POLLING_READING ? WL_SOCKET_READABLE : WL_SOCKET_WRITEABLE,
					deadline);
	}
	if (PQsetnonblocking(sender_conn, 1) != 0 ||
		!PQsendQuery(sender_conn, TEST_WAL_SERVICE_COMMAND))
		elog(ERROR, "could not start WAL inbox session: %s", PQerrorMessage(sender_conn));
	sender_flush(deadline);
	sender_result = sender_get_result(deadline);
	if (!sender_result || PQresultStatus(sender_result) != PGRES_COPY_BOTH)
		elog(ERROR, "could not start WAL inbox: %s",
			 sender_result ? PQresultErrorMessage(sender_result) : PQerrorMessage(sender_conn));
	PQclear(sender_result);
	sender_result = NULL;
	sender_response(&greeting, deadline);
	if (pq_getmsgbyte(&greeting) != 'h' ||
		pq_getmsgint(&greeting, 4) != TEST_PAGE_SERVICE_VERSION ||
		pq_getmsgint(&greeting, 4) != PG_VERSION_NUM ||
		pq_getmsgint(&greeting, 4) != BLCKSZ)
		elog(ERROR, "incompatible WAL inbox protocol");
	/* This is the service's own sysid, not the compute stream's identity. */
	if (pq_getmsgint64(&greeting) == 0)
		elog(ERROR, "invalid WAL inbox service identity");
	pq_getmsgend(&greeting);
	pfree(greeting.data);
}

static XLogRecPtr
sender_exchange(char kind, XLogRecPtr lsn, const char *bytes, int size)
{
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), sender_timeout);
	StringInfoData request;
	StringInfoData response;
	XLogRecPtr	flushed;
	int			sent;

	initStringInfo(&request);
	pq_sendbyte(&request, kind);
	pq_sendint64(&request, GetSystemIdentifier());
	pq_sendint32(&request, sender_tli);
	pq_sendint64(&request, sender_epoch);
	pq_sendint64(&request, lsn);
	if (kind == 'i')
		pq_sendint32(&request, wal_segment_size);
	else
		pq_sendbytes(&request, bytes, size);
	while ((sent = PQputCopyData(sender_conn, request.data, request.len)) == 0)
		sender_flush(deadline);
	if (sent < 0)
		elog(ERROR, "could not send WAL inbox request: %s", PQerrorMessage(sender_conn));
	sender_flush(deadline);
	sender_response(&response, deadline);
	if (pq_getmsgbyte(&response) != kind ||
		(uint64) pq_getmsgint64(&response) != GetSystemIdentifier() ||
		pq_getmsgint(&response, 4) != sender_tli ||
		(uint64) pq_getmsgint64(&response) != sender_epoch ||
		(XLogRecPtr) pq_getmsgint64(&response) != lsn ||
		(XLogRecPtr) pq_getmsgint64(&response) != sender_start)
		elog(ERROR, "WAL inbox response identity does not match");
	flushed = pq_getmsgint64(&response);
	if (pq_getmsgint(&response, 4) != wal_segment_size ||
		flushed < lsn + size)
		elog(ERROR, "invalid WAL inbox durable frontier");
	pq_getmsgend(&response);
	pfree(response.data);
	pfree(request.data);
	return flushed;
}

static void
sender_read(XLogRecPtr lsn, char *bytes, int size)
{
	XLogSegNo	segno;
	char		path[MAXPGPATH];
	int			fd;
	pgoff_t		offset = XLogSegmentOffset(lsn, wal_segment_size);

	XLByteToSeg(lsn, segno, wal_segment_size);
	Assert(offset + size <= wal_segment_size);
	XLogFilePath(path, sender_tli, segno, wal_segment_size);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not open WAL for inbox: %m")));
	while (size > 0)
	{
		ssize_t		n = pg_pread(fd, bytes, size, offset);

		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			ereport(ERROR, (errcode_for_file_access(), errmsg("could not read WAL for inbox: %m")));
		if (n == 0)
			elog(ERROR, "incomplete local WAL for inbox");
		bytes += n;
		offset += n;
		size -= n;
		CHECK_FOR_INTERRUPTS();
	}
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR, (errcode_for_file_access(), errmsg("could not close WAL for inbox: %m")));
}

void
test_page_store_wal_sender_main(Datum arg)
{
	XLogRecPtr	position;
	XLogRecPtr	retained;
	XLogRecPtr	remote;
	char	   *bytes;

	before_shmem_exit(sender_exit, 0);
	BackgroundWorkerUnblockSignals();
	SetProcessingMode(NormalProcessing);
	test_page_store_wal_sender_pid(MyProcPid);

	/* A startup flush implies ControlFile and replication slots are ready. */
	while (XLogRecPtrIsInvalid(test_page_store_wal_requested()) && RecoveryInProgress())
		test_page_store_wal_wait_for_work();
	if (sender_start % wal_segment_size != 0 || sender_start > GetRedoRecPtr())
		elog(ERROR, "WAL inbox start must be segment-aligned and precede the redo point");
	ReplicationSlotInitialize();
	ReplicationSlotAcquire(sender_slot, true, false);
	SpinLockAcquire(&MyReplicationSlot->mutex);
	retained = MyReplicationSlot->data.restart_lsn;
	SpinLockRelease(&MyReplicationSlot->mutex);
	if (SlotIsLogical(MyReplicationSlot) || XLogRecPtrIsInvalid(retained) ||
		retained - XLogSegmentOffset(retained, wal_segment_size) > sender_start)
		elog(ERROR, "physical slot does not retain the start of the test WAL epoch");
	sender_connect();
	remote = sender_exchange('i', sender_start, NULL, 0);
	position = Max(sender_start, test_page_store_wal_confirmed());
	if (remote < position)
		elog(ERROR, "WAL inbox lost an acknowledged prefix");
	/* Earlier WAL belongs to the independently retained bootstrap baseline. */
	test_page_store_wal_confirm(sender_tli, position);
	bytes = palloc(TEST_WAL_STORE_MAX_BYTES);
	for (;;)
	{
		XLogRecPtr	target = test_page_store_wal_requested();
		TimeLineID	tli;
		int			size;

		CHECK_FOR_INTERRUPTS();
		/* Also ship asynchronous commits, without using this API in startup. */
		if (!RecoveryInProgress())
		{
			XLogRecPtr	flushed = GetFlushRecPtr(&tli);

			target = Max(target, flushed);
			if (tli != sender_tli)
				elog(ERROR, "test WAL sender cannot change timeline");
		}
		if (position >= target)
		{
			test_page_store_wal_wait_for_work();
			continue;
		}
		size = Min(target - position, TEST_WAL_STORE_MAX_BYTES);
		size = Min(size, wal_segment_size - XLogSegmentOffset(position, wal_segment_size));
		sender_read(position, bytes, size);
		(void) sender_exchange('a', position, bytes, size);
		position += size;
		INJECTION_POINT("test-wal-sender-before-confirm", NULL);
		/* Do not trust an inbox frontier beyond bytes this process compared. */
		test_page_store_wal_confirm(sender_tli, position);
	}
}
