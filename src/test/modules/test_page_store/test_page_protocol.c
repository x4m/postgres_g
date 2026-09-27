/*-------------------------------------------------------------------------
 *
 * test_page_protocol.c
 *      A test-only physical session for retained page reads.
 *
 * The ordinary postmaster supplies authentication and connection handling.
 * This command needs neither a database connection nor SQL objects.  It does
 * not send WAL or participate in synchronous replication.  The wire format
 * is private to this prototype, not a proposed stable replication protocol.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/protocol.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/walsender.h"
#include "storage/ipc.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"

#include "test_page_store.h"

static physical_replication_command_hook_type previous_command_hook;

static bool page_service_command(const char *command);

void
test_page_store_protocol_init(void)
{
	previous_command_hook = physical_replication_command_hook;
	physical_replication_command_hook = page_service_command;
}

/* All integers use network byte order; responses echo the complete request. */
static void
page_service_request(StringInfo request, StringInfo response)
{
	int			kind = pq_getmsgbyte(request);
	uint64		id = pq_getmsgint64(request);
	TimeLineID	tli = pq_getmsgint(request, 4);
	XLogRecPtr	lsn = pq_getmsgint64(request);

	if (id == 0 || tli == 0 || XLogRecPtrIsInvalid(lsn))
		elog(ERROR, "invalid page service request identity");

	pq_beginmessage(response, PqMsg_CopyData);
	pq_sendbytes(response, request->data, request->len);
	if (kind == 'b')
	{
		pq_getmsgend(request);
		pq_sendint64(response, test_page_store_history_predecessor(tli, lsn));
	}
	else if (kind == 'p')
	{
		RelFileLocator locator;
		ForkNumber	forknum;
		BlockNumber block;
		uint32		count;
		int			wait;
		bool		exists;
		BlockNumber nblocks;
		bytea	   *pages;

		locator.spcOid = pq_getmsgint(request, 4);
		locator.dbOid = pq_getmsgint(request, 4);
		locator.relNumber = pq_getmsgint(request, 4);
		forknum = pq_getmsgbyte(request);
		block = pq_getmsgint(request, 4);
		count = pq_getmsgint(request, 4);
		wait = pq_getmsgbyte(request);
		pq_getmsgend(request);
		if (!OidIsValid(locator.spcOid) ||
			!RelFileNumberIsValid(locator.relNumber) ||
			forknum > MAX_FORKNUM || block == InvalidBlockNumber ||
			count > TEST_PAGE_STORE_MAX_BLOCKS ||
			(uint64) block + count > InvalidBlockNumber || wait > 1)
			elog(ERROR, "invalid physical page request");
		pages = test_page_store_history_fetch(locator, forknum, block, count,
											  tli, lsn, wait, &exists, &nblocks);
		pq_sendbyte(response, exists);
		pq_sendint32(response, nblocks);
		pq_sendbytes(response, VARDATA(pages), VARSIZE(pages) - VARHDRSZ);
	}
	else
		elog(ERROR, "unknown page service request type: %d", kind);
	pq_endmessage(response);
}

static bool
page_service_command(const char *command)
{
	StringInfoData message;
	MemoryContext request_context;
	MemoryContext command_context = CurrentMemoryContext;

	if (strcmp(command, TEST_PAGE_SERVICE_COMMAND) != 0)
		return previous_command_hook ? previous_command_hook(command) : false;
	Assert(am_walsender && !am_db_walsender && !OidIsValid(MyDatabaseId));

	/* The prototype is deliberately narrower than the REPLICATION privilege. */
	StartTransactionCommand();
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to use page service prototype")));
	CommitTransactionCommand();
	if (!test_page_store_history_enabled())
		elog(ERROR, "physical page service requires retained history");
	debug_query_string = command;
	pgstat_report_activity(STATE_RUNNING, command);
	set_ps_display(TEST_PAGE_SERVICE_COMMAND);
	ereport(log_replication_commands ? LOG : DEBUG1,
			(errmsg("received replication command: %s", command)));

	pq_beginmessage(&message, PqMsg_CopyBothResponse);
	pq_sendbyte(&message, 0);
	pq_sendint16(&message, 0);
	pq_endmessage(&message);
	pq_beginmessage(&message, PqMsg_CopyData);
	pq_sendbyte(&message, 'h');
	pq_sendint32(&message, TEST_PAGE_SERVICE_VERSION);
	pq_sendint32(&message, PG_VERSION_NUM);
	pq_sendint32(&message, BLCKSZ);
	pq_sendint64(&message, GetSystemIdentifier());
	pq_endmessage(&message);
	if (pq_flush() != 0)
		proc_exit(0);

	request_context = AllocSetContextCreate(command_context,
											"Page service request",
											ALLOCSET_DEFAULT_SIZES);
	for (;;)
	{
		StringInfoData request;
		int			kind;

		CHECK_FOR_INTERRUPTS();
		MemoryContextReset(request_context);
		MemoryContextSwitchTo(request_context);
		initStringInfo(&request);
		pq_startmsgread();
		kind = pq_getbyte();
		if (kind == EOF)
			proc_exit(0);
		if (kind != PqMsg_CopyData && kind != PqMsg_CopyDone &&
			kind != PqMsg_Terminate)
			ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("unexpected message type in page service")));
		if (pq_getmessage(&request, TEST_PAGE_SERVICE_MAX_REQUEST + 4))
			proc_exit(0);
		if (kind != PqMsg_CopyData)
		{
			pq_getmsgend(&request);
			if (kind == PqMsg_Terminate)
				proc_exit(0);
			break;
		}
		page_service_request(&request, &message);
		if (pq_flush() != 0)
			proc_exit(0);
	}
	MemoryContextSwitchTo(command_context);
	MemoryContextDelete(request_context);
	pq_putmessage(PqMsg_CopyDone, NULL, 0);
	EndReplicationCommand(TEST_PAGE_SERVICE_COMMAND);
	return true;
}
