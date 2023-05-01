/*--------------------------------------------------------------------------
 *
 * autonomous.c
 *		Run SQL commands using a background worker.
 *
 * Copyright (C) 2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/tcop/autonomous.c
 *
 *
 * This implements a C API to open an autonomous session and run SQL queries
 * in it.  The session looks much like a normal database connection, but it is
 * always to the same database, and there is no authentication needed.  The
 * "backend" for that connection is a background worker.  The normal backend
 * and the autonomous session worker communicate over the normal FE/BE
 * protocol.
 *
 * Types:
 *
 * AutonomousSession -- opaque connection handle
 * AutonomousPreparedStatement -- opaque prepared statement handle
 * AutonomousResult -- query result
 *
 * Functions:
 *
 * AutonomousSessionStart() -- start a session (launches background worker)
 * and return a handle
 *
 * AutonomousSessionEnd() -- close session and free resources
 *
 * AutonomousSessionExecute() -- run SQL string and return result (rows or
 * status)
 *
 * AutonomousSessionPrepare() -- prepare an SQL string for subsequent
 * execution
 *
 * AutonomousSessionExecutePrepared() -- run prepared statement
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "commands/async.h"
#include "commands/variable.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "tcop/autonomous.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/resowner.h"

/* Table-of-contents constants for our dynamic shared memory segment. */
#define AUTONOMOUS_MAGIC				0x50674267

#define AUTONOMOUS_KEY_FIXED_DATA		0
#define AUTONOMOUS_KEY_GUC				1
#define AUTONOMOUS_KEY_COMMAND_QUEUE	2
#define AUTONOMOUS_KEY_RESPONSE_QUEUE	3
#define AUTONOMOUS_NKEYS				4

#define AUTONOMOUS_QUEUE_SIZE			16384

/* Fixed-size data passed via our dynamic shared memory segment. */
typedef struct autonomous_session_fixed_data
{
	Oid database_id;
	Oid authenticated_user_id;
	Oid current_user_id;
	int sec_context;
} autonomous_session_fixed_data;

struct AutonomousSession
{
	dsm_segment *seg;
	BackgroundWorkerHandle *worker_handle;
	shm_mq_handle *command_qh;
	shm_mq_handle *response_qh;
	int		transaction_status;
};

struct AutonomousPreparedStatement
{
	AutonomousSession *session;
	Oid		   *argtypes;
	TupleDesc	tupdesc;
};

static void shm_mq_receive_stringinfo(shm_mq_handle *qh, StringInfoData *msg);
static void autonomous_check_client_encoding_hook(void);
static TupleDesc TupleDesc_from_RowDescription(StringInfo msg);
static HeapTuple HeapTuple_from_DataRow(TupleDesc tupdesc, StringInfo msg);
static void forward_NotifyResponse(StringInfo msg);
static void rethrow_errornotice(StringInfo msg, int min_elevel);
static void invalid_protocol_message(char msgtype, char phase) pg_attribute_noreturn();


AutonomousSession *
AutonomousSessionStart(void)
{
	BackgroundWorker worker;
	pid_t		pid;
	AutonomousSession *session;
	shm_toc_estimator e;
	Size		segsize;
	Size		guc_len;
	char	   *gucstate;
	dsm_segment *seg;
	shm_toc	   *toc;
	autonomous_session_fixed_data *fdata;
	shm_mq	   *command_mq;
	shm_mq	   *response_mq;
	BgwHandleStatus bgwstatus;
	StringInfoData msg;
	char		msgtype;

	session = palloc(sizeof(*session));

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(autonomous_session_fixed_data));
	shm_toc_estimate_chunk(&e, AUTONOMOUS_QUEUE_SIZE);
	shm_toc_estimate_chunk(&e, AUTONOMOUS_QUEUE_SIZE);
	guc_len = EstimateGUCStateSpace();
	shm_toc_estimate_chunk(&e, guc_len);
	shm_toc_estimate_keys(&e, AUTONOMOUS_NKEYS);
	segsize = shm_toc_estimate(&e);
	seg = dsm_create(segsize, 0);

	session->seg = seg;

	toc = shm_toc_create(AUTONOMOUS_MAGIC, dsm_segment_address(seg), segsize);

	/* Store fixed-size data in dynamic shared memory. */
	fdata = shm_toc_allocate(toc, sizeof(*fdata));
	fdata->database_id = MyDatabaseId;
	fdata->authenticated_user_id = GetAuthenticatedUserId();
	GetUserIdAndSecContext(&fdata->current_user_id, &fdata->sec_context);
	shm_toc_insert(toc, AUTONOMOUS_KEY_FIXED_DATA, fdata);

	/* Store GUC state in dynamic shared memory. */
	gucstate = shm_toc_allocate(toc, guc_len);
	SerializeGUCState(guc_len, gucstate);
	shm_toc_insert(toc, AUTONOMOUS_KEY_GUC, gucstate);

	command_mq = shm_mq_create(shm_toc_allocate(toc, AUTONOMOUS_QUEUE_SIZE),
							   AUTONOMOUS_QUEUE_SIZE);
	shm_toc_insert(toc, AUTONOMOUS_KEY_COMMAND_QUEUE, command_mq);
	shm_mq_set_sender(command_mq, MyProc);

	response_mq = shm_mq_create(shm_toc_allocate(toc, AUTONOMOUS_QUEUE_SIZE),
								AUTONOMOUS_QUEUE_SIZE);
	shm_toc_insert(toc, AUTONOMOUS_KEY_RESPONSE_QUEUE, response_mq);
	shm_mq_set_receiver(response_mq, MyProc);

	session->command_qh = shm_mq_attach(command_mq, seg, NULL);
	session->response_qh = shm_mq_attach(response_mq, seg, NULL);

	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "autonomous_worker_main");
	sprintf(worker.bgw_type, "autonomous_transaction");
	snprintf(worker.bgw_name, BGW_MAXLEN, "autonomous session by PID %d", MyProcPid);
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &session->worker_handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You might need to increase max_worker_processes.")));

	shm_mq_set_handle(session->command_qh, session->worker_handle);
	shm_mq_set_handle(session->response_qh, session->worker_handle);

	bgwstatus = WaitForBackgroundWorkerStartup(session->worker_handle, &pid);
	if (bgwstatus != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background worker")));

	do
	{
		shm_mq_receive_stringinfo(session->response_qh, &msg);
		msgtype = pq_getmsgbyte(&msg);

		switch (msgtype)
		{
			case 'E':
				rethrow_errornotice(&msg, ERROR);
				break;
			case 'N':
				rethrow_errornotice(&msg, NOTICE);
				break;
			case 'Z':
				session->transaction_status = pq_getmsgbyte(&msg);
				pq_getmsgend(&msg);
				break;
			default:
				invalid_protocol_message(msgtype, 1);
				break;
		}
	}
	while (msgtype != 'Z');

	return session;
}


void
AutonomousSessionEnd(AutonomousSession *session)
{
	StringInfoData msg;

	if (session->transaction_status == 'T')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("autonomous session ended with transaction block open")));

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_beginmessage(&msg, 'X');
	pq_endmessage(&msg);
	pq_stop_redirect_to_shm_mq();

	pfree(session->worker_handle);
	dsm_detach(session->seg);
	pfree(session);
}


AutonomousResult *
AutonomousSessionExecute(AutonomousSession *session, const char *sql)
{
	StringInfoData msg;
	char		msgtype;
	AutonomousResult *result;

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_beginmessage(&msg, 'Q');
	pq_sendstring(&msg, sql);
	pq_endmessage(&msg);
	pq_stop_redirect_to_shm_mq();

	result = palloc0(sizeof(*result));

	do
	{
		shm_mq_receive_stringinfo(session->response_qh, &msg);
		msgtype = pq_getmsgbyte(&msg);

		switch (msgtype)
		{
			case 'A':
				forward_NotifyResponse(&msg);
				break;
			case 'C':
				{
					const char *tag = pq_getmsgstring(&msg);
					result->command = pstrdup(tag);
					pq_getmsgend(&msg);
					break;
				}
			case 'D':
				if (!result->tupdesc)
					elog(ERROR, "no T before D");
				result->tuples = lappend(result->tuples, HeapTuple_from_DataRow(result->tupdesc, &msg));
				pq_getmsgend(&msg);
				break;
			case 'E':
				rethrow_errornotice(&msg, ERROR);
				break;
			case 'N':
				rethrow_errornotice(&msg, NOTICE);
				break;
			case 'T':
				if (result->tupdesc)
					elog(ERROR, "already received a T message");
				result->tupdesc = TupleDesc_from_RowDescription(&msg);
				pq_getmsgend(&msg);
				break;
			case 'Z':
				session->transaction_status = pq_getmsgbyte(&msg);
				pq_getmsgend(&msg);
				break;
			default:
				invalid_protocol_message(msgtype, 2);
				break;
		}
	}
	while (msgtype != 'Z');

	return result;
}


AutonomousPreparedStatement *
AutonomousSessionPrepare(AutonomousSession *session, const char *sql, int16 nargs,
						 Oid argtypes[], const char *argnames[])
{
	AutonomousPreparedStatement *result;
	StringInfoData msg;
	int16		i;
	char		msgtype;

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_beginmessage(&msg, 'P');
	pq_sendstring(&msg, "");
	pq_sendstring(&msg, sql);
	pq_sendint(&msg, nargs, 2);
	for (i = 0; i < nargs; i++)
		pq_sendint(&msg, argtypes[i], 4);
	if (argnames)
		for (i = 0; i < nargs; i++)
			pq_sendstring(&msg, argnames[i]);
	pq_endmessage(&msg);
	pq_stop_redirect_to_shm_mq();

	result = palloc0(sizeof(*result));
	result->session = session;
	result->argtypes = palloc(nargs * sizeof(*result->argtypes));
	memcpy(result->argtypes, argtypes, nargs * sizeof(*result->argtypes));

	shm_mq_receive_stringinfo(session->response_qh, &msg);
	msgtype = pq_getmsgbyte(&msg);

	switch (msgtype)
	{
		case '1':
			break;
		case 'E':
			rethrow_errornotice(&msg, ERROR);
			break;
		case 'N':
			rethrow_errornotice(&msg, NOTICE);
			break;
		default:
			invalid_protocol_message(msgtype, 3);
			break;
	}

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_beginmessage(&msg, 'D');
	pq_sendbyte(&msg, 'S');
	pq_sendstring(&msg, "");
	pq_endmessage(&msg);
	pq_stop_redirect_to_shm_mq();

	do
	{
		shm_mq_receive_stringinfo(session->response_qh, &msg);
		msgtype = pq_getmsgbyte(&msg);

		switch (msgtype)
		{
			case 'A':
				forward_NotifyResponse(&msg);
				break;
			case 'E':
				rethrow_errornotice(&msg, ERROR);
				break;
			case 'N':
				rethrow_errornotice(&msg, NOTICE);
				break;
			case 'n':
				break;
			case 't':
			case '1':
			case 'Z':
				/* ignore for now */
				break;
			case 'T':
				if (result->tupdesc)
					elog(ERROR, "already received a T message");
				result->tupdesc = TupleDesc_from_RowDescription(&msg);
				pq_getmsgend(&msg);
				break;
			default:
				invalid_protocol_message(msgtype, 4);
				break;
		}
	}
	while (msgtype != 'n' && msgtype != 'T');

	return result;
}


AutonomousResult *
AutonomousSessionExecutePrepared(AutonomousPreparedStatement *stmt, int16 nargs, Datum *values, bool *nulls)
{
	AutonomousSession *session;
	StringInfoData msg;
	AutonomousResult *result;
	char		msgtype;
	int16		i;

	session = stmt->session;

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_beginmessage(&msg, 'B');
	pq_sendstring(&msg, "");
	pq_sendstring(&msg, "");
	pq_sendint(&msg, 1, 2);  /* number of parameter format codes */
	pq_sendint(&msg, 1, 2);
	pq_sendint(&msg, nargs, 2);  /* number of parameter values */
	for (i = 0; i < nargs; i++)
	{
		if (nulls[i])
			pq_sendint(&msg, -1, 4);
		else
		{
			Oid			typsend;
			bool		typisvarlena;
			bytea	   *outputbytes;

			getTypeBinaryOutputInfo(stmt->argtypes[i], &typsend, &typisvarlena);
			outputbytes = OidSendFunctionCall(typsend, values[i]);
			pq_sendint(&msg, VARSIZE(outputbytes) - VARHDRSZ, 4);
			pq_sendbytes(&msg, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
			pfree(outputbytes);
		}
	}
	pq_sendint(&msg, 1, 2);  /* number of result column format codes */
	pq_sendint(&msg, 1, 2);
	pq_endmessage(&msg);
	pq_stop_redirect_to_shm_mq();

	shm_mq_receive_stringinfo(session->response_qh, &msg);
	msgtype = pq_getmsgbyte(&msg);

	switch (msgtype)
	{
		case '2':
			break;
		case 'E':
			rethrow_errornotice(&msg, ERROR);
			break;
		case 'N':
			rethrow_errornotice(&msg, NOTICE);
			break;
		default:
			invalid_protocol_message(msgtype, 5);
			break;
	}

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_beginmessage(&msg, 'E');
	pq_sendstring(&msg, "");
	pq_sendint(&msg, 0, 4);
	pq_endmessage(&msg);
	pq_stop_redirect_to_shm_mq();

	result = palloc0(sizeof(*result));
	result->tupdesc = stmt->tupdesc;

	do
	{
		shm_mq_receive_stringinfo(session->response_qh, &msg);
		msgtype = pq_getmsgbyte(&msg);

		switch (msgtype)
		{
			case 'A':
				forward_NotifyResponse(&msg);
				break;
			case '2':
				break;
			case 'C':
				{
					const char *tag = pq_getmsgstring(&msg);
					result->command = pstrdup(tag);
					pq_getmsgend(&msg);
					break;
				}
			case 'D':
				if (!stmt->tupdesc)
					elog(ERROR, "did not expect any rows");
				result->tuples = lappend(result->tuples, HeapTuple_from_DataRow(stmt->tupdesc, &msg));
				pq_getmsgend(&msg);
				break;
			case 'E':
				rethrow_errornotice(&msg, ERROR);
				break;
			case 'N':
				rethrow_errornotice(&msg, NOTICE);
				break;
			default:
				invalid_protocol_message(msgtype, 6);
				break;
		}
	}
	while (msgtype != 'C');

	pq_redirect_to_shm_mq(session->seg, session->command_qh);
	pq_putemptymessage('S');
	pq_stop_redirect_to_shm_mq();

	shm_mq_receive_stringinfo(session->response_qh, &msg);
	msgtype = pq_getmsgbyte(&msg);

	switch (msgtype)
	{
		case 'A':
			forward_NotifyResponse(&msg);
			break;
		case 'Z':
			session->transaction_status = pq_getmsgbyte(&msg);
			pq_getmsgend(&msg);
			break;
		case 'E':
			rethrow_errornotice(&msg, ERROR);
			break;
		case 'N':
			rethrow_errornotice(&msg, NOTICE);
			break;
		default:
			invalid_protocol_message(msgtype, 7);
			break;
	}

	return result;
}

extern void init_row_description_buf();

void
autonomous_worker_main(Datum main_arg)
{
	dsm_segment *seg;
	shm_toc	   *toc;
	autonomous_session_fixed_data *fdata;
	char	   *gucstate;
	shm_mq	   *command_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *command_qh;
	shm_mq_handle *response_qh;
	StringInfoData msg;
	char		msgtype;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	//elog(WARNING,"1");

	/* Set up a memory context and resource owner. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "autonomous");
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
												 "autonomous session",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	init_row_description_buf();

	seg = dsm_attach(DatumGetInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));

	toc = shm_toc_attach(AUTONOMOUS_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));
	//elog(WARNING,"2");

	/* Find data structures in dynamic shared memory. */
	fdata = shm_toc_lookup(toc, AUTONOMOUS_KEY_FIXED_DATA, false);

	gucstate = shm_toc_lookup(toc, AUTONOMOUS_KEY_GUC, false);

	command_mq = shm_toc_lookup(toc, AUTONOMOUS_KEY_COMMAND_QUEUE, false);
	shm_mq_set_receiver(command_mq, MyProc);
	command_qh = shm_mq_attach(command_mq, seg, NULL);

	response_mq = shm_toc_lookup(toc, AUTONOMOUS_KEY_RESPONSE_QUEUE, false);
	shm_mq_set_sender(response_mq, MyProc);
	response_qh = shm_mq_attach(response_mq, seg, NULL);

	pq_redirect_to_shm_mq(seg, response_qh);
	BackgroundWorkerInitializeConnectionByOid(fdata->database_id,
											  fdata->authenticated_user_id, 0);
	//elog(WARNING,"3");

	SetClientEncoding(GetDatabaseEncoding());

	StartTransactionCommand();
	RestoreGUCState(gucstate);
	CommitTransactionCommand();

	process_session_preload_libraries();

	SetUserIdAndSecContext(fdata->current_user_id, fdata->sec_context);

	whereToSendOutput = DestRemote;
	ReadyForQuery(whereToSendOutput);

	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);
	//elog(WARNING,"4");

	do
	{
		MemoryContextSwitchTo(MessageContext);
		MemoryContextResetAndDeleteChildren(MessageContext);
		InvalidateCatalogSnapshotConditionally();

		ProcessCompletedNotifies();
		pgstat_report_stat(false);
		pgstat_report_activity(STATE_IDLE, NULL);

		//elog(WARNING,"5");
		shm_mq_receive_stringinfo(command_qh, &msg);
		//elog(WARNING,"5a");
		msgtype = pq_getmsgbyte(&msg);
		//elog(WARNING,"5b");

		switch (msgtype)
		{
			case 'B':
				{
					//elog(WARNING,"5c");
					SetCurrentStatementStartTimestamp();
					exec_bind_message(&msg);
					break;
				}
			case 'D':
				{
					int         describe_type;
					const char *describe_target;
					//elog(WARNING,"5d");

					SetCurrentStatementStartTimestamp();

					describe_type = pq_getmsgbyte(&msg);
					describe_target = pq_getmsgstring(&msg);
					pq_getmsgend(&msg);

					switch (describe_type)
					{
						case 'S':
						//elog(WARNING,"5e %c %s", (char)describe_type, describe_target);
							exec_describe_statement_message(describe_target);
							break;
#ifdef TODO
						case 'P':
						//elog(WARNING,"5f");
							exec_describe_portal_message(describe_target);
							break;
#endif
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("invalid DESCRIBE message subtype %d",
											describe_type)));
							break;
					}
				}
				break;
			case 'E':
				{
					const char *portal_name;
					int			max_rows;
					//elog(WARNING,"5g");

					SetCurrentStatementStartTimestamp();

					portal_name = pq_getmsgstring(&msg);
					max_rows = pq_getmsgint(&msg, 4);
					pq_getmsgend(&msg);

					exec_execute_message(portal_name, max_rows);
				}
				break;

			case 'P':
				{
					const char *stmt_name;
					const char *query_string;
					int			numParams;
					Oid		   *paramTypes = NULL;
					const char **paramNames = NULL;
					//elog(WARNING,"5h");

					SetCurrentStatementStartTimestamp();

					stmt_name = pq_getmsgstring(&msg);
					query_string = pq_getmsgstring(&msg);
					numParams = pq_getmsgint(&msg, 2);
					if (numParams > 0)
					{
						int			i;

						paramTypes = palloc(numParams * sizeof(Oid));
						for (i = 0; i < numParams; i++)
							paramTypes[i] = pq_getmsgint(&msg, 4);
					}
					/* If data left in message, read parameter names. */
					if (msg.cursor != msg.len)
					{
						int			i;

						paramNames = palloc(numParams * sizeof(char *));
						for (i = 0; i < numParams; i++)
							paramNames[i] = pq_getmsgstring(&msg);
					}
					pq_getmsgend(&msg);

					exec_parse_message(query_string, stmt_name,
									   paramTypes, numParams, paramNames);
					break;
				}
			case 'Q':
				{
					//elog(WARNING,"5h1");
					const char *sql;
					int save_log_statement;
					bool save_log_duration;
					int save_log_min_duration_statement;

					sql = pq_getmsgstring(&msg);
					pq_getmsgend(&msg);

					/* XXX room for improvement */
					save_log_statement = log_statement;
					save_log_duration = log_duration;
					save_log_min_duration_statement = log_min_duration_statement;

					check_client_encoding_hook = autonomous_check_client_encoding_hook;
					log_statement = LOGSTMT_NONE;
					log_duration = false;
					log_min_duration_statement = -1;

					SetCurrentStatementStartTimestamp();
					//elog(WARNING,"5h1a %s", sql);
					exec_simple_query(sql, 1);
					//elog(WARNING,"5h1b");

					log_statement = save_log_statement;
					log_duration = save_log_duration;
					log_min_duration_statement = save_log_min_duration_statement;
					check_client_encoding_hook = NULL;

					ReadyForQuery(whereToSendOutput);
					break;
				}
			case 'S':
				{
					//elog(WARNING,"5i");
					pq_getmsgend(&msg);
					finish_xact_command();
					ReadyForQuery(whereToSendOutput);
					break;
				}
			case 'X':
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid protocol message type from autonomous session leader: %c",
								msgtype)));
				break;
		}
	}
	while (msgtype != 'X');
}


static void
shm_mq_receive_stringinfo(shm_mq_handle *qh, StringInfoData *msg)
{
	shm_mq_result res;
	Size		nbytes;
	void	   *data;

	res = shm_mq_receive(qh, &nbytes, &data, false);
	if (res != SHM_MQ_SUCCESS)
		elog(ERROR, "shm_mq_receive failed: %d", res);

	initStringInfo(msg);
	appendBinaryStringInfo(msg, data, nbytes);
}


static void
autonomous_check_client_encoding_hook(void)
{
	elog(ERROR, "cannot set client encoding in autonomous session");
}


static TupleDesc
TupleDesc_from_RowDescription(StringInfo msg)
{
	TupleDesc	tupdesc;
	int16		natts = pq_getmsgint(msg, 2);
	int16		i;

	tupdesc = CreateTemplateTupleDesc(natts, false);
	for (i = 0; i < natts; i++)
	{
		const char *colname;
		Oid     type_oid;
		int32	typmod;
		int16	format;

		colname = pq_getmsgstring(msg);
		(void) pq_getmsgint(msg, 4);   /* table OID */
		(void) pq_getmsgint(msg, 2);   /* table attnum */
		type_oid = pq_getmsgint(msg, 4);
		(void) pq_getmsgint(msg, 2);   /* type length */
		typmod = pq_getmsgint(msg, 4);
		format = pq_getmsgint(msg, 2);
		(void) format;
#ifdef TODO
		/* XXX The protocol sometimes sends 0 (text) if the format is not
		 * determined yet.  We always use binary, so this check is probably
		 * not useful. */
		if (format != 1)
			elog(ERROR, "format must be binary");
#endif

		TupleDescInitEntry(tupdesc, i + 1, colname, type_oid, typmod, 0);
	}
	return tupdesc;
}


static HeapTuple
HeapTuple_from_DataRow(TupleDesc tupdesc, StringInfo msg)
{
	int16		natts = pq_getmsgint(msg, 2);
	int16		i;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData buf;

	Assert(tupdesc);

	if (natts != tupdesc->natts)
		elog(ERROR, "malformed DataRow");

	values = palloc(natts * sizeof(*values));
	nulls = palloc(natts * sizeof(*nulls));
	initStringInfo(&buf);

	for (i = 0; i < natts; i++)
	{
		int32 len = pq_getmsgint(msg, 4);

		if (len < 0)
			nulls[i] = true;
		else
		{
			Oid recvid;
			Oid typioparams;

			nulls[i] = false;

			getTypeBinaryInputInfo(tupdesc->attrs[i].atttypid,
								   &recvid,
								   &typioparams);
			resetStringInfo(&buf);
			appendBinaryStringInfo(&buf, pq_getmsgbytes(msg, len), len);
			values[i] = OidReceiveFunctionCall(recvid, &buf, typioparams,
											   tupdesc->attrs[i].atttypmod);
		}
	}

	return heap_form_tuple(tupdesc, values, nulls);
}


static void
forward_NotifyResponse(StringInfo msg)
{
	int32	pid;
	const char *channel;
	const char *payload;

	pid = pq_getmsgint(msg, 4);
	channel = pq_getmsgrawstring(msg);
	payload = pq_getmsgrawstring(msg);
	pq_endmessage(msg);

	NotifyMyFrontEnd(channel, payload, pid);
}


static void
rethrow_errornotice(StringInfo msg, int min_elevel)
{
	ErrorData   edata;

	pq_parse_errornotice(msg, &edata);
	edata.elevel = Min(edata.elevel, min_elevel);
	ThrowErrorData(&edata);
}


static void
invalid_protocol_message(char msgtype, char phase)
{
	ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			 errmsg("invalid protocol message type %c from autonomous session during phase %c",
					msgtype, phase)));
}
