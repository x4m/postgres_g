#ifndef BGSESSION_H
#define BGSESSION_H

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

struct BackgroundSession;
typedef struct BackgroundSession BackgroundSession;

struct BackgroundSessionPreparedStatement;
typedef struct BackgroundSessionPreparedStatement BackgroundSessionPreparedStatement;

typedef struct BackgroundSessionResult
{
	TupleDesc	tupdesc;
	List	   *tuples;
	const char *command;
} BackgroundSessionResult;

BackgroundSession *BackgroundSessionStart(void);
void BackgroundSessionEnd(BackgroundSession *session);

void BackgroundSessionSend(BackgroundSession *session, const char *sql);
BackgroundSessionResult *BackgroundSessionGetResult(BackgroundSession *session);
BackgroundSessionResult *BackgroundSessionExecute(BackgroundSession *session, const char *sql);

BackgroundSessionPreparedStatement *BackgroundSessionPrepare(BackgroundSession *session, const char *sql, int nargs, Oid argtypes[], const char *argnames[]);
BackgroundSessionResult *BackgroundSessionExecutePrepared(BackgroundSessionPreparedStatement *stmt, int nargs, Datum values[], bool nulls[]);

#endif /* BGSESSION_H */
