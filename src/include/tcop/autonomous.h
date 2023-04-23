#ifndef AUTONOMOUS_H
#define AUTONOMOUS_H

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

struct AutonomousSession;
typedef struct AutonomousSession AutonomousSession;

struct AutonomousPreparedStatement;
typedef struct AutonomousPreparedStatement AutonomousPreparedStatement;

typedef struct AutonomousResult
{
	TupleDesc	tupdesc;
	List	   *tuples;
	char	   *command;
} AutonomousResult;

AutonomousSession *AutonomousSessionStart(void);
void AutonomousSessionEnd(AutonomousSession *session);
AutonomousResult *AutonomousSessionExecute(AutonomousSession *session, const char *sql);
AutonomousPreparedStatement *AutonomousSessionPrepare(AutonomousSession *session, const char *sql, int16 nargs,
							  Oid argtypes[], const char *argnames[]);
AutonomousResult *AutonomousSessionExecutePrepared(AutonomousPreparedStatement *stmt, int16 nargs, Datum *values, bool *nulls);

#endif /* AUTONOMOUS_H */
