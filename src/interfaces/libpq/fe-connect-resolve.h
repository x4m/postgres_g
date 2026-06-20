/*-------------------------------------------------------------------------
 *
 * fe-connect-resolve.h
 *	  Internal driver for libpq's asynchronous host resolution.
 *
 * The public resolver interface (PQresolvedEndpoint, PQresolverMethods,
 * PQsetResolver) lives in libpq-fe.h.  This header declares the internal
 * entry points that PQconnectPoll() uses to drive the registered resolver.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-connect-resolve.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FE_CONNECT_RESOLVE_H
#define FE_CONNECT_RESOLVE_H

#include "libpq-int.h"

/*
 * Kick off asynchronous resolution of conn->discoverhost using the
 * registered resolver.  Returns the initial polling status; if no resolver
 * is registered this fails with an error in conn.
 */
extern PostgresPollingStatusType pqResolveStart(PGconn *conn);

/* Advance an in-progress resolution; updates conn->sock to the fd to wait on. */
extern PostgresPollingStatusType pqResolvePoll(PGconn *conn);

/* After PGRES_POLLING_OK: fetch the resolved endpoints (count returned). */
extern int	pqResolveResults(PGconn *conn, PQresolvedEndpoint **endpoints);

/* Release the resolver handle (success or error path). */
extern void pqResolveCleanup(PGconn *conn);

#endif							/* FE_CONNECT_RESOLVE_H */
