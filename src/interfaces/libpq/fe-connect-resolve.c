/*-------------------------------------------------------------------------
 *
 * fe-connect-resolve.c
 *	  Asynchronous host-resolution driver for libpq.
 *
 * libpq can resolve a connection's host list through a pluggable, registered
 * resolver.  The resolver maps a name to a list of endpoints; libpq builds
 * its per-host connection array from that list, so all existing multi-host
 * machinery - failover, target_session_attrs, load_balance_hosts - works
 * unchanged on the result.
 *
 * The resolver is general: the name may be a cluster name to expand for
 * service discovery (e.g. a DNS SVCB backend), or an ordinary host name to
 * resolve to its A/AAAA addresses asynchronously.  This file owns the
 * backend-independent part: driving the registered resolver asynchronously
 * from PQconnectPoll().
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-connect-resolve.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "fe-connect-resolve.h"
#include "libpq-int.h"

/*
 * The registered resolver.  NULL until an application or driver installs one
 * with PQsetResolver().  A single global is sufficient: resolver selection is
 * a process-wide policy, like the SSL key-password hook.
 */
static const PQresolverMethods *pq_resolver = NULL;

/*
 * PQsetResolver
 *
 * Register the resolver used for asynchronous host resolution, or NULL to
 * disable it.  Like PQsetSSLKeyPassHook_OpenSSL(), this installs a
 * process-wide hook; a resolver backend (for example one built on c-ares)
 * can thus be shipped separately from libpq and plugged in at runtime.
 */
void
PQsetResolver(const PQresolverMethods *methods)
{
	pq_resolver = methods;
}

PostgresPollingStatusType
pqResolveStart(PGconn *conn)
{
	if (pq_resolver == NULL)
	{
		libpq_append_conn_error(conn,
								"no resolver is registered for asynchronous host resolution");
		return PGRES_POLLING_FAILED;
	}

	conn->resolve_state = pq_resolver->start(conn, conn->discoverhost);
	if (conn->resolve_state == NULL)
		return PGRES_POLLING_FAILED;	/* start() appended the message */

	return pqResolvePoll(conn);
}

PostgresPollingStatusType
pqResolvePoll(PGconn *conn)
{
	PostgresPollingStatusType status;

	status = pq_resolver->poll(conn, conn->resolve_state);

	if (status == PGRES_POLLING_READING || status == PGRES_POLLING_WRITING)
	{
		int			sock = -1;
		int			forwrite = 0;

		/*
		 * Expose the resolver's socket as the connection socket so that the
		 * caller (and PQsocket()) can wait on it just like a connection
		 * socket.  The fd is borrowed from the resolver; it is released by
		 * the resolver's finish(), not by libpq.
		 */
		pq_resolver->socket(conn->resolve_state, &sock, &forwrite);
		conn->sock = (pgsocket) sock;
	}

	return status;
}

int
pqResolveResults(PGconn *conn, PQresolvedEndpoint **endpoints)
{
	return pq_resolver->results(conn->resolve_state, endpoints);
}

void
pqResolveCleanup(PGconn *conn)
{
	if (conn->resolve_state != NULL)
	{
		pq_resolver->finish(conn->resolve_state);
		conn->resolve_state = NULL;
	}

	/*
	 * Stop borrowing the resolver's socket; the real connection socket is
	 * opened fresh once we advance to CONNECTION_NEEDED.
	 */
	conn->sock = PGINVALID_SOCKET;
}
