/*-------------------------------------------------------------------------
 *
 * fe-connect-svcb.c
 *	  Built-in DNS SVCB resolver for libpq service discovery (prototype).
 *
 * Implements the PQresolverMethods interface (see libpq-fe.h) on top of the
 * c-ares asynchronous resolver (https://c-ares.org/, >= 1.22.0 for structured
 * SVCB parsing).  It resolves the SVCB record (RFC 9460) at
 * _postgresql.<discover> and returns one endpoint per ServiceMode answer:
 * the target host, the port SvcParam (default 5432) and, when present, the
 * address from the ipv4hint/ipv6hint SvcParams or a matching A/AAAA record in
 * the Additional section.  A single SVCB query therefore yields host, port
 * and address together, avoiding a follow-up A/AAAA lookup in the common case.
 *
 * Resolution is driven asynchronously: libpq waits on the c-ares socket via
 * the CONNECTION_RESOLVING state, so name lookup never blocks PQconnectPoll().
 *
 * This file is compiled only when libpq is built with c-ares (USE_CARES), and
 * the resolver is active only after PQinitSvcbResolver() registers it, so the
 * libpq core has no build- or link-time dependency on c-ares.
 *
 * For local testing, set PGSVCB_DNS to a comma-separated list of resolver
 * addresses (e.g. "127.0.0.1:5353") to bypass the system resolver.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-connect-svcb.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "fe-connect-svcb.h"

#ifdef USE_CARES

#include <arpa/inet.h>
#include <sys/select.h>

#include <ares.h>
#include <ares_dns_record.h>

#include "libpq-int.h"

#define SVCB_SERVICE_PREFIX "_postgresql."
#define SVCB_DEFAULT_PORT 5432

/* Opaque resolver handle: c-ares channel plus accumulated results. */
typedef struct
{
	ares_channel_t *channel;
	PQresolvedEndpoint *eps;
	int			n;
	int			maxn;
	bool		oom;			/* allocation failed while parsing */
	bool		done;			/* callback has fired */
	ares_status_t status;		/* result reported by the callback */
	char		qname[272];		/* "_postgresql." + discover name */
} SvcbState;

static PQresolvedEndpoint *
svcb_append(SvcbState *st)
{
	if (st->n >= st->maxn)
	{
		int			newmax = st->maxn ? st->maxn * 2 : 4;
		PQresolvedEndpoint *tmp = realloc(st->eps,
										  newmax * sizeof(PQresolvedEndpoint));

		if (tmp == NULL)
		{
			st->oom = true;
			return NULL;
		}
		st->eps = tmp;
		st->maxn = newmax;
	}
	memset(&st->eps[st->n], 0, sizeof(PQresolvedEndpoint));
	return &st->eps[st->n++];
}

/*
 * Scan the Additional section for an A/AAAA record matching target and write
 * its presentation-form address into addr (size addrlen) if found.
 */
static void
svcb_addr_from_additional(const ares_dns_record_t *dnsrec, const char *target,
						  char *addr, size_t addrlen)
{
	size_t		cnt = ares_dns_record_rr_cnt(dnsrec, ARES_SECTION_ADDITIONAL);

	addr[0] = '\0';
	for (size_t i = 0; i < cnt; i++)
	{
		const ares_dns_rr_t *rr =
			ares_dns_record_rr_get_const(dnsrec, ARES_SECTION_ADDITIONAL, i);
		ares_dns_rec_type_t type;
		const char *name;

		if (rr == NULL)
			continue;
		name = ares_dns_rr_get_name(rr);
		if (name == NULL || pg_strcasecmp(name, target) != 0)
			continue;

		type = ares_dns_rr_get_type(rr);
		if (type == ARES_REC_TYPE_A)
		{
			const struct in_addr *ia = ares_dns_rr_get_addr(rr, ARES_RR_A_ADDR);

			if (ia != NULL && inet_ntop(AF_INET, ia, addr, addrlen) != NULL)
				return;
		}
		else if (type == ARES_REC_TYPE_AAAA)
		{
			const struct ares_in6_addr *i6 =
				ares_dns_rr_get_addr6(rr, ARES_RR_AAAA_ADDR);

			if (i6 != NULL && inet_ntop(AF_INET6, i6, addr, addrlen) != NULL)
				return;
		}
	}
}

/* c-ares callback: parse SVCB answers into the SvcbState. */
static void
svcb_callback(void *arg, ares_status_t status, size_t timeouts,
			  const ares_dns_record_t *dnsrec)
{
	SvcbState  *st = (SvcbState *) arg;
	size_t		cnt;

	(void) timeouts;
	st->done = true;
	st->status = status;
	if (status != ARES_SUCCESS || dnsrec == NULL)
		return;

	cnt = ares_dns_record_rr_cnt(dnsrec, ARES_SECTION_ANSWER);
	for (size_t i = 0; i < cnt; i++)
	{
		const ares_dns_rr_t *rr =
			ares_dns_record_rr_get_const(dnsrec, ARES_SECTION_ANSWER, i);
		const char *target;
		const unsigned char *val;
		size_t		val_len;
		size_t		tlen;
		PQresolvedEndpoint *ep;

		if (rr == NULL || ares_dns_rr_get_type(rr) != ARES_REC_TYPE_SVCB)
			continue;

		target = ares_dns_rr_get_str(rr, ARES_RR_SVCB_TARGET);
		if (target == NULL || target[0] == '\0')
			continue;			/* AliasMode (".") not supported in prototype */

		ep = svcb_append(st);
		if (ep == NULL)
			return;				/* OOM; flagged in st */

		ep->priority = ares_dns_rr_get_u16(rr, ARES_RR_SVCB_PRIORITY);
		ep->port = SVCB_DEFAULT_PORT;

		strlcpy(ep->target, target, sizeof(ep->target));
		tlen = strlen(ep->target);
		if (tlen > 0 && ep->target[tlen - 1] == '.')
			ep->target[tlen - 1] = '\0';	/* strip trailing dot */

		/* port SvcParam */
		if (ares_dns_rr_get_opt_byid(rr, ARES_RR_SVCB_PARAMS,
									 ARES_SVCB_PARAM_PORT, &val, &val_len) &&
			val_len >= 2)
			ep->port = (uint16_t) ((val[0] << 8) | val[1]);

		/* address hints: ipv4hint first, then ipv6hint */
		if (ares_dns_rr_get_opt_byid(rr, ARES_RR_SVCB_PARAMS,
									 ARES_SVCB_PARAM_IPV4HINT, &val, &val_len) &&
			val_len >= 4)
		{
			struct in_addr ia;

			memcpy(&ia, val, 4);
			inet_ntop(AF_INET, &ia, ep->addr, sizeof(ep->addr));
		}
		else if (ares_dns_rr_get_opt_byid(rr, ARES_RR_SVCB_PARAMS,
										  ARES_SVCB_PARAM_IPV6HINT, &val,
										  &val_len) && val_len >= 16)
		{
			struct in6_addr i6;

			memcpy(&i6, val, 16);
			inet_ntop(AF_INET6, &i6, ep->addr, sizeof(ep->addr));
		}
		else
		{
			/* no hint: try the Additional section for this target */
			svcb_addr_from_additional(dnsrec, ep->target,
									  ep->addr, sizeof(ep->addr));
		}
	}
}

/* PQresolverMethods.start */
static void *
svcb_start(PGconn *conn, const char *name)
{
	SvcbState  *st;
	struct ares_options options;
	const char *dns_override;

	if (ares_library_init(ARES_LIB_INIT_ALL) != ARES_SUCCESS)
	{
		libpq_append_conn_error(conn, "could not initialize c-ares library");
		return NULL;
	}

	st = calloc(1, sizeof(SvcbState));
	if (st == NULL)
	{
		libpq_append_conn_error(conn, "out of memory");
		ares_library_cleanup();
		return NULL;
	}

	memset(&options, 0, sizeof(options));
	if (ares_init_options(&st->channel, &options, 0) != ARES_SUCCESS)
	{
		libpq_append_conn_error(conn, "could not initialize DNS resolver");
		free(st);
		ares_library_cleanup();
		return NULL;
	}

	/* Optional resolver override for local testing. */
	dns_override = getenv("PGSVCB_DNS");
	if (dns_override != NULL && dns_override[0] != '\0')
		ares_set_servers_csv(st->channel, dns_override);

	snprintf(st->qname, sizeof(st->qname), "%s%s", SVCB_SERVICE_PREFIX, name);

	ares_query_dnsrec(st->channel, st->qname, ARES_CLASS_IN,
					  ARES_REC_TYPE_SVCB, svcb_callback, st, NULL);

	return st;
}

/* PQresolverMethods.socket: report the c-ares fd to wait on. */
static void
svcb_socket(void *handle, int *sock, int *forwrite)
{
	SvcbState  *st = (SvcbState *) handle;
	ares_socket_t socks[ARES_GETSOCK_MAXNUM];
	int			bits = ares_getsock(st->channel, socks, ARES_GETSOCK_MAXNUM);

	*sock = -1;
	*forwrite = 0;
	for (int i = 0; i < ARES_GETSOCK_MAXNUM; i++)
	{
		if (ARES_GETSOCK_READABLE(bits, i))
		{
			*sock = (int) socks[i];
			*forwrite = 0;
			return;
		}
		if (ARES_GETSOCK_WRITABLE(bits, i))
		{
			*sock = (int) socks[i];
			*forwrite = 1;
			return;
		}
	}
}

/* PQresolverMethods.poll: process any ready c-ares I/O, report progress. */
static PostgresPollingStatusType
svcb_poll(PGconn *conn, void *handle)
{
	SvcbState  *st = (SvcbState *) handle;
	fd_set		rfds,
				wfds;
	ares_socket_t socks[ARES_GETSOCK_MAXNUM];
	int			bits;
	int			nfds = 0;
	struct timeval tv = {0, 0};

	FD_ZERO(&rfds);
	FD_ZERO(&wfds);
	bits = ares_getsock(st->channel, socks, ARES_GETSOCK_MAXNUM);
	for (int i = 0; i < ARES_GETSOCK_MAXNUM; i++)
	{
		if (ARES_GETSOCK_READABLE(bits, i))
		{
			FD_SET(socks[i], &rfds);
			if ((int) socks[i] + 1 > nfds)
				nfds = (int) socks[i] + 1;
		}
		if (ARES_GETSOCK_WRITABLE(bits, i))
		{
			FD_SET(socks[i], &wfds);
			if ((int) socks[i] + 1 > nfds)
				nfds = (int) socks[i] + 1;
		}
	}

	/*
	 * A zero-timeout select tells c-ares which of its sockets are ready right
	 * now (we are called either initially, to send the query, or after the
	 * caller's own select reported readiness).  This never blocks.
	 */
	if (nfds > 0 && select(nfds, &rfds, &wfds, NULL, &tv) < 0 &&
		errno != EINTR)
	{
		libpq_append_conn_error(conn, "select() failed during DNS resolution");
		return PGRES_POLLING_FAILED;
	}
	ares_process(st->channel, &rfds, &wfds);

	if (st->done)
	{
		if (st->oom)
		{
			libpq_append_conn_error(conn, "out of memory");
			return PGRES_POLLING_FAILED;
		}
		if (st->status != ARES_SUCCESS)
		{
			libpq_append_conn_error(conn, "DNS SVCB lookup for \"%s\" failed: %s",
									st->qname, ares_strerror(st->status));
			return PGRES_POLLING_FAILED;
		}
		if (st->n == 0)
		{
			libpq_append_conn_error(conn, "no SVCB records found for \"%s\"",
									st->qname);
			return PGRES_POLLING_FAILED;
		}
		return PGRES_POLLING_OK;
	}

	/* Still pending: report the direction the caller should wait for. */
	bits = ares_getsock(st->channel, socks, ARES_GETSOCK_MAXNUM);
	for (int i = 0; i < ARES_GETSOCK_MAXNUM; i++)
	{
		if (ARES_GETSOCK_READABLE(bits, i))
			return PGRES_POLLING_READING;
		if (ARES_GETSOCK_WRITABLE(bits, i))
			return PGRES_POLLING_WRITING;
	}
	return PGRES_POLLING_READING;
}

/* PQresolverMethods.results */
static int
svcb_results(void *handle, PQresolvedEndpoint **endpoints)
{
	SvcbState  *st = (SvcbState *) handle;

	*endpoints = st->eps;
	return st->n;
}

/* PQresolverMethods.finish */
static void
svcb_finish(void *handle)
{
	SvcbState  *st = (SvcbState *) handle;

	if (st == NULL)
		return;
	if (st->channel != NULL)
		ares_destroy(st->channel);
	free(st->eps);
	free(st);
	ares_library_cleanup();
}

static const PQresolverMethods svcb_resolver_methods = {
	.start = svcb_start,
	.socket = svcb_socket,
	.poll = svcb_poll,
	.results = svcb_results,
	.finish = svcb_finish,
};

void
PQinitSvcbResolver(void)
{
	PQsetResolver(&svcb_resolver_methods);
}

#else							/* !USE_CARES */

/*
 * Without c-ares the SVCB resolver is unavailable, so registering it is a
 * no-op; "discover" then reports that no resolver is registered.
 */
void
PQinitSvcbResolver(void)
{
}

#endif							/* USE_CARES */
