/*-------------------------------------------------------------------------
 *
 * ip.c
 *	  IPv6-aware network access.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/ip.c
 *
 * This file and the IPV6 implementation were initially provided by
 * Nigel Kukard <nkukard@lbsd.net>, Linux Based Systems Design
 * http://www.lbsd.net.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#if defined(HAVE_GETADDRINFO_A)
#define ASYNC_LOOKUP_SUPPORTED
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/file.h>

#ifdef ASYNC_LOOKUP_SUPPORTED
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#endif

#include "common/ip.h"

static int	getaddrinfo_unix(const char *path,
							 const struct addrinfo *hintsp,
							 struct addrinfo **result);

static int	getnameinfo_unix(const struct sockaddr_un *sa, int salen,
							 char *node, int nodelen,
							 char *service, int servicelen,
							 int flags);


/*
 *	pg_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets
 *
 * The API of this routine differs from the standard getaddrinfo() definition
 * in that it requires a valid hintp, a null pointer is not allowed.
 */
int
pg_getaddrinfo_all(const char *hostname, const char *servname,
				   const struct addrinfo *hintp, struct addrinfo **result)
{
	int			rc;

	/* not all versions of getaddrinfo() zero *result on failure */
	*result = NULL;

	if (hintp->ai_family == AF_UNIX)
		return getaddrinfo_unix(servname, hintp, result);

	/* NULL has special meaning to getaddrinfo(). */
	hostname = (!hostname || hostname[0] == '\0') ? NULL : hostname;

#if defined(HAVE_GETADDRINFO_A)
	{
		struct gaicb cb = {
			.ar_name = hostname,
			.ar_service = servname,
			.ar_request = hintp,
		};
		struct gaicb *cb_list[] = {&cb};

		rc = getaddrinfo_a(GAI_WAIT, cb_list, lengthof(cb_list), NULL);
		if (rc)
			return rc;			/* TODO: it may be useful to fall back? */

		rc = gai_error(&cb);
		if (rc)
			return rc;

		*result = cb.ar_result;
	}
#else
	rc = getaddrinfo(hostname, servname, hintp, result);
#endif

	return rc;
}

struct async_lookup_ctx
{
	atomic_int_fast8_t refcnt;	/* reference count for the allocation */

	struct gaicb cb;			/* the getattrinfo() call description */
	char	   *hostname;
	char	   *servname;
	struct addrinfo *hintp;

	int			self_pipe;		/* write end of the pipe waking up select() */
};

static void
release_async_lookup_ctx(struct async_lookup_ctx *ctx)
{
	if (atomic_fetch_sub(&ctx->refcnt, 1) != 1)
		return;

	free(ctx->hostname);
	free(ctx->servname);
	free(ctx->hintp);
	free(ctx);
}

static void
lookup_finished(union sigval val)
{
	struct async_lookup_ctx *ctx = val.sival_ptr;

	close(ctx->self_pipe);		/* wake up any select()ors */

	release_async_lookup_ctx(ctx);
}

/*
 *	pg_getaddrinfo_all_async - get address info for Unix, IPv4 and IPv6 sockets,
 *	possibly asynchronously
 *
 * If async DNS is supported for the given arguments on this platform, address
 * lookup will be queued in the background.  If successful, *async will be set
 * to a non-NULL pointer, which must be passed to pg_getaddrinfo_all_finish() to
 * retrieve the results. Otherwise, *async will be set to NULL and the function
 * will behave identically to pg_getaddrinfo_all().
 *
 * (TODO: intentionally fall back to sync if queuing fails?)
 *
 * As with pg_getaddrinfo_all(), hintp is mandatory.
 *
 * self_pipe must be the write side of an fd pair that will be used to wake up
 * any select/poll clients once resolution completes. If the return code is zero
 * and *async is set to non-NULL upon return, then ownership of this file
 * descriptor has been passed to a background thread; it MUST NOT be written to,
 * duplicated, or closed by the caller. (If the return code is nonzero, or if
 * *async is NULL on return, ownership remains with the caller: the request has
 * either completed or failed synchronously, and nothing needs to write to the
 * pipe in that case.)
 */
int
pg_getaddrinfo_all_async(const char *hostname, const char *servname,
						 const struct addrinfo *hintp, struct addrinfo **result,
						 int self_pipe, void **async)
{
#ifdef ASYNC_LOOKUP_SUPPORTED
	int			rc;
	struct async_lookup_ctx *ctx;
	struct gaicb *cb_list[1];
	struct sigevent sev = {0};
	pthread_attr_t attrs;

	*result = NULL;
	*async = NULL;

	if (hintp->ai_family == AF_UNIX)
	{
		/* AF_UNIX lookup is nonblocking; defer to pg_getaddrinfo_all(). */
		return pg_getaddrinfo_all(hostname, servname, hintp, result);
	}

	/* ctx holds all our asynchronous state. */
	ctx = calloc(1, sizeof(struct async_lookup_ctx));
	if (!ctx)
		return EAI_MEMORY;

	/* (match pg_getaddrinfo_all behavior) */
	hostname = (!hostname || hostname[0] == '\0') ? NULL : hostname;

	/* Make copies of our inputs, since they may be stack-allocated. */
	if (hostname)
		ctx->hostname = strdup(hostname);
	if (servname)
		ctx->servname = strdup(servname);
	ctx->hintp = malloc(sizeof(*hintp));

	if (!ctx->hostname || !ctx->servname || !ctx->hintp)
	{
		rc = EAI_MEMORY;
		goto fail;
	}

	memcpy(ctx->hintp, hintp, sizeof(*hintp));

	/*
	 * ctx->cb corresponds to the getaddrinfo() arguments. ctx->cb.ar_result
	 * will be set upon completion and doesn't need to be initialized.
	 */
	ctx->cb.ar_name = ctx->hostname;
	ctx->cb.ar_service = ctx->servname;
	ctx->cb.ar_request = ctx->hintp;

	/* The notification thread will close this, if async queueing succeeds. */
	ctx->self_pipe = self_pipe;

	/* Set the refcount to 2 (pg_getaddrinfo_all_finish + lookup_finished). */
	atomic_init(&ctx->refcnt, 2);

	cb_list[0] = &ctx->cb;

	/*
	 * Set up lookup_finished as our notification callback. It will be called
	 * from the background, on an unspecified thread.
	 */
	sev.sigev_notify = SIGEV_THREAD;
	sev.sigev_value.sival_ptr = ctx;
	sev.sigev_notify_function = lookup_finished;

	/*
	 * XXX: possibly-pointless paranoia. getaddrinfo_a takes a list of
	 * optional thread attributes for the SIGEV_THREAD notification mode. The
	 * default list of attributes isn't documented (but it is *not* the same
	 * as the default pthread_attr_t!). In particular, we want to make sure
	 * that these tiny ephemeral threads are detached, not joinable.
	 *
	 * In practice, if you pass NULL, glibc does indeed assume that you want
	 * detachable threads as of 2026. But it feels dangerous to implicitly
	 * rely on that very important detail...
	 *
	 * TODO: possibly pull down the stack size? Anything else?
	 */
	if (pthread_attr_init(&attrs)
		|| pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED))
	{
		/*
		 * Documented to be impossible in the glibc implementation, but if the
		 * impossible happens anyway, errno should contain the failure mode.
		 */
		rc = EAI_SYSTEM;
		goto fail;
	}

	sev.sigev_notify_attributes = &attrs;

	/* Setup complete; queue our async request. */
	rc = getaddrinfo_a(GAI_NOWAIT, cb_list, lengthof(cb_list), &sev);
	if (rc)
		goto fail;				/* TODO: it may be useful to fall back? */

	/*
	 * Past this point, we race against the notification thread, which touches
	 * the contents of ctx. Nothing is allowed to free ctx without going
	 * through the refcount, and nothing is allowed to touch self_pipe except
	 * the notification thread.
	 */
	*async = ctx;
	return 0;

fail:
	free(ctx->hostname);
	free(ctx->servname);
	free(ctx->hintp);
	free(ctx);

	return rc;

#else							/* !ASYNC_LOOKUP_SUPPORTED */
	*async = NULL;
	return pg_getaddrinfo_all(hostname, servname, hintp, result);
#endif
}

/*
 * Finish a started call to pg_getaddrinfo_all_async(). This is only relevant if
 * the async pointer was set by that call, but it MUST be called to release the
 * allocated resources, even if the database connection has failed for some
 * other reason. That resource cleanup may complete at some unspecified point in
 * the future; we don't wait for it here.
 *
 * async is the pointer that was provided by pg_getaddrinfo_all_async(). Its
 * contents must not be touched after this call, because a concurrent thread may
 * free it without warning.
 *
 * If the background lookup request has finished, the (optional) result array
 * will be pointed to the fetched information. The return code matches the
 * behavior of getaddrinfo: zero on success, or a non-zero EAI_* code on
 * failure.
 *
 * If the request hasn't finished by this point, we will attempt to cancel it.
 * (Even if the request cannot be canceled, the outstanding resources will be
 * released once it completes.) *result will be set to NULL, and a relevant
 * EAI_* code is returned.
 *
 * TODO: test the failure mode where many outstanding uncancellable requests
 * pile up (high DNS latency?)
 */
int
pg_getaddrinfo_all_finish(void *async, struct addrinfo **result)
{
	struct async_lookup_ctx *ctx = async;
	int			rc;

	if (result)
		*result = NULL;

	/* First figure out the current state of things. */
	rc = gai_cancel(&ctx->cb);
	switch (rc)
	{
		case EAI_CANCELED:

			/*
			 * We have successfully removed the DNS request from the queue.
			 * The notification callback will still be invoked and we are
			 * actively racing against it now.
			 *
			 * TODO: is this true? *Will* the callback be invoked?
			 */
			Assert(false);		/* TODO when does this happen? */
			break;

		case EAI_NOTCANCELED:

			/*
			 * The DNS request is already in flight. We are racing against
			 * both its manipulation of ctx->cb and the notification thread
			 * (once lookup completes), but we won't wait for its results.
			 */
			break;

		case EAI_ALLDONE:

			/*
			 * The request has finished, and we can take a look at the result.
			 * But we're *still* racing against the notification thread! We
			 * must not assume that it's already done with the ctx pointer.
			 */
			rc = gai_error(&ctx->cb);
			if (!rc && result)
				*result = ctx->cb.ar_result;
			break;

		default:
			/* Still want to clean up, but this should not be possible. */
			Assert(false);
	}

	release_async_lookup_ctx(ctx);

	return rc;
}

/*
 *	pg_freeaddrinfo_all - free addrinfo structures for IPv4, IPv6, or Unix
 *
 * Note: the ai_family field of the original hint structure must be passed
 * so that we can tell whether the addrinfo struct was built by the system's
 * getaddrinfo() routine or our own getaddrinfo_unix() routine.  Some versions
 * of getaddrinfo() might be willing to return AF_UNIX addresses, so it's
 * not safe to look at ai_family in the addrinfo itself.
 */
void
pg_freeaddrinfo_all(int hint_ai_family, struct addrinfo *ai)
{
	if (hint_ai_family == AF_UNIX)
	{
		/* struct was built by getaddrinfo_unix (see pg_getaddrinfo_all) */
		while (ai != NULL)
		{
			struct addrinfo *p = ai;

			ai = ai->ai_next;
			free(p->ai_addr);
			free(p);
		}
	}
	else
	{
		/* struct was built by getaddrinfo() */
		if (ai != NULL)
			freeaddrinfo(ai);
	}
}


/*
 *	pg_getnameinfo_all - get name info for Unix, IPv4 and IPv6 sockets
 *
 * The API of this routine differs from the standard getnameinfo() definition
 * in two ways: first, the addr parameter is declared as sockaddr_storage
 * rather than struct sockaddr, and second, the node and service fields are
 * guaranteed to be filled with something even on failure return.
 */
int
pg_getnameinfo_all(const struct sockaddr_storage *addr, int salen,
				   char *node, int nodelen,
				   char *service, int servicelen,
				   int flags)
{
	int			rc;

	if (addr && addr->ss_family == AF_UNIX)
		rc = getnameinfo_unix((const struct sockaddr_un *) addr, salen,
							  node, nodelen,
							  service, servicelen,
							  flags);
	else
		rc = getnameinfo((const struct sockaddr *) addr, salen,
						 node, nodelen,
						 service, servicelen,
						 flags);

	if (rc != 0)
	{
		if (node)
			strlcpy(node, "???", nodelen);
		if (service)
			strlcpy(service, "???", servicelen);
	}

	return rc;
}


/* -------
 *	getaddrinfo_unix - get unix socket info using IPv6-compatible API
 *
 *	Bugs: only one addrinfo is set even though hintsp is NULL or
 *		  ai_socktype is 0
 *		  AI_CANONNAME is not supported.
 * -------
 */
static int
getaddrinfo_unix(const char *path, const struct addrinfo *hintsp,
				 struct addrinfo **result)
{
	struct addrinfo hints = {0};
	struct addrinfo *aip;
	struct sockaddr_un *unp;

	*result = NULL;

	if (strlen(path) >= sizeof(unp->sun_path))
		return EAI_FAIL;

	if (hintsp == NULL)
	{
		hints.ai_family = AF_UNIX;
		hints.ai_socktype = SOCK_STREAM;
	}
	else
		memcpy(&hints, hintsp, sizeof(hints));

	if (hints.ai_socktype == 0)
		hints.ai_socktype = SOCK_STREAM;

	if (hints.ai_family != AF_UNIX)
	{
		/* shouldn't have been called */
		return EAI_FAIL;
	}

	aip = calloc(1, sizeof(struct addrinfo));
	if (aip == NULL)
		return EAI_MEMORY;

	unp = calloc(1, sizeof(struct sockaddr_un));
	if (unp == NULL)
	{
		free(aip);
		return EAI_MEMORY;
	}

	aip->ai_family = AF_UNIX;
	aip->ai_socktype = hints.ai_socktype;
	aip->ai_protocol = hints.ai_protocol;
	aip->ai_next = NULL;
	aip->ai_canonname = NULL;
	*result = aip;

	unp->sun_family = AF_UNIX;
	aip->ai_addr = (struct sockaddr *) unp;
	aip->ai_addrlen = sizeof(struct sockaddr_un);

	strcpy(unp->sun_path, path);

	/*
	 * If the supplied path starts with @, replace that with a zero byte for
	 * the internal representation.  In that mode, the entire sun_path is the
	 * address, including trailing zero bytes.  But we set the address length
	 * to only include the length of the original string.  That way the
	 * trailing zero bytes won't show up in any network or socket lists of the
	 * operating system.  This is just a convention, also followed by other
	 * packages.
	 */
	if (path[0] == '@')
	{
		unp->sun_path[0] = '\0';
		aip->ai_addrlen = offsetof(struct sockaddr_un, sun_path) + strlen(path);
	}

	return 0;
}

/*
 * Convert an address to a hostname.
 */
static int
getnameinfo_unix(const struct sockaddr_un *sa, int salen,
				 char *node, int nodelen,
				 char *service, int servicelen,
				 int flags)
{
	int			ret;

	/* Invalid arguments. */
	if (sa == NULL || sa->sun_family != AF_UNIX ||
		(node == NULL && service == NULL))
		return EAI_FAIL;

	if (node)
	{
		ret = snprintf(node, nodelen, "%s", "[local]");
		if (ret < 0 || ret >= nodelen)
			return EAI_MEMORY;
	}

	if (service)
	{
		/*
		 * Check whether it looks like an abstract socket, but it could also
		 * just be an empty string.
		 */
		if (sa->sun_path[0] == '\0' && sa->sun_path[1] != '\0')
			ret = snprintf(service, servicelen, "@%s", sa->sun_path + 1);
		else
			ret = snprintf(service, servicelen, "%s", sa->sun_path);
		if (ret < 0 || ret >= servicelen)
			return EAI_MEMORY;
	}

	return 0;
}
