/*-------------------------------------------------------------------------
 *
 * fe-connect-svcb.h
 *	  Built-in DNS SVCB resolver for libpq service discovery (c-ares).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-connect-svcb.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FE_CONNECT_SVCB_H
#define FE_CONNECT_SVCB_H

#include "libpq-fe.h"

/*
 * Register the built-in DNS SVCB resolver (implemented with c-ares) as the
 * active service-discovery resolver, i.e. call PQsetResolver() with this
 * backend's methods.  Applications and drivers that want SVCB-based discovery
 * call this once at startup.  In a production layout this resolver would ship
 * as a separately installable shared object that links against libpq's public
 * PQsetResolver(); here it is built into libpq for the prototype.
 */
extern void PQinitSvcbResolver(void);

#endif							/* FE_CONNECT_SVCB_H */
