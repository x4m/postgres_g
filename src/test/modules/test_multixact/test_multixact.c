/*--------------------------------------------------------------------------
 *
 * test_multixact.c
 *		Test integer set data structure.
 *
 * Copyright (c) 2019-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_multixact/test_multixact.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/multixact.h"
#include "access/xact.h"

bool	multixact_test_stats;
int		multixact_test_scale;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_multixact);

/*
 * SQL-callable entry point to perform all test.
 */
Datum
test_multixact(PG_FUNCTION_ARGS)
{
	TransactionId xid = GetCurrentTransactionId();
	MultiXactIdSetOldestMember();
	TransactionId new_mxid = MultiXactIdCreate(xid, MultiXactStatusForKeyShare, xid - 1, MultiXactStatusForKeyShare);
	new_mxid = MultiXactIdExpand(new_mxid, xid - 2, MultiXactStatusForKeyShare);
	new_mxid = MultiXactIdExpand(new_mxid, xid - 3, MultiXactStatusForKeyShare);
	MultiXactIdExpand(new_mxid, xid - 4, MultiXactStatusForKeyShare);

	PG_RETURN_VOID();
}