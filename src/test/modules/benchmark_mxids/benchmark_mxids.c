/*--------------------------------------------------------------------------
 *
 * benchmark_mxids.c
 *		Test performance of multixact.
 *
 * Copyright (c) 2018-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/benchmark_mxids/benchmark_mxids.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/multixact.h"
#include "access/xact.h"


PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(benchmark_mxids);

/*
 * SQL-callable entry point to perform benchmark.
 */
Datum
benchmark_mxids(PG_FUNCTION_ARGS)
{
	int array_size = 65536;
	int mxid_size = 128;
	int tests_count = 65536 * 10;
	MultiXactMember members[65536];
	MultiXactIdSetOldestMember();
	TransactionId current_xid = GetCurrentTransactionId();

	MultiXactId *array = palloc(sizeof(MultiXactId) * array_size);

	for (int i = 0; i < array_size; i++)
	{
		for (int o = 0; o < mxid_size; o++)
		{
			//members[o].status = o % (MultiXactStatusUpdate + 1);
			members[o].xid = current_xid + (i*7 + o * 3) % array_size;
		}
		array[i] = MultiXactIdCreateFromMembers(mxid_size, members);
		//elog(WARNING, "mxid %d", array[i]);
	}

	for (int i = 0; i < tests_count; i++)
	{
		MultiXactMember *members;
		GetMultiXactIdMembers(array[(i*17) % array_size], &members, false, false);
		pfree(members);
	}

	PG_RETURN_VOID();
}
