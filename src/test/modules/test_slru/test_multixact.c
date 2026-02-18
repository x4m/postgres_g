/*-------------------------------------------------------------------------
 *
 * test_multixact.c
 *		Support code for multixact testing
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/test_slru/test_multixact.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/multixact.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/pg_lsn.h"

PG_FUNCTION_INFO_V1(test_create_multixact);
PG_FUNCTION_INFO_V1(test_create_multixacts);
PG_FUNCTION_INFO_V1(test_multixact_write_truncate_wal);

/*
 * Produces multixact with 2 current xids
 */
Datum
test_create_multixact(PG_FUNCTION_ARGS)
{
	MultiXactId id;

	MultiXactIdSetOldestMember();
	id = MultiXactIdCreate(GetCurrentTransactionId(), MultiXactStatusUpdate,
						   GetCurrentTransactionId(), MultiXactStatusForShare);
	PG_RETURN_TRANSACTIONID(id);
}

/*
 * Create n multixacts.  Used to quickly fill offset pages for truncation tests.
 *
 * Each iteration uses a subtransaction so that GetCurrentTransactionId()
 * returns a different xid, preventing mXactCacheGetBySet from returning a
 * cached result and ensuring a new MultiXactId is allocated every time.
 */
Datum
test_create_multixacts(PG_FUNCTION_ARGS)
{
	int32		n = PG_GETARG_INT32(0);
	MultiXactId first_id = InvalidMultiXactId;

	if (n <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("n must be positive")));

	for (int i = 0; i < n; i++)
	{
		MultiXactId id;

		BeginInternalSubTransaction(NULL);
		MultiXactIdSetOldestMember();
		id = MultiXactIdCreate(GetCurrentTransactionId(), MultiXactStatusUpdate,
							   GetCurrentTransactionId(), MultiXactStatusForShare);
		ReleaseCurrentSubTransaction();

		if (i == 0)
			first_id = id;
	}

	PG_RETURN_TRANSACTIONID(first_id);
}

/*
 * Write a TRUNCATE_ID WAL record with the given endTruncOff.
 *
 * This is used to simulate a truncation that sets latest_page_number to a
 * specific page during standby replay, without actually truncating anything
 * on the primary.  The standby's multixact_redo handler will reset
 * latest_page_number = MultiXactIdToOffsetPage(endTruncOff).
 */
Datum
test_multixact_write_truncate_wal(PG_FUNCTION_ARGS)
{
	MultiXactId endTruncOff = PG_GETARG_TRANSACTIONID(0);
	xl_multixact_truncate xlrec;
	XLogRecPtr	recptr;

	xlrec.oldestMultiDB = MyDatabaseId;
	xlrec.startTruncOff = 1;
	xlrec.endTruncOff = endTruncOff;
	xlrec.startTruncMemb = 0;
	xlrec.endTruncMemb = 0;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfMultiXactTruncate);
	recptr = XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID);
	XLogFlush(recptr);

	PG_RETURN_LSN(recptr);
}
