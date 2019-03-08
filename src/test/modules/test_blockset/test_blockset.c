/*--------------------------------------------------------------------------
 *
 * test_blockset.c
 *		Test block set data structure.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_blockset/test_blockset.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "lib/blockset.h"
#include "nodes/bitmapset.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_blockset);

static void test_blockset_bms_compliance();
static void test_blockset_big_block_numbers();

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_blockset(PG_FUNCTION_ARGS)
{
	test_blockset_bms_compliance(0);
	test_blockset_bms_compliance(1);
	test_blockset_bms_compliance(2);
	test_blockset_bms_compliance(1337);
	test_blockset_bms_compliance(100000);
	test_blockset_big_block_numbers(1337);
	test_blockset_big_block_numbers(31337);
	PG_RETURN_VOID();
}

/*
 * This test creates random bitmap with test_limit members
 * and checks that block set behavior is similar to Bitmapset
 */
static void test_blockset_bms_compliance(int32_t test_limit)
{
	BlockSet bs = NULL;
	Bitmapset *bms = NULL;
	BlockNumber blockno;
	int index;
	int32_t point_index = 0;

	for (int32_t i = 0; i < test_limit; i++)
	{
		blockno = random() & INT32_MAX;
		/* bms does not support block numbers above INT32_MAX */
		bs = blockset_set(bs, blockno);
		bms = bms_add_member(bms, (int)blockno);
	}

	index = -1;
	blockno = InvalidBlockNumber;

	while (true)
	{
		point_index++;
		BlockNumber next_bn = blockset_next(bs, blockno);
		int next_index = bms_next_member(bms, index);


		if (next_bn == InvalidBlockNumber && next_index == -2)
			return; /* We have found everything */

		if (((BlockNumber)next_index) != next_bn)
		{
			elog(ERROR,
				 "Bitmapset returned value %X different from block set %X,"
				 " test_limit %d, point index %d",
				 next_index, next_bn, test_limit, point_index);
		}

		if (!blockset_get(next_bn, bs))
		{
			elog(ERROR,
				 "Block set did not found present item %X"
				 " test_limit %d, point index %d",
				 next_bn, test_limit, point_index);
		}

		index = next_index;
		blockno = next_bn;
	}

	for (int32_t i = 0; i < test_limit; i++)
	{
		blockno = random() & INT32_MAX;
		if (blockset_get(blockno, bs) != bms_is_member((int)blockno, bms))
		{
			elog(ERROR,
				 "Block set did agree with bitmapset item %X"
				 " test_limit %d, point index %d",
				 blockno, test_limit, point_index);
		}
	}

	blockset_free(bs);
	bms_free(bms);
}

/* 
 * This test is similar to test_blockset_bms_compliance()
 * except that it shifts BlockNumbers by one bit to ensure that blockset
 * operates correctly on values higher that INT32_MAX
 * This function is copy-pasted from previous with the exception of barrel
 * shifts for BlockNumbers. I've tried various refactorings, but they all
 * looked ugly.
 */
static void test_blockset_big_block_numbers(int32_t test_limit)
{
	BlockSet bs = NULL;
	Bitmapset *bms = NULL;
	BlockNumber blockno;
	int index;
	int32_t point_index = 0;

	for (int32_t i = 0; i < test_limit; i++)
	{
		blockno = random() & INT32_MAX;
		/* bms does not support block numbers above INT32_MAX */
		bs = blockset_set(bs, blockno << 1);
		bms = bms_add_member(bms, (int)blockno);
	}

	index = -1;
	blockno = InvalidBlockNumber;

	while (true)
	{
		point_index++;
		BlockNumber next_bn = blockset_next(bs, blockno);
		int next_index = bms_next_member(bms, index);


		if (next_bn == InvalidBlockNumber && next_index == -2)
			return; /* We have found everything */

		if (((BlockNumber)next_index) != (next_bn >> 1))
		{
			elog(ERROR,
				 "Bitmapset returned value %X different from block set %X,"
				 " test_limit %d, point index %d",
				 next_index, next_bn, test_limit, point_index);
		}

		if (!blockset_get(next_bn, bs))
		{
			elog(ERROR,
				 "Block set did not found present item %X"
				 " test_limit %d, point index %d",
				 next_bn, test_limit, point_index);
		}

		index = next_index;
		blockno = next_bn;
	}

	for (int32_t i = 0; i < test_limit; i++)
	{
		blockno = random() & INT32_MAX;
		if (blockset_get(blockno << 1, bs) != bms_is_member((int)blockno, bms))
		{
			elog(ERROR,
				 "Block set did agree with bitmapset item %X"
				 " test_limit %d, point index %d",
				 blockno, test_limit, point_index);
		}
	}

	blockset_free(bs);
	bms_free(bms);
}
