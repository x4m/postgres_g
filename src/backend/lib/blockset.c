/*-------------------------------------------------------------------------
 *
 * blockset.c
 *		Data structure for operations on set of block numbers
 *
 * This data structure resembles bitmap set in idea and operations, but
 * has two main differences:
 * 1. It handles unsigned BlockNumber as position in set instead of int32_t
 * This allows to work with relation forks bigger than 2B blocks
 * 2. It is more space efficient for sparse bitmaps: regions are allocated
 * in chunks of 256 items at once.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/blockset.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/blockset.h"

/* Lowest level of radix tree is represented by bitmap */
typedef struct
{
	char data[256/8];
} BlockSetLevel4Data;

typedef BlockSetLevel4Data *BlockSetLevel4;

/* statically typed inner level chunks points to ground level */
typedef struct
{
	/* null references denote empty subtree */
	BlockSetLevel4 next[256];
} BlockSetLevel3Data;

typedef BlockSetLevel3Data *BlockSetLevel3;

/* inner level points to another inner level */
typedef struct
{
	BlockSetLevel3 next[256];
} BlockSetLevel2Data;

typedef BlockSetLevel2Data *BlockSetLevel2;

/* Radix tree root */
typedef struct BlockSetData
{
	BlockSetLevel2 next[256];
} BlockSetData;

/* multiplex block number into indexes of radix tree */
#define BLOCKSET_SPLIT_BLKNO				\
	uint32_t i1, i2, i3, i4, byte_no, byte_mask;	\
	i4 = blkno % 256;						\
	blkno /= 256;							\
	i3 = blkno % 256;						\
	blkno /= 256;							\
	i2 = blkno % 256;						\
	blkno /= 256;							\
	i1 = blkno;								\
	byte_no = i4 / 8;						\
	byte_mask = 1 << (i4 % 8)				\

/* indicate presence of block number in set */
BlockSet blockset_set(BlockSet bs, BlockNumber blkno)
{
	BLOCKSET_SPLIT_BLKNO;
	if (bs == NULL)
	{
		bs = palloc0(sizeof(BlockSetData));
	}
	BlockSetLevel2 bs2 = bs->next[i1];
	if (bs2 == NULL)
	{
		bs2 = palloc0(sizeof(BlockSetLevel2Data));
		bs->next[i1] = bs2;
	}
	BlockSetLevel3 bs3 = bs2->next[i2];
	if (bs3 == NULL)
	{
		bs3 = palloc0(sizeof(BlockSetLevel3Data));
		bs2->next[i2] = bs3;
	}
	BlockSetLevel4 bs4 = bs3->next[i3];
	if (bs4 == NULL)
	{
		bs4 = palloc0(sizeof(BlockSetLevel4Data));
		bs3->next[i3] = bs4;
	}
	bs4->data[byte_no] = byte_mask | bs4->data[byte_no];
	return bs;
}

/* Test presence of block in set */
bool blockset_get(BlockNumber blkno, BlockSet bs)
{
	BLOCKSET_SPLIT_BLKNO;
	if (bs == NULL)
		return false;
	BlockSetLevel2 bs2 = bs->next[i1];
	if (bs2 == NULL)
		return false;
	BlockSetLevel3 bs3 = bs2->next[i2];
	if (bs3 == NULL)
		return false;
	BlockSetLevel4 bs4 = bs3->next[i3];
	if (bs4 == NULL)
		return false;
	return (bs4->data[byte_no] & byte_mask);
}

/* 
 * Find nearest block number in set no less than blkno
 * Return InvalidBlockNumber if nothing to return
 * If given InvalidBlockNumber - returns minimal element in set
 */
BlockNumber blockset_next(BlockSet bs, BlockNumber blkno)
{
	if (blkno == InvalidBlockNumber)
		blkno = 0; /* equvalent to ++, left for clear code */
	else
		blkno++;

	BLOCKSET_SPLIT_BLKNO;

	if (bs == NULL)
		return InvalidBlockNumber;
	for (; i1 < 256; i1++)
	{
		BlockSetLevel2 bs2 = bs->next[i1];
		if (!bs2)
			continue;
		for (; i2 < 256; i2++)
		{
			BlockSetLevel3 bs3 = bs2->next[i2];
			if (!bs3)
				continue;
			for (; i3 < 256; i3++)
			{
				BlockSetLevel4 bs4 = bs3->next[i3];
				if (!bs4)
					continue;
				for (; byte_no < 256 / 8; byte_no++)
				{
					if (!bs4->data[byte_no])
						continue;
					while (byte_mask < 256)
					{
						if ((byte_mask & bs4->data[byte_no]) == byte_mask)
						{
							i4 = byte_no * 8;
							while (byte_mask >>= 1) i4++;
							return i4 + 256 * (i3 + 256 * (i2 + 256 * i1));
						}
						byte_mask <<= 1;
					}
					byte_mask = 1;
				}
				byte_no = 0;
			}
			i3 = 0;
		}
		i2 = 0;
	}
	return InvalidBlockNumber;
}

/* free anything palloced */
void blockset_free(BlockSet bs)
{
	BlockNumber blkno = 0;
	BLOCKSET_SPLIT_BLKNO;
	if (bs == NULL)
		return;
	for (; i1 < 256; i1++)
	{
		BlockSetLevel2 bs2 = bs->next[i1];
		if (!bs2)
			continue;
		for (; i2 < 256; i2++)
		{
			BlockSetLevel3 bs3 = bs2->next[i2];
			if (!bs3)
				continue;
			for (; i3 < 256; i3++)
			{
				BlockSetLevel4 bs4 = bs3->next[i3];
				if (bs4)
					pfree(bs4);
			}
			pfree(bs3);
		}
		pfree(bs2);
	}
	pfree(bs);
}