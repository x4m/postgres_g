/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * The shared buffer mapping table is a flat, index-linked hash table.  It is
 * made of two shared-memory arrays:
 *
 *	  buckets[num_buckets] - one chain head per hash bucket
 *	  entries[NBuffers]     - one entry per buffer, indexed by buf_id
 *
 * Each buffer slot i permanently owns entry slot i, so no freelist is needed:
 * bufmgr always removes a buffer's old mapping before inserting a new tag for
 * that same buf_id.  Empty entry slots are marked by tag.blockNum == P_NEW;
 * chains are linked by int index and terminated by BUF_TABLE_CHAIN_END.
 *
 * num_buckets is a power of two and a multiple of NUM_BUFFER_PARTITIONS, so the
 * bucket index shares its low bits with the partition index.  Every tag that
 * maps to a given bucket therefore maps to a single partition, and the caller's
 * BufMappingLock fully serializes each chain.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"

#define BUF_TABLE_CHAIN_END  (-1)

typedef struct
{
	int			head;
}			BufferLookupBucket;

typedef struct
{
	BufferTag	tag;
	int			next;
} BufferLookupEnt;

static BufferLookupBucket * buckets;
static BufferLookupEnt *entries;
static int	num_buckets;

static void BufTableShmemRequest(void *arg);
static void BufTableShmemInit(void *arg);
static void BufTableShmemAttach(void *arg);

const ShmemCallbacks BufTableShmemCallbacks = {
	.request_fn = BufTableShmemRequest,
	.init_fn = BufTableShmemInit,
	.attach_fn = BufTableShmemAttach,
};

static inline int
BufTableNumBuckets(void)
{
	return Max(NUM_BUFFER_PARTITIONS, pg_nextpower2_32(NBuffers));
}

void
BufTableShmemRequest(void *arg)
{
	num_buckets = BufTableNumBuckets();
	Assert(num_buckets % NUM_BUFFER_PARTITIONS == 0);

	ShmemRequestStruct(.name = "Shared Buffer Lookup Buckets",
					   .size = (Size) num_buckets * sizeof(BufferLookupBucket),
					   .ptr = (void **) &buckets,
		);
	ShmemRequestStruct(.name = "Shared Buffer Lookup Entries",
					   .size = (Size) NBuffers * sizeof(BufferLookupEnt),
					   .ptr = (void **) &entries,
		);
}

void
BufTableShmemInit(void *arg)
{
	num_buckets = BufTableNumBuckets();

	for (int i = 0; i < num_buckets; i++)
		buckets[i].head = BUF_TABLE_CHAIN_END;

	for (int i = 0; i < NBuffers; i++)
	{
		entries[i].tag.blockNum = P_NEW;
		entries[i].next = BUF_TABLE_CHAIN_END;
	}
}

void
BufTableShmemAttach(void *arg)
{
	num_buckets = BufTableNumBuckets();
}

uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return tag_hash(tagPtr, sizeof(BufferTag));
}

int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	int			id = buckets[hashcode % num_buckets].head;

	while (id != BUF_TABLE_CHAIN_END)
	{
		if (BufferTagsEqual(&entries[id].tag, tagPtr))
			return id;
		id = entries[id].next;
	}
	return -1;
}

int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	int			bucket_id = hashcode % num_buckets;
	int			head = buckets[bucket_id].head;
	int			id = head;

	Assert(buf_id >= 0 && buf_id < NBuffers);
	Assert(tagPtr->blockNum != P_NEW);

	while (id != BUF_TABLE_CHAIN_END)
	{
		if (BufferTagsEqual(&entries[id].tag, tagPtr))
			return id;
		id = entries[id].next;
	}

	Assert(entries[buf_id].tag.blockNum == P_NEW);

	entries[buf_id].tag = *tagPtr;
	entries[buf_id].next = head;
	buckets[bucket_id].head = buf_id;

	return -1;
}

void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	int			bucket_id = hashcode % num_buckets;
	int			prev = BUF_TABLE_CHAIN_END;
	int			id = buckets[bucket_id].head;

	while (id != BUF_TABLE_CHAIN_END)
	{
		if (BufferTagsEqual(&entries[id].tag, tagPtr))
		{
			if (prev == BUF_TABLE_CHAIN_END)
				buckets[bucket_id].head = entries[id].next;
			else
				entries[prev].next = entries[id].next;
			entries[id].tag.blockNum = P_NEW;
			entries[id].next = BUF_TABLE_CHAIN_END;
			return;
		}
		prev = id;
		id = entries[id].next;
	}

	Assert(false);
	elog(ERROR, "shared buffer hash table corrupted");
}
