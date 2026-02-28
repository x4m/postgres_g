/*-------------------------------------------------------------------------
 *
 * sharedrelsize.c
 *	  Shared relation size cache for safe truncation without
 *	  AccessExclusiveLock.
 *
 * This module maintains a shared-memory hash table mapping
 * (RelFileLocator, ForkNumber) to the known block count. It is
 * populated on truncation and updated on extension, allowing
 * concurrent readers to detect truncation and avoid accessing
 * pages beyond the new end of the relation.
 *
 * Entries are created on first truncation and removed when the
 * relation is dropped. If no entry exists for a relation, the
 * cache is not authoritative and callers should fall through to
 * the normal smgrnblocks() path.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/sharedrelsize.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/sharedrelsize.h"
#include "utils/hsearch.h"

/*
 * Maximum number of entries in the shared rel size cache.
 *
 * This should be large enough to cover all relations that might be
 * truncated concurrently with active queries. In practice, this is
 * bounded by the number of distinct relations in the database.
 * For now, use a generous fixed size; this can be made configurable
 * or replaced with dshash later.
 */
#define SHARED_REL_SIZE_CACHE_SIZE	4096

typedef struct SharedRelSizeKey
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
} SharedRelSizeKey;

typedef struct SharedRelSizeEntry
{
	SharedRelSizeKey key;		/* hash key - must be first */
	BlockNumber nblocks;		/* current relation size */
	BlockNumber max_nblocks;	/* maximum size ever observed */
} SharedRelSizeEntry;

static HTAB *SharedRelSizeHash = NULL;

Size
SharedRelSizeCacheShmemSize(void)
{
	return hash_estimate_size(SHARED_REL_SIZE_CACHE_SIZE,
							  sizeof(SharedRelSizeEntry));
}

void
SharedRelSizeCacheShmemInit(void)
{
	HASHCTL		info;

	info.keysize = sizeof(SharedRelSizeKey);
	info.entrysize = sizeof(SharedRelSizeEntry);

	SharedRelSizeHash = ShmemInitHash("Shared Rel Size Cache",
									  SHARED_REL_SIZE_CACHE_SIZE,
									  SHARED_REL_SIZE_CACHE_SIZE,
									  &info,
									  HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
}

BlockNumber
SharedRelSizeCacheGet(RelFileLocator *rlocator, ForkNumber forknum,
					  BlockNumber *max_nblocks)
{
	SharedRelSizeKey key;
	SharedRelSizeEntry *entry;

	memset(&key, 0, sizeof(key));
	key.rlocator = *rlocator;
	key.forknum = forknum;

	LWLockAcquire(SharedRelSizeCacheLock, LW_SHARED);
	entry = (SharedRelSizeEntry *) hash_search(SharedRelSizeHash,
											   &key, HASH_FIND, NULL);
	if (entry)
	{
		BlockNumber result = entry->nblocks;

		if (max_nblocks)
			*max_nblocks = entry->max_nblocks;
		LWLockRelease(SharedRelSizeCacheLock);
		return result;
	}
	LWLockRelease(SharedRelSizeCacheLock);

	if (max_nblocks)
		*max_nblocks = InvalidBlockNumber;
	return InvalidBlockNumber;
}

void
SharedRelSizeCacheTruncate(RelFileLocator *rlocator, ForkNumber forknum,
						   BlockNumber old_nblocks, BlockNumber new_nblocks)
{
	SharedRelSizeKey key;
	SharedRelSizeEntry *entry;
	bool		found;

	Assert(new_nblocks <= old_nblocks);

	memset(&key, 0, sizeof(key));
	key.rlocator = *rlocator;
	key.forknum = forknum;

	LWLockAcquire(SharedRelSizeCacheLock, LW_EXCLUSIVE);
	entry = (SharedRelSizeEntry *) hash_search(SharedRelSizeHash,
											   &key, HASH_ENTER, &found);
	if (found)
	{
		if (old_nblocks > entry->max_nblocks)
			entry->max_nblocks = old_nblocks;
	}
	else
	{
		entry->max_nblocks = old_nblocks;
	}
	entry->nblocks = new_nblocks;
	LWLockRelease(SharedRelSizeCacheLock);
}

void
SharedRelSizeCacheExtend(RelFileLocator *rlocator, ForkNumber forknum,
						 BlockNumber new_nblocks)
{
	SharedRelSizeKey key;
	SharedRelSizeEntry *entry;

	memset(&key, 0, sizeof(key));
	key.rlocator = *rlocator;
	key.forknum = forknum;

	LWLockAcquire(SharedRelSizeCacheLock, LW_EXCLUSIVE);
	entry = (SharedRelSizeEntry *) hash_search(SharedRelSizeHash,
											   &key, HASH_FIND, NULL);
	if (entry)
	{
		entry->nblocks = new_nblocks;
		if (new_nblocks > entry->max_nblocks)
			entry->max_nblocks = new_nblocks;
	}
	/* If no entry exists, do nothing - no truncation recorded for this rel */
	LWLockRelease(SharedRelSizeCacheLock);
}

void
SharedRelSizeCacheRemove(RelFileLocator *rlocator, ForkNumber forknum)
{
	SharedRelSizeKey key;

	memset(&key, 0, sizeof(key));
	key.rlocator = *rlocator;
	key.forknum = forknum;

	LWLockAcquire(SharedRelSizeCacheLock, LW_EXCLUSIVE);
	hash_search(SharedRelSizeHash, &key, HASH_REMOVE, NULL);
	LWLockRelease(SharedRelSizeCacheLock);
}
