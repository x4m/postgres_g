/*-------------------------------------------------------------------------
 *
 * sharedrelsize.h
 *	  Shared relation size cache for safe truncation without
 *	  AccessExclusiveLock.
 *
 * This cache stores relation fork sizes in shared memory, allowing
 * concurrent readers to detect that a relation has been truncated
 * without requiring AccessExclusiveLock during VACUUM truncation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sharedrelsize.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHAREDRELSIZE_H
#define SHAREDRELSIZE_H

#include "storage/block.h"
#include "storage/relfilelocator.h"

/*
 * Initialize the shared relation size cache in shared memory.
 * Called during CreateOrAttachShmemStructs().
 */
extern void SharedRelSizeCacheShmemInit(void);

/*
 * Return the required shared memory size for the cache.
 */
extern Size SharedRelSizeCacheShmemSize(void);

/*
 * Look up the cached size for a relation fork.
 *
 * Returns the cached nblocks, or InvalidBlockNumber if no entry exists.
 * When max_nblocks is not NULL, it is set to the maximum size ever observed
 * (useful for distinguishing truncation from corruption).
 */
extern BlockNumber SharedRelSizeCacheGet(RelFileLocator *rlocator,
										 ForkNumber forknum,
										 BlockNumber *max_nblocks);

/*
 * Record a truncation: set nblocks to the new (smaller) size and
 * remember the old size as max_nblocks if it was larger.
 *
 * This must be called BEFORE DropRelationBuffers and physical truncation,
 * so that concurrent readers see the new size and stop accessing
 * truncated pages.
 */
extern void SharedRelSizeCacheTruncate(RelFileLocator *rlocator,
									   ForkNumber forknum,
									   BlockNumber old_nblocks,
									   BlockNumber new_nblocks);

/*
 * Record a relation extension: update nblocks to the new (larger) size.
 *
 * Only updates if an entry already exists (i.e., was previously populated
 * by a truncation or explicit set). This avoids bloating the cache with
 * every relation that is ever extended.
 */
extern void SharedRelSizeCacheExtend(RelFileLocator *rlocator,
									 ForkNumber forknum,
									 BlockNumber new_nblocks);

/*
 * Remove the cache entry for a relation fork.
 * Called when a relation is dropped.
 */
extern void SharedRelSizeCacheRemove(RelFileLocator *rlocator,
									 ForkNumber forknum);

#endif							/* SHAREDRELSIZE_H */
