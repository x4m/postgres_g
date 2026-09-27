/*-------------------------------------------------------------------------
 *
 * test_page_overlay.c
 *      Bounded, epoch-local exact images for the writable compute experiment.
 *
 * Ordinary redo need not preserve command IDs or other live-backend state.
 * Until that contract is strengthened, retain every written page for the
 * entire compute epoch.  This is NOT durable storage or an evictable cache.
 * An exhausted overlay fails writes; restarting this epoch is prohibited.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

#include "test_page_store.h"

PG_FUNCTION_INFO_V1(test_page_store_overlay_status);

#define OVERLAY_GUARD "test_page_store.writer_epoch"

typedef struct OverlayFile
{
	bool		initialized;
	bool		exists;
	BlockNumber nblocks;
	/* No baseline pages survive a truncate followed by reextension. */
	BlockNumber baseline_blocks;
} OverlayFile;

typedef struct OverlayControl
{
	LWLock		lock;
	pg_atomic_uint32 active;
	uint32		free_head;
	uint32		used;
	uint64		reads;
	uint64		writes;
} OverlayControl;

static int	overlay_capacity;
static bool overlay_after_recovery;
static int	overlay_files;
static int	overlay_max_blocks;
static OverlayControl *overlay;
static OverlayFile *files;
static PGAlignedBlock *images;
static uint32 *mapping;
static uint32 *free_list;

static void overlay_request(void *arg);
static void overlay_initialize(void *arg);

static const ShmemCallbacks overlay_callbacks = {
	.request_fn = overlay_request,
	.init_fn = overlay_initialize,
};

void
test_page_store_overlay_init(int nrelations, int max_blocks, bool after_recovery)
{
	DefineCustomIntVariable("test_page_store.writer_overlay_pages",
							"Maximum epoch-local exact images for the test writer.",
							"Zero disables the writable compute experiment.",
							&overlay_capacity, 0, 0, 131072, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	overlay_files = nrelations * (MAX_FORKNUM + 1);
	overlay_max_blocks = max_blocks;
	overlay_after_recovery = after_recovery;
	if (overlay_capacity && !nrelations)
		elog(ERROR, "writer overlay needs selected relations");
	RegisterShmemCallbacks(&overlay_callbacks);
}

bool
test_page_store_overlay_enabled(void)
{
	return overlay_capacity > 0;
}

bool
test_page_store_overlay_active(void)
{
	if (!overlay || pg_atomic_read_u32(&overlay->active) == 0)
		return false;
	pg_read_barrier();
	return true;
}

/* No SQL sessions exist in the opt-in recovery-to-writer experiment. */
void
test_page_store_overlay_activate(void)
{
	Assert(overlay_after_recovery && overlay && !test_page_store_overlay_active());
	for (int file = 0; file < overlay_files; file++)
	{
		ForkNumber	forknum = file % (MAX_FORKNUM + 1);
		BlockNumber size;
		bool		exists;

		if (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM && forknum != FSM_FORKNUM)
			continue;
		exists = test_page_store_baseline_fetch(file / (MAX_FORKNUM + 1), forknum,
												0, NULL, 0, &size);
		if ((forknum == MAIN_FORKNUM && !exists) || size > overlay_max_blocks)
			elog(ERROR, "invalid recovered writer baseline");
		files[file].exists = exists;
		files[file].nblocks = size;
		files[file].baseline_blocks = forknum == FSM_FORKNUM ? 0 : size;
		files[file].initialized = true;
	}
	/* Subsequent writes must retain images; redo-time discards are over. */
	pg_write_barrier();
	pg_atomic_write_u32(&overlay->active, 1);
}

static void
overlay_request(void *arg)
{
	if (!overlay_capacity)
		return;
	ShmemRequestStruct(.name = "test_page_store overlay control",
					   .size = sizeof(OverlayControl), .ptr = (void **) &overlay);
	ShmemRequestStruct(.name = "test_page_store overlay files",
					   .size = mul_size(overlay_files, sizeof(OverlayFile)),
					   .ptr = (void **) &files);
	ShmemRequestStruct(.name = "test_page_store overlay images",
					   .size = mul_size(overlay_capacity, sizeof(PGAlignedBlock)),
					   .ptr = (void **) &images);
	ShmemRequestStruct(.name = "test_page_store overlay mapping",
					   .size = mul_size(mul_size(overlay_files, overlay_max_blocks), sizeof(uint32)),
					   .ptr = (void **) &mapping);
	ShmemRequestStruct(.name = "test_page_store overlay free list",
					   .size = mul_size(overlay_capacity, sizeof(uint32)),
					   .ptr = (void **) &free_list);
}

static void
overlay_initialize(void *arg)
{
	int			fd;

	if (!overlay)
	{
		if (access(OVERLAY_GUARD, F_OK) == 0)
			elog(FATAL, "cannot disable the writer overlay and reuse its PGDATA");
		return;
	}

	/*
	 * Even a clean checkpoint cannot replace lost runtime images with the
	 * original baseline.  Refuse a second epoch, including postmaster's crash
	 * restart.  Recovery into a new writer uses a fresh seed instead.  Do not
	 * remove this guard on shutdown: the local relation files are no longer
	 * authoritative.
	 */
	fd = OpenTransientFile(OVERLAY_GUARD, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create test writer epoch guard: %m"),
				 errhint("The writer overlay cannot be restarted. Restore a fresh seed from storage; do not remove the guard to reuse this PGDATA.")));
	if (pg_fsync(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(), errmsg("could not sync writer epoch guard: %m")));
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(), errmsg("could not close writer epoch guard: %m")));
	fsync_fname(".", true);
	memset(overlay, 0, sizeof(*overlay));
	pg_atomic_init_u32(&overlay->active, !overlay_after_recovery);
	memset(files, 0, overlay_files * sizeof(OverlayFile));
	memset(mapping, 0, (size_t) overlay_files * overlay_max_blocks * sizeof(uint32));
	LWLockInitialize(&overlay->lock, LWLockNewTrancheId("test_page_store overlay"));
	overlay->free_head = 1;
	for (int i = 0; i < overlay_capacity; i++)
		free_list[i] = i + 1 == overlay_capacity ? 0 : i + 2;
}

static int
overlay_file(int relation, ForkNumber forknum)
{
	int			file = relation * (MAX_FORKNUM + 1) + forknum;

	Assert(overlay && relation >= 0 && file < overlay_files);
	if (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM && forknum != FSM_FORKNUM)
		elog(ERROR, "writer overlay supports main, VM and FSM forks only");
	if (RecoveryInProgress() &&
		(!overlay_after_recovery || !test_page_store_overlay_active()))
		elog(ERROR, "writer overlay cannot be used in recovery");
	return file;
}

/* Fetch baseline metadata without holding the overlay lock across network I/O. */
static int
overlay_ensure_file(int relation, ForkNumber forknum)
{
	int			file = overlay_file(relation, forknum);
	bool		initialized;
	bool		exists = false;
	BlockNumber size = 0;

	LWLockAcquire(&overlay->lock, LW_SHARED);
	initialized = files[file].initialized;
	LWLockRelease(&overlay->lock);
	if (initialized)
		return file;
	/* An initially absent FSM is a valid, conservative allocation hint. */
	if (forknum != FSM_FORKNUM)
		exists = test_page_store_baseline_fetch(relation, forknum, 0, NULL, 0, &size);
	if (size > overlay_max_blocks)
		elog(ERROR, "writer baseline exceeds follow_max_blocks");
	LWLockAcquire(&overlay->lock, LW_EXCLUSIVE);
	if (!files[file].initialized)
	{
		files[file].exists = exists;
		files[file].nblocks = size;
		files[file].baseline_blocks = size;
		files[file].initialized = true;
	}
	LWLockRelease(&overlay->lock);
	return file;
}

bool
test_page_store_overlay_read(int relation, ForkNumber forknum, BlockNumber block,
							 void **buffers, BlockNumber count, BlockNumber *nblocks)
{
	int			file = overlay_ensure_file(relation, forknum);
	bool		exists;

	LWLockAcquire(&overlay->lock, LW_SHARED);
	exists = files[file].exists;
	*nblocks = files[file].nblocks;
	if (count && (!exists || (uint64) block + count > *nblocks))
		elog(ERROR, "writer overlay read exceeds its size");
	LWLockRelease(&overlay->lock);
	for (BlockNumber i = 0; i < count; i++)
	{
		uint32		slot;
		BlockNumber baseline_blocks;
		BlockNumber ignored;

		LWLockAcquire(&overlay->lock, LW_EXCLUSIVE);
		slot = mapping[(size_t) file * overlay_max_blocks + block + i];
		baseline_blocks = files[file].baseline_blocks;
		if (slot)
		{
			memcpy(buffers[i], images[slot - 1].data, BLCKSZ);
			overlay->reads++;
		}
		LWLockRelease(&overlay->lock);
		if (!slot)
		{
			if (forknum == FSM_FORKNUM)
			{
				memset(buffers[i], 0, BLCKSZ);
				continue;
			}
			if (block + i >= baseline_blocks)
				elog(ERROR, "writer overlay lost an extended page");
			(void) test_page_store_baseline_fetch(relation, forknum, block + i,
												  &buffers[i], 1, &ignored);
		}
	}
	return exists;
}

/* Caller owns buffer I/O or relation extension; images never expire on commit. */
void
test_page_store_overlay_write(int relation, ForkNumber forknum, BlockNumber block,
							  const void **buffers, BlockNumber count, bool extending)
{
	int			file = overlay_ensure_file(relation, forknum);
	uint32		needed = 0;

	if ((uint64) block + count > overlay_max_blocks)
		elog(ERROR, "writer overlay exceeds follow_max_blocks");
	LWLockAcquire(&overlay->lock, LW_EXCLUSIVE);
	if (!files[file].exists || (!extending && block + count > files[file].nblocks) ||
		(extending && block != files[file].nblocks))
		elog(ERROR, "invalid writer overlay write range");
	for (BlockNumber i = 0; i < count; i++)
		if (!mapping[(size_t) file * overlay_max_blocks + block + i])
			needed++;
	if (needed > overlay_capacity - overlay->used)
		elog(ERROR, "test writer overlay is full; runtime images cannot be evicted");
	for (BlockNumber i = 0; i < count; i++)
	{
		uint32	   *entry = &mapping[(size_t) file * overlay_max_blocks + block + i];

		if (*entry == 0)
		{
			*entry = overlay->free_head;
			overlay->free_head = free_list[*entry - 1];
			overlay->used++;
		}
		if (buffers)
			memcpy(images[*entry - 1].data, buffers[i], BLCKSZ);
		else
			memset(images[*entry - 1].data, 0, BLCKSZ);
		overlay->writes++;
	}
	if (extending)
		files[file].nblocks = block + count;
	LWLockRelease(&overlay->lock);
}

void
test_page_store_overlay_create(int relation, ForkNumber forknum)
{
	int			file = overlay_ensure_file(relation, forknum);

	LWLockAcquire(&overlay->lock, LW_EXCLUSIVE);
	files[file].exists = true;
	LWLockRelease(&overlay->lock);
}

void
test_page_store_overlay_truncate(int relation, ForkNumber forknum, BlockNumber size,
								 bool unlinking)
{
	/*
	 * Unlink runs after the commit decision: no network I/O or missing-fork
	 * errors.
	 */
	int			file = unlinking ? relation * (MAX_FORKNUM + 1) + forknum :
		overlay_ensure_file(relation, forknum);

	Assert(file >= 0 && file < overlay_files);
	LWLockAcquire(&overlay->lock, LW_EXCLUSIVE);
	if (size > files[file].nblocks)
		elog(ERROR, "writer overlay truncation would extend the file");
	for (BlockNumber i = size; i < files[file].nblocks; i++)
	{
		uint32	   *entry = &mapping[(size_t) file * overlay_max_blocks + i];

		if (*entry)
		{
			free_list[*entry - 1] = overlay->free_head;
			overlay->free_head = *entry;
			*entry = 0;
			overlay->used--;
		}
	}
	files[file].nblocks = size;
	files[file].baseline_blocks = Min(files[file].baseline_blocks, size);
	if (unlinking)
	{
		files[file].initialized = true;
		files[file].exists = false;
	}
	LWLockRelease(&overlay->lock);
}

Datum
test_page_store_overlay_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false};

	if (!overlay)
		elog(ERROR, "writer overlay is not configured");
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	LWLockAcquire(&overlay->lock, LW_SHARED);
	values[0] = Int64GetDatum(overlay->used);
	values[1] = Int64GetDatum(overlay_capacity);
	values[2] = Int64GetDatum(overlay->reads);
	values[3] = Int64GetDatum(overlay->writes);
	LWLockRelease(&overlay->lock);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
