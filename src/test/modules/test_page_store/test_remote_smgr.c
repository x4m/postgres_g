/*-------------------------------------------------------------------------
 *
 * test_remote_smgr.c
 *      Read-only SMgr consumers of the test page service.
 *
 * Frozen mode redirects selected physical relations on a paused standby.
 * Following mode redirects their main and VM forks and replays heap and
 * B-tree WAL into cached pages.
 * All other relations and forks still use md; neither mode is diskless compute.
 * SQL is a temporary transport to the independent test endpoint; the final
 * physical service must also work before database connections are possible.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/nbtxlog.h"
#include "access/relation.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

#include "test_page_store.h"

void		_PG_init(void);

PG_FUNCTION_INFO_V1(test_page_store_io_counts);
PG_FUNCTION_INFO_V1(test_page_store_read_smgr);
PG_FUNCTION_INFO_V1(test_page_store_compute_status);

static char *page_conninfo;
static char *page_lsn;
static int	page_tli;
static int	page_spc;
static int	page_db;
static int	page_rel;
static char *page_locators;
static int	request_timeout;
static PGconn *page_conn;
static PGresult *page_result;
static bool cleanup_registered;
static uint64 readv_count;
static uint64 startreadv_count;
static PgAioHandleCallbackID read_callback;
static ExecutorStart_hook_type previous_executor_start;
static ExecutorRun_hook_type previous_executor_run;

/* Bounded test representation.  No marker eviction or relation fallback. */
#define COMPUTE_MAX_RELATIONS 16

typedef struct ComputeFile
{
	pg_atomic_uint32 nblocks;
	pg_atomic_uint32 exists;
} ComputeFile;

typedef struct ComputeControl
{
	pg_atomic_uint32 active;
	ComputeFile files[COMPUTE_MAX_RELATIONS][MAX_FORKNUM + 1];
	pg_atomic_uint64 completed;
	pg_atomic_uint64 skipped;
	pg_atomic_uint64 cached;
	pg_atomic_uint64 fetches;
	pg_atomic_uint64 startup_fetches;
	ConditionVariable replayed;
} ComputeControl;

static bool follow_replay;
static int	follow_max_blocks;
static ComputeControl *compute;
static pg_atomic_uint64 *required_lsn;
static RelFileLocator selected_locators[COMPUTE_MAX_RELATIONS];
static int	nselected;
static wal_replay_start_hook_type previous_replay_start_hook;
static after_wal_replay_hook_type previous_replay_hook;
static redo_buffer_filter_hook_type previous_filter_hook;

static void compute_shmem_request(void *arg);
static void compute_shmem_init(void *arg);
static void compute_replay_start(XLogRecPtr start, TimeLineID tli);
static void compute_after_replay(XLogReaderState *record, TimeLineID tli);
static bool compute_redo_filter(XLogReaderState *record, uint8 block_id, ReadBufferMode mode);
static bool compute_active(void);
static bool selected_locator(RelFileLocator locator);
static int	selected_relation(RelFileLocator locator);
static void parse_remote_locators(void);
static ComputeFile *compute_file(RelFileLocator locator, ForkNumber forknum);
static void compute_check(void);
static XLogRecPtr compute_read_lsn(RelFileLocator locator, ForkNumber forknum, BlockNumber block,
								   BlockNumber count, TimestampTz deadline);
static void compute_note_lsn(RelFileLocator locator, ForkNumber forknum, BlockNumber block, XLogRecPtr lsn);
static void remote_create(RelFileLocator old, SMgrRelation reln, ForkNumber forknum, bool isRedo, SmgrChainIndex next);
static void remote_unlink(RelFileLocatorBackend locator, ForkNumber forknum, bool isRedo, SmgrChainIndex next);
static void remote_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber block,
						  const void *buffer, bool skipFsync, SmgrChainIndex next);
static void remote_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber block,
							  int count, bool skipFsync, SmgrChainIndex next);
static void remote_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old,
							BlockNumber size, SmgrChainIndex next);
static void remote_immedsync(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next);
static void remote_registersync(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next);
static int	remote_fd(SMgrRelation reln, ForkNumber forknum, BlockNumber block,
					  uint32 *off, SmgrChainIndex next);

static const ShmemCallbacks compute_callbacks = {
	.request_fn = compute_shmem_request,
	.init_fn = compute_shmem_init,
};

static bool remote_relation(SMgrRelation reln, ForkNumber forknum);
static bool remote_fetch(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, void **buffers,
						 BlockNumber count, BlockNumber *nblocks);
static bool remote_fetch_at(RelFileLocator locator, ForkNumber forknum,
							BlockNumber blocknum, void **buffers,
							BlockNumber count, BlockNumber *nblocks,
							XLogRecPtr lsn, TimestampTz deadline);
static XLogRecPtr remote_history_before(XLogRecPtr start);
static bool remote_exists(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next);
static BlockNumber remote_nblocks(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next);
static bool remote_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							int nblocks, SmgrChainIndex next);
static uint32 remote_maxcombine(SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, SmgrChainIndex next);
static void remote_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						 void **buffers, BlockNumber nblocks, SmgrChainIndex next);
static void remote_startreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, void **buffers,
							  BlockNumber nblocks, SmgrChainIndex next);
static void remote_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						  const void **buffers, BlockNumber nblocks, bool skipFsync,
						  SmgrChainIndex next);
static void remote_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							 BlockNumber nblocks, SmgrChainIndex next);
static PgAioResult remote_read_complete(PgAioHandle *ioh, PgAioResult result, uint8 flags);
static void remote_executor_start(QueryDesc *queryDesc, int eflags);
static void remote_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count);

static const f_smgr remote_smgr = {
	.name = "test_page_store",
	.chain_position = SMGR_CHAIN_MODIFIER,
	.smgr_create = remote_create,
	.smgr_unlink = remote_unlink,
	.smgr_extend = remote_extend,
	.smgr_zeroextend = remote_zeroextend,
	.smgr_truncate = remote_truncate,
	.smgr_immedsync = remote_immedsync,
	.smgr_registersync = remote_registersync,
	.smgr_fd = remote_fd,
	.smgr_exists = remote_exists,
	.smgr_nblocks = remote_nblocks,
	.smgr_prefetch = remote_prefetch,
	.smgr_maxcombine = remote_maxcombine,
	.smgr_readv = remote_readv,
	.smgr_startreadv = remote_startreadv,
	.smgr_writev = remote_writev,
	.smgr_writeback = remote_writeback,
};

static const PgAioHandleCallbacks remote_read_callbacks = {
	.complete_shared = remote_read_complete,
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("test_page_store.conninfo", "Page service connection.",
							   NULL, &page_conninfo, "", PGC_POSTMASTER, GUC_SUPERUSER_ONLY,
							   NULL, NULL, NULL);
	DefineCustomStringVariable("test_page_store.replay_lsn", "Frozen replay position.",
							   NULL, &page_lsn, "0/0", PGC_POSTMASTER, 0,
							   NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.replay_tli", "Frozen timeline.",
							NULL, &page_tli, 1, 1, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.tablespace", "Remote tablespace.",
							NULL, &page_spc, 0, 0, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.database", "Remote database.",
							NULL, &page_db, 0, 0, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.relfilenumber", "Remote relation.",
							NULL, &page_rel, 0, 0, INT_MAX, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomStringVariable("test_page_store.locators", "Remote physical relation locators.",
							   "Comma-separated tablespace/database/relfilenumber triples.",
							   &page_locators, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);
	parse_remote_locators();
	DefineCustomIntVariable("test_page_store.request_timeout", "Page request timeout.",
							NULL, &request_timeout, 5000, 1, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MS, NULL, NULL, NULL);
	test_page_store_history_init();
	DefineCustomBoolVariable("test_page_store.follow", "Follow WAL for remote main and VM forks.",
							 NULL, &follow_replay, false, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("test_page_store.follow_max_blocks", "Maximum tracked blocks per relation fork.",
							NULL, &follow_max_blocks, 65536, 1, 1048576,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);
	if (follow_replay && test_page_store_history_enabled())
		elog(ERROR, "following compute and page history must run on different nodes");
	if (follow_replay && nselected == 0)
		elog(ERROR, "following compute needs at least one selected relation");
	RegisterShmemCallbacks(&compute_callbacks);
	previous_replay_start_hook = wal_replay_start_hook;
	wal_replay_start_hook = compute_replay_start;
	previous_replay_hook = after_wal_replay_hook;
	after_wal_replay_hook = compute_after_replay;
	previous_filter_hook = redo_buffer_filter_hook;
	redo_buffer_filter_hook = compute_redo_filter;
	MarkGUCPrefixReserved("test_page_store");
	smgr_register(&remote_smgr, 0);
	read_callback = pgaio_io_register_callback_entry(&remote_read_callbacks,
													 "test_page_store_read");
	previous_executor_start = ExecutorStart_hook;
	ExecutorStart_hook = remote_executor_start;
	previous_executor_run = ExecutorRun_hook;
	ExecutorRun_hook = remote_executor_run;
}

/*
 * Checking just buffer misses would let cached scans continue after promotion
 * or (in frozen mode) replay advancement.  These are guards for the experiment,
 * not a general-purpose provider lease or promotion protocol.
 */
static void
remote_executor_start(QueryDesc *queryDesc, int eflags)
{
	if (nselected > 0)
		compute_check();
	if (previous_executor_start)
		previous_executor_start(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
remote_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	if (nselected > 0)
		compute_check();
	if (previous_executor_run)
		previous_executor_run(queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
	if (nselected > 0)
		compute_check();
}

static bool
selected_locator(RelFileLocator locator)
{
	return selected_relation(locator) >= 0;
}

static int
selected_relation(RelFileLocator locator)
{
	for (int i = 0; i < nselected; i++)
		if (RelFileLocatorEquals(selected_locators[i], locator))
			return i;
	return -1;
}

/* Test configuration only; preserve the original single-locator spelling. */
static void
parse_remote_locators(void)
{
	char	   *raw = pstrdup(page_locators);
	List	   *names;

	if (!SplitGUCList(raw, ',', &names))
		elog(ERROR, "invalid test_page_store.locators list");
	if (names == NIL)
	{
		if (page_rel != 0)
			selected_locators[nselected++] = (RelFileLocator)
		{
			page_spc, page_db, page_rel
		};
	}
	else if (page_spc != 0 || page_db != 0 || page_rel != 0)
		elog(ERROR, "test_page_store.locators cannot be combined with a single locator");
	foreach_ptr(char, name, names)
	{
		char	   *end = name;
		Oid			parts[3];
		RelFileLocator locator;

		if (nselected == COMPUTE_MAX_RELATIONS)
			elog(ERROR, "too many remote relations");
		for (int i = 0; i < 3; i++)
		{
			if (*end < '0' || *end > '9')
				elog(ERROR, "invalid remote physical locator: %s", name);
			parts[i] = uint32in_subr(end, &end, "physical locator", NULL);
			if (i < 2 && *end++ != '/')
				elog(ERROR, "invalid remote physical locator: %s", name);
		}
		if (*end != '\0' || parts[0] == InvalidOid || parts[2] == InvalidOid)
			elog(ERROR, "invalid remote physical locator: %s", name);
		locator = (RelFileLocator)
		{
			parts[0], parts[1], parts[2]
		};
		if (selected_locator(locator))
			elog(ERROR, "duplicate remote physical locator: %s", name);
		selected_locators[nselected++] = locator;
	}
	list_free(names);
	pfree(raw);
}

static ComputeFile *
compute_file(RelFileLocator locator, ForkNumber forknum)
{
	int			relation = selected_relation(locator);

	Assert(relation >= 0);
	return &compute->files[relation][forknum];
}

static bool
remote_relation(SMgrRelation reln, ForkNumber forknum)
{
	if (SmgrIsTemp(reln) || !selected_locator(reln->smgr_rlocator.locator))
		return false;
	if (follow_replay)
		return compute_active() && (forknum == MAIN_FORKNUM || forknum == VISIBILITYMAP_FORKNUM);
	return !AmStartupProcess();
}

static bool
compute_active(void)
{
	if (!compute || pg_atomic_read_u32(&compute->active) == 0)
		return false;
	/* Pair with publication in compute_replay_start()/compute_after_replay(). */
	pg_read_barrier();
	return true;
}

static void
compute_shmem_request(void *arg)
{
	if (!follow_replay)
		return;
	ShmemRequestStruct(.name = "test_page_store compute",
					   .size = sizeof(ComputeControl), .ptr = (void **) &compute);
	ShmemRequestStruct(.name = "test_page_store required LSN",
					   .size = mul_size(mul_size(nselected * (MAX_FORKNUM + 1), follow_max_blocks),
										sizeof(pg_atomic_uint64)),
					   .ptr = (void **) &required_lsn);
}

static void
compute_shmem_init(void *arg)
{
	if (!compute)
		return;
	pg_atomic_init_u32(&compute->active, 0);
	for (int i = 0; i < nselected; i++)
	{
		for (ForkNumber forknum = MAIN_FORKNUM; forknum <= MAX_FORKNUM; forknum++)
		{
			pg_atomic_init_u32(&compute->files[i][forknum].nblocks, 0);
			pg_atomic_init_u32(&compute->files[i][forknum].exists, 0);
		}
	}
	pg_atomic_init_u64(&compute->completed, InvalidXLogRecPtr);
	pg_atomic_init_u64(&compute->skipped, 0);
	pg_atomic_init_u64(&compute->cached, 0);
	pg_atomic_init_u64(&compute->fetches, 0);
	pg_atomic_init_u64(&compute->startup_fetches, 0);
	ConditionVariableInit(&compute->replayed);
	for (int i = 0; i < nselected * (MAX_FORKNUM + 1) * follow_max_blocks; i++)
		pg_atomic_init_u64(&required_lsn[i], InvalidXLogRecPtr);
}

static void
compute_check(void)
{
	TimeLineID	tli;

	if (!follow_replay)
	{
		test_page_store_check_cut(page_tli, pg_lsn_in_safe(page_lsn, NULL));
		return;
	}
	if (!RecoveryInProgress() || !compute_active())
		elog(ERROR, "following page compute is not active in recovery");
	(void) GetXLogReplayRecPtr(&tli);
	if (tli != page_tli)
		elog(ERROR, "following page compute cannot change timeline");
	for (int i = 0; i < nselected; i++)
		if (!pg_atomic_read_u32(&compute->files[i][MAIN_FORKNUM].exists))
			elog(ERROR, "a selected remote relation was dropped");
}

static void
compute_note_lsn(RelFileLocator locator, ForkNumber forknum, BlockNumber block, XLogRecPtr lsn)
{
	uint64		old;
	int			relation = selected_relation(locator);
	pg_atomic_uint64 *marker;

	Assert(relation >= 0);
	if (block >= follow_max_blocks)
		elog(ERROR, "remote relation exceeds follow_max_blocks");
	marker = &required_lsn[(relation * (MAX_FORKNUM + 1) + forknum) * follow_max_blocks + block];
	old = pg_atomic_read_u64(marker);
	while (old < lsn && !pg_atomic_compare_exchange_u64(marker, &old, lsn))
		;
}

/*
 * Shared buffers are empty at startup, so replay can rebuild the per-page
 * markers.  File metadata must describe the state just before the first redo
 * record, not the provider's latest state.  The seed's checkpoint REDO point
 * must therefore be covered by retained history.
 */
static void
compute_replay_start(XLogRecPtr start, TimeLineID tli)
{
	XLogRecPtr	cut;
	XLogRecPtr	baseline;

	if (previous_replay_start_hook)
		previous_replay_start_hook(start, tli);
	if (!compute)
		return;
	baseline = pg_lsn_in_safe(page_lsn, NULL);
	/* Older fixtures replay their local seed up to an exact baseline. */
	if (start < baseline)
		return;
	if (tli != page_tli)
		elog(ERROR, "following page compute cannot change timeline");
	cut = remote_history_before(start);
	if (cut < baseline || cut > start)
		elog(ERROR, "invalid page history recovery boundary");
	for (int i = 0; i < nselected; i++)
	{
		SMgrRelation smgr = smgropen(selected_locators[i], INVALID_PROC_NUMBER);

		for (ForkNumber forknum = MAIN_FORKNUM; forknum <= MAX_FORKNUM; forknum++)
		{
			bool		exists;
			BlockNumber size;
			TimestampTz deadline;

			if (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
				continue;
			deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), request_timeout);
			exists = remote_fetch_at(selected_locators[i], forknum, 0, NULL, 0,
									 &size, cut, deadline);
			if (forknum == MAIN_FORKNUM && !exists)
				elog(ERROR, "selected relation is absent from the retained recovery boundary");
			if (size > follow_max_blocks)
				elog(ERROR, "retained relation exceeds follow_max_blocks");
			pg_atomic_write_u32(&compute->files[i][forknum].nblocks, size);
			pg_atomic_write_u32(&compute->files[i][forknum].exists, exists);
			smgr->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
		}
	}
	pg_atomic_write_u64(&compute->completed, cut);
	pg_write_barrier();
	pg_atomic_write_u32(&compute->active, 1);
}

/* Publish a completed record, or activate at a legacy local-seed baseline. */
static void
compute_after_replay(XLogReaderState *record, TimeLineID tli)
{
	if (previous_replay_hook)
		previous_replay_hook(record, tli);
	if (!compute)
		return;
	if (!compute_active())
	{
		XLogRecPtr	baseline = pg_lsn_in_safe(page_lsn, NULL);

		if (record->EndRecPtr < baseline)
			return;
		if (record->EndRecPtr != baseline || tli != page_tli || nselected == 0)
			elog(ERROR, "following compute must replay its exact configured baseline");
		for (int i = 0; i < nselected; i++)
		{
			SMgrRelation smgr = smgropen(selected_locators[i], INVALID_PROC_NUMBER);

			if (!smgrexists(smgr, MAIN_FORKNUM))
				elog(ERROR, "selected relation is absent from the compute seed");
			for (ForkNumber forknum = MAIN_FORKNUM; forknum <= MAX_FORKNUM; forknum++)
			{
				bool		exists;
				BlockNumber size;

				if (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
					continue;
				exists = smgrexists(smgr, forknum);
				size = exists ? smgrnblocks(smgr, forknum) : 0;
				if (size > follow_max_blocks)
					elog(ERROR, "compute seed exceeds follow_max_blocks");
				pg_atomic_write_u32(&compute->files[i][forknum].nblocks, size);
				pg_atomic_write_u32(&compute->files[i][forknum].exists, exists);
			}
		}
		pg_atomic_write_u64(&compute->completed, baseline);
		pg_write_barrier();
		pg_atomic_write_u32(&compute->active, 1);
	}
	if (tli != page_tli)
		elog(ERROR, "following page compute cannot change timeline");
	/* All rmgr effects and consistency checks precede this publication. */
	pg_atomic_write_u64(&compute->completed, record->EndRecPtr);
	ConditionVariableBroadcast(&compute->replayed);
}

static bool
compute_redo_filter(XLogReaderState *record, uint8 block_id, ReadBufferMode mode)
{
	RelFileLocator locator;
	ForkNumber	forknum;
	BlockNumber block;
	BufferTag	tag;
	uint32		hash;
	LWLock	   *lock;
	bool		skip;
	SMgrRelation smgr;
	ComputeFile *file;
	static XLogRecPtr checked_record;
	static BufferTag checked_blocks[XLR_MAX_BLOCK_ID + 1];
	static int	nchecked;

	XLogRecGetBlockTag(record, block_id, &locator, &forknum, &block);
	if (!compute_active() ||
		(forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM) ||
		!selected_locator(locator))
		return previous_filter_hook ? previous_filter_hook(record, block_id, mode) : false;
	if (XLogRecGetRmid(record) != RM_HEAP_ID && XLogRecGetRmid(record) != RM_HEAP2_ID &&
		XLogRecGetRmid(record) != RM_BTREE_ID && XLogRecGetRmid(record) != RM_XLOG_ID)
		elog(ERROR, "following compute supports heap and B-tree main/VM redo only");
	if (block >= follow_max_blocks)
		elog(ERROR, "remote relation exceeds follow_max_blocks");
	file = compute_file(locator, forknum);
	InitBufferTag(&tag, &locator, forknum, block);
	if (checked_record != record->ReadRecPtr)
	{
		nchecked = 0;
		checked_record = record->ReadRecPtr;
	}

	/*
	 * Audited heap and B-tree paths visit each main or VM block once.  A
	 * second visit could otherwise wait for a backend whose read awaits this
	 * record's completion.  Read-only B-tree descent and sibling walks drop
	 * content locks before reading another page; split and unlink redo can
	 * therefore retain their other page locks while a miss waits here.
	 */
	for (int i = 0; i < nchecked; i++)
		if (BufferTagsEqual(&checked_blocks[i], &tag))
			elog(ERROR, "following compute cannot revisit a block in one WAL record");
	Assert(nchecked < lengthof(checked_blocks));
	checked_blocks[nchecked++] = tag;

	/* A VM can first be created by a heap record, without SMGR_CREATE. */
	if (forknum == VISIBILITYMAP_FORKNUM)
		pg_atomic_write_u32(&file->exists, 1);
	if (pg_atomic_read_u32(&file->nblocks) <= block)
		pg_atomic_write_u32(&file->nblocks, block + 1);
	smgr = smgropen(locator, INVALID_PROC_NUMBER);
	if (smgr->smgr_cached_nblocks[forknum] != InvalidBlockNumber)
		smgr->smgr_cached_nblocks[forknum] = pg_atomic_read_u32(&file->nblocks);

	/*
	 * Init callers need a real buffer, and xlog_redo() requires BLK_RESTORED
	 * for standalone full-page images.  Preserve both contracts.  Restoring
	 * an image itself does not require fetching the previous remote page.
	 */
	if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK ||
		XLogRecGetRmid(record) == RM_XLOG_ID)
		return false;
	hash = BufTableHashCode(&tag);
	lock = BufMappingPartitionLock(hash);
	LWLockAcquire(lock, LW_SHARED);

	/*
	 * A mapped but not-yet-valid buffer belongs to an admitted read, not an
	 * absent page.  Startup might win input I/O ownership if the reader has
	 * pinned it but not yet started I/O; otherwise it waits for the reader.
	 */
	skip = BufTableLookup(&tag, hash) < 0;
	if (skip)
		compute_note_lsn(locator, forknum, block, record->EndRecPtr);
	LWLockRelease(lock);
	if (skip)
	{
		pg_atomic_fetch_add_u64(&compute->skipped, 1);
		if (forknum == VISIBILITYMAP_FORKNUM)
			INJECTION_POINT("test-page-store-after-vm-skip", NULL);
		/* Do not let an earlier pruning record satisfy the UPDATE schedule. */
		if (forknum == MAIN_FORKNUM && XLogRecGetRmid(record) == RM_HEAP_ID &&
			(XLogRecGetInfo(record) & XLOG_HEAP_OPMASK) == XLOG_HEAP_UPDATE)
			INJECTION_POINT("test-page-store-after-update-skip", NULL);
		if (XLogRecGetRmid(record) == RM_BTREE_ID && block_id == 0 &&
			((XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_BTREE_SPLIT_L ||
			 (XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_BTREE_SPLIT_R))
			INJECTION_POINT("test-page-store-after-btree-split-skip", NULL);
	}
	else
		pg_atomic_fetch_add_u64(&compute->cached, 1);
	return skip;
}

/* Page reads arrive after buffer admission; count == 0 asks only for metadata. */
static XLogRecPtr
compute_read_lsn(RelFileLocator locator, ForkNumber forknum, BlockNumber block,
				 BlockNumber count, TimestampTz deadline)
{
	XLogRecPtr	lsn = pg_atomic_read_u64(&compute->completed);
	XLogRecPtr	needed = lsn;
	int			relation = selected_relation(locator);

	Assert(relation >= 0);
	compute_check();
	if ((uint64) block + count > follow_max_blocks)
		elog(ERROR, "remote read exceeds follow_max_blocks");
	for (BlockNumber i = 0; i < count; i++)
		needed = Max(needed, pg_atomic_read_u64(&required_lsn[(relation * (MAX_FORKNUM + 1) + forknum) * follow_max_blocks + block + i]));
	if (!AmStartupProcess() && needed > lsn)
	{
		uint32		wait_event = WaitEventExtensionNew("TestPageStoreReplay");

		ConditionVariablePrepareToSleep(&compute->replayed);
		while (pg_atomic_read_u64(&compute->completed) < needed)
		{
			long		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);

			if (remaining <= 0)
			{
				ConditionVariableCancelSleep();
				elog(ERROR, "timed out waiting for compute replay");
			}
			ConditionVariableTimedSleep(&compute->replayed, remaining, wait_event);
		}
		ConditionVariableCancelSleep();
	}
	return needed;
}

static void
remote_disconnect(int code, Datum arg)
{
	if (page_result)
		PQclear(page_result);
	page_result = NULL;
	if (page_conn)
		PQfinish(page_conn);
	page_conn = NULL;
}

/*
 * SMgr callers hold interrupts, and may already hold locks.  In particular,
 * processing a SMgr release barrier here would reenter the SMgr callbacks.
 * Use nonblocking libpq and a deadline, but do not process interrupts here.
 * A transport worker and a cancellable request lifetime are still needed
 * before this can be used as a general-purpose remote storage manager.
 */
static void
remote_wait(int event, TimestampTz deadline)
{
	long		remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);

	if (remaining <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("page service request timed out")));
	ResetLatch(MyLatch);
	(void) WaitLatchOrSocket(MyLatch,
							 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT | event,
							 PQsocket(page_conn), remaining, PG_WAIT_EXTENSION);
}

static void
remote_connect(TimestampTz deadline)
{
	PostgresPollingStatusType status;

	if (page_conn)
		return;
	if (page_conninfo[0] == '\0')
		elog(ERROR, "test_page_store.conninfo is required");
	if (!cleanup_registered)
	{
		on_proc_exit(remote_disconnect, 0);
		cleanup_registered = true;
	}
	page_conn = PQconnectStart(page_conninfo);
	if (page_conn == NULL)
		elog(ERROR, "could not allocate page service connection");
	for (;;)
	{
		status = PQconnectPoll(page_conn);
		if (status == PGRES_POLLING_OK)
			break;
		if (status == PGRES_POLLING_FAILED)
			elog(ERROR, "could not connect to page service: %s", PQerrorMessage(page_conn));
		remote_wait(status == PGRES_POLLING_READING ? WL_SOCKET_READABLE : WL_SOCKET_WRITEABLE,
					deadline);
	}
	if (PQsetnonblocking(page_conn, 1) != 0)
		elog(ERROR, "could not set page service connection to nonblocking mode");
}

/* Caller owns the result and must disconnect if validation raises an error. */
static void
remote_query(const char *query, int nparams, const char *const *values,
			 TimestampTz deadline)
{
	int			flushed;

	Assert(page_result == NULL);
	remote_connect(deadline);
	if (!PQsendQueryParams(page_conn, query, nparams, NULL, values, NULL, NULL, 1))
		elog(ERROR, "could not send page service request: %s", PQerrorMessage(page_conn));
	while ((flushed = PQflush(page_conn)) > 0)
		remote_wait(WL_SOCKET_WRITEABLE, deadline);
	if (flushed < 0)
		elog(ERROR, "could not flush page service request: %s", PQerrorMessage(page_conn));
	for (;;)
	{
		PGresult   *result;

		while (PQisBusy(page_conn))
		{
			remote_wait(WL_SOCKET_READABLE, deadline);
			if (!PQconsumeInput(page_conn))
				elog(ERROR, "could not receive page service response: %s", PQerrorMessage(page_conn));
		}
		result = PQgetResult(page_conn);
		if (result == NULL)
			break;
		if (page_result)
		{
			PQclear(result);
			elog(ERROR, "unexpected extra page service response");
		}
		page_result = result;
	}
	if (!page_result || PQresultStatus(page_result) != PGRES_TUPLES_OK)
		elog(ERROR, "page service request failed: %s",
			 page_result ? PQresultErrorMessage(page_result) : PQerrorMessage(page_conn));
}

static XLogRecPtr
remote_history_before(XLogRecPtr start)
{
	char		params[3][32];
	const char *values[3];
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), request_timeout);
	uint64		result = InvalidXLogRecPtr;

	snprintf(params[0], 32, UINT64_FORMAT, GetSystemIdentifier());
	snprintf(params[1], 32, "%d", page_tli);
	snprintf(params[2], 32, "%X/%X", LSN_FORMAT_ARGS(start));
	for (int i = 0; i < 3; i++)
		values[i] = params[i];
	PG_TRY();
	{
		remote_query("SELECT public.test_page_store_history_before("
					 "$1::text, $2::bigint, $3::pg_lsn)", 3, values, deadline);
		if (PQntuples(page_result) != 1 || PQnfields(page_result) != 1 ||
			PQgetisnull(page_result, 0, 0) ||
			PQgetlength(page_result, 0, 0) != sizeof(result))
			elog(ERROR, "invalid page history recovery boundary response");
		memcpy(&result, PQgetvalue(page_result, 0, 0), sizeof(result));
		result = pg_ntoh64(result);
		PQclear(page_result);
		page_result = NULL;
	}
	PG_CATCH();
	{
		remote_disconnect(0, (Datum) 0);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

static bool
remote_fetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 void **buffers, BlockNumber count, BlockNumber *nblocks)
{
	XLogRecPtr	lsn = pg_lsn_in_safe(page_lsn, NULL);
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), request_timeout);
	bool		exists;

	Assert(!INTERRUPTS_CAN_BE_PROCESSED());
	if (follow_replay)
	{
		lsn = compute_read_lsn(reln->smgr_rlocator.locator, forknum, blocknum, count, deadline);
		if (count > 0)
		{
			pg_atomic_fetch_add_u64(&compute->fetches, 1);
			if (AmStartupProcess())
				pg_atomic_fetch_add_u64(&compute->startup_fetches, 1);
		}
	}
	else
		compute_check();
	if (count > 0)
		INJECTION_POINT("test-page-store-before-remote-fetch", NULL);
	exists = remote_fetch_at(reln->smgr_rlocator.locator, forknum, blocknum,
							 buffers, count, nblocks, lsn, deadline);
	compute_check();
	return exists;
}

/* The explicit cut also allows metadata lookup before compute activation. */
static bool
remote_fetch_at(RelFileLocator locator, ForkNumber forknum, BlockNumber blocknum,
				void **buffers, BlockNumber count, BlockNumber *nblocks,
				XLogRecPtr lsn, TimestampTz deadline)
{
	char		params[10][32];
	const char *values[10];
	bool		exists = false;
	uint64		size;

	Assert(count <= TEST_PAGE_STORE_MAX_BLOCKS);
	snprintf(params[0], 32, "%u", locator.spcOid);
	snprintf(params[1], 32, "%u", locator.dbOid);
	snprintf(params[2], 32, "%u", locator.relNumber);
	snprintf(params[3], 32, "%d", forknum);
	snprintf(params[4], 32, "%u", blocknum);
	snprintf(params[5], 32, "%u", count);
	snprintf(params[6], 32, UINT64_FORMAT, GetSystemIdentifier());
	snprintf(params[7], 32, "%d", page_tli);
	snprintf(params[8], 32, "%X/%X", LSN_FORMAT_ARGS(lsn));
	strlcpy(params[9], follow_replay ? "true" : "false", 32);
	for (int i = 0; i < 10; i++)
		values[i] = params[i];

	PG_TRY();
	{
		remote_query("SELECT fork_exists, nblocks, pages FROM public.test_page_store_fetch("
					 "$1::oid, $2::oid, $3::oid, $4::int, $5::bigint, $6::int, "
					 "$7::text, $8::bigint, $9::pg_lsn, $10::boolean)",
					 10, values, deadline);
		if (PQntuples(page_result) != 1 || PQnfields(page_result) != 3 ||
			PQgetisnull(page_result, 0, 0) || PQgetisnull(page_result, 0, 1) ||
			PQgetisnull(page_result, 0, 2) ||
			PQgetlength(page_result, 0, 0) != 1 ||
			PQgetlength(page_result, 0, 1) != sizeof(uint64) ||
			PQgetlength(page_result, 0, 2) != count * BLCKSZ)
			elog(ERROR, "invalid page service response");
		exists = PQgetvalue(page_result, 0, 0)[0] != 0;
		memcpy(&size, PQgetvalue(page_result, 0, 1), sizeof(size));
		size = pg_ntoh64(size);
		if (size > InvalidBlockNumber || (!exists && size != 0) ||
			(count > 0 && (!exists || (uint64) blocknum + count > size)))
			elog(ERROR, "invalid page service relation size");
		*nblocks = (BlockNumber) size;
		for (BlockNumber i = 0; i < count; i++)
			memcpy(buffers[i], PQgetvalue(page_result, 0, 2) + i * BLCKSZ, BLCKSZ);
		PQclear(page_result);
		page_result = NULL;
	}
	PG_CATCH();
	{
		remote_disconnect(0, (Datum) 0);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return exists;
}

static bool
remote_exists(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next)
{
	BlockNumber nblocks;

	if (!remote_relation(reln, forknum))
		return smgr_exists_next(reln, forknum, next + 1);
	if (follow_replay && AmStartupProcess())
		return pg_atomic_read_u32(&compute_file(reln->smgr_rlocator.locator, forknum)->exists) != 0;
	return remote_fetch(reln, forknum, 0, NULL, 0, &nblocks);
}

static void
remote_create(RelFileLocator old, SMgrRelation reln, ForkNumber forknum,
			  bool isRedo, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_create_next(old, reln, forknum, isRedo, next + 1);
	else
		compute_check();
}

static void
remote_unlink(RelFileLocatorBackend locator, ForkNumber forknum,
			  bool isRedo, SmgrChainIndex next)
{
	SMgrRelation smgr = smgropen(locator.locator, locator.backend);

	if (follow_replay && remote_relation(smgr, forknum))
	{
		ComputeFile *file = compute_file(locator.locator, forknum);

		pg_atomic_write_u32(&file->exists, 0);
		pg_atomic_write_u32(&file->nblocks, 0);
	}
	else
		smgr_unlink_next(smgr, locator, forknum, isRedo, next + 1);
}

static void
remote_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber block,
				  int count, bool skipFsync, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_zeroextend_next(reln, forknum, block, count, skipFsync, next + 1);
	else
	{
		ComputeFile *file;

		if (!follow_replay || !AmStartupProcess())
			elog(ERROR, "only redo may extend the remote compute relation");
		file = compute_file(reln->smgr_rlocator.locator, forknum);
		if ((uint64) block + count > follow_max_blocks)
			elog(ERROR, "remote relation exceeds follow_max_blocks");
		if (pg_atomic_read_u32(&file->nblocks) < block + count)
			pg_atomic_write_u32(&file->nblocks, block + count);
	}
}

static void
remote_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber block,
			  const void *buffer, bool skipFsync, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_extend_next(reln, forknum, block, buffer, skipFsync, next + 1);
	else
	{
		if (!PageIsNew((Page) buffer))
			elog(ERROR, "following compute cannot discard initialized extension data");
		remote_zeroextend(reln, forknum, block, 1, skipFsync, next);
	}
}

static void
remote_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old,
				BlockNumber size, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_truncate_next(reln, forknum, old, size, next + 1);
	else if (follow_replay && AmStartupProcess())
		pg_atomic_write_u32(&compute_file(reln->smgr_rlocator.locator, forknum)->nblocks, size);
	else
		elog(ERROR, "only redo may truncate the remote compute relation");
}

static void
remote_immedsync(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_immedsync_next(reln, forknum, next + 1);
}

static void
remote_registersync(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_registersync_next(reln, forknum, next + 1);
}

static int
remote_fd(SMgrRelation reln, ForkNumber forknum, BlockNumber block,
		  uint32 *off, SmgrChainIndex next)
{
	if (remote_relation(reln, forknum))
		elog(ERROR, "remote page relation has no local file descriptor");
	return smgr_fd_next(reln, forknum, block, off, next + 1);
}

static BlockNumber
remote_nblocks(SMgrRelation reln, ForkNumber forknum, SmgrChainIndex next)
{
	BlockNumber nblocks;

	if (!remote_relation(reln, forknum))
		return smgr_nblocks_next(reln, forknum, next + 1);
	if (follow_replay && AmStartupProcess())
		return pg_atomic_read_u32(&compute_file(reln->smgr_rlocator.locator, forknum)->nblocks);
	if (!remote_fetch(reln, forknum, 0, NULL, 0, &nblocks))
		elog(ERROR, "remote relation fork does not exist");
	return nblocks;
}

static bool
remote_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				int nblocks, SmgrChainIndex next)
{
	if (remote_relation(reln, forknum))
	{
		/*
		 * Prefetch is advisory.  False means a missing file, not lack of
		 * prefetch support, and makes WAL prefetching report an error.
		 */
		return true;
	}
	return smgr_prefetch_next(reln, forknum, blocknum, nblocks, next + 1);
}

static uint32
remote_maxcombine(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				  SmgrChainIndex next)
{
	if (remote_relation(reln, forknum))
	{
		/*
		 * Do not own I/O for another page while waiting for a skipped record
		 * to finish.  Redo may need that other page to complete the record. A
		 * future transport must arrange per-page admission/completion before
		 * supporting combined reads on a following compute.
		 */
		return follow_replay ? 1 : TEST_PAGE_STORE_MAX_BLOCKS;
	}
	return smgr_maxcombine_next(reln, forknum, blocknum, next + 1);
}

static void
remote_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 void **buffers, BlockNumber nblocks, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
	{
		smgr_readv_next(reln, forknum, blocknum, buffers, nblocks, next + 1);
		return;
	}
	readv_count++;
	while (nblocks > 0)
	{
		BlockNumber count = Min(nblocks, TEST_PAGE_STORE_MAX_BLOCKS);
		BlockNumber size;

		remote_fetch(reln, forknum, blocknum, buffers, count, &size);
		blocknum += count;
		buffers += count;
		nblocks -= count;
	}
}

static PgAioResult
remote_read_complete(PgAioHandle *ioh, PgAioResult result, uint8 flags)
{
	Assert(result.result == pgaio_io_get_target_data(ioh)->smgr.nblocks * BLCKSZ);
	result.result /= BLCKSZ;
	return result;
}

static void
remote_startreadv(PgAioHandle *ioh, SMgrRelation reln, ForkNumber forknum,
				  BlockNumber blocknum, void **buffers, BlockNumber nblocks,
				  SmgrChainIndex next)
{
	struct iovec *iov;
	int			capacity;
	BlockNumber size;

	if (!remote_relation(reln, forknum))
	{
		smgr_startreadv_next(ioh, reln, forknum, blocknum, buffers, nblocks, next + 1);
		return;
	}
	startreadv_count++;
	capacity = pgaio_io_get_iovec(ioh, &iov);
	if (nblocks > capacity || nblocks > TEST_PAGE_STORE_MAX_BLOCKS ||
		(follow_replay && nblocks != 1))
		elog(ERROR, "remote read exceeds the batch limit");
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		iov[i].iov_base = buffers[i];
		iov[i].iov_len = BLCKSZ;
	}
	remote_fetch(reln, forknum, blocknum, buffers, nblocks, &size);
	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	pgaio_io_register_callbacks(ioh, read_callback, 0);
	pgaio_io_complete_readv(ioh, nblocks, nblocks * BLCKSZ);
}

/* Discard writes, but never let eviction lose a replayed page's lower bound. */
static void
remote_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  const void **buffers, BlockNumber nblocks, bool skipFsync, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_writev_next(reln, forknum, blocknum, buffers, nblocks, skipFsync, next + 1);
	else
	{
		compute_check();
		if (follow_replay)
			for (BlockNumber i = 0; i < nblocks; i++)
				if (!PageIsNew((Page) buffers[i]))
					compute_note_lsn(reln->smgr_rlocator.locator, forknum, blocknum + i,
									 PageGetLSN((Page) buffers[i]));
	}
}

static void
remote_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				 BlockNumber nblocks, SmgrChainIndex next)
{
	if (!remote_relation(reln, forknum))
		smgr_writeback_next(reln, forknum, blocknum, nblocks, next + 1);
}

Datum
test_page_store_io_counts(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = Int64GetDatum(readv_count);
	values[1] = Int64GetDatum(startreadv_count);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
test_page_store_compute_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[6];
	bool		nulls[6] = {false};

	if (!compute)
		elog(ERROR, "following compute is not configured");
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = BoolGetDatum(compute_active());
	values[1] = LSNGetDatum(pg_atomic_read_u64(&compute->completed));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&compute->skipped));
	values[3] = Int64GetDatum(pg_atomic_read_u64(&compute->cached));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&compute->fetches));
	values[5] = Int64GetDatum(pg_atomic_read_u64(&compute->startup_fetches));
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
test_page_store_read_smgr(PG_FUNCTION_ARGS)
{
	Relation	rel;
	bytea	   *page;
	int32		block = PG_GETARG_INT32(1);

	if (!superuser())
		elog(ERROR, "must be superuser to use page service prototype");
	if (follow_replay)
		elog(ERROR, "direct reads bypass following compute's buffer admission protocol");
	if (block < 0)
		elog(ERROR, "invalid block number");
	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	page = (bytea *) palloc(VARHDRSZ + BLCKSZ);
	SET_VARSIZE(page, VARHDRSZ + BLCKSZ);
	smgrread(RelationGetSmgr(rel), MAIN_FORKNUM, block, VARDATA(page));
	relation_close(rel, AccessShareLock);
	PG_RETURN_BYTEA_P(page);
}
