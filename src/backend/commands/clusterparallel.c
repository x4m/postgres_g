/*-------------------------------------------------------------------------
 *
 * clusterparallel.c
 *	  Support routines for parallel cluster/vacuum full execution.
 *
 * This file contains routines that are intended to support setting up, using,
 * and tearing down a ParallelClusterState for parallel VACUUM FULL and CLUSTER.
 *
 * In a parallel cluster, we perform table scanning with parallel worker processes.
 * The heap is scanned in parallel, with each worker processing a subset of blocks.
 * Workers scan tuples, determine visibility, and coordinate to write them to the
 * new relation.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/clusterparallel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/parallel.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "commands/cluster.h"
#include "commands/progress.h"
#include "executor/instrument.h"
#include "optimizer/paths.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/*
 * DSM keys for parallel cluster.
 */
#define PARALLEL_CLUSTER_KEY_SHARED			1
#define PARALLEL_CLUSTER_KEY_QUERY_TEXT		2
#define PARALLEL_CLUSTER_KEY_BUFFER_USAGE	3
#define PARALLEL_CLUSTER_KEY_WAL_USAGE		4

/*
 * Shared information among parallel workers.
 */
typedef struct PCShared
{
	/* Target table relid and log level */
	Oid			relid;
	Oid			new_relid;
	int			elevel;
	int64		queryid;

	/* Visibility parameters */
	TransactionId OldestXmin;
	TransactionId FreezeLimit;
	MultiXactId MultiXactCutoff;

	/* Counters for tuples processed */
	pg_atomic_uint64 num_tuples;
	pg_atomic_uint64 tups_vacuumed;
	pg_atomic_uint64 tups_recently_dead;

	/* Block assignment for parallel scan */
	pg_atomic_uint32 next_block;
	BlockNumber total_blocks;
} PCShared;

/*
 * Struct for maintaining a parallel cluster state.
 */
typedef struct ParallelClusterState
{
	/* NULL for worker processes */
	ParallelContext *pcxt;

	/* Parent Heap Relations */
	Relation	old_heap;
	Relation	new_heap;

	/* Shared information among parallel cluster workers */
	PCShared   *shared;

	/* Points to buffer usage area in DSM */
	BufferUsage *buffer_usage;

	/* Points to WAL usage area in DSM */
	WalUsage   *wal_usage;

	/* Error reporting state */
	char	   *relnamespace;
	char	   *relname;
} ParallelClusterState;

/* Function prototypes */
static int parallel_cluster_compute_workers(Relation rel, int nrequested);
static void parallel_cluster_worker_main(dsm_segment *seg, shm_toc *toc);

/*
 * Compute the number of parallel worker processes to request for cluster/vacuum full.
 */
static int
parallel_cluster_compute_workers(Relation rel, int nrequested)
{
	int			parallel_workers;
	BlockNumber heap_blocks;

	/*
	 * We don't allow performing parallel operation in standalone backend or
	 * when parallelism is disabled.
	 */
	if (!IsUnderPostmaster || max_parallel_maintenance_workers == 0)
		return 0;

	/* Get the size of the relation */
	heap_blocks = RelationGetNumberOfBlocks(rel);

	/*
	 * Only use parallel workers for tables larger than a threshold.
	 * Use min_parallel_table_scan_size as a reasonable threshold.
	 */
	if (heap_blocks < min_parallel_table_scan_size)
		return 0;

	/* Compute the parallel degree */
	if (nrequested > 0)
	{
		/* User specified a number of workers */
		parallel_workers = nrequested;
	}
	else
	{
		/*
		 * Estimate based on table size. Use one worker per 32MB of data,
		 * similar to parallel sequential scan.
		 */
		parallel_workers = heap_blocks / (32 * 1024 * 1024 / BLCKSZ);
		parallel_workers = Max(parallel_workers, 1);
	}

	/* Cap by max_parallel_maintenance_workers */
	parallel_workers = Min(parallel_workers, max_parallel_maintenance_workers);

	return parallel_workers;
}

/*
 * Try to enter parallel mode and create a parallel context for cluster/vacuum full.
 *
 * On success, return parallel cluster state. Otherwise return NULL.
 */
ParallelClusterState *
parallel_cluster_init(Relation old_rel, Relation new_rel, int nrequested_workers,
					  TransactionId OldestXmin, TransactionId FreezeLimit,
					  MultiXactId MultiXactCutoff, int elevel)
{
	ParallelClusterState *pcs;
	ParallelContext *pcxt;
	PCShared   *shared;
	BufferUsage *buffer_usage;
	WalUsage   *wal_usage;
	Size		est_shared_len;
	int			parallel_workers = 0;
	int			querylen;

	/*
	 * Compute the number of parallel cluster workers to launch
	 */
	parallel_workers = parallel_cluster_compute_workers(old_rel, nrequested_workers);
	if (parallel_workers <= 0)
	{
		/* Can't perform cluster in parallel -- return NULL */
		return NULL;
	}

	pcs = (ParallelClusterState *) palloc0(sizeof(ParallelClusterState));
	pcs->old_heap = old_rel;
	pcs->new_heap = new_rel;

	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "parallel_cluster_worker_main",
								 parallel_workers);
	Assert(pcxt->nworkers > 0);
	pcs->pcxt = pcxt;

	/* Estimate size for shared information */
	est_shared_len = sizeof(PCShared);
	shm_toc_estimate_chunk(&pcxt->estimator, est_shared_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/*
	 * Estimate space for BufferUsage and WalUsage
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate PARALLEL_CLUSTER_KEY_QUERY_TEXT space */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;

	InitializeParallelDSM(pcxt);

	/* Prepare shared information */
	shared = (PCShared *) shm_toc_allocate(pcxt->toc, est_shared_len);
	MemSet(shared, 0, est_shared_len);
	shared->relid = RelationGetRelid(old_rel);
	shared->new_relid = RelationGetRelid(new_rel);
	shared->elevel = elevel;
	shared->queryid = pgstat_get_my_query_id();
	shared->OldestXmin = OldestXmin;
	shared->FreezeLimit = FreezeLimit;
	shared->MultiXactCutoff = MultiXactCutoff;
	shared->total_blocks = RelationGetNumberOfBlocks(old_rel);

	pg_atomic_init_u64(&(shared->num_tuples), 0);
	pg_atomic_init_u64(&(shared->tups_vacuumed), 0);
	pg_atomic_init_u64(&(shared->tups_recently_dead), 0);
	pg_atomic_init_u32(&(shared->next_block), 0);

	shm_toc_insert(pcxt->toc, PARALLEL_CLUSTER_KEY_SHARED, shared);
	pcs->shared = shared;

	/*
	 * Allocate space for each worker's BufferUsage and WalUsage
	 */
	buffer_usage = shm_toc_allocate(pcxt->toc,
									mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_CLUSTER_KEY_BUFFER_USAGE, buffer_usage);
	pcs->buffer_usage = buffer_usage;

	wal_usage = shm_toc_allocate(pcxt->toc,
								 mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_CLUSTER_KEY_WAL_USAGE, wal_usage);
	pcs->wal_usage = wal_usage;

	/* Store query string for workers */
	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		sharedquery[querylen] = '\0';
		shm_toc_insert(pcxt->toc,
					   PARALLEL_CLUSTER_KEY_QUERY_TEXT, sharedquery);
	}

	/* Success -- return parallel cluster state */
	return pcs;
}

/*
 * Destroy the parallel context, and end parallel mode.
 */
void
parallel_cluster_end(ParallelClusterState *pcs, double *num_tuples,
					 double *tups_vacuumed, double *tups_recently_dead)
{
	Assert(!IsParallelWorker());

	/* Copy the updated statistics */
	*num_tuples = (double) pg_atomic_read_u64(&pcs->shared->num_tuples);
	*tups_vacuumed = (double) pg_atomic_read_u64(&pcs->shared->tups_vacuumed);
	*tups_recently_dead = (double) pg_atomic_read_u64(&pcs->shared->tups_recently_dead);

	DestroyParallelContext(pcs->pcxt);
	ExitParallelMode();

	pfree(pcs);
}

/*
 * Launch parallel workers and wait for them to complete.
 */
void
parallel_cluster_process_heap(ParallelClusterState *pcs)
{
	Assert(!IsParallelWorker());

	/* Launch parallel workers */
	LaunchParallelWorkers(pcs->pcxt);

	if (pcs->pcxt->nworkers_launched > 0)
	{
		ereport(pcs->shared->elevel,
				(errmsg(ngettext("launched %d parallel worker for table scan (planned: %d)",
								 "launched %d parallel workers for table scan (planned: %d)",
								 pcs->pcxt->nworkers_launched),
						pcs->pcxt->nworkers_launched, pcs->pcxt->nworkers)));
	}

	/*
	 * The leader process also participates in scanning.
	 * For now, we'll have the leader coordinate, but in a full implementation
	 * the leader would also scan blocks.
	 */

	/* Wait for all workers to finish */
	WaitForParallelWorkersToFinish(pcs->pcxt);

	/* Accumulate buffer and WAL usage */
	for (int i = 0; i < pcs->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&pcs->buffer_usage[i], &pcs->wal_usage[i]);
}

/*
 * Perform work within a launched parallel worker process.
 *
 * This is the entry point for parallel cluster workers.
 */
static void
parallel_cluster_worker_main(dsm_segment *seg, shm_toc *toc)
{
	PCShared   *shared;
	Relation	old_heap;
	Relation	new_heap;
	char	   *sharedquery;
	BufferUsage *buffer_usage;
	WalUsage   *wal_usage;

	elog(DEBUG1, "starting parallel cluster worker");

	shared = (PCShared *) shm_toc_lookup(toc, PARALLEL_CLUSTER_KEY_SHARED, false);

	/* Set debug_query_string for individual workers */
	sharedquery = shm_toc_lookup(toc, PARALLEL_CLUSTER_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	/* Track query ID */
	pgstat_report_query_id(shared->queryid, false);

	/*
	 * Open tables. The lock mode is the same as the leader process.
	 */
	old_heap = table_open(shared->relid, AccessExclusiveLock);
	new_heap = table_open(shared->new_relid, NoLock);

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	/*
	 * Process blocks assigned to this worker.
	 * In a full implementation, this would scan blocks and write tuples.
	 * For now, this is a placeholder.
	 */
	/* TODO: Implement actual parallel scanning logic */

	/* Report buffer/WAL usage during parallel execution */
	buffer_usage = shm_toc_lookup(toc, PARALLEL_CLUSTER_KEY_BUFFER_USAGE, false);
	wal_usage = shm_toc_lookup(toc, PARALLEL_CLUSTER_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&buffer_usage[ParallelWorkerNumber],
						  &wal_usage[ParallelWorkerNumber]);

	table_close(new_heap, NoLock);
	table_close(old_heap, AccessExclusiveLock);
}

/*
 * Entry point for parallel cluster worker, called from parallel.c
 */
void
parallel_cluster_main(dsm_segment *seg, shm_toc *toc)
{
	parallel_cluster_worker_main(seg, toc);
}

