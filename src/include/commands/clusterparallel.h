/*-------------------------------------------------------------------------
 *
 * clusterparallel.h
 *	  header file for parallel cluster/vacuum full support
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/clusterparallel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTERPARALLEL_H
#define CLUSTERPARALLEL_H

#include "access/parallel.h"
#include "nodes/parsenodes.h"
#include "storage/bufmgr.h"
#include "utils/relcache.h"

/* Abstract type for parallel cluster state */
typedef struct ParallelClusterState ParallelClusterState;

/* in commands/clusterparallel.c */
extern ParallelClusterState *parallel_cluster_init(Relation old_rel,
												   Relation new_rel,
												   int nrequested_workers,
												   TransactionId OldestXmin,
												   TransactionId FreezeLimit,
												   MultiXactId MultiXactCutoff,
												   int elevel);
extern void parallel_cluster_end(ParallelClusterState *pcs,
								 double *num_tuples,
								 double *tups_vacuumed,
								 double *tups_recently_dead);
extern void parallel_cluster_process_heap(ParallelClusterState *pcs);
extern void parallel_cluster_main(dsm_segment *seg, shm_toc *toc);

#endif							/* CLUSTERPARALLEL_H */

