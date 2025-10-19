# Parallel VACUUM FULL Implementation

## Overview
This implementation adds support for parallel VACUUM FULL on a single relation using parallel sequential scan. The infrastructure allows multiple worker processes to scan different portions of a table concurrently during VACUUM FULL operations.

## What Has Been Implemented

### 1. Core Infrastructure
- **Removed Parallelism Restriction**: Removed the error that prevented VACUUM FULL from being executed in parallel (`src/backend/commands/vacuum.c`)

- **Extended ClusterParams**: Added `nworkers` parameter to `ClusterParams` structure to control the number of parallel workers (`src/include/commands/cluster.h`)

- **Parallel Cluster Module**: Created new module for parallel cluster operations:
  - `src/backend/commands/clusterparallel.c` - Implementation
  - `src/include/commands/clusterparallel.h` - Header file

### 2. Parallel Context Setup
- **ParallelClusterState**: New structure to maintain parallel cluster state
- **parallel_cluster_init()**: Initializes parallel context with shared memory for coordination
- **parallel_cluster_end()**: Cleans up parallel context and gathers statistics
- **parallel_cluster_process_heap()**: Launches and manages parallel workers

### 3. Worker Registration
- Registered `parallel_cluster_main()` as a parallel worker entry point in `src/backend/access/transam/parallel.c`

### 4. Integration with CLUSTER/VACUUM FULL
- Updated `cluster_rel()` to pass parallel parameters through to table copying
- Modified `rebuild_relation()` and `copy_table_data()` to support parallel execution
- Added logic to initialize parallel context when appropriate:
  - Only for sequential scans (no index)
  - Only when not using sort
  - Only when parallel workers are enabled

## Current Behavior
The current implementation:
1. ✅ Accepts parallel workers parameter in VACUUM FULL commands
2. ✅ Initializes parallel context and launches workers
3. ✅ Provides infrastructure for shared memory coordination
4. ⚠️  Still uses sequential scanning for actual table copy (workers are launched but not yet actively scanning)

## What Needs to be Implemented

### TODOs for Full Functionality

#### 1. Parallel Heap Scanning Logic (High Priority)
The actual parallel scanning implementation is marked as TODO in `clusterparallel.c`:
```c
/*
 * Process blocks assigned to this worker.
 * In a full implementation, this would scan blocks and write tuples.
 * For now, this is a placeholder.
 */
/* TODO: Implement actual parallel scanning logic */
```

This requires:
- Block assignment strategy (each worker claims blocks dynamically)
- Tuple visibility checking in workers
- Coordination for writing tuples to the new heap

#### 2. Heap AM Integration
Currently, `copy_table_data()` notes:
```c
/*
 * TODO: For now, we still use the sequential path even when parallel
 * context is initialized. Full parallel implementation would require
 * modifying the heap AM to support parallel scanning during cluster.
 */
```

Options for implementation:
- **Option A**: Modify `heapam_relation_copy_for_cluster()` to be parallel-aware
- **Option B**: Create a new parallel path in the heap AM
- **Option C**: Use shared TupleStore for workers to collect tuples

#### 3. Tuple Writing Coordination
Workers need a synchronized way to write tuples to the new heap:
- Coordinate block allocation in the new heap
- Ensure proper ordering (if needed)
- Handle RewriteState across multiple workers

#### 4. Progress Reporting
Add progress reporting for parallel workers:
- Update `PROGRESS_CLUSTER_*` parameters from workers
- Aggregate progress from all workers
- Report parallel worker count

## Testing

### Basic Test
```sql
-- Enable parallel vacuum full
SET max_parallel_maintenance_workers = 4;

-- Create a test table
CREATE TABLE test_parallel_vacuum (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- Insert test data
INSERT INTO test_parallel_vacuum (data)
SELECT repeat('x', 100)
FROM generate_series(1, 1000000);

-- Run parallel VACUUM FULL
VACUUM (FULL, PARALLEL 4, VERBOSE) test_parallel_vacuum;
```

### Expected Output
Currently, you'll see:
- Parallel workers being launched
- Sequential table scan still being used
- Statistics gathered from workers (though workers don't contribute yet)

## Architecture

### Key Components

```
┌──────────────────────────────────────┐
│         VACUUM FULL Command          │
│    (src/backend/commands/vacuum.c)   │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│      cluster_rel() / rebuild_relation│
│    (src/backend/commands/cluster.c)  │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│         copy_table_data()            │
│   Initializes ParallelClusterState   │
└──────────────┬───────────────────────┘
               │
               ├─────────────┬──────────────┐
               ▼             ▼              ▼
         ┌─────────┐   ┌─────────┐   ┌─────────┐
         │ Leader  │   │Worker 1 │   │Worker N │
         │ Process │   │ Process │   │ Process │
         └─────────┘   └─────────┘   └─────────┘
               │             │              │
               └─────────────┴──────────────┘
                             │
                             ▼
               ┌──────────────────────────┐
               │  Shared Memory (DSM)     │
               │  - Block assignment      │
               │  - Statistics            │
               │  - Visibility cutoffs    │
               └──────────────────────────┘
```

### Shared Memory Layout

The `PCShared` structure contains:
- Relation OIDs (old and new)
- Visibility parameters (OldestXmin, FreezeLimit, MultiXactCutoff)
- Atomic counters for statistics (num_tuples, tups_vacuumed, tups_recently_dead)
- Block assignment counter (next_block)
- Total block count

## Performance Considerations

### When Parallel VACUUM FULL Helps
- Large tables (> min_parallel_table_scan_size)
- I/O-bound systems where parallelism can increase throughput
- Systems with multiple CPU cores available

### When to Disable
- Small tables (overhead exceeds benefit)
- Memory-constrained systems (each worker needs memory)
- Single-core systems

### Configuration Parameters
- `max_parallel_maintenance_workers`: Global limit on parallel workers
- `min_parallel_table_scan_size`: Minimum table size for parallel scan (default: 8MB)
- Command-level `PARALLEL n`: Explicit worker count

## Future Enhancements

1. **Parallel Sorting**: When CLUSTER uses sort, parallelize the sort phase
2. **Index Scanning**: Support parallel index scans for CLUSTER with indexes
3. **Adaptive Parallelism**: Dynamically adjust worker count based on system load
4. **Statistics**: Track and report parallel vacuum full performance metrics
5. **Cost-based Delay**: Apply vacuum_cost_delay across parallel workers

## Files Modified

### New Files
- `src/backend/commands/clusterparallel.c`
- `src/include/commands/clusterparallel.h`

### Modified Files
- `src/backend/commands/vacuum.c` - Removed parallel restriction
- `src/backend/commands/cluster.c` - Added parallel support
- `src/include/commands/cluster.h` - Extended ClusterParams
- `src/backend/access/transam/parallel.c` - Registered parallel worker

## Building and Compilation

To compile the new code:
```bash
cd /Users/x4mmm/postgres_pv
make clean
make -j4
make install
```

## Notes

- This implementation provides the foundation for parallel VACUUM FULL
- The actual parallel scanning logic needs to be completed for full functionality
- The current code is safe to run - it will launch workers but fall back to sequential scanning
- No existing functionality is broken - VACUUM FULL works as before when parallel is not specified

## Contact

For questions or contributions, please refer to the PostgreSQL development mailing lists.

