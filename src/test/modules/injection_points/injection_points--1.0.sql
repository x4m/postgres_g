/* src/test/modules/injection_points/injection_points--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION injection_points" to load this file. \quit

--
-- injection_points_attach()
--
-- Attaches the action to the given injection point.
--
CREATE FUNCTION injection_points_attach(IN point_name TEXT,
    IN action text)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_load()
--
-- Load an injection point already attached.
--
CREATE FUNCTION injection_points_load(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_load'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_run()
--
-- Executes the action attached to the injection point.
--
CREATE FUNCTION injection_points_run(IN point_name TEXT,
    IN arg TEXT DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_run'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_cached()
--
-- Executes the action attached to the injection point, from local cache.
--
CREATE FUNCTION injection_points_cached(IN point_name TEXT,
    IN arg TEXT DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_cached'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_wakeup()
--
-- Wakes up a waiting injection point.
--
CREATE FUNCTION injection_points_wakeup(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_wakeup'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_set_local()
--
-- Trigger switch to link any future injection points attached to the
-- current process, useful to make SQL tests concurrently-safe.
--
CREATE FUNCTION injection_points_set_local()
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_set_local'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_detach()
--
-- Detaches the current action, if any, from the given injection point.
--
CREATE FUNCTION injection_points_detach(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_detach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_list()
--
-- List of all the injection points currently attached.
--
CREATE FUNCTION injection_points_list(OUT point_name text,
   OUT library text,
   OUT function text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'injection_points_list'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

--
-- injection_points_stall_wal_buffer_init()
--
-- Test-only: stall WAL buffer page initialization (see XLogTestStallWalBufferInit).
--
CREATE FUNCTION injection_points_stall_wal_buffer_init()
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_stall_wal_buffer_init'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_wal_buffer_init_gap()
--
-- Test-only: bytes between InitializedUpTo and InitializeReserved.
--
CREATE FUNCTION injection_points_wal_buffer_init_gap()
RETURNS bigint
AS 'MODULE_PATHNAME', 'injection_points_wal_buffer_init_gap'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_find_locked_dirty_block()
--
-- Return block number of a buffer for rel held by another backend, or NULL.
--
CREATE FUNCTION injection_points_find_locked_dirty_block(rel regclass)
RETURNS bigint
AS 'MODULE_PATHNAME', 'injection_points_find_locked_dirty_block'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_flush_buffer()
--
-- Take EXCLUSIVE content lock on one buffer and flush it to disk.  Test-only.
--
CREATE FUNCTION injection_points_flush_buffer(rel regclass, blkno bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_flush_buffer'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_flush_vm_buffer()
--
-- Test-only: EXCLUSIVE lock and flush one visibility map fork page to disk.
--
CREATE FUNCTION injection_points_flush_vm_buffer(rel regclass, vmblk bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_flush_vm_buffer'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_flush_heap_buffer_raw()
--
-- Test-only: write one dirty heap page to disk without taking its content lock.
--
CREATE FUNCTION injection_points_flush_heap_buffer_raw(rel regclass, blkno bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_flush_heap_buffer_raw'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_cassert_enabled()
--
-- True when server was built with assertion checking enabled.
--
CREATE FUNCTION injection_points_cassert_enabled()
RETURNS boolean
AS 'MODULE_PATHNAME', 'injection_points_cassert_enabled'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

--
-- injection_points_walbuf_crit_section_assert()
--
-- Enable/disable CritSectionCount assert in WaitEventSetWait (default on).
--
CREATE FUNCTION injection_points_walbuf_crit_section_assert(enable boolean)
RETURNS boolean
AS 'MODULE_PATHNAME', 'injection_points_walbuf_crit_section_assert'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

--
-- injection_points_stats_numcalls()
--
-- Reports statistics, if any, related to the given injection point.
--
CREATE FUNCTION injection_points_stats_numcalls(IN point_name TEXT)
RETURNS bigint
AS 'MODULE_PATHNAME', 'injection_points_stats_numcalls'
LANGUAGE C STRICT;

--
-- injection_points_stats_drop()
--
-- Drop all statistics of injection points.
--
CREATE FUNCTION injection_points_stats_drop()
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_stats_drop'
LANGUAGE C STRICT;

--
-- injection_points_stats_fixed()
--
-- Reports fixed-numbered statistics for injection points.
CREATE FUNCTION injection_points_stats_fixed(OUT numattach int8,
   OUT numdetach int8,
   OUT numrun int8,
   OUT numcached int8,
   OUT numloaded int8)
RETURNS record
AS 'MODULE_PATHNAME', 'injection_points_stats_fixed'
LANGUAGE C STRICT;

--
-- regress_injection.c functions
--
CREATE FUNCTION removable_cutoff(rel regclass)
RETURNS xid8
AS 'MODULE_PATHNAME'
LANGUAGE C CALLED ON NULL INPUT;
