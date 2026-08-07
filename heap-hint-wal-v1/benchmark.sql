\set ON_ERROR_STOP on
\timing on

SELECT pg_current_wal_insert_lsn() AS start_lsn \gset
SELECT count(*) FROM narrow_hints;
SELECT 'narrow' AS workload,
       pg_wal_lsn_diff(pg_current_wal_insert_lsn(), :'start_lsn') AS wal_bytes,
       pg_relation_size('narrow_hints') AS relation_bytes,
       pg_relation_size('narrow_hints') / current_setting('block_size')::int
         AS heap_pages;

SELECT pg_current_wal_insert_lsn() AS start_lsn \gset
SELECT count(*) FROM pgbench_hints;
SELECT 'pgbench_accounts' AS workload,
       pg_wal_lsn_diff(pg_current_wal_insert_lsn(), :'start_lsn') AS wal_bytes,
       pg_relation_size('pgbench_hints') AS relation_bytes,
       pg_relation_size('pgbench_hints') / current_setting('block_size')::int
         AS heap_pages;
