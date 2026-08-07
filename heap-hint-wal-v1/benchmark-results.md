# Heap hint WAL benchmark

Compared `origin/master` at `780fa49746d` with commit `151404e26ff`.

Configuration:

```text
data checksums: off
wal_log_hints = on
full_page_writes = on
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
```

Each table was populated in one transaction and the server was restarted.
The measured operation was the first `SELECT count(*)` after restart.  WAL
volume is the difference between `pg_current_wal_insert_lsn()` immediately
before and after that scan.

Tables:

```sql
CREATE TABLE narrow_hints AS
SELECT g FROM generate_series(1, 1000000) g;

CREATE TABLE pgbench_hints AS
SELECT g AS aid, 1 AS bid, 0 AS abalance,
       repeat(' ', 84)::char(84) AS filler
FROM generate_series(1, 1000000) g;
```

Results:

| table | heap pages | compression | master WAL | patched WAL | reduction |
|---|---:|---|---:|---:|---:|
| narrow | 4,480 | off | 36,753,032 | 4,224,784 | 8.70x |
| pgbench_accounts-shaped | 16,448 | off | 134,739,416 | 4,866,744 | 27.69x |
| narrow | 4,480 | pglz | 12,745,744 | 4,224,784 | 3.02x |
| pgbench_accounts-shaped | 16,448 | pglz | 14,892,312 | 4,866,760 | 3.06x |
| narrow | 4,480 | lz4 | 13,877,344 | 4,224,760 | 3.28x |
| pgbench_accounts-shaped | 16,448 | lz4 | 16,300,752 | 4,866,744 | 3.35x |

The corresponding per-page WAL sizes were 8,204 versus 943 bytes for the
narrow table and 8,192 versus 296 bytes for the pgbench-shaped table without
WAL compression.  With pglz they were 2,845 versus 943 bytes and 905 versus
296 bytes respectively.  With lz4 they were 3,098 versus 943 bytes and 991
versus 296 bytes respectively.

The single-run scan timings were not retained as performance results; this
experiment was designed to measure WAL volume, not elapsed time.
