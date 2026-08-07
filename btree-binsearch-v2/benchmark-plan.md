# Text and UUID page-search benchmark

The benchmark compares commit `0e07ad2850d2` (int4 support only) with
`13194616fe4` (text and UUID support).  Both builds use `-O2`, no assertions,
and the same deterministic data sets.

Each result performs about one million successful prepared index-only
lookups after a five-second warmup.  A pgbench transaction contains 256
constant-key statements spread evenly across the index, avoiding unrelated
SQL-side key construction in the measured path.  Variants are paired and
interleaved, with their order reversed on even rounds.  There are five pairs
at one and sixteen clients.  No checkpoint is allowed in a measured interval.
Per-run `vmstat` samples capture whole-machine user, system, and idle CPU
percentages.

The data sets distinguish the properties that control interpolation quality:

| Workload | Purpose |
|---|---|
| `text_c_dense` | C collation, fixed-width keys with a long common prefix |
| `text_locale_dense` | Same keys under the database libc collation; direct comparison only |
| `text_c_md5` | C collation with uniformly distributed text keys |
| `uuid_dense` | Dense UUID values with a very long common prefix |
| `uuid_md5` | Uniform UUID values resembling UUIDv4 distribution |
| `uuid_v7` | Time-ordered UUID-shaped values with a shared timestamp prefix |

Following Tomas Vondra's WAL compression benchmark methodology, the driver
uses a fixed amount of work, fresh variant-specific clusters, realistic SQL
operations, multiple data distributions, and paired results rather than a
single best run.  His scripts also measure whole-machine CPU and use several
CPU generations.  If the first g2 series shows a material effect, those are
the next validation steps.

References:

- https://github.com/tvondra/compression-charts-oltp
- https://github.com/tvondra/compression-charts-tpch
- https://www.postgresql.org/message-id/3ebe2c65-7fe5-49cb-ad49-84c1d0cf567b%40vondra.me
