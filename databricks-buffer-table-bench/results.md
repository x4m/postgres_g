# Databricks shared buffer table benchmark

Date: 2026-08-05/06

Host: g2, 16 vCPUs, 32 GiB RAM.  Both PostgreSQL builds used `-O2`, no
assertions.  Each measured pgbench run lasted 30 seconds after an 8-second
warmup.  Base and patched runs were interleaved, with their order reversed on
even-numbered rounds.

The standalone shared-buffer-table patch was reconstructed from attachment
`restructured_shared_buffer_table.patch` in the thread "Restructured Shared
Buffer Hash Table".  It was applied to `f585671055d`; the patched commit is
`62c27e43559`.

## Select-only results

The values below are medians of the per-round paired TPS deltas
`patched / base - 1`.

| Dataset | shared_buffers | clients | rounds | delta |
|---|---:|---:|---:|---:|
| scale 100, resident | 4 GB | 1 | 5 | +1.75% |
| scale 100, resident | 4 GB | 16 | 5 | +0.76% |
| scale 100, resident | 4 GB | 64 | 5 | +2.19% |
| scale 500, churn | 512 MB | 1 | 5 | +1.03% |
| scale 500, churn | 512 MB | 16 | 5 | +0.75% |
| scale 500, churn | 512 MB | 64 | 5 | +1.11% |
| scale 500 | 8 GB | 64 | 3 | +0.71% |

The first base run at 8 GB/16 clients achieved only 43.7k TPS, versus roughly
395--405k TPS for all subsequent runs.  The eight-second warmup was not enough
after allocating the large shared buffer pool.  Excluding that pair leaves
only two pairs (+0.13% and -1.92%), which is insufficient for a conclusion.

## Read-write exploratory results

| Dataset | shared_buffers | clients | rounds | paired median delta |
|---|---:|---:|---:|---:|
| scale 500 | 512 MB | 1 | 5 | +0.74% |
| scale 500 | 512 MB | 16 | 5 | -2.79% |
| scale 500 | 512 MB | 64 | 5 | +1.75% |

These read-write figures are not conclusive.  Restarting between variants
forced shutdown checkpoints, while all runs modified the same dataset.  Dirty
buffer and WAL volumes changed substantially over the series, and the patched
16-client runs had 8.4% relative standard deviation.  A proper follow-up must
give every run an identical starting image, preferably using reflink snapshots,
or run on unlogged tables when the intended measurement excludes WAL.

## Validation

- 102 of 102 overnight runs completed.
- No ERROR, FATAL, PANIC, assertion failure, or crash appeared in the driver
  log.
- Core regression tests passed: 245/245.
- Both benchmark clusters were stopped and the host lock was released.

## Isolated runs

Each variant received a fresh cluster and freshly generated scale-100
dataset.  The server used 128 MB of shared buffers, `fsync=off`,
`synchronous_commit=off`, and autovacuum disabled.  Runs used the prepared
protocol, an eight-second warmup, and a 30-second measurement.  Base and
patched runs were interleaved, with their order reversed on even rounds.

| Workload | Clients | Jobs | Rounds | Paired median delta |
|---|---:|---:|---:|---:|
| select-only | 1 | 1 | 5 | +1.52% |
| select-only | 16 | 16 | 5 | -0.08% |
| select-only | 64 | 64 | 5 | +1.01% |
| simple-update | 1 | 1 | 5 | +0.34% |
| simple-update | 16 | 16 | 5 | -1.32% |
| simple-update | 64 | 64 | 5 | -3.59% |
| TPC-B-like | 1 | 1 | 5 | -0.29% |
| TPC-B-like | 16 | 16 | 5 | +1.65% |
| TPC-B-like | 64 | 64 | 5 | +2.31% |
| select-only | 256 | 64 | 5 | -2.56% |
| simple-update | 256 | 64 | 5 | -1.30% |
| TPC-B-like | 256 | 64 | 5 | +1.82% |

The 256-client select-only result was negative in all five pairs: -3.67%,
-3.12%, -0.68%, -2.56%, and -0.43%.  Simple-update and TPC-B-like at 256
clients were noisy and changed sign between rounds.

## Select-only scalability follow-up

This series used fresh clusters for every run and concentrated on higher
client counts.  The churn profile used a scale-100 database with 128 MB of
shared buffers and an 8-second warmup.  The resident profile used scale 5,
256 MB of shared buffers, and a 15-second warmup.  Each measurement lasted
30 seconds, with five interleaved pairs per data point.

| Profile | Clients | Jobs | Rounds | Paired median delta |
|---|---:|---:|---:|---:|
| churn | 128 | 64 | 5 | +1.22% |
| churn | 512 | 64 | 5 | -0.45% |
| resident | 128 | 64 | 5 | +1.10% |
| resident | 256 | 64 | 5 | +0.84% |
| resident | 512 | 64 | 5 | +1.84% |

The churn result at 128 clients was positive in four of five pairs.  At 512
clients it was noisy and changed sign: -1.34%, +3.55%, -3.74%, +2.05%, and
-0.45%.  The resident profile was positive in 12 of 15 pairs across the
three client counts, but individual 256- and 512-client pairs also changed
sign.  These results support a small improvement in the resident read-only
case, rather than an improvement that grows consistently with contention.
