# Int4 benchmark results

Compared master `9825488c13de`, specialization `ccf85bc65d8`, and the
interpolation commit `0e07ad2850d2`.  Values below are medians of paired
ratios, expressed as throughput improvements.  Lookup tests used seven
rounds; pgbench and insertion tests used five.

## Server-side correlated lookups

| Workload | Specialization / master | Second commit / specialization | Total |
|---|---:|---:|---:|
| Dense hits | +8.0% | +33.7% | +46.0% |
| Dense descending hits | +7.8% | +31.3% | +40.6% |
| Dense misses between keys | +12.0% | +32.0% | +47.5% |
| Uniform wide-range hits | +8.4% | +32.1% | +44.2% |
| Uniform pseudo-random hits | +8.0% | +25.0% | +35.3% |
| Clustered hits | +9.0% | +5.0% | +13.9% |
| Misses in large cluster gaps | +7.4% | +1.1% | +7.4% |
| Deduplicated duplicate hits | +3.3% | +2.8% | +6.2% |
| Non-deduplicated duplicate hits | +3.0% | +0.8% | +4.2% |
| Misses below minimum | +10.7% | -1.8% | +8.8% |
| Misses above maximum | +6.0% | -2.0% | +3.9% |

The favorable dense cases reproduce the earlier result: total throughput is
41--47% higher.  Specialization alone consistently contributes roughly
8--12% except with duplicates.  Interpolation degrades gracefully as the
distribution becomes less linear.  The clearest adverse case is a probe
outside the page range, where reading endpoints before falling back costs
about 2% relative to specialized binary search, while remaining faster than
master.

## Pgbench point lookups

| Workload | Clients | Specialization | Second commit | Total |
|---|---:|---:|---:|---:|
| Dense | 1 | +0.6% | +2.3% | +3.0% |
| Dense | 16 | +1.5% | +0.6% | +3.0% |
| Uniform pseudo-random | 1 | +0.2% | +2.6% | +2.6% |
| Uniform pseudo-random | 16 | +4.9% | +0.8% | +3.0% |
| Duplicates | 1 | +0.6% | +2.0% | +2.4% |
| Duplicates | 16 | -1.2% | -0.2% | -1.3% |

Client and executor overhead dilute the page-search improvement.  The
sixteen-client results are noisier.  The repeatable regression for duplicate
lookups at sixteen clients deserves a focused rerun before submission.

## Insertions

Inserting one million odd keys into a freshly rebuilt two-million-row index:

| Variant | Median time | Incremental throughput | Total throughput |
|---|---:|---:|---:|
| Master | 2.40 s | — | — |
| Specialization | 2.32 s | +5.7% | +5.7% |
| Second commit | 2.21 s | +4.5% | +10.1% |

Interpolation is disabled for insertion-bound searches.  The second commit
also refactors fixed-width tuple extraction, so its additional insertion
gain cannot be attributed to interpolation.  A fourth build with that
refactor but without the interpolation probe is required to separate these
effects cleanly.
