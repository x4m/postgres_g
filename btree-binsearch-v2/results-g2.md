# Text and UUID lookup results on g2

Compared `0e07ad2850d2` with `13194616fe4`.  Each entry is the median of
five paired patch/base throughput ratios.

| Workload | 1 client | 16 clients |
|---|---:|---:|
| Dense text, C collation | +0.01% | -1.27% |
| Random MD5 text, C collation | -0.02% | -2.54% |
| Dense text, C.UTF-8 collation | -0.27% | -0.24% |
| Dense UUID | +1.25% | +0.25% |
| Random MD5 UUID | -0.45% | -1.27% |
| UUIDv7-shaped | -0.40% | +0.81% |

The one-client measurements took about 52--54 seconds per variant and are
useful.  Dense UUID improved in all five pairs, by 0.68% to 1.96%.  Other
one-client effects were neutral or slightly negative.

The sixteen-client measurements took only about 2.2 seconds per variant and
produced only two `vmstat` samples.  Their spread is too large for useful
conclusions; they need to be rerun with much more work or a time-based run of
at least 30 seconds.

The dense text keys have a long repeated prefix and are likely compressed in
index tuples.  The prototype rejects extended varlena values and falls back
to the generic search, explaining why this intended best case was neutral.
The next benchmark should distinguish short uncompressed dense text from
long compressed text and count successful specialized/interpolation calls.

The current pgbench script performs 256 separate statements per transaction.
In the one-client runs, client user plus system CPU was about 18.5 of 53
seconds, diluting page-search effects.  A better primary benchmark is a
server-side nested-loop join from a probe table into the indexed table.  It
performs a fixed number of index lookups without a client round trip per
lookup and resembles the workload that motivated the original idea.

Before another broad matrix, isolate three variants:

1. base;
2. specialized direct comparison without interpolation;
3. specialized comparison with interpolation.

This will show whether the small regressions come from the support callback,
byte interpolation, or noise.  UUID interpolation should also be tried only
when the page endpoints have a sufficiently long common prefix: dense UUID
benefited consistently, whereas random and UUIDv7-shaped values did not.
