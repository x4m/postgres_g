# Overnight int4 benchmark

This compares:

- `9825488c13de`: master before the patch set;
- `ccf85bc65d8`: specialized int4 comparison and binary search;
- `0e07ad2850d2`: specialized comparison plus interpolation.

The lookup matrix deliberately includes favorable and adverse distributions:
dense and wide uniform keys, random physical insertion order, clustered keys
with large gaps, successful and unsuccessful searches, values outside the
index range, duplicates with and without posting-list deduplication, and a
descending index.  Seven rounds rotate the three-variant execution order.

Thirty-second pgbench runs cover dense, random, and duplicate lookups with
one and sixteen clients.  Five rounds rotate variant order.  A separate
insertion benchmark rebuilds the same even-key index before every timed run
and inserts odd keys inside its range in a transaction that is rolled back.
Interpolation is disabled for this insertion-bound search, isolating the
specialized comparator.

All builds use `-O2` without assertions.  The script acquires the shared g2
benchmark lock for its entire run.
