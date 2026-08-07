# Interrupt and vacuum delay point audit

Temporary instrumentation recorded call-site file and line together with
`InterruptHoldoffCount`, `QueryCancelHoldoffCount`, `CritSectionCount`, and
the number of tracked LWLocks.  For `CHECK_FOR_INTERRUPTS()`, a second run
recorded both blocked and normal states, deduplicated per backend, to separate
structural cases from call sites that only sometimes inherit a holdoff.

## vacuum_delay_point()

- `src/backend/commands/analyze.c`: moved before
  `table_scan_analyze_next_block()`.  A table AM may retain resources acquired
  by that callback until `table_scan_analyze_next_tuple()` returns false.
- `src/backend/access/gin/ginfast.c`: moved before pending-list locks are
  acquired.  The existing delay point after releasing each page covers later
  iterations.
- `src/backend/access/hash/hash.c`: left in place and marked
  `VACUUM_DELAY_POINT_WITH_INTERRUPTS_HELD`.  Cleanup must lock the next
  bucket page before releasing the previous one so scans cannot overtake it.

## CHECK_FOR_INTERRUPTS()

Twenty call sites were observed with some form of interrupt holdoff.  Eighteen
were also observed with interrupts enabled and were therefore classified as
mixed.  `pqmq.c` was observed blocked-only, but its holdoff came from its
caller and is not an invariant of the call site.

`src/backend/utils/activity/pgstat.c` was the only structural blocked-only
case.  `dshash_seq_next()` returns an entry while retaining the current hash
partition lock, and the API has no unlocked per-entry boundary.  The call is
marked `CHECK_FOR_INTERRUPTS_WITH_INTERRUPTS_HELD`.

The temporary instrumentation and logs were removed.  Core regression passed
245 tests and isolation passed 131 tests after the final changes.
