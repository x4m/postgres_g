# vacuum_delay_point() audit

Temporary instrumentation recorded call-site file and line together with
`InterruptHoldoffCount`, `CritSectionCount`, and the number of tracked
LWLocks whenever `vacuum_delay_point()` was entered with interrupts held.

## Observed call sites

- `src/backend/commands/analyze.c:1315`: heap ANALYZE holds the sampled
  buffer's content lock between `scan_analyze_next_block` and the final
  `scan_analyze_next_tuple` call.
- `src/backend/access/hash/hash.c:800`: `hashbucketcleanup` is entered with
  the primary bucket cleanup-locked and chain-locks overflow pages.
- `src/backend/access/gin/ginfast.c:895`: GIN pending-list cleanup keeps the
  current pending-list page locked in `GIN_SHARE` mode.

All observations had `InterruptHoldoffCount = 1` and
`CritSectionCount = 0`.  No additional call site was found by the isolation,
bloom, or file_fdw regression tests.

The core regression run observed 7548 ANALYZE calls, 328 hash calls, and 179
GIN calls.  The isolation run observed another 980 ANALYZE calls.  The exact
counts are workload-dependent; the relevant result is the set of call sites.

The full `check-world` run could not proceed past `001_password.pl` because
the local `Tty` Perl module was built for Perl 5.34 but loaded by Perl 5.42.
This happened after initdb had already confirmed the ANALYZE call site.

The temporary instrumentation was removed.  The patch leaves only comments
marked `VACUUM_DELAY_POINT_WITH_INTERRUPTS_HELD`.
