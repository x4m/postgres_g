# CHECK_FOR_INTERRUPTS() audit

Temporary instrumentation recorded `__FILE__:__LINE__`,
`InterruptHoldoffCount`, `QueryCancelHoldoffCount`, `CritSectionCount`, and
the number of tracked LWLocks.  A second run recorded both blocked and normal
states, deduplicated per backend, so call sites could be classified as
blocked-only or mixed.

Core regression found 20 call sites that were reached at least once with
some form of interrupt holdoff.  Eighteen were mixed: the same call site was
also reached with interrupts enabled.  These were not marked because the
holdoff came from a particular caller or execution state rather than from an
invariant at the call site.

Two sites were observed only with interrupts held:

- `src/backend/utils/activity/pgstat.c:1732`: `dshash_seq_next()` returns
  while holding the current partition LWLock.  This is structural, so the
  patch marks it.
- `src/backend/libpq/pqmq.c:199`: the only observed blocking wait happened
  under an outer interrupt holdoff.  `pq_mq_putmessage()` does not itself
  require that holdoff, so this site was not marked.

The classification was unchanged after the isolation, bloom, and file_fdw
tests.  Core regression passed 245 tests and isolation passed 131 tests.

The temporary instrumentation and log were removed.  The patch leaves only
the comment marked `CHECK_FOR_INTERRUPTS_WITH_INTERRUPTS_HELD`.
