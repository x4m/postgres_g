# PostgreSQL development session handoff, summer 2026

This is the durable index for a long PostgreSQL hacking session with Andrey
Borodin.  It records where work lives, what was concluded, and what remains.
It complements the task-specific directories and Git branches; it does not
duplicate their patches.

## Working conventions established during the session

- Work in the root `postgres-oa` checkout on named Git branches.
- Base new work explicitly on `origin/master`.
- Keep generated patches and correspondence in visible task-named directories
  at the repository root, not only in `tmp_build` or editor-excluded worktrees.
- Build development patches with autotools, debug, assertions, injection
  points, TAP, LZ4, and Zstd.  Build benchmarks with `-O2` and without
  assertions.
- Commit messages are wrapped, idiomatic PostgreSQL prose, and explain why
  before what.  Include the discussion link.
- Do not bump catversion during development; mention a required bump and leave
  it to the committer.  Development OIDs come from 8000-9000.
- Mailing-list messages should be short.  Do not list routine tests, repeat
  uncontroversial patch properties, or announce that a patch is not Ready for
  Committer.  State the useful finding and the requested next action.
- Canonical detailed rules are in `.cursor/rules/` and exposed to Codex through
  the root `AGENTS.md` symlink.

## B-tree work

The comprehensive research handoff is:

    BTREE-DEVELOPMENT-HANDOFF.md

It covers specialized page search, int4 interpolation, text/UUID negative
results, Eytzinger layout, recent-buffer hints, page merge, amcheck reverse
verification, SSI unique checks, and all benchmark numbers.

Main branches and artifacts:

- `btree-binsearch-support`, `btree-binsearch-v1/`, `btree-binsearch-v2/`
- `btree-int4-overnight/`
- `btree-pointer-swizzling`
- `merge-patch-v1.2`, `salma-merge-v1.2/`
- `iakm_v6`
- `ssi-unique-snapshot-dirty`, `ssi-unique-snapshot-dirty-v1/`

## HA synchronous-replication patches

Branch history and v3 handoff:

    ha-tools-v3/

The useful patches are:

- prevent early data access after recovery until the configured synchronous
  replication level has caught up (`post_recovery_sync_level`);
- optionally continue the synchronous replication wait after query cancel
  (`synchronous_replication_wait_on_query_cancel`).

The old flush-LSN patch was found not useful.  The two retained patches were
rebased, reviewed, renamed, tested, and prepared as v3.  An extension hook was
discussed as an alternative to the second GUC and should be mentioned in the
thread because the patch has lacked expert attention for years.

## Shared WAL archive review

Branch and artifacts:

    shared-archive-v8-review
    shared-archive-v8/

Important conclusion for archive-report failure detection:

- before the first report, use `archive_status_report_interval` to decide that
  upstream never supplied reports;
- after at least one report, start an `archive_timeout`-scale deadline when an
  unconfirmed `.ready` segment appears.

The purpose of shared archive is intentionally to retain WAL without gaps, so
unbounded retention is not itself a review defect.  An archived segment
remains safe across walreceiver lifetimes; resetting `primary_last_archived`
on a new receiver is not required for the shared-archive model.

## Timeline history and recovery

Artifacts:

    stepan-timeline-history/
    patches-ws-tli/

Stepan's history-file work addresses reconstruction of the actual sequence of
timeline switches leading to the current point.  A current history file alone
does not encode enough provenance in all cases for a general WAL verifier.
This is related to, but distinct from, recovery selecting divergent WAL at a
timeline switchpoint.

The latter patch remains tracked in CommitFest as "Fix XLogFileReadAnyTLI
silently applying divergent WAL from wrong timeline".

## WAL switch zero padding

Branch and handoff:

    wal-stream-skip-switch-padding
    wal-stream-zero-padding-v1/

Forced WAL switches can create almost 16 MB of zero padding.  Filesystems
store it cheaply, but physical replication transmits it and can produce
SyncRep latency spikes on many small, low-write clusters using
`archive_timeout`.

The prototype adds a compact physical replication message for zero-filled
end-of-segment padding.  Walreceiver reconstructs the sparse tail using
truncate/extend; pg_receivewal and compressed frontend writers are handled.
No protocol negotiation is proposed because physical replication is only
supported between compatible major versions and the change targets a new
major release.

The concise design email is in `wal-stream-zero-padding-v1/email.txt`.

## Compact hint-bit WAL

Branches and artifacts:

    compact-hint-bit-wal
    compact-hint-bit-wal-2
    heap-hint-wal-v1/

When checksums are disabled and `wal_log_hints` is enabled, the patch replaces
the first hint-bit FPI after a checkpoint with a compact record containing
tuple offsets and visibility hint bits.  Replay preserves the same physical
hint information on standbys.  Checksummed pages keep using FPI_FOR_HINT.

Measured WAL for the first scan of one million rows:

- int-only table: 36.8 MB -> 4.2 MB;
- pgbench_accounts-shaped table: 134.7 MB -> 4.9 MB;
- with `wal_compression=lz4`: 13.9 MB -> 4.2 MB and 16.3 MB -> 4.9 MB.

The motivation is compute/storage separation, but the WAL reduction also
applies to ordinary physical-replication deployments.  Avoid using
"logically" when describing preservation: it is physical replication and the
word creates needless ambiguity with logical replication.

## GiST work

### Multirange contained-by correctness

    review/gist-multirange-fix
    gist-multirange-v2/

`multirange_gist_consistent()` was too strict for `RANGESTRAT_CONTAINED_BY`.
A multirange leaf key is represented by its union range; gaps can make the
union fail containment even when the original multirange is contained.  The
minimal backpatchable fix uses the conservative internal predicate for this
strategy and leaves other strategies' stronger filtering intact.

### GiST intrapage indexing

    gist-intrapage-v3
    gist-intrapage-v3/

The revived 2018 idea adds skip tuples grouping neighboring downlinks on
internal GiST pages.  Search rejects a group using its union key; insertion
uses skip groups when selecting a subtree.  Sorted and buffered builds create
skip tuples.  Maintenance is incremental during insertion, split, VACUUM, and
page deletion rather than deleting all skip metadata.

Skip tuples use `InvalidBlockNumber` plus group length rather than
`INDEX_AM_RESERVED_BIT`.  They are derived metadata and existing GiST WAL can
log complete resulting pages.  Leaf-page support remains excluded because it
would complicate LP_DEAD and tuple-vacuum maintenance.

### Predicate locks and GiST page deletion

    predicate-gist-page-delete/

The suspected SSI problem is that GiST page deletion removes parent routing
to a page without transferring its predicate locks, unlike B-tree's
`PredicateLockPageCombine()`.  Work paused until a deterministic isolation
reproducer is complete.  Do not publish a correctness claim before the
control schedule proves that the same dependency becomes a serialization
failure without page deletion.

## Interrupt and vacuum delay points

Branches and artifacts:

    check-for-interrupts-audit
    interrupt-delay-points
    vacuum-delay-point-audit/
    check-for-interrupts-audit/

The session instrumented `vacuum_delay_point()` and
`CHECK_FOR_INTERRUPTS()` with file/line logging, ran broad tests, and found
calls made with interrupt holdoff, critical sections, or LWLocks.  The final
combined patch moves calls outside locks where possible and documents the
remaining intentional non-interruptible calls.

Kevin Rocker is author, Neil Chen reviewer, and Andrey coauthor of v3.  The
combined thread should be associated with the existing GIN and GiST
vacuum-delay CommitFest entries rather than creating several overlapping
entries.

## pg_surgery large TID array

    pg-surgery-large-tid-array
    pg-surgery-large-tid-array/

Bug #19607: array positions stored in `OffsetNumber` wrap at 65536 and can
restart the loop.  Use `int`, matching array length and related loop indexes.
The useful contribution from this session was insisting on a regression test
at the exact boundary; another author had already posted a correct fix.

## TOAST missing-chunk review

    toast-missing-chunks-v5-review
    toast-missing-chunks-review/

The deterministic injection-point test reproduces the stale-horizon race that
causes VACUUM FULL/CREATE INDEX to report "missing chunk number 0".  Review
focused on backpatchability to PG14-18, ABI preservation, the meaning of
`TOAST_MISSING_OK`, detoasting before partial-index predicates, memory cleanup,
and check/use races in scan-and-sort paths.

Andrey should be registered as reviewer in the CommitFest entry; he supplied
the deterministic reproducer and detailed v5 review.

## Performance review of the Databricks buffer-table patch

    databricks-buffer-table-bench/

The study used paired runs, several client counts including high concurrency,
resident and churn workloads, whole-machine monitoring, and a cooperative host
lock.  The report is in `databricks-buffer-table-bench/email-draft.txt` and
the directory contains the scripts and numbers.  This investigation is also
the reason the B-tree pointer-swizzling direction was deprioritized.

## CommitFest maintenance discovered at session end

Definitely missing from CF61 or not moved into it:

- Reduce WAL volume for heap tuple hint bits;
- Support specialized B-tree page searches;
- Small fixes needed by high-availability tools;
- GiST multirange contained-by fix;
- GiST intrapage indexing;
- GIN posting-tree page deletion;
- Two bugs around ALTER TYPE (old CF #6903);
- Possible G2 anomaly at SERIALIZABLE (old CF #6904).

Likely reviewer registrations missing:

- TOAST missing chunk, CF #6824;
- btree_gist cross-type integers, CF #6854;
- GIN vacuum delay point, CF #7031;
- GiST vacuum delay point, CF #7038;
- pg_rewind divergent timelines, CF #6768 (design review);
- server logging wait events, CF #6819 (technical review).

## Where to resume

1. Register/move the active patches in CF61 and add reviewer records.
2. Send the self-contained SSI duplicate-key patch after final email review.
3. Decide whether the WAL zero-padding prototype is ready to send, then add it
   to the CommitFest.
4. Keep the first upstream B-tree specialization patch small: int4 only;
   text/UUID results do not justify their maintenance burden yet.
5. For B-tree page merge, remain focused on one crash-safe merge plus exact,
   deterministic forward/backward/recovery tests.

