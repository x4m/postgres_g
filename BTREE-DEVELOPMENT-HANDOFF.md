# B-tree development and research handoff (summer 2026)

This document collects the B-tree-related development, experiments, reviews,
benchmarks, and negative results from Andrey Borodin's work in this checkout.
It is intended as source material for scientific papers.  Numbers below are
experimental results, not claims established across all hardware.

## 1. Specialized B-tree page search

### Motivation

PostgreSQL B-tree binary search repeatedly performs a very small unit of work
through generic machinery: call an operator-class comparison function through
fmgr and deform an index tuple.  For a one-column, fixed-width key such as
int4, the indirection and tuple access can cost substantially more than the
integer comparison itself.

The central idea is to let an operator class provide a larger unit of work:
not merely `compare(a, b)`, but a callback that compares a scan/insertion key
with a page tuple and, optionally, searches a complete interval of tuples on a
page.  This allows type-specific code to inline comparison and extract the key
directly.  Unsupported shapes fall back to the existing generic code.

Andrey first presented the idea at PGConf.dev 2024.  The original slides are:

    /Users/x4mmm/Yandex.Disk.localized/WAL-G/24/
        PGConf2024-Borodin-Mem-Tricks_v1.pdf

The implemented patch set is on branch `btree-binsearch-support`:

1. `ccf85bc65d8` - Add B-tree support for specialized page searches
2. `0e07ad2850d2` - Use interpolation for specialized int4 page searches
3. `13194616fe4` - Add specialized B-tree page searches for text and UUID

Generated v1 patches and the original email are in:

    btree-binsearch-v1/

Important files:

    btree-binsearch-v1/v1-0001-Add-B-tree-support-for-specialized-page-searches.patch
    btree-binsearch-v1/v1-0002-Use-interpolation-for-specialized-int4-page-searc.patch
    btree-binsearch-v1/email.txt

### Scope and correctness constraints

The int4 fast path is deliberately narrow:

- one index key attribute;
- scan key and indexed key have the same type;
- fixed-width int4 representation;
- handling remains correct for NULL ordering, DESC indexes, pivot tuples,
  heap-TID tie breakers, posting lists, and deduplicated indexes;
- cross-type searches and unsupported scan shapes use the generic path.

The callback may decline an individual operation.  This is important because
the optimization is an operator-class specialization, not a second B-tree
implementation.

The code contains a maintenance warning: changes to the generic search
invariants must also be considered in each specialized implementation.  This
duplication is a review cost and a major design tradeoff.

Read-only searches do not need to preserve the insertion bound used by the
generic insertion search.  Posting lists remain safe because page search
selects the index tuple boundary; the existing nbtree code still handles the
individual heap TIDs stored in a posting tuple.

### Interpolation probe

For int4, the second patch reads the first and last keys in the current page
interval and estimates the target position by linear interpolation.  The
probe is not trusted.  Adjacent tuple comparisons prove the exact boundary;
otherwise search continues with ordinary specialized binary search.

The useful conceptual decomposition is:

1. specialization removes fmgr and tuple-deformation overhead;
2. interpolation reduces the number of comparisons when key position is
   approximately linear in key value;
3. a guarded fallback bounds the damage on adverse distributions.

Dense keys favor interpolation.  Clustered keys, large gaps, duplicates, and
probes outside the page range weaken it.  The endpoint reads themselves have
a measurable cost when the probe is rejected.

## 2. Int4 benchmark results

The complete benchmark harness and raw analysis are in:

    btree-int4-overnight/README.md
    btree-int4-overnight/benchmark.sh
    btree-int4-overnight/analyze.rb
    btree-int4-overnight/results.md
    btree-int4-overnight/email-performance.txt

Compared variants:

- master `9825488c13de`;
- specialization `ccf85bc65d8`;
- specialization plus interpolation `0e07ad2850d2`.

All builds used `-O2` without assertions.  Tests used paired/interleaved
runs, rotating variant order.  Server-side lookup tests used seven rounds;
pgbench and insertion tests used five.  The shared benchmark host was guarded
by a cooperative lock to avoid concurrent benchmark contamination.

### Server-side correlated point lookups

Values are median throughput changes.

| Workload | Specialization/master | Interpolation/specialization | Total |
|---|---:|---:|---:|
| Dense hits | +8.0% | +33.7% | +46.0% |
| Dense descending hits | +7.8% | +31.3% | +40.6% |
| Dense misses between keys | +12.0% | +32.0% | +47.5% |
| Uniform wide-range hits | +8.4% | +32.1% | +44.2% |
| Uniform pseudorandom hits | +8.0% | +25.0% | +35.3% |
| Clustered hits | +9.0% | +5.0% | +13.9% |
| Misses in large cluster gaps | +7.4% | +1.1% | +7.4% |
| Deduplicated duplicate hits | +3.3% | +2.8% | +6.2% |
| Non-deduplicated duplicate hits | +3.0% | +0.8% | +4.2% |
| Misses below minimum | +10.7% | -1.8% | +8.8% |
| Misses above maximum | +6.0% | -2.0% | +3.9% |

The favorable dense case also produced the headline measurement from the
initial email: one million successful parameterized index-only lookups over
five million dense int4 keys took 2.245 s on master and 1.470 s with both
patches.  That is 35% lower elapsed time or 53% more throughput.

The key scientific observations are:

- specialization alone is a stable 8-12% win on most nonduplicate
  server-side lookup distributions;
- duplicates reduce that gain to about 3%, because work outside page search
  becomes more important;
- interpolation adds 25-34% on dense or uniform distributions;
- it degrades toward zero benefit for clusters, gaps, and duplicates;
- outside-range probes lose about 2% relative to specialized binary search,
  but remain faster than master because specialization still wins.

### Standard pgbench-style point lookups

| Workload | Clients | Specialization | Second patch | Total |
|---|---:|---:|---:|---:|
| Dense | 1 | +0.6% | +2.3% | +3.0% |
| Dense | 16 | +1.5% | +0.6% | +3.0% |
| Uniform pseudorandom | 1 | +0.2% | +2.6% | +2.6% |
| Uniform pseudorandom | 16 | +4.9% | +0.8% | +3.0% |
| Duplicates | 1 | +0.6% | +2.0% | +2.4% |
| Duplicates | 16 | -1.2% | -0.2% | -1.3% |

Executor, protocol, and client overhead dilute an optimization inside one
page search.  This explains why ordinary `pgbench -S` initially showed no
clear effect while a server-side nested-loop/correlated lookup showed a large
one.  The 16-client duplicate regression was repeatable in that run but needs
a focused rerun before being treated as established.

### Insertions

One million odd keys were inserted into a freshly rebuilt two-million-row
even-key index.  The transaction was rolled back after each timed run.

| Variant | Median time | Incremental throughput | Total throughput |
|---|---:|---:|---:|
| Master | 2.40 s | - | - |
| Specialization | 2.32 s | +5.7% | +5.7% |
| Full two-patch set | 2.21 s | +4.5% | +10.1% |

Interpolation is disabled for insertion-bound search.  The second patch also
refactored fixed-width tuple extraction, so its additional insertion gain
cannot be attributed to interpolation.  A necessary follow-up experiment is
a fourth build containing that refactor without the interpolation probe.

## 3. Text and UUID experiment

Commit `13194616fe4` extends the prototype to text and UUID.  Benchmark files:

    btree-binsearch-v2/benchmark-plan.md
    btree-binsearch-v2/benchmark.sh
    btree-binsearch-v2/analyze.rb
    btree-binsearch-v2/results-g2.md

The comparison was int4-only commit `0e07ad2850d2` versus text/UUID commit
`13194616fe4`, using `-O2`, no assertions, deterministic data, five paired
runs, and no checkpoint in measured intervals.

| Workload | 1 client | 16 clients |
|---|---:|---:|
| Dense text, C collation | +0.01% | -1.27% |
| Random MD5 text, C collation | -0.02% | -2.54% |
| Dense text, C.UTF-8 collation | -0.27% | -0.24% |
| Dense UUID | +1.25% | +0.25% |
| Random MD5 UUID | -0.45% | -1.27% |
| UUIDv7-shaped | -0.40% | +0.81% |

These results were not compelling.  Dense UUID improved in all five
one-client pairs (0.68-1.96%), but other effects were neutral or slightly
negative.  The 16-client runs completed in only about 2.2 seconds per variant
and are too noisy for strong conclusions.

The dense text test accidentally used long common prefixes.  Such keys are
likely compressed in index tuples; the prototype rejects extended varlena
values and falls back, so this intended best case did not exercise the fast
path effectively.  A better follow-up would separate short uncompressed text
from long compressed text and instrument accepted/fallback callbacks.

The current conclusion is that int4 is the strong, reviewable result.  Text
and UUID broaden the patch and add maintenance cost without demonstrated
benefit.  They should not be included in the initial upstream patch set.

## 4. Eytzinger layout experiment

An experimental patch is preserved at:

    btree-binsearch-v1/experimental/eytzinger-layout.patch

Eytzinger layout places binary-search elements in breadth-first heap order to
improve cache/prefetch behavior.  Applying it directly to B-tree tuples would
change page layout, complicate insertion, split, WAL, page inspection,
compatibility, and ordered scans.  It does not fit the current research focus
on small operator-class specializations that leave on-disk layout unchanged.

The nearby idea remains scientifically interesting: an operator-class search
callback can choose a different in-memory access pattern without committing
the core B-tree page format to it.  No convincing integrated implementation
or benchmark was completed.

## 5. Recent-buffer cache / pointer-swizzling direction

Old pointer-swizzling prototypes stored a BufferId near or inside a tuple.
That polluted the on-page representation with transient state and could not
be written to disk safely.

A cleaner prototype is commit `48f826dadb2` on branch
`btree-pointer-swizzling`, subject `Cache recent buffers during B-tree
descent`.  It uses a backend-local, direct-mapped array of 4096 entries keyed
by relation locator and block number.  Each entry stores a candidate BufferId.
The candidate is only a hint: `ReadRecentBufferForRelation()` pins it and
verifies the complete buffer tag before use, making buffer recycling safe.

Modified files:

    src/backend/access/nbtree/nbtpage.c
    src/backend/access/nbtree/nbtsearch.c
    src/backend/storage/buffer/bufmgr.c
    src/include/access/nbtree.h
    src/include/storage/bufmgr.h

This avoids a shared buffer mapping hash lookup when repeated descents visit
the same internal pages.  It also preserves normal buffer statistics on the
fast path.

No persuasive final benchmark is recorded for this prototype.  Work was
paused because the Databricks shared-buffer hash-table work attacks the more
general bottleneck and appeared more promising.  The local performance study
of that external patch is in `databricks-buffer-table-bench/`.

The reusable research distinction is:

- pointer swizzling/recent-buffer hints exploit temporal and path locality at
  the B-tree caller;
- a better shared buffer lookup table improves every caller;
- the approaches are not mutually exclusive, but the local cache must beat
  an already improved mapping table to justify B-tree-specific complexity.

## 6. B-tree page merge / bloat reduction

This work is primarily technical mentoring and review of Salma El-Sayed's
GSoC 2026 page-merge prototype.  The accepted design direction uses
watermark/tombstone (MA/M) states and scan recovery.  The immediate milestone
is:

> One L -> R merge, crash-safe and proven by deterministic tests.

Complete notes are in:

    salma-merge-v1.2/andrey-meeting-notes.md
    salma-merge-v1.2/links-for-salma-chat.txt
    salma-merge-v1.2/0001-nbtree-WAL-log-page-merges.patch

Main technical result: a critical section does not make changes to left page,
right page, and parent atomic on disk.  Without one WAL record covering the
three-buffer transition, a checkpointer can write any subset and crash or
standby replay can observe an inconsistent tree.  Cleanup of merge groups is
also durable state transition and needs restartable, idempotent WAL.

Principal scan invariant:

> Every expected physical heap TID is returned exactly once: no omissions and
> no duplicates.

`count(*)` alone is insufficient because one omission and one duplicate can
cancel.  Tests should compare exact `(key, ctid)` sets and separately check
physical-TID uniqueness.

The historical reproducer from Andrey's older page-merge patch stopped scans
between leaf pages, merged concurrently, and observed:

- forward scan: 364 rows instead of 250 (duplicates);
- backward scan: 142 rows instead of 250 (omissions).

Its semantic schedule should be reused, but not its hardcoded block number,
`sleep(1)`, relation-OID assumption, shared injection point, or count-only
oracle.  Peter Geoghegan's commit `e395fbd32a07557de4ac98088928c1749d4845d8`
is the reference for deterministic isolation orchestration and namespaced
injection points in backward scans.

Current thread:

https://www.postgresql.org/message-id/CANBEAPFq3YAOydjUS3xwcUG9L6e3WE5Z4nGPk_Q3RsjSFWTJNA%40mail.gmail.com

Older Andrey thread:

https://www.postgresql.org/message-id/CCD000DB-67CB-4D64-A912-B7514D546058%40yandex-team.ru

## 7. amcheck reverse key verification

Branch `iakm_v6` contains the `indexallkeysmatch` work.  Important commits:

- `75290f5dbc6` - add indexallkeysmatch verification;
- `978d6b42633` - detect dangling index entries where safe;
- `ce05a7df141` - documentation.

Existing `heapallindexed` checks heap -> index coverage.  The new direction
checks index -> heap agreement: each B-tree leaf tuple should point to a heap
tuple with the same indexed key.  A Bloom filter over visible `(key, tid)`
pairs avoids most random heap fetches; filter misses trigger exact heap fetch
and `FormIndexDatum()` comparison.  Posting-list entries are included.

This detects an important class of corruption that structural ordering checks
and heapallindexed can miss: an index tuple can contain the wrong key while
still pointing to a real heap tuple.

Concurrency limits matter.  Under AccessShareLock, a missing tuple can be
dead or concurrently pruned and is not automatically corruption.  Under the
stronger lock used by `bt_index_parent_check()`, out-of-range offsets and
LP_UNUSED slots can be diagnosed more aggressively.  Direct line-pointer
inspection is heap-specific and must be skipped for other table AMs.

Thread:

https://www.postgresql.org/message-id/flat/432626F9-65DF-4F0D-B345-26CFC3E2CFAC@yandex-team.ru

CommitFest entry:

https://commitfest.postgresql.org/patch/6526/

## 8. SSI and B-tree unique checks

Branch `ssi-unique-snapshot-dirty` and commit `70c4eac09a7` contain a proposed
fix for a serializable anomaly reported by Jacob Brazeal.  Artifacts are in:

    ssi-unique-snapshot-dirty-v1/

B-tree uniqueness checks use `SnapshotDirty` to account for concurrent tuple
changes.  Special snapshots do not participate in SSI.  A serializable
transaction can therefore:

1. read a row, establishing that it must be ordered before a concurrent
   deleter;
2. keep its old MVCC snapshot while the deletion commits;
3. have `_bt_check_unique()` rely on that deletion through SnapshotDirty;
4. insert the same unique key, which requires ordering after the deleter;
5. observe both the old snapshot-visible tuple and its own new tuple and
   commit.

The result does not leave a persistent duplicate or corrupt the index, but the
successful transaction has no serial ordering.  `INSERT ... ON CONFLICT DO
NOTHING` can reach the same path.

An early prototype aborted whenever the current transaction had any
rw-conflict out.  That has false positives: the existing conflict can be to an
unrelated transaction, while the deleter can safely be ordered before the
inserting transaction.  The current master-only proposal extends table AM so
the uniqueness probe reports the exact deleting XID when SnapshotDirty skips
a tuple still visible to the transaction snapshot.  SSI aborts only when an
rw-conflict to that transaction already exists.

The SSI transaction is marked doomed before reporting the serialization
failure.  Consequently, rolling back a savepoint cannot swallow the error and
later commit the top-level transaction.  Tests cover regular insertion,
partial unique checks used by ON CONFLICT, savepoint handling, and the
unrelated-conflict false-positive control.

The exact fix is proposed for HEAD because adding the XID output extends the
table AM interface.  An ABI-preserving back-branch variant remains an open
design problem.

Thread:

https://www.postgresql.org/message-id/flat/CA%2BCOZaBOiiRPmEfX00oE%3DN6HBSZVe0Y-y-ZqqaXq8BAAj1gu%2BQ%40mail.gmail.com

Related older ON CONFLICT/SSI thread:

https://www.postgresql.org/message-id/flat/165342c0-0c75-461e-b334-b997639ad48d%40aphyr.com

## 9. Open research questions

1. Can the insertion improvement be separated cleanly into comparator
   specialization versus tuple-extraction refactoring?
2. Can interpolation be enabled only when endpoint/key statistics predict a
   net win, avoiding the roughly 2% rejected-probe cost?
3. Is the 16-client duplicate regression reproducible on an otherwise idle
   host and on more than one CPU generation?
4. Does a server-side nested-loop join reproduce the int4 result across AMD,
   Intel, and ARM, especially the original M3 laptop?
5. Can short, noncompressed text or a restricted UUID shape demonstrate a
   benefit large enough to justify specialized code?
6. How does specialization interact with multicolumn indexes after prefix and
   suffix truncation?  A useful generalization must preserve lexicographic
   stopping rules without compiling a combinatorial set of type sequences.
7. Does the B-tree recent-buffer cache still help on top of a modernized
   shared buffer mapping table?
8. For page merge, can page/scan invariants be stated compactly enough that
   WAL redo, cleanup, forward/backward scans, mark/restore, parallel scans,
   and predicate locks can each be checked against the same model?

## 10. Practical benchmark lessons

- Build performance variants with `-O2` and without assertions.
- Use paired and interleaved runs; rotate order to reduce thermal and temporal
  bias.
- Prevent concurrent users of a shared benchmark host with a cooperative
  lock.
- Avoid checkpoints inside measured intervals or record them explicitly.
- Prefer fixed work and server-side repeated lookups when studying a tiny
  executor primitive; network/client overhead can hide a large local gain.
- Include favorable and adversarial distributions rather than reporting only
  dense keys.
- Separate total patch-set speedup from incremental speedup of each commit.
- Treat very short high-client runs as invalid even when their medians look
  stable; ensure enough duration for CPU and OS telemetry.
- Preserve negative results.  The text/UUID experiment and out-of-range int4
  probes define where the idea does not currently pay for itself.
