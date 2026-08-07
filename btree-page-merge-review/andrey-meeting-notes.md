# GSoC B-tree page merge: technical meeting notes

## Suggested outcome of the meeting

Suggested milestone:

> One L -> R merge, crash-safe and proven by deterministic tests.

## What the patch already achieves

The branch is a useful executable design sketch.  It has concrete on-page
representations for MA/M pages, copies L into R, redirects the parent downlink,
propagates M state across page splits and deduplication, implements scan
recovery paths, adds VACUUM cleanup code, and teaches amcheck about the new
states.  This is enough code to replace abstract design discussion with
specific invariants and schedules.

The watermark/tombstone direction is accepted for this review.  A saved TID
set is also a valid implementation direction for now.  If it remains, record
why it is needed and keep a deterministic counterexample that fails without
it.

## Issues in merge-patch-v1.2

### 1. A permanent index can be changed without WAL

`_bt_mergepage()` changes L, R, and their parent inside a critical section,
marks all three buffers dirty, but emits no WAL record and sets no page LSN.
VACUUM cleanup also clears M state and converts MA to half-dead without WAL.

A critical section does not make three buffers atomic on disk.  It only makes
an ERROR after modification escalate to PANIC.  After the locks are released,
the checkpointer may write any subset of the three dirty pages.  A crash can
therefore leave a parent, L, and R from different logical states.  A standby
receives none of the changes.

Until merge WAL exists, `_bt_merge_index()` must reject
`RelationNeedsWAL(rel)`.  That is a temporary safety guard, not a substitute
for WAL.

### 2. There are no merge tests

### 3. The new injection points are test-specific probes in production code

`after_left_lock` has a generic name and no corresponding test.  The two scan
points run only for a relation literally named `merge_test_idx` and depend on
block numbers 4 and 2.  This makes them unusable as semantic synchronization
points and permits accidental name collisions.

Use namespaced points describing a stable phase, for example:

- `nbtree-merge-after-lock-left` only if that lock boundary is truly needed;
- `nbtree-scan-before-read-next`;
- `nbtree-scan-before-read-prev`;
- diagnostic notice points for distinct merge-recovery branches.

Do not test relation names or block numbers in backend code.  Use
`injection_points_set_local()` to limit the waiting backend, as Peter's new
test does.

### 4. Direct descent to an M page loses the merge-group identity

`_bt_readfirstpage()` sets `skipMergeRecovery = true` when it starts on an M
page, but does not set `mergedAwayBlkno` from that page.  On the next M page,
the mismatch with `InvalidBlockNumber` clears `skipMergeRecovery`, and the
scan enters recovery even though it descended into the group after the merge.

This affects both directions when an M page has split into several M pages.
At minimum, the initial page must initialize the group identity, and this
needs a deterministic test that descends directly into a multi-page M group.

### 5. Scan-local state is not covered by existing scan contracts

The patch adds flags, a saved TID array, and an MA block number to
`BTScanOpaqueData`, but does not preserve them in `btmarkpos()` /
`btrestrpos()` and does not add them to `BTParallelScanDescData`.

The team should state an invariant for each of these cases:

- mark, cross a merge-group boundary, and restore;
- change scan direction after marking/restoring;
- parallel workers handing the next page to another worker.

If correctness is not implemented in this milestone, the affected execution
modes should be explicitly disabled for indexes that can contain merge groups,
rather than silently relying on backend-local state.

### 6. Cleanup is another WAL operation, not a follow-up detail

VACUUM clears M flags one page at a time from right to left and later changes
MA to half-dead.  This deliberately supports restart after an interrupt, but
every durable transition still needs WAL and an idempotent redo rule.  Tests
must interrupt cleanup after a proper prefix of pages has been cleared and
show that a later VACUUM finishes safely.

## Questions that require proof or a written invariant

These are questions, not claims that the design is wrong:

1. What exact states of L, R, and parent may another backend observe while
   their buffer locks are acquired and released?  State the lock order and
   why it cannot deadlock with split completion and page deletion.
2. What identifies one merge group after R splits repeatedly?  Which
   operations must copy the MA block number, and which operations are
   forbidden on M or MA pages?
3. What is the cleanup invariant after each right-to-left M flag clear, and
   after an interrupt?  Why can a new scan safely encounter a partially
   cleaned group?
4. Which `BTScanOpaque` fields are required for forward, backward,
   mark/restore, direction reversal, index-only, bitmap, and parallel scans?
5. Does `savedMergeTids` contain exactly the already-returned physical TIDs,
   rather than every matching item copied into `currPos`?  Give a test where
   page quals filter most tuples and where posting-list tuples are present.
6. Can an M page be deduplicated, split, emptied, or deleted?  For each allowed
   transition, where is its MA identity preserved?
7. What stops a second merge from overlapping a group whose cleanup was
   interrupted?
8. What predicate-lock transfer is required for serializable scans, and at
   what point relative to the WAL-protected page changes must it occur?

## Milestone deliverables

### Invariants note

A short document should state at least:

- Parent search routes the former key ranges of L and R to R after merge.
- L is an MA tombstone, remains linked at leaf level, and has no parent
  downlink.
- Every M descendant of R names the same MA page until the drain horizon.
- Splits preserve that identity on both resulting pages.
- Cleanup is right-to-left, interruptible, and never admits a new overlapping
  merge group.
- For every scan direction, each expected physical heap TID is returned
  exactly once: no omissions and no duplicates.

### WAL

Add an nbtree merge WAL record covering all three buffers in one record:

- enough data to recreate the MA form of L;
- the complete post-merge R contents and M/MA identity;
- the parent downlink replacement/deletion;
- `safemergexid`;
- redo that is idempotent and applies each registered buffer according to its
  LSN;
- `PageSetLSN()` for every modified permanent-relation buffer;
- `nbtdesc`/identify output for the new record.

Cleanup needs WAL records for clearing an M page's flag/MA identity and for
the MA lifecycle transition.  Since a merge group can span an unbounded
number of split descendants, cleanup should remain restartable page by page;
it cannot rely on one unbounded record.

### Deterministic functional tests

1. Construct a sparse tree with deduplication disabled and identify candidate
   pages from page structure, not fixed block numbers.
2. Force exactly one L -> R merge through the prototype driver.
3. Compare forward and backward index scans against an immutable expected
   table containing `(key, ctid)`.
4. Assert both set equality and uniqueness of physical TIDs.  Do not use only
   `count(*)`.
5. Run `bt_index_check()` and `bt_index_parent_check()` after the merge.
6. Force R to split after merge, then repeat scans and amcheck.
7. Cover duplicate keys/posting lists and selective scan quals.
8. Hold an old snapshot across merge, prove cleanup is deferred, release it,
   run VACUUM again, and prove cleanup completes.

### Isolation tests

Keep forward and backward cases in separate specs or clearly independent
permutations:

- forward scan has finished L and waits before acquiring the next leaf;
  merge L into R; resume and compare exact `(key, ctid)` results;
- forward scan encounters MA after merge and reads the M group normally;
- backward scan has finished R and waits before acquiring L; merge L into R;
  resume through tail discovery and deduplication;
- perform the merge and then split R while the backward scan waits;
- descend directly into the middle of an already split M group;
- interrupt VACUUM during right-to-left cleanup, then resume cleanup;
- mark/restore and direction reversal across the same boundaries;
- either exercise parallel scan handoff or prove that parallel scan is
  disabled until its shared state is implemented.

Every permutation should use a wait point for orchestration and notice points
to prove which semantic recovery branch ran.

### Recovery tests

Use a permanent relation once WAL is implemented:

1. Checkpoint before the merge, perform one merge, immediately crash-stop,
   restart, compare exact scans, and run both amcheck functions.
2. Repeat with a crash after part of merge-group cleanup has completed.
3. Replay merge, post-merge split, and cleanup records on a physical standby;
   run exact scans and amcheck after promotion or on a hot standby where
   permitted.
4. Run with `wal_consistency_checking = btree`, assertions, checksums where
   available, and forced full-page-write boundaries.

## What to reuse from 008_btree_merge_scan_correctness.pl

Reuse the semantic schedule:

- create many leaf pages and then make them sparse;
- start a scan and stop it between leaf pages after it has consumed one page;
- merge while the scan is stopped;
- resume in both directions;
- compare with a precomputed expected result.

Do not reuse its incidental mechanics:

- no `relid > 16384` gate;
- no block 20 or any other hardcoded block number;
- no `sleep(1)`;
- no shared injection point for two concurrent scans;
- no forward/backward cases mixed into one timing window;
- no `count(*)`-only oracle.

The new oracle should compare sorted `(key, ctid)` values and separately
assert `count(*) = count(DISTINCT ctid)` before running amcheck.

## How Peter's e395fbd32a test maps to this work

Peter's test supplies the orchestration pattern, not the merge scenario:

- isolation permutations define the interleaving without sleeps;
- `injection_points_set_local()` prevents unrelated backends from waiting;
- `nbtree-walk-left` is the single wait point;
- `nbtree-walk-left-step-right`, `-deleted`, and `-restart` are diagnostic
  notice points;
- assertions are expressed in terms of semantic recovery paths, not a chosen
  leaf block number;
- `pg_backend_pid()` keeps the scan in the leader under debug parallel query.

The merge tests should add similarly named semantic points for crossing MA/M
boundaries and entering forward/backward merge recovery.  They should reuse
Peter's existing `nbtree-walk-left*` points when testing the ordinary backward
left-link recovery that happens around a merge.
