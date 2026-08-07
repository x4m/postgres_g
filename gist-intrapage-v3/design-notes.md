# GiST intrapage indexing revival

Base: PostgreSQL `origin/master` at `780fa49746d`.

Historical threads:

- 2016 proposal: https://postgr.es/m/CAJEAwVE0rrr%2BOBT-P0gDCtXbVDkBBG_WcXwCBK%3DGHo4fewu3Yg%40mail.gmail.com
- 2018 patch: https://postgr.es/m/7780A07B-4D04-41E2-B228-166B41D07EEE%40yandex-team.ru
- 2026 multi-entry GiST: https://postgr.es/m/8E23543E-F1E4-40AE-BDA0-8A5BEA7DADA1%40enterprisedb.com

## Representation invariants

- A skip tuple is derived metadata.  Removing every skip tuple must leave a
  valid ordinary GiST page with exactly the same real index tuples.
- A skip tuple is immediately followed by exactly `n` tuples covered by its
  key.  Groups never overlap and never contain another skip tuple.
- A scan may skip a group only when `consistent(skip_key, query)` is false.
- Skip tuples are never returned as heap TIDs or followed as downlinks.
- Every page mutation, page split, WAL replay, vacuum operation, and build
  method must preserve the representation or remove the derived metadata.
- Old pages without skip tuples remain readable and writable.

## Marker

Do not use `INDEX_AM_RESERVED_BIT`: the 2026 multi-entry GiST proposal uses it
for multiplied heap TIDs.  A prospective marker is an invalid block number in
`t_tid`, with the group size stored in `ip_posid`.  Real heap TIDs and GiST
downlinks always have a valid block number.  Marker recognition must use the
no-check ItemPointer accessors.

## Required paths

- ordinary insert and parent downlink adjustment;
- root and non-root page splits;
- sorted and buffered builds;
- plain, bitmap, index-only, KNN, and multi-entry scans;
- LP_DEAD hinting, bulk delete, page deletion, and parent traversal;
- WAL insertion, redo, consistency checking, and unlogged indexes;
- pageinspect and amcheck awareness.

## Build and page-level scope

- Sorted build must create skip tuples too.  It is not enough to add them to
  the incremental and buffered insertion paths: the current bottom-up sorted
  builder writes already packed pages and therefore needs to reserve space
  for, and form, skip tuples as part of page packing.
- The historical patch restricted skip tuples to internal pages.  Treat that
  as an implementation choice to revisit, not as a representation invariant.
  The invalid-block marker and scan pruning also work on leaf pages.  Leaf
  support is useful only if insertion, LP_DEAD handling, vacuum, page split,
  index-only scans, and WAL all maintain the same invariants; otherwise the
  first correctness milestone should explicitly remain internal-only.
- Group size is a policy parameter, not part of the on-disk marker.  The
  stored count is authoritative, so readers can handle groups created with a
  different threshold.

## Confirmed defects in the 2018 v2

- Rebuilt groups pass `tlen` to `gistSplitBySkipgroup()` instead of the
  constructed `totalsize`.
- The recursive size check for the left half tests `spl_nright` instead of
  `spl_nleft`.
- `INDEX_AM_RESERVED_BIT` now conflicts with multi-entry GiST development.
- Incrementally mutating only the skip count complicates WAL and makes the
  marker key/count easy to desynchronize.  Prefer rebuilding derived metadata
  from ordinary tuples during page rewrites where practical.

## Bugs found while porting

- A full internal-page rewrite can register more than the default maximum of
  20 WAL data segments.  Reserve record space before entering the critical
  section; allocating a packed tuple vector inside `gistXLogUpdate()` is not
  allowed there.
- Buffered build's `gistGetMaxLevel()` followed the tuple at
  `FirstOffsetNumber` directly.  It, root-split parent-map maintenance, and
  `gistMemorizeAllDownlinks()` must ignore skip tuples just like
  `gistchoose()`.
- Any operation that removes a real member from a group must update the group
  first.  In particular, VACUUM page deletion cannot leave an old count in
  place, because it could skip into the following group.

## Current prototype status

- Core regression suite: all 245 tests passed with assertions enabled.
- Skip tuples are deliberately created only on internal pages.  The marker
  and scan algorithm can represent leaf groups, but leaf support should be a
  separate change covering LP_DEAD hints and tuple-level vacuuming.
- Ordinary and buffered insertion paths rebuild derived metadata through a
  standard GiST page-update WAL record; splits use the existing split record.
- Sorted build partitions internal levels with marker space included in its
  page-fit decision.  A page containing more than one ordinary tuple therefore
  gets skip groups even when metadata requires an additional output page.
- Before deleting a downlink, VACUUM moves it out of its group and decrements
  the group count in a separate WAL-logged update.  It then re-finds the now
  ungrouped downlink.  This keeps crash states valid without extending the
  page-deletion WAL record or discarding unrelated groups.
- A dedicated TAP test covers sorted and forced-buffered builds, exact scan
  results, VACUUM/page deletion, WAL consistency, standby replay, and crash
  recovery.
