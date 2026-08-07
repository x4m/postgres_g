# GiST page deletion and predicate locks

The suspected loss of a serializable conflict could not be reproduced.

The test establishes a dangerous structure with two serializable
transactions.  Without VACUUM, one transaction is aborted as expected.
VACUUM then removes a leaf page carrying one transaction's SIREAD lock and
the same serialization failure still occurs.

The reason is that a GiST scan takes predicate locks on internal pages as
well as leaves.  After a locked leaf and its downlink have been removed, an
insertion matching the earlier scan has two possibilities:

1. It can enter a surviving subtree whose key was already consistent with
   the scan.  The scan visited and predicate-locked that subtree.
2. It can enter a subtree whose key was not consistent with the scan.  Its
   downlink must then be expanded to cover the inserted key.  The scan
   predicate-locked the ancestor from which the old path was selected, and
   `gistinserttuples()` calls `CheckForSerializableConflictIn()` before
   changing that internal page.

GiST does not delete internal pages, so the ancestor lock remains useful.
The argument relies on the no-false-negatives contract of a correct GiST
operator class.

Temporary tracing confirmed checks on locked root/internal pages before an
insertion reached a different, unlocked leaf.  Both the control and the
VACUUM permutation ended with `could not serialize access due to read/write
dependencies among transactions`.

Consequently, adding `PageIsPredicateLocked()` to prevent leaf deletion does
not appear necessary and would retain empty pages for no correctness gain.
No mailing-list report should be sent based on the original hypothesis.
