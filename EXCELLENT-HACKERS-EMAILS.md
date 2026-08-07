# Concise pgsql-hackers email exemplars

This index preserves the strongest correspondence produced during the summer
2026 session.  The source files are the canonical editable copies.

## Design proposal: specialized B-tree page search

File: `btree-binsearch-v1/email.txt`

Why it works:

- starts with one concrete hot-path cost;
- explains the narrow extension point without enumerating implementation;
- gives reproducible SQL and honest data-shape assumptions;
- reports one striking result and the neutral standard-pgbench result;
- ends with a direct design question.

## Performance follow-up: B-tree interpolation matrix

File: `btree-int4-overnight/email-performance.txt`

Why it works:

- separates specialization, interpolation, and total gains;
- includes adverse distributions and a regression;
- explains why pgbench dilutes the effect;
- reports the text/UUID negative result instead of hiding it.

## Design proposal: compact hint-bit WAL

File: `heap-hint-wal-v1/reply.txt`

Why it works:

- motivation and mechanism fit in a few paragraphs;
- is candid about the downstream compute/storage motivation;
- shows relevance to ordinary PostgreSQL configurations;
- uses two pairs of numbers that directly support the claim;
- avoids distracting discussion of pg_rewind and hint-bit philosophy.

## Design proposal: skip WAL switch padding in streaming replication

File: `wal-stream-zero-padding-v1/email.txt`

Why it works:

- begins with the mismatch between sparse filesystem storage and network
  transmission;
- gives a real operational symptom and an unusually revealing workaround;
- states the protocol change and receiver behavior in one paragraph;
- quantifies the nearly empty-segment case as 16 MB versus 128 kB plus 33
  bytes.

## Small bugfix reply: GiST multirange contained-by

Files:

    gist-multirange-v2/reply-v2.txt
    gist-multirange-v2/reply-v2-fix.txt

Why they work:

- agree with Peter's test immediately;
- identify the exact problem: `consistent` rejects too much;
- present the test and fix as a two-patch series;
- explain in one sentence why the weaker predicate is limited to
  `RANGESTRAT_CONTAINED_BY` rather than weakening all strategies;
- contain no routine test recital or readiness commentary.

## Review: missing TOAST chunks

File: `toast-missing-chunks-review/review-email.txt`

Why it works:

- admits the reason for entering the thread and corrects the earlier mistaken
  impression without drama;
- focuses on backpatch behavior and ABI constraints;
- distinguishes already-discussed issues from one newly observed issue;
- treats optional defensive tests as protection against future regression,
  not as proof that currently correct code is broken.

## Self-contained SSI unique-check fix

File: `ssi-unique-snapshot-dirty-v1/email.txt`

Why it works:

- relates the report to the older ON CONFLICT issue while distinguishing the
  transaction ordering;
- gives a calibrated importance estimate: narrow window, no index corruption,
  but a real SERIALIZABLE violation;
- describes exact-XID checking and doom-before-error succinctly;
- explains why the proposed implementation is HEAD-only.

## HA tools v3

File: `ha-tools-v3/reply-v3.txt`

Why it works:

- revives an old thread with concrete, rebased behavior;
- keeps two related but separable HA guarantees together;
- mentions the extension-hook alternative so reviewers can redirect the
  interface without losing the use case.

## GiST intrapage indexing

File: `gist-intrapage-v3/email-draft.txt`

Why it works:

- explains skip tuples as a second page-local level rather than as a bag of
  implementation details;
- states how search, insertion, build, split, and VACUUM maintain them;
- makes the intentional internal-page limitation explicit;
- frames derived metadata so the WAL argument is understandable.

## Timeline history reconstruction

File: `stepan-timeline-history/email-v1.txt`

Why it works:

- describes the missing information rather than beginning with a proposed
  file format;
- distinguishes reconstructing actual history from merely detecting a bad
  timeline;
- keeps the request understandable to readers unfamiliar with wal-verify.

## Performance report: shared buffer table

File: `databricks-buffer-table-bench/email-draft.txt`

Why it works:

- labels workloads in plain operational terms instead of unexplained
  "resident" and "churn" shorthand;
- specifies the exact patch version and paired-run methodology;
- uses compact tables and does not narrate every cell;
- distinguishes a missing measurement from a measured zero effect.

## Reusable composition pattern

The most successful short messages generally follow this order:

1. State the concrete problem in one or two sentences.
2. Give one production symptom, correctness failure, or benchmark that makes
   it matter.
3. Describe the proposed mechanism at the level needed for discussion.
4. State the most important limitation or uncertainty honestly.
5. Provide only measurements that test the claim.
6. End with `PFA`, `WDYT?`, or one precise question.

Avoid:

- routine "I tested with make check" paragraphs;
- repeating properties nobody challenged;
- generic approval or "not Ready for Committer yet" statements;
- exhaustive review diaries;
- inflated severity;
- burying the result below implementation details.
