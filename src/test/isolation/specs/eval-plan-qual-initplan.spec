# BUG #19779: EPQ reset must not copy unevaluated InitPlan params from the
# parent estate into the recheck estate.  Dilip Kumar's simplified repro: a
# FOR UPDATE join to a non-materialized UNION ALL whose second arm references
# an InitPlan, with a concurrent UPDATE that forces EPQ recheck on both rows.

setup
{
  CREATE TABLE epq_ip (id int PRIMARY KEY, val text);
  INSERT INTO epq_ip VALUES (1, 'a'), (2, 'a');
  ANALYZE epq_ip;
}

teardown
{
  DROP TABLE epq_ip;
}

session s1
setup { SET default_transaction_isolation = 'read committed'; }
step begin1 { BEGIN; }
step update1 { UPDATE epq_ip SET id = id + 10; }
step update_one1 { UPDATE epq_ip SET id = id + 10 WHERE id = 1; }
step negate1 { UPDATE epq_ip SET id = -id; }
step commit1 { COMMIT; }

session s2
setup { SET default_transaction_isolation = 'read committed'; }
step lock2
{
  SELECT t.id, t.val, u.id AS uid
  FROM epq_ip t
  JOIN (
    SELECT id FROM epq_ip WHERE id > 0
    UNION ALL
    SELECT id FROM epq_ip WHERE val = (SELECT 'a'::text OFFSET 0)
  ) u ON t.id = u.id
  FOR UPDATE OF t;
}
step explain2
{
  EXPLAIN (COSTS OFF)
  SELECT t.id, t.val, u.id AS uid
  FROM epq_ip t
  JOIN (
    SELECT id FROM epq_ip WHERE id > 0
    UNION ALL
    SELECT id FROM epq_ip WHERE val = (SELECT 'a'::text OFFSET 0)
  ) u ON t.id = u.id
  FOR UPDATE OF t;
}

step literal2
{
  SELECT t.id, t.val, u.id AS uid
  FROM epq_ip t
  JOIN (
    SELECT id FROM epq_ip WHERE id > 0
    UNION ALL
    SELECT id FROM epq_ip WHERE val = 'a'
  ) u ON t.id = u.id
  FOR UPDATE OF t;
}

# The InitPlan's value changes between LATERAL rescans.  A computed parent
# value can still be stale for the child; execPlan == NULL is insufficient.
step lateral2
{
  SELECT o.i, q.* FROM (VALUES (1), (2)) o(i)
  CROSS JOIN LATERAL (
    SELECT t.id, t.val, u.id AS uid
    FROM epq_ip t
    JOIN (
      SELECT id FROM epq_ip WHERE id > 0
      UNION ALL
      SELECT -id FROM epq_ip WHERE (SELECT o.i OFFSET 0) = o.i
    ) u ON t.id = u.id
    FOR UPDATE OF t
  ) q;
}
step lateral_ref2
{
  SELECT o.i, q.* FROM (VALUES (1), (2)) o(i)
  CROSS JOIN LATERAL (
    SELECT t.id, t.val, u.id AS uid
    FROM epq_ip t
    JOIN (
      SELECT id FROM epq_ip WHERE id > 0
      UNION ALL
      SELECT -id FROM epq_ip WHERE o.i = o.i
    ) u ON t.id = u.id
    FOR UPDATE OF t
  ) q;
}

# Baseline: no concurrent update, no EPQ.
permutation begin1 update1 commit1 lock2

# Show the InitPlan under Append so a plan change is visible in review.
permutation explain2

# Both locked rows are concurrently renumbered; EPQ must recheck without
# crashing.  After recheck the join finds no matching ids, so zero rows.
permutation begin1 update1 lock2 commit1

# Only one locked row is renumbered; the untouched row survives EPQ twice
# (once per UNION ALL arm).
permutation begin1 update_one1 lock2 commit1

# Literal references check the exact results, including UNION ALL duplicates.
permutation begin1 update1 literal2 commit1
permutation begin1 update_one1 literal2 commit1

# Rechecked rows must survive for both external parameter values.
permutation begin1 negate1 lateral2 commit1
permutation begin1 negate1 lateral_ref2 commit1
