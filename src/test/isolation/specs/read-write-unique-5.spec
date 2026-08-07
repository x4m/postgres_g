# Test SSI conflict detection when a unique check observes a committed
# deletion through SnapshotDirty.

setup
{
  CREATE TABLE test (k integer PRIMARY KEY, j integer);
  INSERT INTO test VALUES (1, 1000000);
  INSERT INTO test SELECT g, g FROM generate_series(100, 2000) g;
  CREATE INDEX test_j_idx ON test (j);
  ANALYZE test;
}

teardown
{
  DROP TABLE test;
}

session s1
step b1 { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step r1 { SELECT * FROM test WHERE k = 1; }
step w1 { INSERT INTO test VALUES (1, 2); }
step w1conflict { INSERT INTO test VALUES (1, 2) ON CONFLICT DO NOTHING; }
step r1again { SELECT * FROM test WHERE k = 1 ORDER BY j; }
step c1 { COMMIT; }

session s2
setup { SET enable_seqscan = off; }
step b2 { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step d2 { DELETE FROM test WHERE j = 1000000; }
step c2 { COMMIT; }

# s1's initial read must precede s2, while its INSERT relies on s2's deletion.
# There is no serial order in which both observations are possible.
permutation b1 r1 b2 d2 c2 w1 r1again c1

# ON CONFLICT uses a partial unique check, but must detect the same anomaly.
permutation b1 r1 b2 d2 c2 w1conflict r1again c1

# A deletion committed before s1 takes its snapshot is visible normally and
# permits the key to be reused.
permutation b2 d2 c2 b1 w1 r1again c1
