
SET enable_seqscan to false;

-- test search for "not equals"

CREATE TABLE test_ne (
   a  TIMESTAMP,
   b  NUMERIC
);
CREATE INDEX test_ne_idx ON test_ne USING gist (a, b);

INSERT INTO test_ne SELECT '2009-01-01', 10.7 FROM generate_series(1,1000);
INSERT INTO test_ne VALUES('2007-02-03', -91.3);
INSERT INTO test_ne VALUES('2011-09-01', 43.7);
INSERT INTO test_ne SELECT '2009-01-01', 10.7 FROM generate_series(1,1000);

SET enable_indexscan to false;

EXPLAIN (COSTS OFF) SELECT * FROM test_ne WHERE a <> '2009-01-01' AND b <> 10.7;

SELECT * FROM test_ne WHERE a <> '2009-01-01' AND b <> 10.7;

RESET enable_indexscan;

-- test search for "not equals" using an exclusion constraint

CREATE TABLE zoo (
   cage   INTEGER,
   animal TEXT,
   EXCLUDE USING gist (cage WITH =, animal WITH <>)
);

INSERT INTO zoo VALUES(123, 'zebra');
INSERT INTO zoo VALUES(123, 'zebra');
INSERT INTO zoo VALUES(123, 'lion');
INSERT INTO zoo VALUES(124, 'lion');

-- "not equals" on a multi-level index over a variable-length, prefix-truncated
-- opclass (bit/varbit).  Internal-node keys are truncated common prefixes, and
-- for bit/varbit they are plain bytea blobs rather than varbit values; the <>
-- consistent branch must not apply the type's equality function to them.  Doing
-- so silently dropped matching rows (so a conflicting tuple could escape an
-- exclusion constraint) and could pass a negative length to memcmp() and crash
-- the backend.
CREATE TABLE ne_vbit (a varbit);
-- many short values, enough to build a tree taller than one page
INSERT INTO ne_vbit
  SELECT B'1' || (g % 7)::bit(3)::varbit FROM generate_series(1, 20000) g;
CREATE INDEX ne_vbit_idx ON ne_vbit USING gist (a);

-- B'1' is shorter than (hence distinct from) every stored value, so the index
-- must return every row -- exactly like a sequential scan.
SET enable_seqscan = on; SET enable_indexscan = off; SET enable_bitmapscan = off;
CREATE TEMP TABLE ne_seq AS SELECT count(*) AS c FROM ne_vbit WHERE a <> B'1';
SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = on;
SELECT count(*) = (SELECT c FROM ne_seq) AS index_matches_seqscan
  FROM ne_vbit WHERE a <> B'1';
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
