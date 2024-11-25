-- bool check

DROP TABLE IF EXISTS booltmp;
CREATE TABLE booltmp (a bool);

INSERT INTO booltmp VALUES (false), (true);

CREATE INDEX boolidx ON booltmp USING gist ( a ) WITH(buffering=on);

SET enable_seqscan=off;

SELECT count(*) FROM booltmp WHERE a <  true;

SELECT count(*) FROM booltmp WHERE a <= true;

SELECT count(*) FROM booltmp WHERE a  = true;

SELECT count(*) FROM booltmp WHERE a >= true;

SELECT count(*) FROM booltmp WHERE a >  true;

-- Test index-only scans
SET enable_bitmapscan=off;

EXPLAIN (COSTS OFF)
SELECT * FROM booltmp WHERE a;
SELECT * FROM booltmp WHERE a;

EXPLAIN (COSTS OFF)
SELECT * FROM booltmp WHERE NOT a;
SELECT * FROM booltmp WHERE NOT a;
