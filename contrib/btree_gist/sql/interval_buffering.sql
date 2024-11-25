-- interval check

DROP TABLE IF EXISTS intervaltmp;
CREATE TABLE intervaltmp (a interval);

\copy intervaltmp from 'data/interval.data'

CREATE INDEX intervalidx ON intervaltmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM intervaltmp WHERE a <  '199 days 21:21:23'::interval;

SELECT count(*) FROM intervaltmp WHERE a <= '199 days 21:21:23'::interval;

SELECT count(*) FROM intervaltmp WHERE a  = '199 days 21:21:23'::interval;

SELECT count(*) FROM intervaltmp WHERE a >= '199 days 21:21:23'::interval;

SELECT count(*) FROM intervaltmp WHERE a >  '199 days 21:21:23'::interval;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '199 days 21:21:23' FROM intervaltmp ORDER BY a <-> '199 days 21:21:23' LIMIT 3;
SELECT a, a <-> '199 days 21:21:23' FROM intervaltmp ORDER BY a <-> '199 days 21:21:23' LIMIT 3;

SET enable_indexonlyscan=off;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '199 days 21:21:23' FROM intervaltmp ORDER BY a <-> '199 days 21:21:23' LIMIT 3;
SELECT a, a <-> '199 days 21:21:23' FROM intervaltmp ORDER BY a <-> '199 days 21:21:23' LIMIT 3;
