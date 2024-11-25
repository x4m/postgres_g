-- date check

DROP TABLE IF EXISTS datetmp;
CREATE TABLE datetmp (a date);

\copy datetmp from 'data/date.data'

CREATE INDEX dateidx ON datetmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM datetmp WHERE a <  '2001-02-13'::date;

SELECT count(*) FROM datetmp WHERE a <= '2001-02-13'::date;

SELECT count(*) FROM datetmp WHERE a  = '2001-02-13'::date;

SELECT count(*) FROM datetmp WHERE a >= '2001-02-13'::date;

SELECT count(*) FROM datetmp WHERE a >  '2001-02-13'::date;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '2001-02-13' FROM datetmp ORDER BY a <-> '2001-02-13' LIMIT 3;
SELECT a, a <-> '2001-02-13' FROM datetmp ORDER BY a <-> '2001-02-13' LIMIT 3;
