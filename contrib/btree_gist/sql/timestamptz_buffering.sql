-- timestamptz check

DROP TABLE IF EXISTS timestamptztmp;
CREATE TABLE timestamptztmp (a timestamptz);

\copy timestamptztmp from 'data/timestamptz.data'

CREATE INDEX timestamptzidx ON timestamptztmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM timestamptztmp WHERE a <  '2018-12-18 10:59:54 GMT+3'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a <= '2018-12-18 10:59:54 GMT+3'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a  = '2018-12-18 10:59:54 GMT+3'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a >= '2018-12-18 10:59:54 GMT+3'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a >  '2018-12-18 10:59:54 GMT+3'::timestamptz;


SELECT count(*) FROM timestamptztmp WHERE a <  '2018-12-18 10:59:54 GMT+2'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a <= '2018-12-18 10:59:54 GMT+2'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a  = '2018-12-18 10:59:54 GMT+2'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a >= '2018-12-18 10:59:54 GMT+2'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a >  '2018-12-18 10:59:54 GMT+2'::timestamptz;


SELECT count(*) FROM timestamptztmp WHERE a <  '2018-12-18 10:59:54 GMT+4'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a <= '2018-12-18 10:59:54 GMT+4'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a  = '2018-12-18 10:59:54 GMT+4'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a >= '2018-12-18 10:59:54 GMT+4'::timestamptz;

SELECT count(*) FROM timestamptztmp WHERE a >  '2018-12-18 10:59:54 GMT+4'::timestamptz;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '2018-12-18 10:59:54 GMT+2' FROM timestamptztmp ORDER BY a <-> '2018-12-18 10:59:54 GMT+2' LIMIT 3;
SELECT a, a <-> '2018-12-18 10:59:54 GMT+2' FROM timestamptztmp ORDER BY a <-> '2018-12-18 10:59:54 GMT+2' LIMIT 3;
