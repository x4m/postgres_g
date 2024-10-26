-- timestamp check

DROP TABLE IF EXISTS timestamptmp;
CREATE TABLE timestamptmp (a timestamp);

\copy timestamptmp from 'data/timestamp.data'

CREATE INDEX timestampidx ON timestamptmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM timestamptmp WHERE a <  '2004-10-26 08:55:08'::timestamp;

SELECT count(*) FROM timestamptmp WHERE a <= '2004-10-26 08:55:08'::timestamp;

SELECT count(*) FROM timestamptmp WHERE a  = '2004-10-26 08:55:08'::timestamp;

SELECT count(*) FROM timestamptmp WHERE a >= '2004-10-26 08:55:08'::timestamp;

SELECT count(*) FROM timestamptmp WHERE a >  '2004-10-26 08:55:08'::timestamp;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '2004-10-26 08:55:08' FROM timestamptmp ORDER BY a <-> '2004-10-26 08:55:08' LIMIT 3;
SELECT a, a <-> '2004-10-26 08:55:08' FROM timestamptmp ORDER BY a <-> '2004-10-26 08:55:08' LIMIT 3;
