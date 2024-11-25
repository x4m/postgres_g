-- enum check

DROP TABLE IF EXISTS enumtmp;
CREATE TABLE enumtmp (a rainbow);

\copy enumtmp from 'data/enum.data'

CREATE INDEX enumidx ON enumtmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM enumtmp WHERE a <  'g'::rainbow;

SELECT count(*) FROM enumtmp WHERE a <= 'g'::rainbow;

SELECT count(*) FROM enumtmp WHERE a  = 'g'::rainbow;

SELECT count(*) FROM enumtmp WHERE a >= 'g'::rainbow;

SELECT count(*) FROM enumtmp WHERE a >  'g'::rainbow;

EXPLAIN (COSTS OFF)
SELECT count(*) FROM enumtmp WHERE a >= 'g'::rainbow;
