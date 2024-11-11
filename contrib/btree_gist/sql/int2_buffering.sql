-- int2 check

DROP TABLE IF EXISTS int2tmp;
CREATE TABLE int2tmp (a int2);

\copy int2tmp from 'data/int2.data'

CREATE INDEX int2idx ON int2tmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM int2tmp WHERE a <  237::int2;

SELECT count(*) FROM int2tmp WHERE a <= 237::int2;

SELECT count(*) FROM int2tmp WHERE a  = 237::int2;

SELECT count(*) FROM int2tmp WHERE a >= 237::int2;

SELECT count(*) FROM int2tmp WHERE a >  237::int2;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '237' FROM int2tmp ORDER BY a <-> '237' LIMIT 3;
SELECT a, a <-> '237' FROM int2tmp ORDER BY a <-> '237' LIMIT 3;
