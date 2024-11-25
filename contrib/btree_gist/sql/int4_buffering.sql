-- int4 check

DROP TABLE IF EXISTS int4tmp;
CREATE TABLE int4tmp (a int4);

\copy int4tmp from 'data/int2.data'

CREATE INDEX int4idx ON int4tmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM int4tmp WHERE a <  237::int4;

SELECT count(*) FROM int4tmp WHERE a <= 237::int4;

SELECT count(*) FROM int4tmp WHERE a  = 237::int4;

SELECT count(*) FROM int4tmp WHERE a >= 237::int4;

SELECT count(*) FROM int4tmp WHERE a >  237::int4;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '237' FROM int4tmp ORDER BY a <-> '237' LIMIT 3;
SELECT a, a <-> '237' FROM int4tmp ORDER BY a <-> '237' LIMIT 3;
