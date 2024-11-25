-- char check

DROP TABLE IF EXISTS chartmp;
CREATE TABLE chartmp (a char(32));

\copy chartmp from 'data/char.data'

CREATE INDEX charidx ON chartmp USING GIST ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM chartmp WHERE a <   '31b0'::char(32);

SELECT count(*) FROM chartmp WHERE a <=  '31b0'::char(32);

SELECT count(*) FROM chartmp WHERE a  =  '31b0'::char(32);

SELECT count(*) FROM chartmp WHERE a >=  '31b0'::char(32);

SELECT count(*) FROM chartmp WHERE a >   '31b0'::char(32);

-- Test index-only scans
SET enable_bitmapscan=off;
EXPLAIN (COSTS OFF)
SELECT * FROM chartmp WHERE a BETWEEN '31a' AND '31c';
SELECT * FROM chartmp WHERE a BETWEEN '31a' AND '31c';
