-- macaddr check

DROP TABLE IF EXISTS macaddrtmp;
CREATE TABLE macaddrtmp (a macaddr);

\copy macaddrtmp from 'data/macaddr.data'

CREATE INDEX macaddridx ON macaddrtmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM macaddrtmp WHERE a <  '22:00:5c:e5:9b:0d'::macaddr;

SELECT count(*) FROM macaddrtmp WHERE a <= '22:00:5c:e5:9b:0d'::macaddr;

SELECT count(*) FROM macaddrtmp WHERE a  = '22:00:5c:e5:9b:0d'::macaddr;

SELECT count(*) FROM macaddrtmp WHERE a >= '22:00:5c:e5:9b:0d'::macaddr;

SELECT count(*) FROM macaddrtmp WHERE a >  '22:00:5c:e5:9b:0d'::macaddr;

-- Test index-only scans
SET enable_bitmapscan=off;
EXPLAIN (COSTS OFF)
SELECT * FROM macaddrtmp WHERE a < '02:03:04:05:06:07'::macaddr;
SELECT * FROM macaddrtmp WHERE a < '02:03:04:05:06:07'::macaddr;
