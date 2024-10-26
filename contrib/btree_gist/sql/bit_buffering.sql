-- bit check with buffer index builds

DROP TABLE IF EXISTS bittmp;
CREATE TABLE bittmp (a bit(33));

\copy bittmp from 'data/bit.data'

CREATE INDEX bitidx ON bittmp USING GIST ( a ) WITH(buffering=on);

SET enable_seqscan=off;

SELECT count(*) FROM bittmp WHERE a <   '011011000100010111011000110000100';

SELECT count(*) FROM bittmp WHERE a <=  '011011000100010111011000110000100';

SELECT count(*) FROM bittmp WHERE a  =  '011011000100010111011000110000100';

SELECT count(*) FROM bittmp WHERE a >=  '011011000100010111011000110000100';

SELECT count(*) FROM bittmp WHERE a >   '011011000100010111011000110000100';

-- Test index-only scans
SET enable_bitmapscan=off;
EXPLAIN (COSTS OFF)
SELECT a FROM bittmp WHERE a BETWEEN '1000000' and '1000001';
