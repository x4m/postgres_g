-- money check

DROP TABLE IF EXISTS moneytmp;
CREATE TABLE moneytmp (a money);

\copy moneytmp from 'data/cash.data'

CREATE INDEX moneyidx ON moneytmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM moneytmp WHERE a <  '22649.64'::money;

SELECT count(*) FROM moneytmp WHERE a <= '22649.64'::money;

SELECT count(*) FROM moneytmp WHERE a  = '22649.64'::money;

SELECT count(*) FROM moneytmp WHERE a >= '22649.64'::money;

SELECT count(*) FROM moneytmp WHERE a >  '22649.64'::money;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '21472.79' FROM moneytmp ORDER BY a <-> '21472.79' LIMIT 3;
SELECT a, a <-> '21472.79' FROM moneytmp ORDER BY a <-> '21472.79' LIMIT 3;
