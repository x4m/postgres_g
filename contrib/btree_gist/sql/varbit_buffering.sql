-- varbit check

DROP TABLE IF EXISTS varbittmp;
CREATE TABLE varbittmp (a varbit);

\copy varbittmp from 'data/varbit.data'

CREATE INDEX varbitidx ON varbittmp USING GIST ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM varbittmp WHERE a <   '1110100111010'::varbit;

SELECT count(*) FROM varbittmp WHERE a <=  '1110100111010'::varbit;

SELECT count(*) FROM varbittmp WHERE a  =  '1110100111010'::varbit;

SELECT count(*) FROM varbittmp WHERE a >=  '1110100111010'::varbit;

SELECT count(*) FROM varbittmp WHERE a >   '1110100111010'::varbit;

-- Test index-only scans
SET enable_bitmapscan=off;
EXPLAIN (COSTS OFF)
SELECT a FROM bittmp WHERE a BETWEEN '1000000' and '1000001';
