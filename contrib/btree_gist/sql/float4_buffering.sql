-- float4 check

DROP TABLE IF EXISTS float4tmp;
CREATE TABLE float4tmp (a float4);

\copy float4tmp from 'data/float4.data'

CREATE INDEX float4idx ON float4tmp USING gist ( a ) WITH (buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM float4tmp WHERE a <  -179.0::float4;

SELECT count(*) FROM float4tmp WHERE a <= -179.0::float4;

SELECT count(*) FROM float4tmp WHERE a  = -179.0::float4;

SELECT count(*) FROM float4tmp WHERE a >= -179.0::float4;

SELECT count(*) FROM float4tmp WHERE a >  -179.0::float4;

EXPLAIN (COSTS OFF)
SELECT a, a <-> '-179.0' FROM float4tmp ORDER BY a <-> '-179.0' LIMIT 3;
SELECT a, a <-> '-179.0' FROM float4tmp ORDER BY a <-> '-179.0' LIMIT 3;
