-- char check

DROP TABLE IF EXISTS vchartmp;
CREATE TABLE vchartmp (a varchar(32));

\copy vchartmp from 'data/char.data'

CREATE INDEX vcharidx ON vchartmp USING GIST ( text(a) ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM vchartmp WHERE a <   '31b0'::varchar(32);

SELECT count(*) FROM vchartmp WHERE a <=  '31b0'::varchar(32);

SELECT count(*) FROM vchartmp WHERE a  =  '31b0'::varchar(32);

SELECT count(*) FROM vchartmp WHERE a >=  '31b0'::varchar(32);

SELECT count(*) FROM vchartmp WHERE a >   '31b0'::varchar(32);
