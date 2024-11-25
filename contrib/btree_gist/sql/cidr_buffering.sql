-- cidr check

DROP TABLE IF EXISTS cidrtmp;
CREATE TABLE cidrtmp AS
  SELECT cidr(a) AS a FROM inettmp ;

CREATE INDEX cidridx ON cidrtmp USING gist ( a ) WITH(buffering = on);

SET enable_seqscan=off;

SELECT count(*) FROM cidrtmp WHERE a <  '121.111.63.82'::cidr;

SELECT count(*) FROM cidrtmp WHERE a <= '121.111.63.82'::cidr;

SELECT count(*) FROM cidrtmp WHERE a  = '121.111.63.82'::cidr;

SELECT count(*) FROM cidrtmp WHERE a >= '121.111.63.82'::cidr;

SELECT count(*) FROM cidrtmp WHERE a >  '121.111.63.82'::cidr;
