CREATE EXTENSION pg_background;

--run 6 workers which wait .0, .1, .2, .3, .4, .5 seconds respectively
CREATE TABLE input as SELECT x FROM generate_series(0,.5,0.1) x ORDER BY x DESC;
CREATE TABLE output(place int,value float);
--sequence for indication of who's finished on which place
CREATE sequence s start 1;
CREATE TABLE handles as SELECT pg_background_launch(format('select pg_sleep(%s); insert into output values (nextval(''s''),%s);',x,x)) h FROM input;
--wait until everyone finishes
SELECT (SELECT * FROM pg_background_result(h) as (x text) limit 1) FROM handles;
--output results
SELECT * FROM output ORDER BY place;
