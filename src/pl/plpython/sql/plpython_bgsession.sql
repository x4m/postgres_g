CREATE TABLE test1 (a int, b text);

CREATE FUNCTION bgsession_test() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
    for i in range(0, 10):
        a.execute("BEGIN")
        a.execute("INSERT INTO test1 (a) VALUES (%d)" % i)
        if i % 2 == 0:
            a.execute("COMMIT")
        else:
            a.execute("ROLLBACK")

return 42
$$;

SELECT bgsession_test();

SELECT * FROM test1;


CREATE FUNCTION bgsession_test2() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
        a.execute("BEGIN")
        a.execute("INSERT INTO test1 (a) VALUES (11)")
        rv = a.execute("SELECT * FROM test1")
        plpy.info(rv)
        a.execute("ROLLBACK")

return 42
$$;

SELECT bgsession_test2();

SELECT * FROM test1;


CREATE FUNCTION bgsession_test3() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
    a.execute("DO $_$ BEGIN RAISE NOTICE 'notice'; END $_$")
    a.execute("DO $_$ BEGIN RAISE EXCEPTION 'error'; END $_$")

return 42
$$;

SELECT bgsession_test3();


CREATE FUNCTION bgsession_test4() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
    a.execute("SET client_encoding TO SJIS")

return 42
$$;

SELECT bgsession_test4();


TRUNCATE test1;

CREATE FUNCTION bgsession_test5() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
    plan = a.prepare("INSERT INTO test1 (a, b) VALUES ($1, $2)", ["int4", "text"])
    a.execute_prepared(plan, [1, "one"])
    a.execute_prepared(plan, [2, "two"])

return 42
$$;

SELECT bgsession_test5();

SELECT * FROM test1;


TRUNCATE test1;

CREATE FUNCTION bgsession_test7() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
        a.execute("BEGIN")
        plan = a.prepare("INSERT INTO test1 (a) VALUES ($1)", ["int4"])
        a.execute_prepared(plan, [11])
        plan = a.prepare("SELECT * FROM test1")
        rv = a.execute_prepared(plan, [])
        plpy.info(rv)
        a.execute("ROLLBACK")

return 42
$$;

SELECT bgsession_test7();

SELECT * FROM test1;


CREATE FUNCTION bgsession_test8() RETURNS integer
LANGUAGE plpythonu
AS $$
with plpy.BackgroundSession() as a:
        a.execute("BEGIN")

return 42
$$;

SELECT bgsession_test8();


TRUNCATE test1;

CREATE FUNCTION bgsession_test9a() RETURNS integer
LANGUAGE plpythonu
AS $$
bg = plpy.BackgroundSession()
GD['bg'] = bg
bg.execute("BEGIN")
bg.execute("INSERT INTO test1 VALUES (1)")

return 1
$$;

CREATE FUNCTION bgsession_test9b() RETURNS integer
LANGUAGE plpythonu
AS $$
bg = GD['bg']
bg.execute("INSERT INTO test1 VALUES (2)")
bg.execute("COMMIT")
bg.close()

return 2
$$;

SELECT bgsession_test9a();
SELECT bgsession_test9b();

SELECT * FROM test1;


DROP TABLE test1;
