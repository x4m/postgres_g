CREATE TABLE test1 (a int);

CREATE FUNCTION autonomous_test() RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  FOR i IN 0..9 LOOP
    START TRANSACTION;
    EXECUTE 'INSERT INTO test1 VALUES (' || i::text || ')';
    IF i % 2 = 0 THEN
        COMMIT;
    ELSE
        ROLLBACK;
    END IF;
  END LOOP;

  RETURN 42;
END;
$$;


SELECT autonomous_test();

SELECT * FROM test1;

TRUNCATE test1;


CREATE FUNCTION autonomous_test2() RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  FOR i IN 0..9 LOOP
    START TRANSACTION;
    INSERT INTO test1 VALUES (i);
    IF i % 2 = 0 THEN
        COMMIT;
    ELSE
        ROLLBACK;
    END IF;
  END LOOP;

  RETURN 42;
END;
$$;


SELECT autonomous_test2();

SELECT * FROM test1;

DROP TABLE test1;