-- Simple test for log_dropped_tables extension
-- Make sure the extension is loaded via shared_preload_libraries or session_preload_libraries

-- Test 1: Single table drop
CREATE TABLE test_single (id int, name text);
INSERT INTO test_single VALUES (1, 'test');
DROP TABLE test_single;
-- Expected: INFO message with table name and LSN

-- Test 2: Multiple tables in one transaction
BEGIN;
CREATE TABLE test_multi1 (x int);
CREATE TABLE test_multi2 (y int);
DROP TABLE test_multi1, test_multi2;
COMMIT;
-- Expected: INFO message showing 2 tables dropped

-- Test 3: Rollback (should not log)
BEGIN;
CREATE TABLE test_rollback (z int);
DROP TABLE test_rollback;
ROLLBACK;
-- Expected: No INFO messages (transaction rolled back)

-- Test 4: Schema-qualified table
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_table (id int);
DROP TABLE test_schema.test_table;
DROP SCHEMA test_schema;
-- Expected: INFO showing test_schema.test_table

\echo 'Check the PostgreSQL server log for INFO messages'
\echo 'Each commit with dropped tables should show:'
\echo '  - Transaction commit LSN'
\echo '  - Table name (schema.table)'
\echo '  - OID and relfilenode'

