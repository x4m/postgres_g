-- Test script for log_dropped_tables extension
-- This should be run against a running PostgreSQL instance with the extension installed

-- First, we need to preload the library
-- This should be done in postgresql.conf: shared_preload_libraries = 'log_dropped_tables'
-- or session_preload_libraries = 'log_dropped_tables'

-- Create a test database
CREATE DATABASE test_log_drops;
\c test_log_drops

-- Create some test tables
CREATE TABLE test_table1 (id int, name text);
CREATE TABLE test_table2 (id int, value numeric);
CREATE TABLE test_table3 (id int);

-- Insert some data
INSERT INTO test_table1 VALUES (1, 'test');
INSERT INTO test_table2 VALUES (1, 3.14);

-- Drop the tables (this should trigger our logging)
DROP TABLE test_table1;
DROP TABLE test_table2;

-- Try dropping multiple tables in one transaction
BEGIN;
CREATE TABLE multi_drop1 (x int);
CREATE TABLE multi_drop2 (y int);
DROP TABLE multi_drop1, multi_drop2;
COMMIT;

-- Check that our extension is loaded
-- The log messages should appear in the PostgreSQL server log
\echo 'Check the PostgreSQL server log for INFO messages about dropped tables'

