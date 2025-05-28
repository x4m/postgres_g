-- Test for log_ddl_lsn (GUC) bug: repeated drop log after rollback
-- (This test reproduces the bug where a drop callback is not unregistered, so it logs a drop message even after rollback.)

SET log_ddl_lsn TO on;

CREATE TABLE test_guc_log_ddl_lsn (x int);

BEGIN;
DROP TABLE test_guc_log_ddl_lsn;
ROLLBACK;

-- Now commit (no transaction is active). This should not log a drop message.
COMMIT;

-- Clean up (drop the table if it still exists).
DROP TABLE IF EXISTS test_guc_log_ddl_lsn; 