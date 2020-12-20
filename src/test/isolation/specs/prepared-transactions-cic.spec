# This test verifies that CREATE INDEX CONCURRENTLY interact with
# prepared transactions correctly.
setup
{
    CREATE TABLE cic_test (a int);
}

teardown
{
    DROP TABLE cic_test;
}


# Sessions for CREATE INDEX CONCURRENTLY test
session "s1"
step "w1" { BEGIN; INSERT INTO cic_test VALUES (1); }
step "p1" { PREPARE TRANSACTION 's1'; }
step "c1" { COMMIT PREPARED 's1'; }

session "s2"
setup { SET lock_timeout = 10; }
step "cic2"
{
    CREATE INDEX CONCURRENTLY on cic_test(a);
}
step "r2"
{
    SET enable_seqscan to off;
    SET enable_bitmapscan to off;
    SELECT * FROM cic_test WHERE a = 1;
}


permutation  "w1" "p1" "cic2" "c1" "r2"
