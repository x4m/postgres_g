--
-- check that using the module's functions with unsupported relations will fail
--

-- partitioned tables (the parent ones) don't have visibility maps
create table test_partitioned (a int, b text default repeat('x', 5000)) partition by list (a);
-- these should all fail
select * from heapcheck_relation('test_partitioned');

create table test_partition partition of test_partitioned for values in (1);
create index test_index on test_partition (a);
-- indexes do not, so these all fail
select * from heapcheck_relation('test_index');

create view test_view as select 1;
-- views do not have vms, so these all fail
select * from heapcheck_relation('test_view');

create sequence test_sequence;
-- sequences do not have vms, so these all fail
select * from heapcheck_relation('test_sequence');

create foreign data wrapper dummy;
create server dummy_server foreign data wrapper dummy;
create foreign table test_foreign_table () server dummy_server;
-- foreign tables do not have vms, so these all fail
select * from heapcheck_relation('test_foreign_table');


