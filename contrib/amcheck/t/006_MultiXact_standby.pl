
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Minimal test testing multixacts with streaming replication
use strict;
use warnings;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 4;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
# A specific role is created for replication purposes
$node_primary->init(
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);
$node_primary->append_conf('postgresql.conf', 'lock_timeout = 180000');
$node_primary->append_conf('postgresql.conf', 'max_connections = 500');
$node_primary->start;
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby_1 = PostgreSQL::Test::Cluster->new('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_1->start;

# Create some content on primary and check its presence in standby nodes
$node_primary->safe_psql('postgres', q(create table tbl2 (
    id int primary key,
    val int
);
insert into tbl2 select i, 0 from generate_series(1,100000) i;
));

# Wait for standbys to catch up
my $primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby_1, 'replay', $primary_lsn);

#
# Stress CIC with pgbench
#

# Run background pgbench with bt_index_check on standby
my $pgbench_out   = '';
my $pgbench_timer = IPC::Run::timeout(180);
my $pgbench_h     = $node_standby_1->background_pgbench(
	'--no-vacuum --report-per-command -M prepared -c 10 -j 2 -T 10',
	{
		'006_pgbench_standby_check_1' => q(
			begin;
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			select sum(val) from tbl2;
			\sleep 10 ms
			commit;
		   )
	},
	\$pgbench_out,
	$pgbench_timer);

# Run pgbench with data data manipulations and REINDEX on primary.
# pgbench might try to launch more than one instance of the RIC
# transaction concurrently.  That would deadlock, so use an advisory
# lock to ensure only one CIC runs at a time.
$node_primary->pgbench(
	'--no-vacuum --report-per-command -M prepared -c 10 -j 2 -T 10',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent updates',
	{
		'004_pgbench_updates' => q(
			\set id random(1, 10000)
			begin;
			select * from tbl2 where id = :id for no key update;
			\sleep 10 ms
			savepoint s1;
			update tbl2 set val = val+1 where id = :id;
			\sleep 10 ms
			commit;
		  )
	});

$pgbench_h->pump_nb;
$pgbench_h->finish();
my $result =
    ($Config{osname} eq "MSWin32")
  ? ($pgbench_h->full_results)[0]
  : $pgbench_h->result(0);
is($result, 0, "pgbench with bt_index_check() on standby works");


# Check that no deadlock occured
$primary_lsn = $node_primary->lsn('write');
$node_primary->wait_for_catchup($node_standby_1, 'replay', $primary_lsn);

# done
$node_primary->stop;
$node_standby_1->stop;
done_testing();