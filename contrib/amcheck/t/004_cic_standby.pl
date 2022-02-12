
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Minimal test testing REINDEX CONCURRENTLY with streaming replication
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
$node_primary->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node_primary->safe_psql('postgres', q(CREATE TABLE tbl(i int)));
$node_primary->safe_psql('postgres', q(CREATE INDEX idx ON tbl(i)));

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
	'--no-vacuum --client=1 --time=10',
	{
		'004_pgbench_standby_check_1' => q(
			set enable_bitmapscan to off;
			set enable_seqscan to off;
			\set id random(1, 1000000)
			select i from tbl where i = :id;
		   )
	},
	\$pgbench_out,
	$pgbench_timer);

# Run pgbench with data data manipulations and REINDEX on primary.
# pgbench might try to launch more than one instance of the RIC
# transaction concurrently.  That would deadlock, so use an advisory
# lock to ensure only one CIC runs at a time.
$node_primary->pgbench(
	'--no-vacuum --client=5 --time=10',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent INSERTs and CIC',
	{
		'004_pgbench_concurrent_transaction' => q(
			BEGIN;
			INSERT INTO tbl VALUES((random()*1000000)::int);
			COMMIT;
		  ),
		'004_pgbench_concurrent_ric' => q(
			SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
			\if :gotlock
                REINDEX INDEX CONCURRENTLY idx;
				SELECT pg_advisory_unlock(42);
			\endif
		  )
	});

$pgbench_h->pump_nb;
$pgbench_h->finish();
my $result =
    ($Config{osname} eq "MSWin32")
  ? ($pgbench_h->full_results)[0]
  : $pgbench_h->result(0);
is($result, 0, "pgbench with bt_index_check() on standby works");

# done
$node_primary->stop;
$node_standby_1->stop;
done_testing();