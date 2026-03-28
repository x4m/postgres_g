# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Regression test for false positive in indexallkeysmatch under concurrent
# VACUUM.  bt_index_check holds only AccessShareLock, which is compatible
# with VACUUM's ShareUpdateExclusiveLock.  Before the fix, VACUUM Phase 1
# could prune heap pages (LP_DEAD) between the Bloom filter probe and the
# SnapshotAny heap fetch, causing a spurious "non-existent heap tuple" error.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use IPC::Run ();

my $node = PostgreSQL::Test::Cluster->new('vacuum_race');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
shared_buffers = 32MB
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION amcheck');

my $port = $node->port;
my $host = $node->host;

# A large table so that VACUUM Phase 1 (heap scan + pruning) takes real time
# and overlaps with the bt_index_check run.  ~30K heap pages.
$node->safe_psql('postgres', q{
	CREATE TABLE race_tbl (id int, pad text);
	INSERT INTO race_tbl
		SELECT i, repeat('x', 200) FROM generate_series(1, 1000000) i;
	CREATE INDEX race_idx ON race_tbl (id);
	ANALYZE race_tbl;
});

my $false_positive = 0;

for my $attempt (1..10)
{
	# Delete all rows.  After commit, tuples are LP_NORMAL with a committed
	# xmax -- candidates for pruning by VACUUM or any reader.
	$node->safe_psql('postgres', 'DELETE FROM race_tbl');

	# Race VACUUM (which prunes heap to LP_DEAD) against bt_index_check
	# (which probes Bloom filter then does a heap fetch).
	my ($v_out, $v_err, $c_out, $c_err) = ('', '', '', '');

	my $vacuum_h = IPC::Run::start(
		['psql', '-X', '-h', $host, '-p', $port,
		 '-d', 'postgres', '-c', 'VACUUM race_tbl'],
		\my $v_in, \$v_out, \$v_err);

	my $check_h = IPC::Run::start(
		['psql', '-X', '-h', $host, '-p', $port,
		 '-d', 'postgres', '-c',
		 "SELECT bt_index_check('race_idx', false, false, true)"],
		\my $c_in, \$c_out, \$c_err);

	$check_h->finish;
	$vacuum_h->finish;

	if ($c_err =~ /non-existent heap tuple/)
	{
		$false_positive = 1;
		diag("Race triggered on attempt $attempt: $c_err");
		last;
	}

	diag("Attempt $attempt: ok (check stderr='$c_err')");

	# Restore data for next round.
	$node->safe_psql('postgres', q{
		TRUNCATE race_tbl;
		INSERT INTO race_tbl
			SELECT i, repeat('x', 200) FROM generate_series(1, 1000000) i;
		REINDEX INDEX race_idx;
	});
}

ok(!$false_positive,
   'no false positive corruption report during concurrent VACUUM');

$node->stop;
done_testing();
