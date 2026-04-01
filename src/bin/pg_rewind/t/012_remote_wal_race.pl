# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Test for a race condition in pg_rewind --source-server mode:
# If the source server generates WAL between the file list snapshot
# and the pg_current_wal_insert_lsn() call, the target can end up
# with a minRecoveryPoint beyond the copied WAL, causing silent
# incomplete recovery and data loss.
#
# Strategy: launch pg_rewind and heavy WAL generation concurrently,
# then verify that the target's WAL actually covers minRecoveryPoint
# by running pg_waldump up to that LSN.  This is a deterministic
# check that does not depend on whether the server can start.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy qw(copy);

my $tmp_folder = PostgreSQL::Test::Utils::tempdir;

# Set up primary
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', qq(
wal_keep_size = 320MB
wal_log_hints = on
));
$node_primary->start;
$node_primary->safe_psql('postgres', 'CREATE DATABASE testdb');

# Create test data on primary
$node_primary->safe_psql('testdb',
	"CREATE TABLE t1 (d text)");
$node_primary->safe_psql('testdb',
	"INSERT INTO t1 VALUES ('before promotion')");
$node_primary->safe_psql('testdb', "CHECKPOINT");

# Set up standby
$node_primary->backup('my_backup');
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, 'my_backup',
	has_streaming => 1);
$node_standby->start;
$node_primary->wait_for_catchup($node_standby, 'write');

# Promote standby
$node_standby->promote;
$node_standby->poll_query_until('testdb',
	"SELECT NOT pg_is_in_recovery()");

# Pre-generate a large dataset on the promoted standby.
# This makes pg_rewind's file copy take significant time, widening
# the window for the race.
$node_standby->safe_psql('testdb', qq{
	CREATE TABLE bulk (id serial, data text);
	INSERT INTO bulk (data) SELECT repeat('x', 200)
		FROM generate_series(1, 500000);
	CREATE TABLE counter (id serial, batch int);
	INSERT INTO counter (batch) SELECT 0 FROM generate_series(1, 1000);
	CHECKPOINT;
});

# Generate divergent WAL on old primary, then stop it
$node_primary->safe_psql('testdb',
	"INSERT INTO t1 VALUES ('after promotion on old primary')");
$node_primary->stop('fast');

# Save primary's postgresql.conf
my $primary_pgdata = $node_primary->data_dir;
copy("$primary_pgdata/postgresql.conf",
	"$tmp_folder/primary-postgresql.conf.tmp")
  or die "copy postgresql.conf: $!";

# Launch pg_rewind in the background so we can start WAL generation
# concurrently.
my ($rewind_out, $rewind_err);
my $rewind_handle = IPC::Run::start(
	[
		'pg_rewind', '--debug',
		'--source-server' => $node_standby->connstr('testdb'),
		'--target-pgdata' => $primary_pgdata,
		'--no-sync',
		'--config-file'   => "$tmp_folder/primary-postgresql.conf.tmp",
	],
	'<', \undef, '>', \$rewind_out, '2>', \$rewind_err);

# Immediately start background WAL generation
my @bg_handles;
for my $worker (1 .. 8)
{
	my ($out, $err);
	my $h = IPC::Run::start(
		[
			'psql', '-X', '-q',
			'-d', $node_standby->connstr('testdb'),
			'-c', qq{
DO \$\$
BEGIN
  FOR i IN 1..200 LOOP
    INSERT INTO counter (batch)
      SELECT ${worker} * 10000 + i FROM generate_series(1, 2000);
    COMMIT;
  END LOOP;
END \$\$;
}
		],
		'<', \undef, '>', \$out, '2>', \$err);
	push @bg_handles, $h;
}

# Wait for pg_rewind to complete
IPC::Run::finish($rewind_handle);
my $rewind_exit = $?;
ok($rewind_exit == 0, 'pg_rewind with concurrent WAL generation succeeds');
if ($rewind_exit != 0)
{
	diag "pg_rewind stderr: $rewind_err";
}

# Wait for all background workers to finish
for my $h (@bg_handles)
{
	IPC::Run::finish($h);
}

# Get minRecoveryPoint and divergence LSN for verification
my $controldata = `pg_controldata '$primary_pgdata'`;
my ($min_recovery_point) =
  ($controldata =~ /Minimum recovery ending location:\s+(\S+)/);
note "target minRecoveryPoint: $min_recovery_point";

my $history_content =
  slurp_file("$primary_pgdata/pg_wal/00000002.history");
my ($diverge_lsn) = ($history_content =~ /^1\t([0-9A-F\/]+)\t/m);
die "could not parse divergence LSN from timeline history"
  unless $diverge_lsn;
note "divergence LSN: $diverge_lsn";

# Diagnostics
my @target_wal = sort grep { /^00000002/ && !/history/ }
  map { s/.*\///r } glob("$primary_pgdata/pg_wal/00000002*");
note "target TL2 WAL segments: @target_wal";

# verify that the WAL on the target actually reaches
# minRecoveryPoint.  Without the fix, pg_rewind sets
# minRecoveryPoint to a position beyond the available WAL, so
# pg_waldump will fail trying to read up to that point.
command_ok(
	[
		'pg_waldump', '--quiet',
		'--path' => "$primary_pgdata/pg_wal",
		'--timeline' => '2',
		'--start' => $diverge_lsn,
		'--end'   => $min_recovery_point,
	],
	'WAL on target covers minRecoveryPoint');

# Also verify the server can start and has consistent data
copy("$tmp_folder/primary-postgresql.conf.tmp",
	"$primary_pgdata/postgresql.conf")
  or die "restore postgresql.conf: $!";

$node_primary->start;

my $t1_value = $node_primary->safe_psql('testdb', 'SELECT d FROM t1');
is($t1_value, 'before promotion',
	't1 has correct data after rewind');

my $batch0 = $node_primary->safe_psql('testdb',
	'SELECT count(*) FROM counter WHERE batch = 0');
is($batch0, '1000',
	'pre-rewind data is present after rewind');

$node_primary->teardown_node;
$node_standby->teardown_node;

done_testing();
