# Copyright (c) 2026, PostgreSQL Global Development Group

# Test verification of WAL through an explicit recovery target.

use strict;
use warnings FATAL => 'all';
use File::Copy qw(copy);
use File::Path qw(make_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;
$primary->safe_psql('postgres',
	'CREATE TABLE test_reachability (a integer)');
$primary->backup('early');

# Choose the point at which the second node will leave the primary's history.
$primary->safe_psql('postgres', 'CHECKPOINT');
my $fork_lsn = $primary->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');

my $branch = PostgreSQL::Test::Cluster->new('branch');
$branch->init_from_backup($primary, 'early', has_streaming => 1);
$branch->append_conf('postgresql.conf', qq(
recovery_target_lsn = '$fork_lsn'
recovery_target_action = 'pause'
));
$branch->start;
$branch->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die "timed out waiting for recovery to pause";

# Advance the old timeline beyond the future branch point and take a backup
# which cannot be recovered along the branch's history.
$primary->safe_psql('postgres',
	'INSERT INTO test_reachability SELECT g FROM generate_series(1, 100) g');
$primary->backup('late');

$branch->promote;
$branch->poll_query_until('postgres',
	'SELECT NOT pg_is_in_recovery()')
  or die "timed out waiting for promotion";
$branch->safe_psql('postgres',
	'INSERT INTO test_reachability VALUES (200)');
my $target_lsn = $branch->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
my $target_tli = $branch->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
$branch->safe_psql('postgres', 'SELECT pg_switch_wal()');

# pg_verifybackup expects directly readable WAL files.  Materialize the union
# of the two backup pg_wal directories and the branch's current pg_wal.
my $wal_path = $primary->backup_dir . '/target_wal';
make_path($wal_path);
for my $source (
	$branch->data_dir . '/pg_wal',
	$primary->backup_dir . '/early/pg_wal',
	$primary->backup_dir . '/late/pg_wal')
{
	for my $name (grep { /^[0-9A-F]{24}$|^[0-9A-F]{8}\.history$/ }
		slurp_dir($source))
	{
		copy("$source/$name", "$wal_path/$name")
		  or die "could not copy $source/$name: $!";
	}
}

command_ok(
	[
		'pg_verifybackup',
		'--wal-path' => $wal_path,
		'--target-timeline' => $target_tli,
		'--target-lsn' => $target_lsn,
		$primary->backup_dir . '/early'
	],
	'early backup can reach target on child timeline');

command_fails_like(
	[
		'pg_verifybackup',
		'--wal-path' => $wal_path,
		'--target-timeline' => $target_tli,
		'--target-lsn' => $target_lsn,
		$primary->backup_dir . '/late'
	],
	qr/target timeline \d+ does not descend from backup end/,
	'backup taken after fork cannot reach target timeline');

done_testing();
