# Copyright (c) 2026, PostgreSQL Global Development Group

# A recent base backup need not contain intermediate timeline history files.
# Request them directly from an ordinary standby, without a downstream standby.
use strict;
use warnings FATAL => 'all';

use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $helper = "$FindBin::RealBin/walsender_restore";
my $scratch_count = q{SELECT count(*) FROM pg_ls_dir('pg_wal') AS f
  WHERE f LIKE 'xlogtemp.%.walsender'};
my $primary = PostgreSQL::Test::Cluster->new('history_primary');
$primary->init(allows_streaming => 1, has_archiving => 1);
$primary->start;
$primary->safe_psql('postgres',
	'CREATE TABLE history_test (i int); INSERT INTO history_test VALUES (1)');

# Create timelines 2 and 3 before taking a fresh backup on timeline 3.
for (1 .. 2)
{
	$primary->stop('immediate');
	$primary->set_standby_mode;
	$primary->start;
	$primary->promote;
}
my $archive = $primary->archive_dir;
$primary->poll_query_until('postgres',
	"SELECT (pg_stat_file('$archive/00000002.history', true)).size > 0")
	  or BAIL_OUT('intermediate history was not archived');
$primary->backup('base');

my $standby = PostgreSQL::Test::Cluster->new('history_standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
my $restore_command = qq{"$^X" "$helper" "$archive" "%f" "%p" "%r"};
$restore_command =~ s/\\/\\\\/g;
$restore_command =~ s/'/''/g;
$standby->append_conf('postgresql.conf', qq(
archive_mode = off
restore_command = '$restore_command'
));
$standby->start;
$primary->wait_for_catchup($standby, 'replay', $primary->lsn('flush'));
is($standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	't', 'node remains a standby');
is($standby->safe_psql('postgres', 'SELECT count(*) FROM pg_stat_replication'),
	'0', 'standby has no downstream replication');
ok(-f $standby->data_dir . '/pg_wal/00000003.history',
	'backup supplied the current timeline history');
ok(!-e $standby->data_dir . '/pg_wal/00000002.history',
	'backup did not supply the intermediate timeline history');

my $history_command = [
	'psql', '-XAt',
	'--dbname' => $standby->connstr . ' replication=1',
	'--command' => 'TIMELINE_HISTORY 2'
];
my $history_error = qr/could not open file "pg_wal\/00000002\.history"/;
command_fails_like($history_command, $history_error,
	'history fallback is disabled by default');
is($standby->safe_psql('postgres', $scratch_count),
	'0', 'disabled fallback creates no scratch copy');

# The current history can still be read locally with fallback disabled.
my $current = slurp_file($standby->data_dir . '/pg_wal/00000003.history');
my $expected_current = "00000003.history|$current";
command_like(
	[
		'psql', '-XAt',
		'--dbname' => $standby->connstr . ' replication=1',
		'--command' => 'TIMELINE_HISTORY 3'
	],
	qr/\Q$expected_current\E/, 'current history is served from pg_wal');

$standby->append_conf('postgresql.conf', 'wal_sender_restore = on');
$standby->reload;
$standby->poll_query_until('postgres',
	"SELECT current_setting('wal_sender_restore')::boolean")
	  or BAIL_OUT('history fallback was not enabled');
rename("$archive/00000002.history", "$archive/00000002.history.saved")
	  or die "hide history file: $!";
command_fails_like($history_command, $history_error,
	'a missing archived history file fails the command');
rename("$archive/00000002.history.saved", "$archive/00000002.history")
	  or die "restore history file: $!";
$standby->poll_query_until('postgres', $scratch_count, '0')
	  or BAIL_OUT('failed history restore left a scratch copy');

my $history = slurp_file("$archive/00000002.history");
my $expected = "00000002.history|$history";
command_like($history_command, qr/\Q$expected\E/,
	'physical replication connection receives the archived history contents');

# Reuse one database replication connection to check repeated cleanup.
command_like(
	[
		'psql', '-XAt',
		'--dbname' => $standby->connstr('postgres') . ' replication=database',
		'--command' => 'TIMELINE_HISTORY 2',
		'--command' => 'TIMELINE_HISTORY 2'
	],
	qr/\Q$expected\E.*\Q$expected\E/s,
	'repeated history requests on one database replication connection');
$standby->poll_query_until('postgres', $scratch_count, '0')
	  or BAIL_OUT('history requests left scratch copies');
ok(!-e $standby->data_dir . '/pg_wal/00000002.history',
	'restored history was not installed in pg_wal');

# The node continues to receive ordinary streaming replication throughout.
$primary->safe_psql('postgres', 'INSERT INTO history_test VALUES (2)');
$primary->wait_for_catchup($standby, 'replay', $primary->lsn('insert'));
is($standby->safe_psql('postgres', 'SELECT sum(i) FROM history_test'),
	'3', 'ordinary standby continues replaying upstream WAL');
is($standby->safe_psql('postgres', 'SELECT count(*) FROM pg_stat_replication'),
	'0', 'standby still has no downstream replication');
$standby->stop;
$primary->stop;
done_testing();
