# Copyright (c) 2026, PostgreSQL Global Development Group

# Test archive_mode=shared for coordinated WAL archiving between primary and standby
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Path qw(rmtree);

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}


# Helper: Wait for a session to hit an injection point.
# Optional second argument is timeout in seconds.
# Returns true if found, false if timeout.
# On timeout, logs diagnostic information about all active queries.
sub wait_for_injection_point
{
	my ($node, $point_name, $timeout) = @_;
	$timeout //= $PostgreSQL::Test::Utils::timeout_default / 2;

	for (my $elapsed = 0; $elapsed < $timeout * 10; $elapsed++)
	{
		my $pid = $node->safe_psql(
			'postgres', qq[
			SELECT pid FROM pg_stat_activity
			WHERE wait_event_type = 'InjectionPoint'
			  AND wait_event = '$point_name'
			LIMIT 1;
		]);
		return 1 if $pid ne '';
		sleep(1);
	}

	# Timeout - report diagnostic information
	my $activity = $node->safe_psql(
		'postgres', q[
		SELECT format('pid=%s, state=%s, wait_event_type=%s, wait_event=%s, backend_xmin=%s, backend_xid=%s, query=%s',
			pid, state, wait_event_type, wait_event, backend_xmin, backend_xid, left(query, 100))
		FROM pg_stat_activity
		ORDER BY pid;
	]);
	diag(   "wait_for_injection_point timeout waiting for: $point_name\n"
		  . "Current queries in pg_stat_activity:\n$activity");

	return 0;
}

# Initialize primary node with archiving
my $archive_dir = PostgreSQL::Test::Utils::tempdir();
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(has_archiving => 1, allows_streaming => 1);

my $archive_command =
  $PostgreSQL::Test::Utils::windows_os
  ? qq{copy "%p" "$archive_dir\\%f"}
  : qq{cp "%p" "$archive_dir/%f"};

$primary->append_conf('postgresql.conf', "
archive_mode = shared
archive_status_report_interval = 10ms
archive_command = '$archive_command'
wal_keep_size = 128MB
");
$primary->start;

###############################################################################
# Test 1: Basic testing
###############################################################################

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$primary->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$primary->safe_psql('postgres', q(CREATE EXTENSION injection_points));

# Ensure WAL activity exists in the current segment before switching.
# pg_switch_wal() is a no-op when called at the very start of a segment,
# so we write a bump transaction counter
# first to guarantee there is WAL to switch away from.
$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");

# Wait for archiver to archive segments
$primary->poll_query_until('postgres',
	"SELECT archived_count > 0 FROM pg_stat_archiver")
	or die "Timed out waiting for archiver to complete archiving";

my $archived_count = () = glob("$archive_dir/*");
ok($archived_count > 0, "primary has archived WAL files to shared archive");
note("Primary archived $archived_count files");

# Take backup for standby
my $backup_name = 'standby_backup';
$primary->backup($backup_name);

# Exclude possible race condition when backup WAL is last archived
$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");

# Set up standby with archive_mode=shared
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', "
archive_mode = shared
archive_status_report_interval = 10ms
archive_command = '$archive_command'
wal_receiver_status_interval = 1s
");
$standby->start;

# Pause the standby's archiver so it stays idle during recovery.
$standby->safe_psql('postgres',
	q{SELECT injection_points_attach('pgarch-main-loop', 'wait')});

# Wait for standby to catch up
$primary->wait_for_catchup($standby);

# Generate more WAL on primary (these are new segments not yet archived)
$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");

# Wait for standby to receive the new WAL
$primary->wait_for_catchup($standby);

# Check that standby has .ready or .done files for the newly received segments.
# Normally they should be .ready (not yet archived by primary), but in rare cases
# the archiver could be very fast and an archive report sent immediately, creating
# .done files instead. Both are correct behavior - the key is that files exist.
my $standby_archive_status = $standby->data_dir . '/pg_wal/archive_status';
my $status_count = 0;
if (opendir(my $dh, $standby_archive_status))
{
	my @files = grep { /\.(ready|done)$/ } readdir($dh);
	$status_count = scalar(@files);
	my $ready_count = scalar(grep { /\.ready$/ } @files);
	my $done_count = scalar(grep { /\.done$/ } @files);
	note("Standby has $ready_count .ready files and $done_count .done files");
	closedir($dh);
}
cmp_ok($status_count, '>', 0, "standby creates archive status files for received WAL");

# Generate more WAL and wait for archiving on primary
my $initial_archived = $primary->safe_psql('postgres', 'SELECT archived_count FROM pg_stat_archiver');
my $primary_last_archived = $primary->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn())});

$standby->safe_psql('postgres',
	q{SELECT injection_points_attach('walreceiver-after-primary-last-archived', 'wait')});

$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");

# Wait for primary to archive the new segments
$primary->poll_query_until('postgres',
	"SELECT archived_count > $initial_archived FROM pg_stat_archiver")
	or die "Timed out waiting for primary to archive new segments";

$standby->safe_psql('postgres',
	q{SELECT injection_points_attach('walreceiver-after-force-done', 'wait')});

$standby->safe_psql(
	'postgres', qq[
SELECT injection_points_detach('walreceiver-after-primary-last-archived');
SELECT injection_points_wakeup('walreceiver-after-primary-last-archived');
]);

wait_for_injection_point($standby, 'walreceiver-after-force-done');

$standby->safe_psql(
	'postgres', qq[
SELECT injection_points_detach('walreceiver-after-force-done');
SELECT injection_points_wakeup('walreceiver-after-force-done');
]);

# Wait for standby to catch up (archive status is sent during replication)
$primary->wait_for_catchup($standby);

# Wait for primary to send archival status updates and standby to process them
# The standby should mark segments as .done after receiving archive status from primary
my $done_count = 0;
if (opendir(my $dh, $standby_archive_status))
{
	$done_count = scalar(grep { /\.done$/ } readdir($dh));
	closedir($dh);
}

ok($done_count > 0, "standby marked segments as .done after primary's archival report");
note("Standby has $done_count .done files");

# The primary_last_archived done status for WAL file itself must also be present.
ok(-f "$standby_archive_status/$primary_last_archived.done",
	"WAL segment $primary_last_archived done file exists in standby pg_wal/archive_status");

is($primary_last_archived, $standby->safe_psql('postgres',
	q{SELECT primary_last_archived from pg_stat_wal_receiver; }),  "standby wal receiver stat view updated with primary last archived wal");

###############################################################################
# Test 2: Cascading replication
###############################################################################

# Take a backup from the promoted standby (now the new primary)
my $promoted_backup = 'promoted_backup';
$standby->backup($promoted_backup);

# Set up second-level standby (cascading from first standby, now promoted)
my $cascade_standby = PostgreSQL::Test::Cluster->new('cascade_standby');
$cascade_standby->init_from_backup($standby, $promoted_backup, has_streaming => 1);
$cascade_standby->append_conf('postgresql.conf', "
archive_mode = shared
archive_status_report_interval = 10ms
archive_command = '$archive_command'
wal_receiver_status_interval = 1s
");
$cascade_standby->start;

# Generate WAL
my $cascading_archived_before = $primary->safe_psql('postgres', 'SELECT archived_count FROM pg_stat_archiver');

my $current_walfile = $primary->safe_psql('postgres', "SELECT pg_walfile_name(pg_current_wal_lsn());");

$cascade_standby->safe_psql('postgres',
	q{SELECT injection_points_attach('walreceiver-after-primary-last-archived', 'wait')});

$primary->safe_psql(
	'postgres', q{
	CHECKPOINT;
	SELECT pg_switch_wal();
});

my $walfile_ready = "pg_wal/archive_status/$current_walfile.ready";
my $walfile_done = "pg_wal/archive_status/$current_walfile.done";

# Wait for the primary to send archive status
$primary->poll_query_until('postgres',
	"SELECT archived_count > $cascading_archived_before FROM pg_stat_archiver")
	or die "Timed out waiting for primary to archive segment in cascading test";


wait_for_injection_point($cascade_standby, 'walreceiver-after-primary-last-archived');

$cascade_standby->safe_psql(
	'postgres', qq[
SELECT injection_points_detach('walreceiver-after-primary-last-archived');
SELECT injection_points_wakeup('walreceiver-after-primary-last-archived');
]);

# Wait for cascading standby to catch up
$standby->wait_for_catchup($cascade_standby);

my $cascade_data = $cascade_standby->data_dir;
my $cascade_standby_archive_status = $cascade_standby->data_dir . '/pg_wal/archive_status';

for (my $i = 0; $i < $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	if (-f "$cascade_data/$walfile_done")
	{
		last;
	}
	sleep(1);
}

# Wait for cascading standby to receive archive status and mark segments as .done
ok( !-f "$cascade_data/$walfile_ready",
	".ready file exists on cascade replica for WAL segment $current_walfile"
);
ok( -f "$cascade_data/$walfile_done",
	".done file exists on cascade replica for WAL segment $current_walfile"
);


is($current_walfile, $cascade_standby->safe_psql('postgres',
	q{SELECT primary_last_archived from pg_stat_wal_receiver; }), 
	"cascade standby wal receiver stat view updated with primary last archived wal");

done_testing();
