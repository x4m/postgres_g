# Copyright (c) 2026, PostgreSQL Global Development Group

# Tests for configuration hazards involving archive_mode=shared.
#
# Test 1 checks that a shared-mode standby whose upstream does not archive
# (archive_mode=off) is rejected at connection startup, since the upstream
# cannot provide the archival status reports the standby relies on.
#
# Test 2 exercises a mixed archiving topology:
#
#   primary (archive_mode=on)
#       |
#   standby (archive_mode=on)
#       |
#   cascade (archive_mode=shared)
#
# The cascading standby runs in shared mode and therefore asks its upstream
# (the standby) for archival status reports. The standby itself runs in plain
# "on" mode: it does not request reports from the primary, so it has nothing
# to relay downstream. As a result the cascading standby never receives an
# archival report: its received segments stay as .ready and
# pg_stat_wal_receiver.primary_last_archived stays empty. The test then shows
# that a shared-mode standby falls back to archiving segments itself: once its
# own archiver runs, it marks the segment as .done and starts archiving.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Shared archive directory used by every node that archives.
my $archive_dir = PostgreSQL::Test::Utils::tempdir();
my $archive_command =
  $PostgreSQL::Test::Utils::windows_os
  ? qq{copy "%p" "$archive_dir\\%f"}
  : qq{cp "%p" "$archive_dir/%f"};



###############################################################################
# Test 1: Primary archive_mode=off, standby archive_mode=shared
#
# A standby running in shared mode connects requesting archival status
# reports. If the primary does not archive at all (archive_mode=off), it
# cannot provide such reports, so the primary must reject the connection with
# a FATAL error and replication must never start.
###############################################################################

my $noarch_primary = PostgreSQL::Test::Cluster->new('noarch_primary');
$noarch_primary->init(allows_streaming => 1);
$noarch_primary->append_conf('postgresql.conf', "
archive_mode = off
wal_keep_size = 128MB
");
$noarch_primary->start;

# Sanity check: archiving is really disabled on the primary
is($noarch_primary->safe_psql('postgres', "SHOW archive_mode;"), 'off',
	"primary has archive_mode=off");

my $noarch_backup = 'noarch_backup';
$noarch_primary->backup($noarch_backup);

my $shared_standby = PostgreSQL::Test::Cluster->new('shared_standby');
$shared_standby->init_from_backup($noarch_primary, $noarch_backup,
	has_streaming => 1);
$shared_standby->append_conf('postgresql.conf', "
archive_mode = shared
archive_status_report_interval = 10ms
archive_command = '$archive_command'
wal_receiver_status_interval = 1s
");

# Note the current log position so wait_for_log() only inspects new output.
my $logstart = -s $shared_standby->logfile;
$shared_standby->start;

# The primary must reject the walreceiver's connection because it cannot
# provide archival status reports while archive_mode=off.
$shared_standby->wait_for_log(
	qr/FATAL:  archive status report requested, but archiving is not enabled/,
	$logstart);

# Generate WAL on the primary; it must not reach the standby since the
# walreceiver connection is refused.
$noarch_primary->safe_psql('postgres',
	"SELECT txid_current();SELECT pg_switch_wal();");

# The standby's walreceiver must never establish a connection.
is($shared_standby->safe_psql('postgres',
		"SELECT count(*) FROM pg_stat_wal_receiver;"),
	'0',
	"standby has no active wal receiver when connection is rejected");

# ... and the primary must see no walsender for it either.
is($noarch_primary->safe_psql('postgres',
		"SELECT count(*) FROM pg_stat_replication;"),
	'0',
	"primary has no replication connection when standby is rejected");


###############################################################################
# Test 2: Primary with archive_mode=on, standby with archive_mode=on streaming
# from the primary, and a cascading standby with archive_mode=shared streaming
# from the standby.
#
# The cascading standby running in shared mode connects requesting archival
# status reports from the standby, which has no report from the primary to
# relay.
###############################################################################

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(has_archiving => 1, allows_streaming => 1);
$primary->append_conf('postgresql.conf', "
archive_mode = on
archive_command = '$archive_command'
archive_status_report_interval = 10ms
wal_keep_size = 128MB
");
$primary->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$primary->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$primary->safe_psql('postgres', q(CREATE EXTENSION injection_points));

is($primary->safe_psql('postgres', "SHOW archive_mode;"), 'on',
	"primary has archive_mode=on");

# Make sure the primary actually archives something.
$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");
$primary->poll_query_until('postgres',
	"SELECT archived_count > 0 FROM pg_stat_archiver")
	or die "timed out waiting for primary to archive";

###############################################################################
# Standby with archive_mode=on, streaming from the primary
###############################################################################

my $standby_backup = 'standby_backup';
$primary->backup($standby_backup);

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $standby_backup, has_streaming => 1);
$standby->append_conf('postgresql.conf', "
archive_mode = on
archive_command = '$archive_command'
archive_status_report_interval = 10ms
wal_receiver_status_interval = 1s
");
$standby->start;

$primary->wait_for_catchup($standby);

is($standby->safe_psql('postgres', "SELECT count(*) FROM pg_stat_wal_receiver;"),
	'1', "standby wal receiver is connected to the primary");

###############################################################################
# Cascading standby with archive_mode=shared, streaming from the standby
###############################################################################

my $cascade_backup = 'cascade_backup';
$standby->backup($cascade_backup);

my $cascade = PostgreSQL::Test::Cluster->new('cascade');
$cascade->init_from_backup($standby, $cascade_backup, has_streaming => 1);
$cascade->append_conf('postgresql.conf', "
archive_mode = shared
archive_status_report_interval = 10ms
archive_command = '$archive_command'
wal_receiver_status_interval = 1s
");
$cascade->start;

# Pause the cascading standby's archiver so we can first observe the state
# before it archives anything on its own, then let it run in a controlled way.
$cascade->safe_psql('postgres',
	q{SELECT injection_points_attach('pgarch-main-loop', 'wait')});
$cascade->safe_psql('postgres',
	q{SELECT injection_points_attach('pgarch-main-loop-after-copy', 'wait')});

# The cascading standby connects successfully: its upstream (the standby) has
# archive_mode=on, so XLogArchivingActive() is true there and the connection
# is not rejected.
$standby->wait_for_catchup($cascade);
is($cascade->safe_psql('postgres', "SELECT count(*) FROM pg_stat_wal_receiver;"),
	'1', "cascading standby wal receiver is connected to the standby");

###############################################################################
# Generate WAL, archive it on the primary, and observe the cascade
###############################################################################

my $before = $primary->safe_psql('postgres',
	"SELECT archived_count FROM pg_stat_archiver");

my $current_walfile = $primary->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn())});

my $walfile_ready = "$current_walfile.ready";
my $walfile_done  = "$current_walfile.done";

$primary->safe_psql('postgres', "SELECT txid_current();SELECT pg_switch_wal();");
$primary->poll_query_until('postgres',
	"SELECT archived_count > $before FROM pg_stat_archiver")
	or die "timed out waiting for primary to archive new segment";

# Propagate the new WAL all the way down the chain.
$primary->wait_for_catchup($standby);
$standby->wait_for_catchup($cascade);

my $cascade_status = $cascade->data_dir . '/pg_wal/archive_status';

# The cascading standby must create status files for received WAL.
my $ready_seen = 0;
for (my $i = 0; $i < $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	$ready_seen = 0;
	if (opendir(my $dh, $cascade_status))
	{
		$ready_seen = scalar(grep { /\.ready$/ } readdir($dh));
		closedir($dh);
	}
	last if $ready_seen > 0;
	sleep(1);
}

# The cascading standby must have created a .ready status file for the segment.
ok( -f "$cascade_status/$walfile_ready",
	".ready file exists on cascade replica for WAL segment $current_walfile");

# The intermediate standby runs in "on" mode and never requested reports from
# the primary, so it cannot relay any report.  With the cascading standby's
# archiver still paused, the segment must remain .ready and no report can have
# marked it .done.
ok( -f "$cascade_status/$walfile_ready",
	"segment stays .ready on cascading standby without an archival report");
ok( !-f "$cascade_status/$walfile_done",
	"segment is not marked .done without an archival report");

# And its view column must stay empty: no report ever arrived.
is($cascade->safe_psql('postgres',
		"SELECT primary_last_archived FROM pg_stat_wal_receiver;"),
	'',
	"cascading standby primary_last_archived stays empty");

# Let the cascading standby's archiver run.  Since no archival report will ever
# arrive, a shared-mode standby must fall back to archiving the segment itself.
$cascade->safe_psql(
	'postgres', qq[
SELECT injection_points_detach('pgarch-main-loop');
SELECT injection_points_wakeup('pgarch-main-loop');
]);

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

# Wait until the archiver has run one copy cycle and parked on the injection
# point that follows it.
wait_for_injection_point($cascade, 'pgarch-main-loop-after-copy');

# The archiver archived the segment on its own: .ready is gone and .done exists.
ok( !-f "$cascade_status/$walfile_ready",
	"segment no longer .ready after cascading standby archives it itself");
ok( -f "$cascade_status/$walfile_done",
	"segment marked .done after cascading standby archives it itself");

# And pg_stat_archiver must reflect the self-archived segment.
is($cascade->safe_psql('postgres',
		"SELECT archived_count FROM pg_stat_archiver"),
	'1',
	"cascading standby starts archiving on its own");

done_testing();
