
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Tests for connection behavior prior to authentication.

use strict;
use warnings FATAL => 'all';

use File::Path qw(make_path remove_tree);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
log_connections = 'receipt,authentication'
]);

$node->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Establish the control connections before attaching the waitpoint.
my $psql = $node->background_psql('postgres');
my $other = $node->background_psql('postgres');
my $other_pid = $other->query_safe('SELECT pg_backend_pid()');

# Creating the directory opts this SQL-attached point into publishing a
# separate file for each waiter. Removing a file releases only that PID.
my $wait_root = $node->data_dir . '/pg_injection_points';
my $wait_dir = "$wait_root/init-pre-auth";
make_path($wait_dir);
$psql->query_safe("SELECT injection_points_attach('init-pre-auth', 'wait')");

# Model an unrelated process reaching the point before the new client.
# A name-only wakeup would release this waiter instead of the client.
$other->query_until(
	qr/other-waiter/, qq[
\\echo other-waiter
SELECT injection_points_run('init-pre-auth');
]);
wait_for_file("$wait_dir/$other_pid", qr/\A/);

# From this point on, all new connections will hang during startup, just before
# authentication. Use the $psql connection handle for server interaction.
my $conn = $node->background_psql('postgres', wait => 0);

# Wait for the connection to show up in pg_stat_activity, with the wait_event
# of the injection point.
my $pid = '';
for (1 .. 10 * $PostgreSQL::Test::Utils::timeout_default)
{
	$pid = $psql->query_safe(
		qq{SELECT pid FROM pg_stat_activity
  WHERE backend_type = 'client backend'
    AND state = 'starting'
    AND wait_event = 'init-pre-auth';});
	last if $pid ne "";

	usleep(100_000);
}

note "backend $pid is authenticating";
like($pid, qr/^\d+$/,
	'authenticating connections are recorded in pg_stat_activity')
  or die 'client did not reach the pre-authentication waitpoint';

# The wait event is reported before the waiter file is published. Wait for
# the file too, then release this client without waking an unrelated waiter.
wait_for_file("$wait_dir/$pid", qr/\A/);
unlink "$wait_dir/$pid" or die "could not release backend $pid: $!";
$conn->wait_connect();

# Make sure the pgstat entry is updated eventually.
my $state = '';
for (1 .. 10 * $PostgreSQL::Test::Utils::timeout_default)
{
	$state = $psql->query_safe(
		"SELECT state FROM pg_stat_activity WHERE pid = $pid;");
	last if $state eq "idle";

	note "state for backend $pid is '$state'; waiting for 'idle'...";
	usleep(100_000);
}

is($state, 'idle',
	'authenticated connections reach idle state in pg_stat_activity');
ok(-f "$wait_dir/$other_pid", 'unrelated waiter remains blocked');

$psql->query_safe("SELECT injection_points_detach('init-pre-auth');");
unlink "$wait_dir/$other_pid"
  or die "could not release backend $other_pid: $!";
$other->query_safe('SELECT 1');
$other->quit();
$psql->quit();
$conn->quit();

# Other processes may also have reached the point. Some background processes
# handle SIGTERM only after initialization, so use immediate shutdown before
# removing the markers. The module was loaded through SQL, not preloaded.
$node->stop('immediate');
remove_tree($wait_root);

done_testing();
