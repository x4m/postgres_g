# Copyright (c) 2026, PostgreSQL Global Development Group

# Demonstrate detecting and releasing an injection point wait from outside the
# server, without issuing any SQL.  The injection point shared state is backed
# by a file in the data directory (injection_points.shm); the standalone
# injection_points_state client maps that file the same way the backend does
# and is able to observe which point is being waited on and wake it up.
#
# This is the synchronization primitive needed when the waiting process has no
# PGPROC or no wait-event visibility (postmaster, early startup, ...), where a
# SQL-driven wakeup is not available and a fixed sleep would be unreliable.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $client = $ENV{INJECTION_POINTS_STATE};
if (!defined $client || $client eq '')
{
	plan skip_all => 'injection_points_state client not available';
}

# Preload the module so the postmaster creates the state file at startup.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'injection_points'");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $datadir = $node->data_dir;

# The backing file must exist as soon as the server is up.
ok(-f "$datadir/injection_points.shm",
	'injection point state file created at startup');

# Attach a wait point and trigger it from a background session, which blocks
# inside injection_wait().
$node->safe_psql('postgres',
	"SELECT injection_points_attach('external-wait', 'wait');");

my $session = $node->background_psql('postgres', on_error_stop => 0);
$session->query_until(
	qr/start/, qq[
	\\echo start
	SELECT injection_points_run('external-wait');
]);

# Use the external client to detect that the wait point was reached.  It polls
# the mapped name array instead of guessing with a sleep, so it works the same
# on a fast or a slow machine.
$node->command_ok(
	[ $client, $datadir, 'wait', 'external-wait' ],
	'external client detected the wait point without SQL');

# Release the waiter by bumping its counter through the mapped file, again
# without any SQL or backend connection.
$node->command_ok(
	[ $client, $datadir, 'wakeup', 'external-wait' ],
	'external client woke the waiter without SQL');

# The blocked SELECT must now finish.
$session->query_safe('SELECT 1;');
$session->quit;

# A second attempt to detect the (now cleared) point must time out rather than
# block forever, proving the tool reflects live state.
$node->command_fails_like(
	[ $client, $datadir, 'wait', 'external-wait', '1' ],
	qr/timed out/,
	'external client times out when no process waits');

$node->stop;

# The state file is removed together with the cluster.
ok(!-f "$datadir/injection_points.shm",
	'injection point state file removed at shutdown');

done_testing();
