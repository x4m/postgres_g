# Copyright (c) 2026, PostgreSQL Global Development Group

# Drive an injection point entirely from outside the server, without issuing
# any SQL to arm or coordinate it.  Two files in the data directory back the
# shared state:
#
#   - injection_points.shm: the core registry of attached points (see
#     src/backend/utils/misc/injection_point.c).  Writing it arms a point.
#   - injection_points_wait.shm: this module's wait/wakeup coordination (see
#     injection_points.c).
#
# The standalone injection_points_state client maps these files the same way
# the backend does and is able to arm a "wait" point, detect that a process
# reached it, release it, and disarm it -- all without a backend connection.
# This is the synchronization primitive needed when the cooperating process
# has no PGPROC or no wait-event visibility (postmaster, early startup, ...),
# where SQL-driven attach/wakeup is not available and a fixed sleep would be
# unreliable.  SQL is used here only to *trigger* the point (the code path that
# would normally contain the INJECTION_POINT() macro) and to observe state.

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

# Preload the module so the wait file is created at startup and the library is
# available in every backend.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'injection_points'");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $datadir = $node->data_dir;

# Both backing files must exist as soon as the server is up.
ok(-f "$datadir/injection_points.shm",
	'core registry file created at startup');
ok(-f "$datadir/injection_points_wait.shm",
	'wait state file created at startup');

# Arm a "wait" injection point by writing the registry file directly, with no
# SQL involved.
$node->command_ok(
	[ $client, $datadir, 'attach', 'external-wait' ],
	'external client armed a wait point without SQL');

# The backend must see the externally-armed point in its registry.
my $listed = $node->safe_psql('postgres',
	"SELECT point_name || ',' || library || ',' || function"
	  . " FROM injection_points_list() WHERE point_name = 'external-wait';");
is( $listed,
	'external-wait,injection_points,injection_wait',
	'backend sees the externally-armed injection point');

# Trigger the point from a background session, which blocks in injection_wait().
my $session = $node->background_psql('postgres', on_error_stop => 0);
$session->query_until(
	qr/start/, qq[
	\\echo start
	SELECT injection_points_run('external-wait');
]);

# Detect that the wait point was reached.  The client polls the mapped wait
# file instead of guessing with a sleep, so it behaves the same on fast and
# slow machines.
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

# Disarm the point through the registry file, still without SQL.
$node->command_ok(
	[ $client, $datadir, 'detach', 'external-wait' ],
	'external client disarmed the point without SQL');

# The backend must no longer see it.
my $still = $node->safe_psql('postgres',
	"SELECT count(*) FROM injection_points_list()"
	  . " WHERE point_name = 'external-wait';");
is($still, '0', 'backend no longer sees the disarmed injection point');

# A wait for the now-cleared point must time out rather than block forever,
# proving the tool reflects live state.
$node->command_fails_like(
	[ $client, $datadir, 'wait', 'external-wait', '1' ],
	qr/timed out/,
	'external client times out when no process waits');

$node->stop;

# Both backing files are removed together with the cluster.
ok(!-f "$datadir/injection_points.shm",
	'core registry file removed at shutdown');
ok(!-f "$datadir/injection_points_wait.shm",
	'wait state file removed at shutdown');

done_testing();
