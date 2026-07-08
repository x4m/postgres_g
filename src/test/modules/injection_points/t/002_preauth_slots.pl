# Copyright (c) 2026, PostgreSQL Global Development Group

# Check that a connection which has not authenticated yet already counts
# against max_connections.
#
# A backend claims its PGPROC entry in InitProcessPhase2(), and "sorry, too
# many clients already" is reported at slot acquisition time -- both happen
# before authentication.  The invariant under test is therefore that
# max_connections bounds all in-flight connections, not just authenticated
# sessions.  If that ever regressed (e.g. slots were accounted only after
# authentication), unauthenticated clients could overrun the limit, and the
# reserved_connections logic, which counts free PGPROCs including the pre-auth
# ones, would hand out reserved slots it should not.
#
# Testing this needs a connection frozen in the narrow window after it has
# taken a slot but before it authenticates, and such a backend cannot be
# driven or even identified over SQL or pg_stat_activity.  The injection_points
# module's "init-pre-auth" wait point and its filesystem markers are just the
# tool used to hold a backend there and to observe/release it out of process.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Small, fully deterministic connection budget.  Client backends draw from the
# regular PGPROC freelist sized by max_connections; autovacuum, background
# workers and walsenders use separate pools, so this arithmetic is exact.
my $max_conn = 4;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
max_connections = $max_conn
superuser_reserved_connections = 0
reserved_connections = 0
]);
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $inj_dir = $node->data_dir . '/pg_injection_points/init-pre-auth';
my $timeout_us = $PostgreSQL::Test::Utils::timeout_default * 1_000_000;

# Return the set of pids currently parked at init-pre-auth, as a hash ref.
sub parked_pids
{
	my %pids;
	if (opendir(my $dh, $inj_dir))
	{
		%pids = map { $_ => 1 } grep { /^\d+$/ } readdir $dh;
		closedir $dh;
	}
	return \%pids;
}

# Wait until a pid that is not in %$before shows up, and return it.
sub wait_for_new_parked_pid
{
	my ($before) = @_;
	my $waited = 0;
	while (1)
	{
		my $now = parked_pids();
		for my $pid (keys %$now)
		{
			return $pid unless $before->{$pid};
		}
		die "timed out waiting for a backend to park at init-pre-auth"
		  if $waited > $timeout_us;
		usleep(10_000);
		$waited += 10_000;
	}
}

# The controller session stays connected for the whole armed window; it arms
# the wait point and never exits while it is attached, so it never parks.
my $ctl = $node->background_psql('postgres');
$ctl->query_safe("SELECT injection_points_attach('init-pre-auth', 'wait')");

# Open victims that will hang before authentication, each consuming one slot.
# We do not (indeed cannot) authenticate them; we only learn their pids from
# the marker directory.  With the controller holding one slot, max_connections
# - 1 victims exactly fill the remaining slots.
my @victims;
my @victim_pids;
for (my $i = 0; $i < $max_conn - 1; $i++)
{
	my $before = parked_pids();
	my $v = $node->background_psql('postgres', wait => 0);
	push @victims, $v;
	push @victim_pids, wait_for_new_parked_pid($before);
}

is(scalar(@victim_pids), $max_conn - 1,
	'all pre-auth victims are parked and visible in the filesystem');

# Every slot is now taken by pre-authentication backends plus the controller.
# A fresh connection is refused before it ever reaches the wait point, which
# proves the parked backends really do hold connection slots.
$node->connect_fails(
	'dbname=postgres',
	'pre-auth backends occupy connection slots',
	expected_stderr => qr/FATAL:  sorry, too many clients already/);

# Detach the point so that released backends, and the probe connection below,
# do not park again.  Backends already parked keep waiting on their own marker
# files, so they keep holding their slots until we remove those files.
$ctl->query_safe("SELECT injection_points_detach('init-pre-auth')");

# Let one pre-auth backend proceed (unlink its marker) and disconnect.  If the
# slot it was holding while unauthenticated is now reusable, the connection
# limit was accounting that pre-auth backend and nothing else.
my $freed_pid = shift @victim_pids;
my $freed_victim = shift @victims;
unlink "$inj_dir/$freed_pid" or die "unlink $inj_dir/$freed_pid: $!";
$freed_victim->wait_connect;
$freed_victim->quit;

is($node->safe_psql('postgres', 'SELECT 1'),
	'1', 'freed pre-auth slot becomes available for a new connection');

# Release and reap the remaining victims, then shut everything down.
while (@victims)
{
	my $pid = shift @victim_pids;
	my $v = shift @victims;
	unlink "$inj_dir/$pid";
	eval {
		$v->wait_connect;
		$v->quit;
	};
}
$ctl->quit;
$node->stop;

done_testing();
