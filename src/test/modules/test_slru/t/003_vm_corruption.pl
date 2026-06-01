# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# This test verifies edge case of visibilitymap_clear: a backend that clears a
# VM bit inside heap_insert()'s critical section and then is frozen there (via
# an injection point) before the change is WAL-logged.  If the frozen backend
# bails out of the critical section on postmaster death (releasing its buffer
# lock), the never-WAL-logged VM clear can become durable on the primary while
# the standby still believes the page is all-frozen/all-visible -- VM
# corruption, observable as wrong answers (deleted rows) on the standby.
#
# Reproducer originally from Andrey Borodin, "VM corruption on standby"
# (pgsql-hackers, Aug 2025).  NOT intended for upstream commit: timing is faked
# with sleep(1) and it lives in test_slru where it was easy to write.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);

use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}
if ($windows_os)
{
	plan skip_all => 'Kill9 works unpredicatably on Windows';
}

my ($node, $result);

$node = PostgreSQL::Test::Cluster->new('mike');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_slru,injection_points'");
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION pg_visibility));
$node->safe_psql('postgres', q(CREATE EXTENSION test_slru));

my $backup_name = 'my_backup';
$node->backup($backup_name);
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, $backup_name, has_streaming => 1);
$standby->start;

my $bg_psql = $node->background_psql('postgres');

# prepare the table, index and freeze some rows.
# index is here only to demonstrate selecting deleted data on standby
my $multi = $bg_psql->query_safe(
	q(
	CREATE TABLE x(i int);
	CREATE INDEX on x(i);
	INSERT INTO x VALUES (1);
	VACUUM FREEZE x;
	));

# next backend clearing VM in heap_insert() will hang after releasing buffer lock
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('heap-insert:visibilitymap-clear','wait');}
);

# we expect this backend to hang forever
$bg_psql->query_until(
	qr/deploying lost VM bit/, q(
\echo deploying lost VM bit
	INSERT INTO x VALUES (1);
));

$node->wait_for_event('client backend', 'heap-insert:visibilitymap-clear');

# Capture the frozen backend's PID.  With the atomic-counter injection wait it
# will NOT react to postmaster death, so after the hard restart it would keep
# shared memory attached and block recovery start.  We kill it explicitly
# below (a "smarter kill9").
my $frozen_pid = $node->safe_psql(
	'postgres', q{
	SELECT pid FROM pg_stat_activity
	WHERE wait_event = 'heap-insert:visibilitymap-clear'});

$node->safe_psql('postgres',
	q{SELECT injection_points_detach('heap-insert:visibilitymap-clear')});

# now we want VM on-disk. Checkpoint will hang too, hence another background session.
my $bg_psql1 = $node->background_psql('postgres');
$bg_psql1->query_until(
	qr/flush data to disk/, q(
\echo flush data to disk
	checkpoint;
));

# Capture the postmaster and the PIDs of all its children before the crash.
# Every postgres backend/auxiliary process is a direct child of the
# postmaster.  We can't rely on the process group: a backend frozen in a
# critical section ends up reparented and (on macOS) is not reliably in the
# postmaster's group, so we remember every PID explicitly and kill them by PID.
my $pmpid = $node->{_pid};
my @cluster_pids = ($pmpid);
push @cluster_pids, grep { /^\d+$/ } split /\s+/, `pgrep -P $pmpid`;

sleep(1);
# All set and done, it's time for hard restart
$node->kill9;
$node->stop('immediate', fail_ok => 1);

# With the atomic injection wait, the backend frozen inside heap_insert()'s
# critical section ignores postmaster death and stays attached to shared
# memory.  Killing only the postmaster therefore leaves an orphan that blocks
# recovery startup ("pre-existing shared memory block ... still in use").
# First confirm it really outlived the postmaster (the whole point of the new
# machinery)...
ok($frozen_pid =~ /^\d+$/ && kill(0, $frozen_pid),
	'backend frozen in critical section survived postmaster death');

# ...then kill -9 the WHOLE cluster by PID and wait until every process is
# gone, so the shared memory segment is released before we attempt recovery.
foreach my $i (1 .. 100)
{
	my @alive = grep { kill 0, $_ } @cluster_pids;
	last unless @alive;
	kill 'KILL', @alive;
	usleep(100_000);
}

$node->poll_start;
$bg_psql->{run}->finish;
$bg_psql1->{run}->finish;

# Now VMs are different on primary and standby. Insert and some data to demonstrate problems.
$node->safe_psql('postgres',
	q{
	INSERT INTO x VALUES (1);
	INSERT INTO x VALUES (2);
	DELETE FROM x WHERE i = 2;
	});

$node->wait_for_catchup($standby);

# corruption tests
my $corrupted_all_frozen = $standby->safe_psql('postgres',
	qq{SELECT pg_check_frozen('x')});
my $corrupted_all_visible = $standby->safe_psql('postgres',
	qq{SELECT pg_check_visible('x')});
my $observed_deleted_data = $standby->safe_psql('postgres',
	qq{set enable_seqscan to false; SELECT * FROM x WHERE i = 2;});

# debug printing
print("\n");
print($corrupted_all_frozen);
print("\n\n");
print($corrupted_all_visible);
print("\n\n");
print($observed_deleted_data);
print("\n\n");

# fail the test if any corruption is reported
is($corrupted_all_frozen, '', 'pg_check_frozen() observes corruption');
is($corrupted_all_visible, '', 'pg_check_visible() observes corruption');
is($observed_deleted_data, '', 'deleted data returned by select');

$node->stop;
$standby->stop;

done_testing();
