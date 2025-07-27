# Copyright (c) 2024-2025, PostgreSQL Global Development Group

# This test verifies edge case of reading a multixact:
# when we have multixact that is followed by exactly one another multixact,
# and another multixact have no offset yet, we must wait until this offset
# becomes observable. Previously we used to wait for 1ms in a loop in this
# case, but now we use CV for this. This test is exercising such a sleep.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

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
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_slru,injection_points'");
# Set the cluster's next multitransaction to 0xFFFFFFF0.
my $node_pgdata = $node->data_dir;
command_ok(
	[
		'pg_resetwal',
		'--multixact-ids' => '0xFFFFFFF0,0xFFFFFFF0',
		$node_pgdata
	],
	"set the cluster's next multitransaction to 0xFFFFFFF0");
command_ok(
	[
		'dd',
		'if=/dev/zero',
		"of=$node_pgdata/pg_multixact/offsets/FFFF",
		'bs=4',
		'count=65536'
	],
	"init SLRU file");

command_ok(
	[
		'rm',
		"$node_pgdata/pg_multixact/offsets/0000",
	],
	"drop old SLRU file");

$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION test_slru));

# Another multixact test: loosing some multixact must not affect reading near
# multixacts, even after a crash.
my $bg_psql = $node->background_psql('postgres');

my $multi = $bg_psql->query_safe(
	q(SELECT test_create_multixact();));

# The space for next multi will be allocated, but it will never be actually
# recorded.
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('multixact-create-from-members','wait');}
);

$bg_psql->query_until(
	qr/deploying lost multi/, q(
\echo deploying lost multi
	SELECT test_create_multixact();
));

$node->wait_for_event('client backend', 'multixact-create-from-members');
$node->safe_psql('postgres',
	q{SELECT injection_points_detach('multixact-create-from-members')});

$node->safe_psql('postgres',
	q{checkpoint;});

# One more multitransaction to effectivelt emit WAL record about next
# multitransaction (to avaoid corener case 1).
$node->safe_psql('postgres',
	q{SELECT test_create_multixact();});

# All set and done, it's time for hard restart
$node->kill9;
$node->stop('immediate', fail_ok => 1);
$node->poll_start;
$bg_psql->{run}->finish;

# Verify thet recorded multi is readble, this call must not hang.
# Also note that all injection points disappeared after server restart.
my $timed_out = 0;
$node->safe_psql(
	'postgres',
	qq{SELECT test_read_multixact('$multi'::xid);},
	timeout => $PostgreSQL::Test::Utils::timeout_default,
	timed_out => \$timed_out);
ok($timed_out == 0, 'recorded multi is readble');

# Test mxidwraparound
foreach my $i (1 .. 32) {
$node->safe_psql('postgres',q{SELECT test_create_multixact();});
}

$node->stop;

# If we reached this point - everything is OK.
ok(1);
done_testing();
