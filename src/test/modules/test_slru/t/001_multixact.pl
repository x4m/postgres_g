
# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my ($node, $result);

$node = PostgreSQL::Test::Cluster->new('multixact_CV_sleep');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_slru'");
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION test_slru));

# Test for Multixact generation edge case
$node->safe_psql('postgres', q(select injection_points_attach('test_read_multixact','wait')));
$node->safe_psql('postgres', q(select injection_points_attach('GetMultiXactIdMembers-CV-sleep','wait')));

# This session must observe sleep on CV when generating multixact.
# To achive this it first will create a multixact, then pause before reading it.
my $observer = $node->background_psql('postgres');

$observer->query_until(qr/start/,
q(
	\echo start
	select test_read_multixact(test_create_multixact());
));
$node->wait_for_event('client backend', 'test_read_multixact');

# This session will create next Multixact, it's necessary to avoid edge case 1 (see multixact.c)
my $creator = $node->background_psql('postgres');
$node->safe_psql('postgres', q(select injection_points_wakeup('');select injection_points_attach('GetNewMultiXactId-done','wait');));

# We expect this query to hand in critical section after generating new multixact,
# but before filling it's offset into SLRU
$creator->query_until(qr/start/, q(
	\echo start
	select injection_points_wakeup('');
	select test_create_multixact();
));

$node->wait_for_event('client backend', 'GetNewMultiXactId-done');

# Now we are sure we can reach edge case 2. Proceed session that is reading that multixact.
$node->safe_psql('postgres', q(select injection_points_detach('test_read_multixact')));


$node->wait_for_event('client backend', 'GetMultiXactIdMembers-CV-sleep');

$node->safe_psql('postgres', q(select injection_points_detach('GetNewMultiXactId-done')));
$node->safe_psql('postgres', q(select injection_points_detach('GetMultiXactIdMembers-CV-sleep')));

$observer->quit;

$creator->quit;

$node->stop;
done_testing();
