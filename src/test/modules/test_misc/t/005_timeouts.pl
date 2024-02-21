
# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test timeouts that will FATAL-out.
# This test relies on an injection points to await timeout ocurance.
# Relying on sleep prooved to be unstable on buildfarm.
# It's difficult to rely on NOTICE injection point, because FATALed
# backend can look differently under different circumstances.

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Node initialization
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
$node->safe_psql('postgres',
	"SELECT injection_points_attach('transaction-timeout', 'wait');");


#
# 1. Test of transaction timeout
#

my $psql_session =
  $node->background_psql('postgres');

# Following query will generate a stream of SELECT 1 queries. This is done to
# excersice transaction timeout in presence of short queries.
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET transaction_timeout to '10ms';
   BEGIN;
   SELECT 1 \watch 0.001
   \q
));

# Wait until the backend is in the timeout.
# In case if anything get broken this waiting will error-out
$node->wait_for_event('client backend','transaction-timeout');

# Remove injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('transaction-timeout');");

# If we send \q with $psql_session->quit it can get to pump already closed.
# So \q is in initial script, here we only finish IPC::Run.
$psql_session->{run}->finish;


#
# 2. Test of idle in transaction timeout
#

$node->safe_psql('postgres',
	"SELECT injection_points_attach('idle-in-transaction-session-timeout', 'wait');");

# We begin a transaction and the hand on the line
$psql_session =
  $node->background_psql('postgres');
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET idle_in_transaction_session_timeout to '10ms';
   BEGIN;
));

# Wait until the backend is in the timeout.
$node->wait_for_event('client backend','idle-in-transaction-session-timeout');

# Remove injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('idle-in-transaction-session-timeout');");
ok($psql_session->quit);


#
# 3. Test of idle session timeout
#
$node->safe_psql('postgres',
	"SELECT injection_points_attach('idle-session-timeout', 'wait');");

# We just initialize GUC and wait. No transaction required.
$psql_session =
  $node->background_psql('postgres');
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET idle_session_timeout to '10ms';
));

# Wait until the backend is in the timeout.
$node->wait_for_event('client backend','idle-session-timeout');

# Remove injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('idle-session-timeout');");
ok($psql_session->quit);


# Tests above will hang if injection points are not reached
ok(1);

done_testing();
