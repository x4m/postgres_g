# Copyright (c) 2024, PostgreSQL Global Development Group

# Test signaling autovacuum worker backend by non-superuser role.
#
# Only non-superuser roles granted pg_signal_autovacuum_worker are allowed
# to signal autovacuum workers.  This test uses an injection point located
# at the beginning of the autovacuum worker startup.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize postgres
my $psql_err = '';
my $psql_out = '';
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;

# This ensures a quick worker spawn.
$node->append_conf(
	'postgresql.conf', 'autovacuum_naptime = 1');
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

$node->safe_psql(
	'postgres', qq(
    CREATE ROLE regular_role;
    CREATE ROLE signal_autovacuum_worker_role;
    GRANT pg_signal_autovacuum_worker TO signal_autovacuum_worker_role;
));

# From this point, autovacuum worker will wait at startup.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('autovacuum-worker-start', 'wait');");

# Create some content and set an aggressive autovacuum.
$node->safe_psql(
	'postgres', qq(
    CREATE TABLE tab_int(i int);
    ALTER TABLE tab_int SET (autovacuum_vacuum_cost_limit = 1);
    ALTER TABLE tab_int SET (autovacuum_vacuum_cost_delay = 100);
));

$node->safe_psql(
	'postgres', qq(
    INSERT INTO tab_int VALUES(1);
));

# Wait until an autovacuum worker starts.
$node->wait_for_event('autovacuum worker', 'autovacuum-worker-start');

my $av_pid = $node->safe_psql(
	'postgres', qq(
    SELECT pid FROM pg_stat_activity WHERE backend_type = 'autovacuum worker';
));

# Regular role cannot terminate autovacuum worker.
my $terminate_with_no_pg_signal_av = $node->psql(
	'postgres', qq(
    SET ROLE regular_role;
    SELECT pg_terminate_backend($av_pid);
),
	stdout => \$psql_out,
	stderr => \$psql_err);

like(
	$psql_err,
	qr/ERROR:  permission denied to terminate process\nDETAIL:  Only roles with privileges of the "pg_signal_autovacuum_worker" role may terminate autovacuum worker processes./,
	"autovacuum worker not signaled with regular role");

my $offset = -s $node->logfile;

# Role with pg_signal_autovacuum can terminate autovacuum worker.
my $terminate_with_pg_signal_av = $node->psql(
	'postgres', qq(
    SET ROLE signal_autovacuum_worker_role;
    SELECT pg_terminate_backend($av_pid);
),
	stdout => \$psql_out,
	stderr => \$psql_err);

# Check that the primary server logs a FATAL indicating that autovacuum
# is terminated.
ok( $node->log_contains(
		qr/FATAL:  terminating autovacuum process due to administrator command/,
		$offset),
	"autovacuum worker signaled with pg_signal_autovacuum_worker granted"
);

# Release injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_detach('autovacuum-worker-start');");

done_testing();