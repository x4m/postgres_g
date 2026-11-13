
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use locale;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

# Test timeouts that will cause FATAL errors.  This test relies on injection
# points to await a timeout occurrence. Relying on sleep proved to be unstable
# on buildfarm. It's difficult to rely on the NOTICE injection point because
# the backend under FATAL error can behave differently.

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Node initialization
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# Check timer ownership and GUC restoration with long timeouts, so that slow
# machines cannot expire a timer before reaching the cleanup being tested.
my $state_session = $node->background_psql('postgres', on_error_stop => 0);

is( $state_session->query_safe(q{
	BEGIN;
	SAVEPOINT s;
	SET LOCAL transaction_timeout = '1d';
	ROLLBACK TO SAVEPOINT s;
	SELECT transaction_timeout_deadline() IS NULL;
}), 't', 'savepoint rollback disables a timeout restored to zero');

is( $state_session->query_safe(q{
	COMMIT;
	SELECT transaction_timeout_deadline() IS NULL;
}), 't', 'transaction timeout does not survive commit');

is( $state_session->query_safe(q{
	BEGIN;
	SET LOCAL transaction_timeout = '2d';
	SELECT transaction_timeout_deadline() >
		clock_timestamp() + interval '47 hours';
	COMMIT;
}), 't', 'next transaction uses its own timeout');

is( $state_session->query_safe(q{
	BEGIN;
	SET LOCAL transaction_timeout = '1d';
	SAVEPOINT s;
	SET LOCAL transaction_timeout = 0;
	ROLLBACK TO SAVEPOINT s;
	SELECT transaction_timeout_deadline() IS NOT NULL;
	COMMIT;
}), 't', 'savepoint rollback re-enables a restored positive timeout');

is( $state_session->query_safe(q{
	DO $$
	BEGIN
		PERFORM set_config('transaction_timeout', '1d', true);
		RAISE EXCEPTION 'rollback';
	EXCEPTION WHEN raise_exception THEN
		NULL;
	END
	$$;
	SELECT transaction_timeout_deadline() IS NULL;
}), 't', 'caught PL/pgSQL exception restores the timeout');

$state_session->query_safe(q{
	BEGIN;
	SET LOCAL transaction_timeout = '1d';
	SELECT transaction_timeout_deadline() AS deadline \gset
	SAVEPOINT s;
});

for my $attempt (1 .. 2)
{
	my $result = $state_session->query(q{
		SELECT 1 / 0;
		ROLLBACK TO SAVEPOINT s;
		SELECT transaction_timeout_deadline() = :'deadline'::timestamptz;
	});
	like($state_session->{stderr}, qr/division by zero/,
		"recoverable error $attempt occurred");
	$state_session->{stderr} = '';
	is($result, 't', "recoverable error $attempt preserves the deadline");
}

$state_session->query_safe('COMMIT');
ok($state_session->quit);

#
# 1. Test of the transaction timeout
#

$node->safe_psql('postgres',
	"SELECT injection_points_attach('transaction-timeout', 'wait');");

my $psql_session = $node->background_psql('postgres');

# The following query will generate a stream of SELECT 1 queries. This is done
# so to exercise transaction timeout in the presence of short queries.
# Note: the interval value is parsed with locale-aware strtod()
$psql_session->query_until(
	qr/starting_bg_psql/,
	sprintf(
		q(\echo starting_bg_psql
		SET transaction_timeout to '10ms';
		BEGIN;
		SELECT 1 \watch %g
		\q
), 0.001));

# Wait until the backend enters the timeout injection point. Will get an error
# here if anything goes wrong.
$node->wait_for_event('client backend', 'transaction-timeout');

my $log_offset = -s $node->logfile;

# Remove the injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('transaction-timeout');");

# Check that the timeout was logged.
$node->wait_for_log('terminating connection due to transaction timeout',
	$log_offset);

# If we send \q with $psql_session->quit the command can be sent to the session
# already closed. So \q is in initial script, here we only finish IPC::Run.
$psql_session->{run}->finish;


#
# 2. Test of the idle in transaction timeout
#

$node->safe_psql('postgres',
	"SELECT injection_points_attach('idle-in-transaction-session-timeout', 'wait');"
);

# We begin a transaction and the hand on the line
$psql_session = $node->background_psql('postgres');
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET idle_in_transaction_session_timeout to '10ms';
   BEGIN;
));

# Wait until the backend enters the timeout injection point.
$node->wait_for_event('client backend',
	'idle-in-transaction-session-timeout');

$log_offset = -s $node->logfile;

# Remove the injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('idle-in-transaction-session-timeout');");

# Check that the timeout was logged.
$node->wait_for_log(
	'terminating connection due to idle-in-transaction timeout', $log_offset);

ok($psql_session->quit);


#
# 3. Test of the idle session timeout
#
$node->safe_psql('postgres',
	"SELECT injection_points_attach('idle-session-timeout', 'wait');");

# We just initialize the GUC and wait. No transaction is required.
$psql_session = $node->background_psql('postgres');
$psql_session->query_until(
	qr/starting_bg_psql/, q(
   \echo starting_bg_psql
   SET idle_session_timeout to '10ms';
));

# Wait until the backend enters the timeout injection point.
$node->wait_for_event('client backend', 'idle-session-timeout');

$log_offset = -s $node->logfile;

# Remove the injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('idle-session-timeout');");

# Check that the timeout was logged.
$node->wait_for_log('terminating connection due to idle-session timeout',
	$log_offset);

ok($psql_session->quit);

# A reload can set transaction_timeout while a block is already aborted,
# without arming its timer.  This lets us check the idle timeout independently
# of how quickly a transaction can be made to fail.
$node->safe_psql('postgres', q{
	SELECT injection_points_detach('transaction-timeout');
});

# Keep a control connection with a session-level setting that overrides the
# shorter transaction timeout we will load from the configuration file.
my $control_session = $node->background_psql('postgres',
	timeout => 2 * $PostgreSQL::Test::Utils::timeout_default);
$control_session->query_safe('SET transaction_timeout = 0');

$psql_session = $node->background_psql('postgres',
	on_error_stop => 0,
	timeout => 2 * $PostgreSQL::Test::Utils::timeout_default);
my $pid = $psql_session->query_safe('SELECT pg_backend_pid()');
# Attach the callback's shared memory before releasing transaction resources.
$psql_session->query_safe(q{
	SELECT injection_points_load('idle-in-transaction-session-timeout');
});
$psql_session->query('BEGIN; SELECT 1 / 0;');
like($psql_session->{stderr}, qr/division by zero/,
	'transaction is aborted before enabling the timeouts');
$psql_session->{stderr} = '';

$log_offset = -s $node->logfile;
$node->append_conf('postgresql.conf', q{
transaction_timeout = '1ms'
idle_in_transaction_session_timeout = '10ms'
});
$node->reload;

# Send commands until the backend processes the reload and reaches the idle
# timeout injection point.  Do not wait for a response after sending a command:
# the timer could fire before the client receives it.  The injection point
# prevents FATAL from racing with the next command.
my $idle_timeout_fired = 0;
for my $attempt (1 .. $PostgreSQL::Test::Utils::timeout_default * 10)
{
	if ($control_session->query_safe(qq{
		SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $pid
			AND wait_event = 'idle-in-transaction-session-timeout');
	}) eq 't')
	{
		$idle_timeout_fired = 1;
		last;
	}
	$psql_session->{stdin} .= "SELECT 1;\n";
	$psql_session->{run}->pump_nb;
	usleep(100_000);
}
ok($idle_timeout_fired, 'inactive transaction timer does not suppress idle timeout');

if ($idle_timeout_fired)
{
	$control_session->query_safe(q{
		SELECT injection_points_wakeup('idle-in-transaction-session-timeout');
	});
	$node->wait_for_log(
		qr/\[$pid\].*terminating connection due to idle-in-transaction timeout/,
		$log_offset);
}

# A command may still be queued behind the injection point, so psql can exit
# with a connection-lost error.  The server log above checks the FATAL reason.
$psql_session->{stdin} .= "\\q\n";
$psql_session->{run}->finish;
ok($control_session->quit);

done_testing();
