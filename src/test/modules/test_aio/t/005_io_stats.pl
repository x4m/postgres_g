# Copyright (c) 2026, PostgreSQL Global Development Group

# Test pg_stat_io accounting of waits for IO initiated by another backend
# ("foreign" IO).
#
# When a backend finds the IO for a buffer already in progress in another
# backend, it waits for that IO in WaitReadBuffers() and, with
# track_io_timing enabled, used to record IOOP_READ time without ever
# incrementing the read count (the read is counted by the initiating
# backend).  Once such a backend flushes its IO statistics, the shared
# stats contain read time without reads, which trips the
# pgstat_bktype_io_stats_valid() Assert in assert-enabled builds.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use TestAio;

if (($ENV{enable_injection_points} // 'no') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('worker');
$node->init();
TestAio::configure($node);
$node->append_conf('postgresql.conf', qq(io_method=worker));
$node->start();

$node->safe_psql(
	'postgres', qq(
CREATE EXTENSION test_aio;
CREATE TABLE tbl_foreign(data int not null) WITH (AUTOVACUUM_ENABLED = false);
INSERT INTO tbl_foreign SELECT generate_series(1, 10000);
CHECKPOINT;
));

my $psql_victim = $node->background_psql('postgres', on_error_stop => 0);
my $psql_reader = $node->background_psql('postgres', on_error_stop => 0);
my $psql_ctrl = $node->background_psql('postgres', on_error_stop => 0);

my $pid_victim = $psql_victim->query_safe(qq(SELECT pg_backend_pid()));
my $pid_reader = $psql_reader->query_safe(qq(SELECT pg_backend_pid()));

# The victim needs track_io_timing to record the wait time.
$psql_victim->query_safe(qq(SET track_io_timing = on));

# Warm caches (catalogs and the table block used below), then flush the
# victim's pending IO stats, so that at the final flush the victim's only
# pending read stats stem from the foreign IO wait below.
$psql_victim->query_safe(
	qq(SELECT count(*) FROM tbl_foreign WHERE ctid = '(1,1)'));
$psql_victim->query_safe(qq(SELECT pg_stat_force_next_flush()));

# Flush the other sessions' pending stats too, so that their
# connection-establishment reads cannot end up in the shared stats after
# the reset below.
$psql_reader->query_safe(qq(SELECT pg_stat_force_next_flush()));
$psql_ctrl->query_safe(qq(SELECT pg_stat_force_next_flush()));

# Hold the completion of the reader's IO...
$psql_reader->query_safe(
	qq(SELECT inj_io_completion_wait(pid=>$pid_reader,
		relfilenode=>pg_relation_filenode('tbl_foreign'))));

# ... and start a read of block 1.  The query blocks waiting for its own
# IO, whose completion is held by the injection point.
$psql_reader->{stdin} .= qq(SELECT read_rel_block_ll('tbl_foreign', 1);\n);
$psql_reader->{run}->pump_nb();

$node->poll_query_until(
	'postgres', qq(SELECT wait_event FROM pg_stat_activity
		WHERE wait_event = 'completion_wait'),
	'completion_wait');
ok(1, "reader's IO is in progress with completion held");

# Now read the same block in the victim.  It finds the IO already in
# progress in the reader and waits for it in WaitReadBuffers().
$psql_victim->{stdin} .=
  qq(SELECT count(*) FROM tbl_foreign WHERE ctid = '(1,1)';\n);
$psql_victim->{run}->pump_nb();

$node->poll_query_until('postgres',
	qq(SELECT wait_event FROM pg_stat_activity WHERE pid = $pid_victim),
	'AioIoCompletion');
ok(1, "victim is waiting for the foreign IO");

# Zap the accumulated shared IO stats, guaranteeing that the victim's
# subsequent flush is the first one for its backend type.  If that flush
# contains read time without any reads, the shared stats become
# inconsistent; in assert-enabled builds the flush itself trips the Assert
# in pgstat_io_flush_cb().
$psql_ctrl->query_safe(qq(SELECT pg_stat_reset_shared('io')));

# Let the IO complete, unblocking both the reader and the victim.
$psql_ctrl->query_safe(qq(SELECT inj_io_completion_continue()));

pump_until($psql_victim->{run}, $psql_victim->{timeout},
	\$psql_victim->{stdout}, qr/1/);
$psql_victim->{stdout} = '';
ok(1, "victim finished its read");

# Flush the victim's stats.  In an assert-enabled build without the fix
# this crashes the victim (and, with restart_after_crash=false, the node).
my ($output, $ret);
($output, $ret) = $psql_victim->query(qq(SELECT pg_stat_force_next_flush()));
($output, $ret) = $psql_victim->query(qq(SELECT 1));
is($ret, 0, "victim survived flushing its IO stats");
is($output, '1', "victim can still run queries");

# On any build, the flushed stats must not contain read time without reads.
my $inconsistent = $psql_ctrl->query_safe(
	qq(SELECT count(*) FROM pg_stat_io
		WHERE backend_type = 'client backend' AND object = 'relation'
			AND context = 'normal' AND reads = 0 AND read_time > 0));
is($inconsistent, '0', "no read time without reads in pg_stat_io");

$psql_victim->quit();
$psql_reader->quit();
$psql_ctrl->quit();

$node->stop();
done_testing();
