# Copyright (c) 2026, PostgreSQL Global Development Group
#
# One worker owns the physical connection.  Its bounded queue must survive
# caller timeout, reuse of a slot and worker exit without publishing a reply
# into a different request.  Frozen pages isolate transport from redo races;
# 011/012 exercise the same worker during following compute startup/restart.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'Injection points are not available'
  unless check_pg_config('#define USE_INJECTION_POINTS 1');

my $primary = PostgreSQL::Test::Cluster->new('queue_writer');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION test_aio;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION injection_points;
CREATE TABLE queue_heap (id int, payload text);
INSERT INTO queue_heap SELECT i, repeat(md5(i::text), 4)
  FROM generate_series(1, 500) i;
VACUUM (FREEZE, ANALYZE) queue_heap;
SELECT pg_create_physical_replication_slot('queue_storage', true);
SELECT pg_create_physical_replication_slot('queue_compute', true);
});
my $digest_sql = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM queue_heap
};
my $digest = $primary->safe_psql('postgres', $digest_sql);
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my ($spc, $db, $rel) = split /\|/, $primary->safe_psql(
	'postgres', q{
SELECT (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
       (SELECT oid FROM pg_database WHERE datname = current_database()),
       pg_relation_filenode('queue_heap')
});
my $relpath = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('queue_heap')");
$primary->backup('queue_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('queue-cut')");
my $storage = PostgreSQL::Test::Cluster->new('queue_storage');
$storage->init_from_backup($primary, 'queue_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'queue_storage'
recovery_target_name = 'queue-cut'
recovery_target_action = 'pause'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 1024
});
$storage->start;
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not reach the read cut';
my $cut = $storage->safe_psql('postgres',
	"SELECT test_page_store_retain('queue_heap')");
my @pages;
for my $block (0, 1)
{
	push @pages, $storage->safe_psql(
		'postgres', qq{
SELECT md5(pages) FROM test_page_store_fetch(
  $spc, $db, $rel, 0, $block, 1, '$sysid', $tli, '$cut')
});
}
isnt($pages[0], $pages[1], 'fixture pages distinguish misrouted replies');

my $compute = PostgreSQL::Test::Cluster->new('queue_compute');
$compute->init_from_backup($primary, 'queue_seed', has_streaming => 1);
my $conninfo =
  $storage->connstr('postgres') . ' application_name=queue_transport';
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'queue_compute'
recovery_target_name = 'queue-cut'
recovery_target_action = 'pause'
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.physical_service = true
test_page_store.transport_slots = 1
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.tablespace = $spc
test_page_store.database = $db
test_page_store.relfilenumber = $rel
test_page_store.request_timeout = '30s'
max_parallel_workers_per_gather = 0
});
$compute->start;
$compute->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'compute did not reach the read cut';

sub evict
{
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('queue_heap')"
	) or die 'could not evict queue_heap';
	is( $compute->safe_psql(
			'postgres', qq{
SELECT count(*) FROM pg_buffercache WHERE reldatabase = $db
AND reltablespace = $spc AND relfilenode = $rel
}),
		'0',
		'no cached target page can bypass transport');
}

sub status
{
	my ($expression) = @_;
	return $compute->safe_psql('postgres',
		"SELECT $expression FROM test_page_store_transport_status()");
}

sub wait_status
{
	my ($condition) = @_;
	$compute->poll_query_until('postgres',
		"SELECT $condition FROM test_page_store_transport_status()")
	  or die "transport did not reach: $condition";
}

my $point = 'test-page-store-worker-before-publish';

sub attach
{
	$compute->safe_psql('postgres',
		"SELECT injection_points_attach('$point', 'wait')");
}

sub release
{
	$compute->safe_psql(
		'postgres', qq{
SELECT injection_points_detach('$point');
SELECT injection_points_wakeup('$point');
});
}

sub start_read
{
	my ($session, $sql) = @_;
	$session->query_until(qr/request_started/,
		"\\echo request_started\n$sql;\n");
}

evict();
my $local_file = $compute->data_dir . '/' . $relpath;
rename($local_file, "$local_file.held-for-queue-test")
  or die "could not move $local_file aside: $!";
is($compute->safe_psql('postgres', $digest_sql),
	$digest, 'SQL reads all values and physical TIDs through the worker');
is(status('slots = 1 AND completed > 0'),
	't', 'reads use the one-slot transport queue');

my $first = $compute->background_psql('postgres', on_error_stop => 0);
my $second = $compute->background_psql('postgres', on_error_stop => 0);
my $second_pid = $second->query_safe('SELECT pg_backend_pid()');
my $read0 = "SELECT md5(test_page_store_read_smgr('queue_heap', 0))";
my $read1 = "SELECT md5(test_page_store_read_smgr('queue_heap', 1))";

# The second caller cannot allocate another slot or open its own connection.
attach();
my $submitted = status('submitted');
start_read($first, $read0);
wait_status("worker_wait_event = '$point'");
start_read($second, $read1);
$compute->poll_query_until(
	'postgres', qq{
SELECT wait_event = 'TestPageStoreQueue' FROM pg_stat_activity
WHERE pid = $second_pid
}) or die 'second caller did not wait for queue capacity';
is( status('submitted'),
	$submitted + 1,
	'queue capacity applies backpressure before admitting a second request');
is( $storage->safe_psql(
		'postgres', q{
SELECT count(*) FROM pg_stat_activity
WHERE application_name = 'queue_transport' AND backend_type = 'walsender'
}),
	'1',
	'concurrent callers share one physical service connection');
release();
is($first->query_safe(''), $pages[0], 'first caller receives its own page');
is($second->query_safe(''),
	$pages[1], 'waiting caller receives the other page');

# Timeout while an AIO handle is owned must release both that handle and the
# queue slot.  A late reply must not overwrite the new owner's request.
evict();
$first->query_safe("SET test_page_store.request_timeout = '500ms'");
my $aio_before =
  $first->query_safe('SELECT startreadv FROM test_page_store_io_counts()');
attach();
start_read($first, "SELECT count(*) FROM read_buffers('queue_heap', 0, 1)");
wait_status("worker_wait_event = '$point'");
my ($result, $error) = $first->query('');
ok( $error && $first->{stderr} =~ /page transport request timed out/,
	'caller timeout does not wait for the blocked transport worker'
) or diag($first->{stderr});
$first->{stderr} = '';
$first->query_safe('RESET test_page_store.request_timeout');
is( $first->query_safe(
		"SELECT startreadv > $aio_before FROM test_page_store_io_counts()"),
	't',
	'timeout occurred inside the AIO entry point');
is(status('ready + running + done'), '0', 'timeout releases the sole slot');
start_read($second, $read1);
wait_status('ready = 1 AND running = 0');
release();
my ($reply, $failed) = $second->query('');
is($failed ? "ERROR: $second->{stderr}" : $reply,
	$pages[1],
	'reused slot receives the new response, not the timed-out page');
$second->{stderr} = '';
is(status('discarded'), '1', 'late reply is explicitly discarded');
is($first->query_safe($digest_sql),
	$digest, 'same backend reads all values after the failed AIO');

# Graceful worker exit must fail its outstanding request, not strand it until
# the deadline.  The postmaster then restarts only this worker.
attach();
start_read($first, $read0);
wait_status("worker_wait_event = '$point'");
my $old_pid = status('worker_pid');
# A worker without InitPostgres is not in ProcArray, so pg_terminate_backend
# cannot address it.  pg_ctl kill also handles PostgreSQL signals on Windows.
is(PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'TERM', $old_pid),
	0, 'terminate the worker with a request outstanding');
($result, $error) = $first->query('');
ok( $error && $first->{stderr} =~ /page transport worker exited/,
	'worker exit delivers an explicit failure to the waiting caller'
) or diag($first->{stderr});
$first->{stderr} = '';
$compute->safe_psql('postgres', "SELECT injection_points_detach('$point')");
wait_status("worker_pid > 0 AND worker_pid <> $old_pid");
is($first->query_safe($read0),
	$pages[0], 'same backend can read through the restarted worker');
is(status('ready + running + done'), '0', 'no slot leaked after worker exit');

# A storage error also leaves the worker available for a later request.
my $new_pid = status('worker_pid');
($result, $error) =
  $first->query("SELECT test_page_store_read_smgr('queue_heap', 1000000)");
ok($error && $first->{stderr} =~ /page transport request failed/,
	'storage error reaches the caller');
$first->{stderr} = '';
is($first->query_safe($read1),
	$pages[1], 'worker reconnects after a storage error');
is(status('worker_pid'), $new_pid,
	'storage error does not restart the worker');
ok(!-e $local_file, 'no test recreated the selected local file');
$first->quit;
$second->quit;
$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
