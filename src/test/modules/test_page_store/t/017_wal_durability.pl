# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Local flush, a receiver's write acknowledgement, and another receiver's
# flush acknowledgement cannot satisfy the storage durability obligation.
# Exercise both a transaction commit and a flush that finds WAL already
# flushed locally.  This test intentionally does not need remote data files.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'injection points not supported by this build';
}

my $writer = PostgreSQL::Test::Cluster->new('durability_writer');
$writer->init(allows_streaming => 1);
$writer->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
shared_buffers = '16MB'
synchronous_standby_names = ''
synchronous_commit = local
});
$writer->start;
$writer->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION injection_points;
CREATE TABLE durable_rows (id int PRIMARY KEY);
SELECT pg_create_physical_replication_slot('durable_storage', true);
SELECT pg_create_physical_replication_slot('other_storage', true);
});
$writer->backup('seed');
my $storage = PostgreSQL::Test::Cluster->new('durable_storage');
my $other = PostgreSQL::Test::Cluster->new('other_storage');
for my $node ($storage, $other)
{
	$node->init_from_backup($writer, 'seed', has_streaming => 1);
	$node->append_conf('postgresql.conf',
		"primary_slot_name = '" . $node->name . "'\n");
	$node->start;
}

# Do not enable the barrier until storage exists.  A clean restart resets
# the cached acknowledgement, so even old WAL requires fresh feedback.
$writer->stop;
$writer->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_durability_slot = 'durable_storage'
test_page_store.wal_flush_timeout = '60s'
});
$writer->start;
$writer->safe_psql('postgres', 'INSERT INTO durable_rows VALUES (1)');
$writer->wait_for_catchup($storage, 'replay', $writer->lsn('insert'));
is($storage->safe_psql('postgres', 'SHOW fsync'),
	'on', 'the designated storage really fsyncs WAL');

my $point = 'walreceiver-before-wal-flush';

sub pause_flush
{
	$storage->safe_psql('postgres',
		"SELECT injection_points_attach('$point', 'wait')");
}

sub wait_flush
{
	$storage->poll_query_until(
		'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity
               WHERE backend_type = 'walreceiver' AND wait_event = '$point')
}) or die 'storage did not stop before WAL fsync';
}

sub resume_flush
{
	$storage->safe_psql(
		'postgres', qq{
SELECT injection_points_detach('$point');
SELECT injection_points_wakeup('$point');
});
}

sub wait_barrier
{
	my ($pid, $query) = @_;
	$writer->poll_query_until(
		'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity
               WHERE pid = $pid AND query LIKE '$query%'
               AND (wait_event_type = 'Extension' OR state = 'idle'))
}) or die 'writer did not wait for storage durability';
	is( $writer->safe_psql(
			'postgres',
			"SELECT wait_event_type FROM pg_stat_activity WHERE pid = $pid"),
		'Extension',
		"$query waits for storage rather than completing early");
}

pause_flush();
my $record = $writer->safe_psql('postgres',
	"SELECT pg_create_restore_point('before-storage-fsync')");
$writer->safe_psql('postgres',
	"SELECT test_page_store_flush_wal('$record', true)");
wait_flush();

# Use the receiver's actual written position, not an assumed message/batch
# boundary.  This proves that bytes already received are still insufficient.
my $target = $storage->safe_psql('postgres',
	'SELECT written_lsn FROM pg_stat_wal_receiver');
is( $storage->safe_psql(
		'postgres',
		'SELECT written_lsn > flushed_lsn FROM pg_stat_wal_receiver'),
	't',
	'storage has written WAL without flushing it');
is( $writer->safe_psql(
		'postgres', "SELECT pg_current_wal_flush_lsn() >= '$target'::pg_lsn"),
	't',
	'the target WAL is already flushed locally');
$writer->wait_for_catchup($other, 'flush', $target);
$writer->poll_query_until(
	'postgres', qq{
SELECT write_lsn >= '$target'::pg_lsn AND flush_lsn < '$target'::pg_lsn
FROM pg_stat_replication WHERE application_name = 'durable_storage'
}) or die 'writer did not receive the write-only acknowledgement';
is( $writer->safe_psql(
		'postgres', "SELECT test_page_store_wal_needs_flush('$target')"),
	't',
	'local and unrelated receiver flushes do not satisfy XLogNeedsFlush');

my $session = $writer->background_psql('postgres');
my $pid = $session->query_safe('SELECT pg_backend_pid()');
$session->query_until(
	qr/flushing/, qq{
SELECT 'flushing';
SELECT test_page_store_flush_wal('$target');
});
wait_barrier($pid, 'SELECT test_page_store_flush_wal');
is( $writer->safe_psql(
		'postgres',
		"SELECT flushed < '$target'::pg_lsn AND waiters > 0 FROM test_page_store_wal_status()"
	),
	't',
	'XLogFlush waits even on its local-already-flushed path');

# Outside a critical section cancellation may raise ERROR, but it cannot
# report a successful flush or update the acknowledged position.
my $cancel = $writer->background_psql('postgres', on_error_stop => 0);
my $cancel_pid = $cancel->query_safe('SELECT pg_backend_pid()');
$cancel->query_until(
	qr/cancelable/, qq{
SELECT 'cancelable';
SELECT test_page_store_flush_wal('$target');
});
wait_barrier($cancel_pid, 'SELECT test_page_store_flush_wal');
$writer->safe_psql('postgres', "SELECT pg_cancel_backend($cancel_pid)");
my ($result, $error) = $cancel->query('');
ok( $error && $cancel->{stderr} =~ /canceling statement due to user request/,
	'cancellation raises ERROR rather than satisfying the barrier');
$cancel->{stderr} = '';
$cancel->quit;
is( $writer->safe_psql(
		'postgres', "SELECT test_page_store_wal_needs_flush('$target')"),
	't',
	'cancellation has not advanced durability');
resume_flush();
$session->query_safe('SELECT 1');
is( $writer->safe_psql(
		'postgres', "SELECT test_page_store_wal_needs_flush('$target')"),
	'f',
	'the actual storage flush satisfies both flush and probe');

# The COMMIT path calls XLogFlush inside a critical section.  This barrier
# must not allocate or use a backend network connection in that context.
pause_flush();
$session->query_safe('BEGIN; INSERT INTO durable_rows VALUES (2)');
$session->query_until(
	qr/committing/, qq{
SELECT 'committing';
COMMIT;
});
wait_flush();
wait_barrier($pid, 'COMMIT');
is($writer->safe_psql('postgres', 'SELECT count(*) FROM durable_rows'),
	'1', 'the transaction has not become visible before storage flush');
resume_flush();
$session->query_safe('SELECT 1');
is($writer->safe_psql('postgres', 'TABLE durable_rows ORDER BY id'),
	"1\n2", 'commit completes after storage flush');

# A checkpoint is a non-transaction caller.  Async commit alone does not
# impose durability, but it must not allow a later checkpoint to bypass it.
pause_flush();
$writer->safe_psql('postgres',
	'SET synchronous_commit = off; INSERT INTO durable_rows VALUES (3)');
$session->query_until(
	qr/checkpointing/, qq{
SELECT 'checkpointing';
CHECKPOINT;
});
wait_flush();
$writer->poll_query_until(
	'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity
               WHERE backend_type = 'checkpointer' AND wait_event_type = 'Extension')
       OR EXISTS (SELECT FROM pg_stat_activity
                  WHERE pid = $pid AND query = 'CHECKPOINT;' AND state = 'idle')
}) or die 'checkpointer did not wait for storage durability';
is( $writer->safe_psql(
		'postgres',
		"SELECT wait_event_type FROM pg_stat_activity WHERE backend_type = 'checkpointer'"
	),
	'Extension',
	'checkpoint also waits for storage flush');
resume_flush();
$session->query_safe('SELECT 1');
$session->quit;

# Replay is a different frontier: a durable WAL copy suffices even when
# storage is not yet able to serve pages reflecting that commit.
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage replay did not pause';
my $write_lsn = $writer->safe_psql(
	'postgres', q{
BEGIN;
INSERT INTO durable_rows VALUES (4);
SELECT pg_current_wal_insert_lsn();
COMMIT;
});
is( $storage->safe_psql(
		'postgres',
		"SELECT pg_last_wal_receive_lsn() >= '$write_lsn'::pg_lsn AND pg_last_wal_replay_lsn() < '$write_lsn'::pg_lsn"
	),
	't',
	'commit requires storage flush, not storage replay');
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$writer->wait_for_catchup($storage, 'replay', $writer->lsn('insert'));
is($storage->safe_psql('postgres', 'TABLE durable_rows ORDER BY id'),
	"1\n2\n3\n4", 'storage replays all acknowledged transactions');

# The final checkpoint needs a live receiver.  The physical walsender must
# keep shipping locally flushed WAL while checkpointer waits for the ACK.
$other->stop;
$writer->stop;
pass('writer shutdown completes while storage is still running');
$storage->stop;
done_testing();
