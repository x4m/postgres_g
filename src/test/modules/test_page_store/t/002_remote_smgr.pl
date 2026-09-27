# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A genuine SQL consumer of remotely fetched buffers.  The selected heap's
# local file is moved aside after recovery pauses, so md cannot hide a missing
# remote-read path.  This is not a test of cached-only redo or remote durability.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('writer');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
});
$primary->start;
$primary->safe_psql('postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION test_aio;
CREATE TABLE remote_heap (id integer, payload text);
INSERT INTO remote_heap
  SELECT i, repeat(md5(i::text), 3) FROM generate_series(1, 12000) i;
CREATE TABLE empty_heap (id integer);
VACUUM (FREEZE, ANALYZE) remote_heap;
SELECT pg_create_physical_replication_slot('page_storage', true);
SELECT pg_create_physical_replication_slot('page_compute', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my $signature_sql = q{
SELECT count(*)::text || ':' || md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
FROM remote_heap
};
my $signature = $primary->safe_psql('postgres', $signature_sql);
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my ($spc, $db, $rel) = split '/', $primary->safe_psql('postgres', q{
SELECT (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')::text || '/' ||
       (SELECT oid FROM pg_database WHERE datname = current_database())::text || '/' ||
       pg_relation_filenode('remote_heap')::text
});
my $empty_rel = $primary->safe_psql('postgres',
	"SELECT pg_relation_filenode('empty_heap')");
my $relpath = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('remote_heap')");
$primary->backup('seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('page-store-cut'); SELECT pg_switch_wal()");

my $storage = PostgreSQL::Test::Cluster->new('storage');
$storage->init_from_backup($primary, 'seed', has_streaming => 1);
$storage->append_conf('postgresql.conf', q{
primary_slot_name = 'page_storage'
recovery_target_name = 'page-store-cut'
recovery_target_action = 'pause'
});
$storage->start;
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not reach the read cut';
my $cut = $storage->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');

sub fetch_sql
{
	my ($number, $fork, $start, $count) = @_;
	return "test_page_store_fetch($spc, $db, $number, $fork, $start, $count, " .
	  "'$sysid', $tli, '$cut')";
}

my $fetch = fetch_sql($rel, 0, 0, 2);
is($storage->safe_psql('postgres',
	"SELECT fork_exists AND nblocks > 64 AND octet_length(pages) = " .
	"2 * current_setting('block_size')::int FROM $fetch"),
	't', 'physical endpoint returns a bounded batch and fork size');
$fetch = fetch_sql($empty_rel, 0, 0, 0);
is($storage->safe_psql('postgres',
	"SELECT fork_exists AND nblocks = 0 AND octet_length(pages) = 0 FROM $fetch"),
	't', 'present but empty fork remains distinguishable from an absent fork');
$fetch = fetch_sql($empty_rel, 3, 0, 0);
is($storage->safe_psql('postgres',
	"SELECT NOT fork_exists AND nblocks = 0 FROM $fetch"),
	't', 'absent init fork is reported as absent');
$fetch = fetch_sql($rel, 0, 0, 65);
my ($ret, $out, $err) = $storage->psql('postgres', "SELECT * FROM $fetch");
ok($ret != 0 && $err =~ /invalid physical page request/,
	'oversized batch is rejected');

my $compute = PostgreSQL::Test::Cluster->new('reader');
$compute->init_from_backup($primary, 'seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres') . ' application_name=remote_page_test';
$conninfo =~ s/'/''/g;
$compute->append_conf('postgresql.conf', qq{
primary_slot_name = 'page_compute'
recovery_target_name = 'page-store-cut'
recovery_target_action = 'pause'
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.tablespace = $spc
test_page_store.database = $db
test_page_store.relfilenumber = $rel
test_page_store.request_timeout = '5s'
max_parallel_workers_per_gather = 0
});
$compute->start;
$compute->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'compute did not reach the read cut';
is($compute->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()'),
	$cut, 'compute and storage expose the same replay cut');

sub evict_heap
{
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('remote_heap')"
	) or die 'could not evict remote_heap';
	is($compute->safe_psql('postgres', qq{
SELECT count(*) FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc AND relfilenode = $rel
}), '0', 'no cached target buffers can hide a missing remote read');
}

evict_heap();
my $local_file = $compute->data_dir . '/' . $relpath;
my $saved_file = $local_file . '.held-for-page-service-test';
ok(-f $local_file, 'fixture has a local relation file before the test');
rename($local_file, $saved_file) or die "could not move $local_file aside: $!";
ok(!-e $local_file, 'local relation file is unavailable to compute reads');

my $reader = $compute->background_psql('postgres', on_error_stop => 0);
is($reader->query_safe($signature_sql), $signature,
	'ordinary SQL reads all expected values from remote buffers');
is($reader->query_safe(
	'SELECT startreadv > 0 FROM test_page_store_io_counts()'),
	't', 'SQL scan exercised the remote AIO entry point');

# The synchronous path must agree, including finalized page checksums.
my $sync_hash = $reader->query_safe(
	"SELECT md5(test_page_store_read_smgr('remote_heap', 0))");
$fetch = fetch_sql($rel, 0, 0, 1);
is($sync_hash, $storage->safe_psql('postgres', "SELECT md5(pages) FROM $fetch"),
	'synchronous SMgr read returns the same remote page');
is($reader->query_safe('SELECT readv > 0 FROM test_page_store_io_counts()'),
	't', 'synchronous read exercised the remote non-AIO entry point');

evict_heap();
is($reader->query_safe(q{
SET io_combine_limit = 4;
SELECT count(*) = 1 AND sum(nblocks) = 4
FROM read_buffers('remote_heap', 0, 4)
}), 't', 'one remote AIO completion publishes all four requested buffers');

evict_heap();
is($reader->query_safe($signature_sql), $signature,
	'eviction and repeated AIO completion preserve all values');

SKIP:
{
	skip 'Injection points are not available', 4 unless $injection_points;
	evict_heap();
	my $before = $reader->query_safe(
		'SELECT startreadv FROM test_page_store_io_counts()');
	$storage->safe_psql('postgres',
		"SELECT injection_points_attach('test-page-store-after-fetch', 'wait')");
	$reader->query_until(qr/request_started/, qq{
\\echo request_started
SELECT count(*) FROM read_buffers('remote_heap', 0, 4);
});
	$storage->poll_query_until('postgres', q{
SELECT EXISTS (SELECT FROM pg_stat_activity
WHERE application_name = 'remote_page_test'
  AND wait_event = 'test-page-store-after-fetch')
}) or die 'remote request did not reach the service wait point';
	my ($result, $status) = $reader->query('');
	ok($status != 0 && $reader->{stderr} =~ /page service request timed out/,
		'a stalled service fails within the request deadline')
	  or diag("status=$status, result=$result, err=$reader->{stderr}");
	$reader->{stderr} = '';
	is($reader->query_safe(
		"SELECT startreadv > $before FROM test_page_store_io_counts()"),
		't', 'timeout occurred inside the remote AIO path');
	$storage->safe_psql('postgres', q{
SELECT injection_points_wakeup('test-page-store-after-fetch');
SELECT injection_points_detach('test-page-store-after-fetch');
});
	is($reader->query_safe($signature_sql), $signature,
		'backend can reconnect and read after a failed request');
}

# A changed storage role is an error, not permission to use the old local file.
$storage->promote;
evict_heap();
($out, $ret) = $reader->query($signature_sql);
ok($ret != 0 && $reader->{stderr} =~ /page service requires a standby/,
	'provider view loss fails the query without a local fallback')
  or diag("ret=$ret, out=$out, err=$reader->{stderr}");
$reader->quit;

# Keep the disposable fixture complete for normal shutdown and log inspection.
rename($saved_file, $local_file) or die "could not restore $local_file: $!";
$compute->promote;
($ret, $out, $err) = $compute->psql('postgres', 'SELECT 1');
ok($ret != 0 && $err =~ /page service requires a standby/,
	'compute rejects execution outside the frozen view even without a buffer miss');
$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
