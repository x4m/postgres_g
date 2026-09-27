# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A following read-only compute keeps the selected heap's main fork only in
# shared buffers.  Cold redo and buffer admission must agree on which record
# a cache miss needs.  Other forks, indexes and catalogs still use local md.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('follow_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = off
wal_log_hints = on
wal_consistency_checking = 'heap,heap2'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION test_aio;
CREATE EXTENSION pg_buffercache;
CREATE TABLE follow_heap (id int, payload text);
ALTER TABLE follow_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO follow_heap
  SELECT i, repeat(md5(i::text), 3) FROM generate_series(1, 500) i;
VACUUM (FREEZE, ANALYZE) follow_heap;
SELECT pg_create_physical_replication_slot('follow_storage', true);
SELECT pg_create_physical_replication_slot('follow_compute', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my $signature_sql = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM follow_heap
};
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my ($spc, $db, $rel) = split '/', $primary->safe_psql(
	'postgres', q{
SELECT (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')::text || '/' ||
       (SELECT oid FROM pg_database WHERE datname = current_database())::text || '/' ||
       pg_relation_filenode('follow_heap')::text
});
my $relpath = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('follow_heap')");
$primary->backup('follow_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('follow-baseline')");

my $storage = PostgreSQL::Test::Cluster->new('follow_storage');
$storage->init_from_backup($primary, 'follow_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'follow_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 4096
});
$storage->start;
$primary->wait_for_catchup($storage);
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql('postgres',
	"SELECT test_page_store_retain('follow_heap')");
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

my $compute = PostgreSQL::Test::Cluster->new('follow_compute');
$compute->init_from_backup($primary, 'follow_seed', has_streaming => 1);
# Windows permits fewer blocks in one I/O.  Keep the fixture small enough
# that both blocks of the later UPDATE fit in one legal combined read.
my $combine_limit = $primary->safe_psql('postgres',
	"SELECT least(64, max_val::int) FROM pg_settings WHERE name = 'io_max_combine_limit'"
);
my $conninfo =
  $storage->connstr('postgres') . ' application_name=follow_page_test';
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'follow_compute'
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.follow = true
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.tablespace = $spc
test_page_store.database = $db
test_page_store.relfilenumber = $rel
test_page_store.request_timeout = '30s'
max_parallel_workers_per_gather = 0
shared_buffers = '16MB'
io_max_combine_limit = $combine_limit
});
$compute->start;
$primary->wait_for_catchup($compute);
is( $compute->safe_psql(
		'postgres', 'SELECT active FROM test_page_store_compute_status()'),
	't',
	'following compute activates at its seed baseline');

sub evict_heap
{
	# Finish preceding WAL before eviction, including hint records from reads.
	catchup();

	# A background writer can briefly pin a dirty buffer.  Eviction reports
	# that as a skipped buffer, not an error or a promise that it is now cold.
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('follow_heap')"
	) or die 'could not evict follow_heap';
	is( $compute->safe_psql(
			'postgres', qq{
SELECT count(*) FROM pg_buffercache WHERE reldatabase = $db
AND reltablespace = $spc AND relfilenode = $rel AND relforknumber = 0
}),
		'0',
		'no target main-fork buffers remain');
}

sub status
{
	my ($field) = @_;
	return $compute->safe_psql('postgres',
		"SELECT $field FROM test_page_store_compute_status()");
}

sub catchup
{
	# VACUUM and hint records need not have reached the current write position.
	# Wait for an explicit, flushed record end on both replay nodes.
	my $target = $primary->safe_psql('postgres',
		"SELECT pg_create_restore_point('heap-catchup')");
	$primary->safe_psql('postgres',
		"SELECT test_page_store_flush_wal('$target', false)");
	$primary->wait_for_catchup($storage, 'replay', $target);
	$primary->wait_for_catchup($compute, 'replay', $target);
}

sub same_rows
{
	my ($description) = @_;
	is($compute->safe_psql('postgres', $signature_sql),
		$primary->safe_psql('postgres', $signature_sql), $description);
}

sub wait_event
{
	my ($node, $event) = @_;
	$node->poll_query_until(
		'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event = '$event')
}) or die "did not reach $event on " . $node->name;
}

sub wakeup
{
	my ($point) = @_;
	$compute->safe_psql(
		'postgres', qq{
SELECT injection_points_detach('$point');
SELECT injection_points_wakeup('$point');
});
}

evict_heap();
my $local_file = $compute->data_dir . '/' . $relpath;
my $saved_file = $local_file . '.held-for-follow-test';
rename($local_file, $saved_file)
  or die "could not move $local_file aside: $!";
ok(!-e $local_file, 'following compute has no local heap main file');

my $skipped = status('skipped');
$primary->safe_psql('postgres',
	"UPDATE follow_heap SET payload = 'cold change' WHERE id = 1");
catchup();
cmp_ok(status('skipped'), '>', $skipped, 'cold heap redo is skipped');
is(status('startup_fetches'), '0',
	'startup did not fetch the cold page, including for WAL consistency checking'
);
same_rows('first cache miss sees the replayed values and physical TIDs');

my $fetches = status('fetches');
my $cached = status('cached');
$primary->safe_psql('postgres',
	"UPDATE follow_heap SET payload = 'warm change' WHERE id = 2");
catchup();
cmp_ok(status('cached'), '>', $cached, 'warm heap redo updates cached pages');
same_rows('cached redo preserves the exact result');
is(status('fetches'), $fetches, 'warm update and read need no page fetch');

# Discard dirty buffers too: a subsequent miss must not resurrect old data.
evict_heap();
same_rows('eviction preserves the lower bound of replayed pages');

SKIP:
{
	skip 'Injection points are not available', 14 unless $injection_points;

	# A buffer mapping with input I/O in progress is not an absent page.
	evict_heap();
	my $reader = $compute->background_psql('postgres');
	$reader->query_safe(
		q{
SELECT injection_points_set_local();
SELECT injection_points_attach('test-page-store-before-remote-fetch', 'wait');
});
	$reader->query_until(
		qr/read_started/, q{
\echo read_started
SELECT count(*) FROM read_buffers('follow_heap', 0, 1);
});
	wait_event($compute, 'test-page-store-before-remote-fetch');
	$primary->safe_psql('postgres',
		"UPDATE follow_heap SET payload = 'I/O first' WHERE id = 3");
	$primary->wait_for_catchup($storage);
	wait_event($compute, 'BufferIo');
	pass('redo waits for an already mapped, not-yet-valid buffer');
	wakeup('test-page-store-before-remote-fetch');
	is($reader->query_safe(''), '1', 'the original read completes');
	catchup();
	same_rows('redo catches up the page fetched at an older cut');
	$reader->quit;

	# The opposite order: startup has skipped a page before a reader maps it.
	# Make an UPDATE that changes two blocks.  A combined read must not own
	# I/O for the second block while waiting for this record to finish.
	evict_heap();
	$compute->safe_psql('postgres',
		"SELECT injection_points_attach('test-page-store-after-update-skip', 'wait')"
	);
	my ($old_block) = $primary->safe_psql('postgres',
		'SELECT ctid FROM follow_heap WHERE id = 4') =~ /^\((\d+),/;
	my ($new_block) = $primary->safe_psql(
		'postgres', q{
UPDATE follow_heap SET payload = repeat('x', current_setting('block_size')::int / 4)
WHERE id = 4 RETURNING ctid
}) =~ /^\((\d+),/;
	isnt($new_block, $old_block, 'fixture update changes two heap blocks');
	cmp_ok($new_block - $old_block + 1,
		'<=', $combine_limit,
		'both updated blocks fit in one legal combined read');
	$primary->wait_for_catchup($storage);
	wait_event($compute, 'test-page-store-after-update-skip');
	$reader = $compute->background_psql('postgres');
	$reader->query_safe("SET io_combine_limit = $combine_limit");
	$reader->query_until(
		qr/read_started/, qq{
\\echo read_started
SELECT max(nblocks) FROM read_buffers('follow_heap', $old_block,
                                     @{[$new_block - $old_block + 1]});
});
	wait_event($compute, 'TestPageStoreReplay');
	pass('cache miss waits for the record skipped before buffer admission');
	wakeup('test-page-store-after-update-skip');
	is($reader->query_safe(''), '1',
		'following reads complete per-page without circular multi-page waits'
	);
	catchup();
	same_rows(
		'multi-page update and a concurrent read lose no physical TIDs');
	$reader->quit;

	# Storage may lag compute.  It must wait for the requested retained prefix,
	# not answer from the older page it happens to have at request time.
	evict_heap();
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
	$storage->poll_query_until('postgres',
		"SELECT pg_get_wal_replay_pause_state() = 'paused'")
	  or die 'storage did not pause for the lag test';
	$primary->safe_psql('postgres',
		"UPDATE follow_heap SET payload = 'storage lag' WHERE id = 5");
	$primary->wait_for_catchup($compute);
	$reader = $compute->background_psql('postgres');
	$reader->query_until(
		qr/read_started/, q{
\echo read_started
SELECT count(*) FROM read_buffers('follow_heap', 0, 1);
});
	wait_event($storage, 'TestPageStoreHistory');
	pass('page request waits for storage replay to reach the requested cut');
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	is($reader->query_safe(''),
		'1', 'page read resumes after storage catches up');
	catchup();
	same_rows('storage lag cannot return a stale page');
	$reader->quit;
}

# Exercise init-page callers: XLogInitBufferForRedo must return a real buffer
# even when no heap pages were cached before the INSERT.
evict_heap();
$primary->safe_psql(
	'postgres', q{
INSERT INTO follow_heap
SELECT i, repeat('y', current_setting('block_size')::int / 4)
FROM generate_series(1001, 1020) i;
});
catchup();
same_rows(
	'redo initializes and extends the remote heap without a local file');
ok(!-e $local_file,
	'neither replay nor buffer eviction recreated the heap file');

evict_heap();
my $before_truncate =
  $primary->safe_psql('postgres', "SELECT pg_relation_size('follow_heap')");
# Establish a snapshot without reading the remote heap.  Even entirely cold
# pruning must resolve snapshot conflicts before skipping its page work.
$compute->append_conf(
	'postgresql.conf', q{
max_standby_streaming_delay = 0
max_standby_archive_delay = 0
});
$compute->reload;
$compute->poll_query_until('postgres',
	"SELECT current_setting('max_standby_streaming_delay') = '0'")
  or die 'compute did not reload conflict delay';
my $snapshot = $compute->background_psql('postgres', on_error_stop => 0);
$snapshot->query_safe(
	q{
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM pg_class;
});
my $log_start = -s $compute->logfile;
$primary->safe_psql(
	'postgres', q{
DELETE FROM follow_heap WHERE id > 50;
VACUUM follow_heap;
});
catchup();
ok( $compute->wait_for_log(
		qr/User query might have needed to see row versions that must be removed/,
		$log_start),
	'cold pruning still cancels a conflicting old snapshot');
$snapshot->reconnect_and_clear;
$snapshot->quit;
cmp_ok(
	$primary->safe_psql('postgres', "SELECT pg_relation_size('follow_heap')"),
	'<', $before_truncate, 'VACUUM really truncates the fixture');
same_rows('cold pruning and truncation preserve the exact result');
$primary->safe_psql(
	'postgres', q{
INSERT INTO follow_heap
SELECT i, repeat('z', current_setting('block_size')::int / 4)
FROM generate_series(2001, 2020) i;
});
catchup();
evict_heap();
same_rows('reextension cannot resurrect pages discarded by truncation');
ok(!-e $local_file,
	'truncation and reextension do not recreate the heap file');

# xlog_redo() expects BLK_RESTORED for a standalone FPI, not BLK_NOTFOUND.
# Leave an unhinted committed tuple across a checkpoint, then SELECT it.
$primary->append_conf('postgresql.conf', 'full_page_writes = on');
$primary->reload;
$primary->poll_query_until('postgres',
	"SELECT current_setting('full_page_writes') = 'on'")
  or die 'writer did not enable full_page_writes';
$primary->safe_psql('postgres',
	"UPDATE follow_heap SET payload = 'unhinted' WHERE id = 1");
$primary->safe_psql('postgres', 'CHECKPOINT');
catchup();
evict_heap();
my $fpi_start =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
my $fpi_signature = $primary->safe_psql('postgres', $signature_sql);
my $fpi_end =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
command_like(
	[
		'pg_waldump', '-p', $primary->data_dir . '/pg_wal',
		'-s', $fpi_start, '-e', $fpi_end, '-r', 'XLOG'
	],
	qr/FPI_FOR_HINT.*rel $spc\/$db\/$rel\b/,
	'fixture emits a standalone full-page image for the remote heap');
catchup();
is($compute->safe_psql('postgres', $signature_sql),
	$fpi_signature,
	'standalone full-page redo preserves the exact SQL result');
ok(!-e $local_file, 'full-page redo does not recreate the local heap file');

$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
