# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Runtime images, unlike crash redo, must preserve a live backend's command
# IDs.  Keep exact images in an epoch-local overlay while the ordinary WAL
# stream still drives storage and a read-only compute.  WAL flushes must also
# reach storage; restarting the writer after loss of its overlay is not yet
# supported.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $writer = PostgreSQL::Test::Cluster->new('overlay_writer');
$writer->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$writer->append_conf(
	'postgresql.conf', q{
autovacuum = off
fsync = on
checkpoint_timeout = '1h'
full_page_writes = off
wal_log_hints = off
wal_consistency_checking = 'heap,heap2,btree'
max_parallel_workers_per_gather = 0
});
$writer->start;
$writer->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION pageinspect;
CREATE EXTENSION amcheck;
CREATE TABLE overlay_heap (id int, payload text);
ALTER TABLE overlay_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO overlay_heap
  SELECT i, repeat('a', current_setting('block_size')::int / 16)
  FROM generate_series(1, 300) i;
CREATE UNIQUE INDEX overlay_idx ON overlay_heap(id);
CREATE TABLE command_marker (id int);
VACUUM (FREEZE, ANALYZE) overlay_heap;
SELECT pg_create_physical_replication_slot('overlay_storage', true);
SELECT pg_create_physical_replication_slot('overlay_reader', true);
});
my @names = qw(overlay_heap overlay_idx);
my $tli = $writer->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $spc = $writer->safe_psql('postgres',
	q{SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'});
my $db = $writer->safe_psql('postgres',
	q{SELECT oid FROM pg_database WHERE datname = current_database()});
my %locators = map {
	$_ => $writer->safe_psql('postgres', "SELECT pg_relation_filenode('$_')")
} @names;
my %paths = map {
	$_ => $writer->safe_psql('postgres', "SELECT pg_relation_filepath('$_')")
} @names;
my $locator_list = join ', ', map { "$spc/$db/$locators{$_}" } @names;
$writer->backup('overlay_storage_seed');
my $storage = PostgreSQL::Test::Cluster->new('overlay_storage');
$storage->init_from_backup($writer, 'overlay_storage_seed',
	has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'overlay_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 8192
});
$storage->start;
$writer->safe_psql('postgres',
	"SELECT pg_create_restore_point('overlay-baseline')");
$writer->wait_for_catchup($storage, 'replay', $writer->lsn('insert'));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['overlay_heap','overlay_idx']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$writer->backup('overlay_reader_seed');
my $reader = PostgreSQL::Test::Cluster->new('overlay_reader');
$reader->init_from_backup($writer, 'overlay_reader_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
my $common_config = qq{
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.physical_service = true
test_page_store.transport_slots = 4
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.locators = '$locator_list'
test_page_store.request_timeout = '30s'
shared_buffers = '16MB'
};
$reader->append_conf(
	'postgresql.conf', $common_config . q{
primary_slot_name = 'overlay_reader'
test_page_store.follow = true
});

# No user writes have occurred since the retained baseline.  Stop the primary
# cleanly, then remove its selected files before starting this test epoch.
$writer->stop;
$writer->append_conf(
	'postgresql.conf', $common_config . q{
test_page_store.writer_overlay_pages = 2048
test_page_store.wal_durability_slot = 'overlay_storage'
});
my @local_files;
for my $node ($writer, $reader)
{
	for my $name (@names)
	{
		for my $suffix ('', '_vm', '_fsm')
		{
			my $path = $node->data_dir . '/' . $paths{$name} . $suffix;
			push @local_files, $path;
			next unless -f $path;
			rename($path, "$path.held-for-overlay-test")
			  or die "could not move $path: $!";
		}
	}
}
$writer->start;
$writer->safe_psql('postgres',
	"SELECT pg_create_restore_point('overlay-reader-start')");
$reader->start;

my $digest_sql = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM overlay_heap
};

sub same_rows
{
	my ($description) = @_;
	my $lsn = $writer->lsn('insert');
	$writer->wait_for_catchup($storage, 'replay', $lsn);
	$writer->wait_for_catchup($reader, 'replay', $lsn);
	my $expected = $storage->safe_psql('postgres', $digest_sql);
	for my $node ($writer, $reader)
	{
		is( $node->safe_psql('postgres', $digest_sql),
			$expected,
			"$description: " . $node->name . ' values and TIDs match storage'
		);
		$node->safe_psql('postgres',
			"SELECT bt_index_check('overlay_idx', true)");
	}
	pass("$description: amcheck agrees with both heaps");
	is(scalar(grep { -e $_ } @local_files),
		0, "$description: neither compute recreates selected files");
}

my $buffers = qq{
FROM pg_buffercache WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode IN ($locators{overlay_heap}, $locators{overlay_idx})
};

sub evict_writer
{
	$writer->safe_psql('postgres', 'CHECKPOINT');
	$writer->poll_query_until('postgres',
		"SELECT coalesce(bool_and((pg_buffercache_evict(bufferid)).buffer_evicted), true) $buffers"
	) or die 'could not evict the selected writer buffers';
	is($writer->safe_psql('postgres', "SELECT count(*) $buffers"),
		'0', 'all selected writer buffers really evicted');
}

same_rows('cold baseline');
is( $writer->safe_psql(
		'postgres',
		'SELECT submitted > 0 FROM test_page_store_transport_status()'),
	't',
	'writer actually fetched baseline pages from storage');

my $session = $writer->background_psql('postgres');
$session->query_safe(
	q{
BEGIN;
INSERT INTO command_marker VALUES (1);
INSERT INTO overlay_heap VALUES (3001, 'before cursor');
DECLARE old_view CURSOR FOR
  SELECT id, payload FROM overlay_heap WHERE id >= 3001 ORDER BY id;
INSERT INTO overlay_heap VALUES (3002, 'after cursor');
UPDATE overlay_heap SET payload = 'updated after cursor' WHERE id = 3001;
SAVEPOINT s;
UPDATE overlay_heap SET payload = 'aborted child' WHERE id = 3001;
ROLLBACK TO s;
});
my $runtime_sql = q{
SELECT string_agg(ctid::text || ':' || cmin::text || ':' || cmax::text || ':' ||
                 id::text || ':' || payload, ',' ORDER BY id)
FROM overlay_heap WHERE id >= 3001
};
my $runtime_before = $session->query_safe($runtime_sql);
cmp_ok(
	$session->query_safe(
		'SELECT cmin::text::int FROM overlay_heap WHERE id = 3002'),
	'>', 0,
	'fixture has nonzero command IDs');
evict_writer();
is($session->query_safe($runtime_sql), $runtime_before,
	'eviction preserves command IDs, values and TIDs inside a live transaction'
);
is( $session->query_safe('FETCH ALL FROM old_view'),
	'3001|before cursor',
	'older cursor sees neither later insert nor later update');
is( $writer->safe_psql(
		'postgres', 'SELECT count(*) FROM overlay_heap WHERE id >= 3001'),
	'0',
	'another backend still cannot see the uncommitted versions');
is( $writer->safe_psql(
		'postgres',
		'SELECT reads > 0 AND writes > 0 FROM test_page_store_overlay_status()'
	),
	't',
	'exact overlay images were written and read back');
$session->query_safe('COMMIT');
$session->quit;
evict_writer();
same_rows('committed overlay images');

# Cross-page updates, B-tree splits and VM/FSM maintenance must all use the
# same overlay, not just the heap tuple carrying the command IDs above.
$writer->safe_psql(
	'postgres', q{
INSERT INTO overlay_heap
  SELECT i, repeat('b', current_setting('block_size')::int / 16)
  FROM generate_series(4001, 4600) i;
UPDATE overlay_heap SET payload = repeat('c', current_setting('block_size')::int / 8)
  WHERE id >= 4001 AND id % 3 = 0;
});
evict_writer();
same_rows('extension and cross-page update');
my $before_truncate =
  $storage->safe_psql('postgres', "SELECT pg_relation_size('overlay_heap')");
$writer->safe_psql('postgres', 'DELETE FROM overlay_heap WHERE id > 31');
$writer->safe_psql('postgres', 'VACUUM (FREEZE) overlay_heap');
evict_writer();
same_rows('vacuum and truncation');
cmp_ok(
	$storage->safe_psql(
		'postgres', "SELECT pg_relation_size('overlay_heap')"),
	'<',
	$before_truncate,
	'VACUUM really truncated the heap');
$writer->safe_psql(
	'postgres', q{
INSERT INTO overlay_heap
  SELECT i, repeat('d', current_setting('block_size')::int / 16)
  FROM generate_series(7001, 7200) i;
});
evict_writer();
same_rows('reextension after truncation');

$reader->stop;
$writer->safe_psql('postgres', 'DROP TABLE overlay_heap');
is( $writer->safe_psql(
		'postgres', 'SELECT pages FROM test_page_store_overlay_status()'),
	'0',
	'DROP releases selected main, VM and FSM images without reading storage');
$writer->stop;
$storage->stop;
ok( -f $writer->data_dir . '/test_page_store.writer_epoch',
	'epoch guard survives even a clean shutdown');
command_fails_like(
	[ 'pg_ctl', '-D', $writer->data_dir, '-l', $writer->logfile, 'start' ],
	qr/could not start server/,
	'restart without runtime images is rejected');
like(
	slurp_file($writer->logfile),
	qr/could not create test writer epoch guard/,
	'restart diagnoses the lost epoch rather than reading an old baseline');
done_testing();
