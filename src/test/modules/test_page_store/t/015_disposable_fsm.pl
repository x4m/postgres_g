# Copyright (c) 2026, PostgreSQL Global Development Group
#
# FSM is a disposable hint on read-only compute, not a historical page image.
# Exercise heuristic redo, loss on eviction, truncate/reextend and standalone
# FSM full-page images without local files or FSM requests to storage.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('fsm_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = off
wal_log_hints = off
wal_consistency_checking = 'heap,heap2,btree'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION pg_freespacemap;
CREATE EXTENSION amcheck;
CREATE TABLE fsm_heap (id int, payload text);
ALTER TABLE fsm_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO fsm_heap SELECT i, repeat('a', current_setting('block_size')::int / 16)
  FROM generate_series(1, 1000) i;
CREATE UNIQUE INDEX fsm_idx ON fsm_heap(id);
VACUUM (FREEZE, ANALYZE) fsm_heap;
SELECT pg_create_physical_replication_slot('fsm_storage', true);
SELECT pg_create_physical_replication_slot('fsm_compute', true);
});
my @names = qw(fsm_heap fsm_idx);
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $spc = $primary->safe_psql('postgres',
	q{SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'});
my $db = $primary->safe_psql('postgres',
	q{SELECT oid FROM pg_database WHERE datname = current_database()});
my %locators = map {
	$_ => $primary->safe_psql('postgres', "SELECT pg_relation_filenode('$_')")
} @names;
my %paths = map {
	$_ => $primary->safe_psql('postgres', "SELECT pg_relation_filepath('$_')")
} @names;
my $locator_list = join ', ', map { "$spc/$db/$locators{$_}" } @names;
$primary->backup('fsm_storage_seed');
my $storage = PostgreSQL::Test::Cluster->new('fsm_storage');
$storage->init_from_backup($primary, 'fsm_storage_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'fsm_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 8192
});
$storage->start;
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('fsm-baseline')");
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['fsm_heap','fsm_idx']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->backup('fsm_compute_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('fsm-after-seed')");

my $compute = PostgreSQL::Test::Cluster->new('fsm_compute');
$compute->init_from_backup($primary, 'fsm_compute_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'fsm_compute'
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.physical_service = true
test_page_store.transport_slots = 4
test_page_store.follow = true
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.locators = '$locator_list'
test_page_store.request_timeout = '30s'
shared_buffers = '16MB'
max_parallel_workers_per_gather = 0
});
ok(-f $compute->data_dir . '/' . $paths{fsm_heap} . '_fsm',
	'seed initially includes a heap FSM');
my @local_files;
for my $name (@names)
{
	for my $suffix ('', '_fsm', '_vm')
	{
		my $path = $compute->data_dir . '/' . $paths{$name} . $suffix;
		push @local_files, $path;
		next unless -f $path;
		rename($path, "$path.held-for-fsm-test")
		  or die "could not move $path: $!";
	}
}

sub catchup
{
	my $lsn = $primary->lsn('insert');
	$primary->wait_for_catchup($storage, 'replay', $lsn);
	$primary->wait_for_catchup($compute, 'replay', $lsn);
}

my $digest_sql = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM fsm_heap
};

sub same_rows
{
	my ($description) = @_;
	is( $compute->safe_psql('postgres', $digest_sql),
		$primary->safe_psql('postgres', $digest_sql),
		"$description: all values and physical TIDs match");
	$compute->safe_psql('postgres', "SELECT bt_index_check('fsm_idx', true)");
	pass("$description: amcheck agrees with heap");
	is(scalar(grep { -e $_ } @local_files),
		0, "$description: no selected local files exist");
}

my $fsm_buffers = qq{
FROM pg_buffercache WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode = $locators{fsm_heap} AND relforknumber = 1
};

sub evict_fsm
{
	$compute->poll_query_until('postgres',
		"SELECT coalesce(bool_and((pg_buffercache_evict(bufferid)).buffer_evicted), true) $fsm_buffers"
	) or die 'could not evict the FSM';
	is($compute->safe_psql('postgres', "SELECT count(*) $fsm_buffers"),
		'0', 'all heap FSM buffers really evicted');
}

$compute->start;
catchup();
same_rows('startup without FSM');
my $heap_block = $primary->safe_psql('postgres',
	'SELECT (ctid::text::point)[0]::int FROM fsm_heap WHERE id = 1');

# Warm main pages make heap prune redo recalculate their free space.  Hint
# FPIs are disabled here, so they cannot stand in for that heuristic update.
$primary->safe_psql('postgres', 'DELETE FROM fsm_heap WHERE id % 2 = 0');
$primary->safe_psql('postgres', 'VACUUM (TRUNCATE false) fsm_heap');
catchup();
is($compute->safe_psql('postgres', "SELECT count(*) > 0 $fsm_buffers"),
	't', 'heuristic redo materializes FSM buffers');
is( $compute->safe_psql(
		'postgres', "SELECT pg_freespace('fsm_heap', $heap_block) > 0"),
	't',
	'warm prune redo records available space');
same_rows('heuristic FSM redo');

evict_fsm();
my $requests = $compute->safe_psql('postgres',
	'SELECT submitted FROM test_page_store_transport_status()');
is( $compute->safe_psql(
		'postgres', "SELECT pg_freespace('fsm_heap', $heap_block)"),
	'0',
	'evicted FSM is rebuilt as unknown rather than refetched');
is( $compute->safe_psql(
		'postgres',
		'SELECT submitted FROM test_page_store_transport_status()'),
	$requests,
	'reading disposable FSM does not issue a storage request');
same_rows('after losing FSM hints');

my $old_size =
  $primary->safe_psql('postgres', "SELECT pg_relation_size('fsm_heap')");
$primary->safe_psql('postgres', 'DELETE FROM fsm_heap WHERE id > 31');
$primary->safe_psql('postgres', 'VACUUM fsm_heap');
catchup();
cmp_ok($primary->safe_psql('postgres', "SELECT pg_relation_size('fsm_heap')"),
	'<', $old_size, 'fixture truncates the heap');
same_rows('FSM truncate');
$primary->safe_psql(
	'postgres', q{
INSERT INTO fsm_heap SELECT i, repeat('b', current_setting('block_size')::int / 16)
  FROM generate_series(2001, 2500) i
});
catchup();
same_rows('heap reextension');

# FSM full-page images retain the normal redo contract, even though a later
# eviction is allowed to lose their contents.  Use ordinary hint WAL, not a
# test-only mutating helper.
$primary->stop;
$primary->append_conf('postgresql.conf', 'wal_log_hints = on');
$primary->start;
$primary->safe_psql('postgres', 'CHECKPOINT');
my $fpi_start = $primary->lsn('insert');
$primary->safe_psql('postgres', 'DELETE FROM fsm_heap WHERE id > 2250');
$primary->safe_psql('postgres', 'VACUUM fsm_heap');
my $fpi_end = $primary->lsn('insert');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
command_like(
	[
		'pg_waldump', '-p', $primary->data_dir . '/pg_wal',
		'-s', $fpi_start, '-e', $fpi_end, '-r', 'XLOG', '-F', 'fsm'
	],
	qr/FPI.*rel $spc\/$db\/$locators{fsm_heap}\b/,
	'fixture logs a standalone FSM full-page image');
# Wait for a real record, not the unwritten header after the WAL switch.
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('fsm-after-fpi')");
catchup();
same_rows('standalone FSM full-page redo');
evict_fsm();
is( $compute->safe_psql(
		'postgres', "SELECT pg_freespace('fsm_heap', $heap_block)"),
	'0',
	'even WAL-restored FSM contents can be discarded');

$compute->stop('immediate');
$compute->start;
catchup();
same_rows('compute restart without any FSM file');
$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
