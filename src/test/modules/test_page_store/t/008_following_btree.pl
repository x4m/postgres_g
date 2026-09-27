# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Follow heap and B-tree WAL without their local main-fork files.  In
# particular, a scan spanning a concurrent split must neither repeat nor
# omit TIDs when it changes between cached and remotely fetched pages.
use strict;
use warnings FATAL => 'all';

use IPC::Run qw(run);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('btree_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = off
wal_consistency_checking = 'heap,heap2,btree'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION pageinspect;
CREATE EXTENSION amcheck;
CREATE TABLE tree_heap (id int, payload text);
ALTER TABLE tree_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO tree_heap
  SELECT i * 1000, repeat('a', current_setting('block_size')::int / 8)
  FROM generate_series(1, 400) i;
CREATE UNIQUE INDEX tree_idx ON tree_heap(id) INCLUDE (payload)
  WITH (fillfactor = 70);
VACUUM (FREEZE, ANALYZE) tree_heap;
SELECT pg_create_physical_replication_slot('btree_storage', true);
SELECT pg_create_physical_replication_slot('btree_compute', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my @names = qw(tree_heap tree_idx);
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
$primary->backup('btree_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('btree-baseline')");

my $storage = PostgreSQL::Test::Cluster->new('btree_storage');
$storage->init_from_backup($primary, 'btree_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'btree_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 32768
shared_buffers = '16MB'
});
$storage->start;
$primary->wait_for_catchup($storage);
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['tree_heap','tree_idx']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

my $compute = PostgreSQL::Test::Cluster->new('btree_compute');
$compute->init_from_backup($primary, 'btree_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'btree_compute'
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.follow = true
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.locators = '$locator_list'
test_page_store.request_timeout = '30s'
shared_buffers = '16MB'
max_parallel_workers_per_gather = 0
});
$compute->start;
$primary->wait_for_catchup($compute);

sub cached_pages
{
	my ($name) = @_;
	return $compute->safe_psql(
		'postgres', qq{
SELECT count(*) FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode = $locators{$name} AND relforknumber = 0
});
}

sub evict
{
	my ($name) = @_;
	# Eviction can skip a buffer briefly pinned by a background writer.
	# Wait for the operation to succeed before requiring a wholly cold scan.
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('$name')"
	) or die "could not evict $name";
	is(cached_pages($name), '0', "no cached $name main pages remain");
}

sub status
{
	my ($field) = @_;
	return $compute->safe_psql('postgres',
		"SELECT $field FROM test_page_store_compute_status()");
}

sub catchup
{
	# VACUUM need not flush its last WAL record.  Waiting for the current
	# write position can leave metapage initialization racing later eviction.
	# Use an exact record end, including when the insertion position happens
	# to have advanced over a page header.
	my $target = $primary->safe_psql('postgres',
		"SELECT pg_create_restore_point('btree-catchup')");
	$primary->safe_psql('postgres',
		"SELECT test_page_store_flush_wal('$target', false)");
	$primary->wait_for_catchup($storage, 'replay', $target);
	$primary->wait_for_catchup($compute, 'replay', $target);
}

# Preserve output order as well as every value and physical TID.  The
# independent oracle uses a sequential scan and a sort on the writer.
sub digest_sql
{
	my ($dir, $index) = @_;
	return (
		$index
		? q{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
}
		: q{
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
})
	  . qq{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload, ','))
FROM (SELECT ctid, id, payload FROM tree_heap ORDER BY id $dir) s;
};
}

sub same_rows
{
	my ($description) = @_;
	for my $dir (qw(ASC DESC))
	{
		is( $compute->safe_psql('postgres', digest_sql($dir, 1)),
			$primary->safe_psql('postgres', digest_sql($dir, 0)),
			"$description: $dir index scan matches the sequential oracle");
	}

	# amcheck supplements, rather than replaces, the result comparison.
	is( $compute->safe_psql(
			'postgres', "SELECT bt_index_check('tree_idx', true)"),
		'',
		"$description: amcheck with heapallindexed");
}

sub wait_event
{
	my ($event) = @_;
	$compute->poll_query_until(
		'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event = '$event')
}) or die "compute did not reach $event";
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

my @local_files;
for my $name (@names)
{
	evict($name);
	my $local = $compute->data_dir . '/' . $paths{$name};
	rename($local, "$local.held-for-btree-test")
	  or die "could not move $local aside: $!";
	push @local_files, $local;
}

my $skipped = status('skipped');
$primary->safe_psql(
	'postgres', q{
INSERT INTO tree_heap VALUES
  (1001, repeat('b', current_setting('block_size')::int / 8))
});
catchup();
cmp_ok(
	status('skipped'), '>=',
	$skipped + 2,
	'cold heap and B-tree insertion both skip page redo');
is(cached_pages('tree_idx'), '0', 'cold B-tree redo does not load its pages');
is(status('startup_fetches'), '0', 'cold insertion needs no startup fetch');
same_rows('cold heap and index');

for my $dir (qw(ASC DESC))
{
	my $plan = $compute->safe_psql(
		'postgres', qq{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
EXPLAIN (COSTS OFF) SELECT ctid, id, payload FROM tree_heap ORDER BY id $dir
});
	like(
		$plan,
		qr/Index Scan(?: Backward)? using tree_idx/,
		"$dir comparison really uses the selected B-tree");
	unlike($plan, qr/Sort/, "$dir comparison does not sort index output");
}

my $cached = status('cached');
my $fetches = status('fetches');
$primary->safe_psql('postgres',
	'UPDATE tree_heap SET id = 999 WHERE id = 1000');
catchup();
cmp_ok(
	status('cached'), '>=',
	$cached + 2,
	'warm heap and B-tree updates apply local redo');
same_rows('warm heap and index');
is(status('fetches'), $fetches, 'warm redo and checks need no remote fetch');

SKIP:
{
	skip 'Injection points are not available', 17 unless $injection_points;

	# Stop after saving the left link but before reading that sibling.  A
	# split changes the link while the scan has no buffer pins or locks.
	my $left = $primary->safe_psql(
		'postgres', q{
SELECT btpo_prev FROM generate_series(1,
  pg_relation_size('tree_idx') / current_setting('block_size')::int - 1) b,
  LATERAL bt_page_stats('tree_idx', b) s
WHERE type = 'l' AND btpo_next = 0
});
	my $low = $primary->safe_psql(
		'postgres', qq{
SELECT min(id) FROM bt_page_items('tree_idx', $left) i
JOIN tree_heap h ON h.ctid = i.ctid WHERE i.itemoffset > 1
});
	my $expected = $primary->safe_psql('postgres', digest_sql('DESC', 0));
	my $reader = $compute->background_psql('postgres');
	$reader->query_safe(
		q{
SELECT injection_points_set_local();
SELECT injection_points_attach('nbtree-walk-left', 'wait');
SELECT injection_points_attach('nbtree-walk-left-step-right', 'notice');
});
	$reader->query_until(qr/scan_started/,
		"\\echo scan_started\n" . digest_sql('DESC', 1));
	wait_event('nbtree-walk-left');
	evict('tree_idx');
	$primary->safe_psql(
		'postgres', qq{
INSERT INTO tree_heap
  SELECT $low + i, repeat('c', current_setting('block_size')::int / 8)
  FROM generate_series(1, 999) i
});
	catchup();
	wakeup('nbtree-walk-left');
	is(scalar($reader->query('')), $expected,
		'backwards scan across remote split pages returns every old TID once'
	);
	like(
		$reader->{stderr},
		qr/nbtree-walk-left-step-right/,
		'backwards scan took its concurrent-split recovery path');
	$reader->quit;
	same_rows('after the concurrent backwards scan');

	# A forward scan has already consumed its first leaf when it starts I/O
	# on the second.  Split the first while that I/O is admitted: redo must
	# wait for the second leaf, whose left link the split must change.
	my $second = $primary->safe_psql(
		'postgres', q{
SELECT btpo_next FROM generate_series(1,
  pg_relation_size('tree_idx') / current_setting('block_size')::int - 1) b,
  LATERAL bt_page_stats('tree_idx', b) s
WHERE type = 'l' AND btpo_prev = 0
});
	ok( $compute->poll_query_until(
			'postgres', qq{
SELECT coalesce(bool_and((pg_buffercache_evict(bufferid)).buffer_evicted), true)
FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode = $locators{tree_idx} AND relforknumber = 0
AND relblocknumber = $second
}),
		'evicted the next forward leaf, leaving the preceding leaf warm');
	$expected = $primary->safe_psql('postgres', digest_sql('ASC', 0));
	$reader = $compute->background_psql('postgres');
	$reader->query_safe(
		q{
SELECT injection_points_set_local();
SELECT injection_points_attach('test-page-store-before-remote-fetch', 'wait');
});
	$reader->query_until(qr/scan_started/,
		"\\echo scan_started\n" . digest_sql('ASC', 1));
	wait_event('test-page-store-before-remote-fetch');
	$primary->safe_psql(
		'postgres', q{
INSERT INTO tree_heap
  SELECT -i, repeat('d', current_setting('block_size')::int / 8)
  FROM generate_series(1, 500) i
});
	$primary->wait_for_catchup($storage);
	wait_event('BufferIo');
	wakeup('test-page-store-before-remote-fetch');
	is($reader->query_safe(''),
		$expected,
		'forward scan spanning a split returns every old TID once');
	$reader->quit;
	catchup();
	same_rows('after the concurrent forward scan');

	# The opposite admission order: split redo already skipped its original
	# page.  Fetching that page at the preceding completed cut would lose
	# the split's changes.  The scan must wait for this record to finish.
	$expected = $primary->safe_psql('postgres', digest_sql('ASC', 0));
	evict($_) for @names;
	$compute->safe_psql(
		'postgres', q{
SELECT injection_points_attach('test-page-store-after-btree-split-skip', 'wait')
});
	$primary->safe_psql(
		'postgres', q{
INSERT INTO tree_heap
  SELECT -i, repeat('g', current_setting('block_size')::int / 8)
  FROM generate_series(501, 600) i
});
	$primary->wait_for_catchup($storage);
	wait_event('test-page-store-after-btree-split-skip');
	$reader = $compute->background_psql('postgres');
	$reader->query_until(qr/scan_started/,
		"\\echo scan_started\n" . digest_sql('ASC', 1));
	wait_event('TestPageStoreReplay');
	wakeup('test-page-store-after-btree-split-skip');
	is($reader->query_safe(''), $expected,
		'cold scan waits for skipped split and preserves its earlier snapshot'
	);
	$reader->quit;
	catchup();
	same_rows('after the skipped split');
}

# Unlike wholly cold redo below, concurrent reads can admit an invalid
# buffer for which startup wins the race to start input I/O.  Count from
# here rather than requiring that such a race never happened above.
my $startup_fetches = status('startup_fetches');
evict($_) for @names;
$primary->safe_psql(
	'postgres', q{
INSERT INTO tree_heap
  SELECT i, repeat('e', current_setting('block_size')::int / 8)
  FROM generate_series(400001, 403000) i;
});
catchup();
SKIP:
{
	# Larger blocks have more internal downlinks, although the payloads keep
	# the number of leaf tuples per page roughly the same.
	skip 'internal split fixture assumes blocks no larger than 8kB', 1
	  if $primary->safe_psql('postgres', "SHOW block_size") > 8192;
	cmp_ok(
		$primary->safe_psql(
			'postgres', "SELECT level FROM bt_metap('tree_idx')"),
		'>=', 2,
		'fixture includes an internal split and a new root');
}
same_rows('internal and leaf splits');

evict($_) for @names;
$primary->safe_psql(
	'postgres', q{
DELETE FROM tree_heap WHERE id < 402500;
VACUUM tree_heap;
});
catchup();
cmp_ok(
	$primary->safe_psql(
		'postgres', q{
SELECT count(*) FROM generate_series(1,
  pg_relation_size('tree_idx') / current_setting('block_size')::int - 1) b,
  LATERAL bt_page_stats('tree_idx', b) s WHERE type = 'd'
}),
	'>',
	0,
	'VACUUM actually deleted B-tree pages');
same_rows('half-dead and unlinked pages');

# Advance the XID horizon past the deletion stamps and make deleted pages
# available to allocation.  Verify reuse in WAL, not from file-size guesses.
$primary->safe_psql('postgres', 'SELECT pg_current_xact_id()');
$primary->safe_psql('postgres', 'VACUUM tree_heap');
catchup();
evict($_) for @names;
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('btree_wal_inspection', true)"
);
my $reuse_start =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->safe_psql(
	'postgres', q{
INSERT INTO tree_heap
  SELECT i, repeat('f', current_setting('block_size')::int / 8)
  FROM generate_series(500001, 501000) i;
});
catchup();
my $reuse_end =
  $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my ($wal, $wal_err) = ('', '');
ok( run([
			'pg_waldump', '--rmgr=Btree', '-p',
			$primary->data_dir . '/pg_wal',
			'-s', $reuse_start, '-e', $reuse_end
		],
		'>',
		\$wal,
		'2>',
		\$wal_err),
	'read B-tree WAL for the reuse interval') or diag $wal_err;
like($wal, qr/REUSE_PAGE/,
	'allocation reused pages and logged standby conflicts');
$primary->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('btree_wal_inspection')");
same_rows('recycled pages');
is(status('startup_fetches'),
	$startup_fetches,
	'isolated split, deletion and reuse redo need no remote page fetch');
ok(!-e $_, "main fork remains absent: $_") for @local_files;
$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
