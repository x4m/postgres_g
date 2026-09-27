# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Index-only scans must agree with the heap while both forks live remotely.
use strict;
use warnings FATAL => 'all';

use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('vm_writer');
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
CREATE EXTENSION pg_visibility;
CREATE EXTENSION pageinspect;
CREATE EXTENSION amcheck;
CREATE TABLE vm_heap (id int, payload text);
ALTER TABLE vm_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO vm_heap
  SELECT i, repeat('a', current_setting('block_size')::int / 16)
  FROM generate_series(1, 500) i;
CREATE UNIQUE INDEX vm_idx ON vm_heap(id) INCLUDE (payload);
VACUUM (FREEZE, ANALYZE) vm_heap;
CREATE TABLE fresh_vm (id int PRIMARY KEY);
INSERT INTO fresh_vm VALUES (1);
SELECT pg_create_physical_replication_slot('vm_storage', true);
SELECT pg_create_physical_replication_slot('vm_compute', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my @names = qw(vm_heap vm_idx fresh_vm fresh_vm_pkey);
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
$primary->backup('vm_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('vm-baseline')");
my $storage = PostgreSQL::Test::Cluster->new('vm_storage');
$storage->init_from_backup($primary, 'vm_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'vm_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 16384
shared_buffers = '16MB'
});
$storage->start;
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(
  ARRAY['vm_heap','vm_idx','fresh_vm','fresh_vm_pkey']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
my $compute = PostgreSQL::Test::Cluster->new('vm_compute');
$compute->init_from_backup($primary, 'vm_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'vm_compute'
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
$primary->wait_for_catchup($compute, 'replay', $primary->lsn('insert'));

my $ios_settings = q{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
};
my $digest = q{
SELECT md5(string_agg(id::text || ':' || payload, ','))
FROM (SELECT id, payload FROM vm_heap ORDER BY id) s
};

sub oracle
{
	return $primary->safe_psql(
		'postgres', q{
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
} . $digest);
}

sub same_rows
{
	my ($description, $no_heap) = @_;
	is($compute->safe_psql('postgres', $ios_settings . $digest),
		oracle(), "$description: all ordered values match the heap oracle");
	my $plan = decode_json(
		$compute->safe_psql(
			'postgres', $ios_settings . q{
EXPLAIN (ANALYZE, FORMAT JSON) SELECT id, payload FROM vm_heap ORDER BY id
}))->[0]->{Plan};
	is($plan->{'Node Type'}, 'Index Only Scan', "$description: real IOS");
	is($plan->{'Heap Fetches'}, 0, "$description: no heap fetches")
	  if $no_heap;
}

sub evict_vm
{
	$compute->poll_query_until(
		'postgres', qq{
SELECT coalesce(bool_and((pg_buffercache_evict(bufferid)).buffer_evicted), true)
FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode = $locators{vm_heap} AND relforknumber = 2
}) or die 'could not evict the VM';
	is( $compute->safe_psql(
			'postgres', qq{
SELECT count(*) FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode = $locators{vm_heap} AND relforknumber = 2
}),
		'0',
		'VM page really evicted');
}

sub catchup
{
	my $lsn = $primary->lsn('insert');
	$primary->wait_for_catchup($storage, 'replay', $lsn);
	$primary->wait_for_catchup($compute, 'replay', $lsn);
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
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('$name')"
	) or die "could not evict $name";
	my $local = $compute->data_dir . '/' . $paths{$name};
	for my $suffix ('', '_vm')
	{
		next unless -f "$local$suffix";
		rename("$local$suffix", "$local$suffix.held-for-vm-test")
		  or die "could not move $local$suffix aside: $!";
		push @local_files, "$local$suffix";
	}
}
same_rows('baseline with remote VM', 1);

$primary->safe_psql(
	'postgres', q{
UPDATE vm_heap SET payload = repeat('b', current_setting('block_size')::int / 16)
WHERE id = 500;
});
catchup();
same_rows('warm VM clear');
evict_vm();
same_rows('clear survives eviction');

# The VM absent from the seed is created by a heap record, not SMGR_CREATE.
$primary->safe_psql('postgres', 'VACUUM (FREEZE) fresh_vm');
catchup();
is( $compute->safe_psql(
		'postgres', q{
SELECT all_visible AND all_frozen FROM pg_visibility_map('fresh_vm', 0)
}),
	't',
	'implicit VM creation is visible on compute');
my $fresh_vm_file = $compute->data_dir . '/' . $paths{fresh_vm} . '_vm';
ok(!-e $fresh_vm_file, 'implicit VM creation needs no local file');

SKIP:
{
	skip 'Injection points are not available', 6 unless $injection_points;
	$primary->safe_psql('postgres',
		'VACUUM (FREEZE, TRUNCATE false) vm_heap');
	catchup();
	my $old = oracle();
	evict_vm();
	$compute->safe_psql(
		'postgres', q{
SELECT injection_points_attach('test-page-store-after-vm-skip', 'wait')
});
	my $reader = $compute->background_psql('postgres');
	$reader->query_safe(
		q{
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM pg_class;
});
	$primary->safe_psql(
		'postgres', q{
UPDATE vm_heap SET payload = repeat('c', current_setting('block_size')::int / 16)
WHERE id = 1;
});
	wait_event('test-page-store-after-vm-skip');
	$reader->query_until(qr/vm_skip/,
		$ios_settings . "\n\\echo vm_skip\n" . $digest . ";\n");
	wait_event('TestPageStoreReplay');
	wakeup('test-page-store-after-vm-skip');
	is($reader->query_safe(''),
		$old, 'VM miss after skip waits and preserves the older snapshot');
	$reader->query_safe('COMMIT');
	$reader->quit;
	catchup();

	# Reverse admission order: VM I/O has started when redo reaches it.
	$primary->safe_psql('postgres',
		'VACUUM (FREEZE, TRUNCATE false) vm_heap');
	catchup();
	$old = oracle();
	$compute->safe_psql('postgres', $ios_settings . $digest);
	evict_vm();
	$compute->safe_psql(
		'postgres', q{
SELECT injection_points_attach('test-page-store-before-remote-fetch', 'wait')
});
	$reader = $compute->background_psql('postgres');
	$reader->query_until(qr/vm_io/,
		$ios_settings . "\n\\echo vm_io\n" . $digest . ";\n");
	wait_event('test-page-store-before-remote-fetch');
	$primary->safe_psql(
		'postgres', q{
UPDATE vm_heap SET payload = repeat('d', current_setting('block_size')::int / 16)
WHERE id = 2;
});
	wait_event('BufferIo');
	wakeup('test-page-store-before-remote-fetch');
	is($reader->query_safe(''),
		$old, 'admitted VM read completes before redo clears its bits');
	$reader->quit;
	catchup();
	same_rows('after both VM admission orders');
}

# First make every empty tail page visible without truncating.  The second
# VACUUM need only clear the tail, with no FPI to supply the new VM PageLSN.
$primary->safe_psql(
	'postgres', q{
DELETE FROM vm_heap WHERE id > 7 OR id < 3;
VACUUM (FREEZE, TRUNCATE false) vm_heap;
});
catchup();
my $before_lsn = $compute->safe_psql('postgres',
	q{SELECT lsn FROM page_header(get_raw_page('vm_heap', 'vm', 0))});
# pg_relation_size still stats local files; use the writer for the fixture.
my $before_size =
  $primary->safe_psql('postgres', q{SELECT pg_relation_size('vm_heap')});
$primary->safe_psql('postgres',
	'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) vm_heap');
catchup();
cmp_ok($primary->safe_psql('postgres', q{SELECT pg_relation_size('vm_heap')}),
	'<', $before_size, 'heap actually truncated');
is( $compute->safe_psql(
		'postgres', qq{
SELECT lsn > '$before_lsn'::pg_lsn
FROM page_header(get_raw_page('vm_heap', 'vm', 0))
}),
	't',
	'implicit VM tail clear preserves its redo boundary');
evict_vm();
same_rows('after truncation and VM eviction', 1);
$primary->safe_psql(
	'postgres', q{
INSERT INTO vm_heap
  SELECT i, repeat('e', current_setting('block_size')::int / 16)
  FROM generate_series(501, 600) i;
BEGIN;
INSERT INTO vm_heap VALUES (9999, 'aborted');
ROLLBACK;
});
catchup();
same_rows('reextension and aborted insertion');
$primary->safe_psql('postgres', 'VACUUM (FREEZE) vm_heap');
catchup();
evict_vm();
same_rows('vacuum restores IOS', 1);
is( $compute->safe_psql('postgres', "SELECT bt_index_check('vm_idx', true)"),
	'',
	'amcheck with heapallindexed after remote VM scenarios');
ok(!-e $_, "local file $_ was not recreated") for @local_files;
$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
