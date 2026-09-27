# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Two following computes, with different resident pages and replay positions,
# read multiple heap main forks without local files.  Other state stays local.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('multi_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = off
wal_consistency_checking = 'heap,heap2'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE TABLE multi_a (id int, payload text);
CREATE TABLE multi_b (id int, payload text);
ALTER TABLE multi_a ALTER COLUMN payload SET STORAGE PLAIN;
ALTER TABLE multi_b ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO multi_a
  SELECT i, repeat(md5(i::text), 16) FROM generate_series(1, 100) i;
INSERT INTO multi_b
  SELECT i, repeat(md5((-i)::text), 3) FROM generate_series(1, 300) i;
VACUUM (FREEZE, ANALYZE) multi_a;
VACUUM (FREEZE, ANALYZE) multi_b;
SELECT pg_create_physical_replication_slot('multi_storage', true);
SELECT pg_create_physical_replication_slot('multi_first', true);
SELECT pg_create_physical_replication_slot('multi_second', true);
});
my @names = qw(multi_a multi_b);
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
my %signature_sql = map {
	$_ =>
	  "SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload, "
	  . "',' ORDER BY id, ctid)) FROM $_"
} @names;
$primary->backup('multi_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('multi-baseline')");

my $storage = PostgreSQL::Test::Cluster->new('multi_storage');
$storage->init_from_backup($primary, 'multi_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'multi_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 4096
});
$storage->start;
$primary->wait_for_catchup($storage);
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not reach the baseline';
my $cut = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['multi_a','multi_b']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

my @computes;
my @local_files;
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
for my $name (qw(multi_first multi_second))
{
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init_from_backup($primary, 'multi_seed', has_streaming => 1);
	$node->append_conf(
		'postgresql.conf', qq{
primary_slot_name = '$name'
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
	$node->start;
	$primary->wait_for_catchup($node);
	is( $node->safe_psql(
			'postgres', 'SELECT active FROM test_page_store_compute_status()'
		),
		't',
		"$name activates at the common baseline");
	push @computes, $node;
}

sub evict
{
	my ($node, $name) = @_;
	$node->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('$name')"
	) or die "could not evict $name";
	is( $node->safe_psql(
			'postgres', qq{
SELECT count(*) FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc
AND relfilenode = $locators{$name} AND relforknumber = 0
}),
		'0',
		$node->name . " has no cached $name main pages");
}

sub status
{
	my ($node, $field) = @_;
	return $node->safe_psql('postgres',
		"SELECT $field FROM test_page_store_compute_status()");
}

sub same_rows
{
	my ($node, $description) = @_;
	for my $name (@names)
	{
		is( $node->safe_psql('postgres', $signature_sql{$name}),
			$primary->safe_psql('postgres', $signature_sql{$name}),
			$node->name . ": $description: $name");
	}
}

sub catchup
{
	$primary->wait_for_catchup($storage);
	$primary->wait_for_catchup($_) for @computes;
}

for my $node (@computes)
{
	for my $name (@names)
	{
		evict($node, $name);
		my $local = $node->data_dir . '/' . $paths{$name};
		rename($local, "$local.held-for-multi-test")
		  or die "could not move $local aside: $!";
		push @local_files, $local;
		ok(!-e $local, $node->name . " has no local $name main file");
	}
	same_rows($node, 'initial cold reads match all values and TIDs');
}

# The first compute has only B cached; the second has only A.  Both must
# get the same results despite taking opposite redo paths for these heaps.
evict($computes[0], 'multi_a');
evict($computes[1], 'multi_b');
my @before =
  map { [ status($_, 'skipped'), status($_, 'cached') ] } @computes;
$primary->safe_psql(
	'postgres', q{
BEGIN;
UPDATE multi_a SET payload = 'changed a' WHERE id = 1;
UPDATE multi_b SET payload = 'changed b' WHERE id = 1;
COMMIT;
});
catchup();
for my $i (0 .. $#computes)
{
	my $node = $computes[$i];
	cmp_ok(status($node, 'skipped'),
		'>', $before[$i][0], $node->name . ' skipped cold redo');
	cmp_ok(status($node, 'cached'),
		'>', $before[$i][1], $node->name . ' applied warm redo');
	is(status($node, 'startup_fetches'),
		'0', $node->name . ' needed no startup page fetch');
	same_rows($node, 'mixed residency preserves results');
}

# Sizes and extension are tracked per locator, including when the two
# relations have the same block numbers but different EOF positions.
$primary->safe_psql(
	'postgres', q{
INSERT INTO multi_a
  SELECT i, repeat('x', 512) FROM generate_series(1001, 1050) i;
INSERT INTO multi_b
  SELECT i, repeat('y', 96) FROM generate_series(301, 600) i;
});
catchup();
same_rows($_, 'independent extension preserves results') for @computes;

# Let one compute lag with a completely cold cache.  It must fetch its old
# pages, while the other compute reads the newer committed values.
my %old =
  map { $_ => $computes[0]->safe_psql('postgres', $signature_sql{$_}) }
  @names;
$computes[0]->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$computes[0]->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'first compute did not pause';
evict($computes[0], $_) for @names;
$primary->safe_psql(
	'postgres', q{
BEGIN;
UPDATE multi_a SET payload = 'new a' WHERE id = 2;
UPDATE multi_b SET payload = 'new b' WHERE id = 2;
COMMIT;
});
$primary->wait_for_catchup($storage);
$primary->wait_for_catchup($computes[1]);
same_rows($computes[1], 'advanced compute sees the new transaction');
for my $name (@names)
{
	is( $computes[0]->safe_psql('postgres', $signature_sql{$name}),
		$old{$name},
		"lagging compute fetches old $name after newer replay on storage");
}
$computes[0]->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
catchup();
same_rows($computes[0], 'resumed compute updates its older cached pages');

$primary->safe_psql(
	'postgres', q{
DELETE FROM multi_a;
VACUUM multi_a;
INSERT INTO multi_a VALUES (2000, 'after truncation');
});
catchup();
for my $node (@computes)
{
	evict($node, $_) for @names;
	same_rows($node,
		'truncation and reextension are confined to one locator');
}
ok(!-e $_, "remote reads and redo did not recreate $_") for @local_files;
$_->stop for @computes;

# Invalid lists must fail during configuration, before recovery can use
# local files or partially configure the selection.  Use a fresh cluster
# without DB_SHUTDOWNED_IN_RECOVERY in pg_control: pg_ctl would otherwise
# mistake this early failure for an intentional recovery-target shutdown.
my $invalid_node = PostgreSQL::Test::Cluster->new('invalid_locators');
$invalid_node->init;
$invalid_node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_page_store'\n");
for my $case (
	[ '1663/5', qr/invalid remote physical locator/, 'incomplete locator' ],
	[ '1663/5/4294967296', qr/out of range/, 'overflowed relfilenumber' ],
	[
		'1663/5/16384,1663/5/16384',
		qr/duplicate remote physical locator/,
		'duplicate locator'
	])
{
	my $log_start = -s $invalid_node->logfile || 0;
	$invalid_node->append_conf('postgresql.conf',
		"test_page_store.locators = '$case->[0]'\n");
	ok(!$invalid_node->start(fail_ok => 1), "$case->[2] prevents startup");
	ok( $invalid_node->log_contains($case->[1], $log_start),
		"$case->[2] reports its configuration error");
}
$storage->stop;
$primary->stop;
done_testing();
