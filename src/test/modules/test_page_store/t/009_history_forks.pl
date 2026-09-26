# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Retain the visibility map as well as heap pages.  In particular,
# truncation's tail-bit clearing has no WAL block reference of its own.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('fork_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = off
wal_log_hints = off
wal_consistency_checking = 'heap,heap2'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pageinspect;
CREATE EXTENSION pg_visibility;
CREATE TABLE fork_heap (id int, payload text);
ALTER TABLE fork_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO fork_heap
  SELECT i, repeat('a', current_setting('block_size')::int / 16)
  FROM generate_series(1, 500) i;
VACUUM (FREEZE, ANALYZE) fork_heap;
CREATE TABLE new_vm (id int);
INSERT INTO new_vm VALUES (1);
SELECT pg_create_physical_replication_slot('fork_storage', true);
});
my $spc = $primary->safe_psql('postgres',
	q{SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'});
my $db = $primary->safe_psql('postgres',
	q{SELECT oid FROM pg_database WHERE datname = current_database()});
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my %locators = map {
	$_ => $primary->safe_psql('postgres', "SELECT pg_relation_filenode('$_')")
} qw(fork_heap new_vm);
$primary->backup('fork_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('fork-baseline')");

my $storage = PostgreSQL::Test::Cluster->new('fork_storage');
$storage->init_from_backup($primary, 'fork_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'fork_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 8192
test_page_store.history_durable = true
fsync = on
});
$storage->start;

sub paused_cut
{
	# VACUUM can leave its WAL in buffers after returning.  Waiting for the
	# current write position could pause replay before the VM changes.
	$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
	$storage->poll_query_until('postgres',
		"SELECT pg_get_wal_replay_pause_state() = 'paused'")
	  or die 'storage did not pause';
	return $storage->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');
}

sub fetch_sql
{
	my ($name, $fork, $lsn, $count) = @_;
	return
	  "test_page_store_fetch($spc, $db, $locators{$name}, $fork, 0, $count, "
	  . "'$sysid', $tli, '$lsn')";
}

sub page_hash
{
	my ($name, $fork, $lsn) = @_;
	return $storage->safe_psql('postgres',
		'SELECT md5(pages) FROM ' . fetch_sql($name, $fork, $lsn, 1));
}

sub check_page
{
	my ($name, $fork, $lsn, $description) = @_;
	my $fork_name = $fork == 0 ? 'main' : 'vm';
	my $expected = $storage->safe_psql('postgres',
		"SELECT md5(get_raw_page('$name', '$fork_name', 0))");
	is(page_hash($name, $fork, $lsn), $expected, $description);
	return $expected;
}

my $baseline = paused_cut();
is( $storage->safe_psql(
		'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['fork_heap','new_vm']::regclass[])
}),
	$baseline,
	'main and VM forks share one published baseline');
my $main_hash = check_page('fork_heap', 0, $baseline,
	'heap block zero matches the baseline');
my $vm_hash = check_page('fork_heap', 2, $baseline,
	'VM block zero is distinct from heap block zero');
is( $storage->safe_psql(
		'postgres',
		'SELECT NOT fork_exists AND nblocks = 0 FROM '
		  . fetch_sql('new_vm', 2, $baseline, 0)),
	't',
	'an absent VM has its own metadata, not the main fork size');
my $old_size = $storage->safe_psql('postgres',
	'SELECT nblocks FROM ' . fetch_sql('fork_heap', 0, $baseline, 0));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

$primary->safe_psql(
	'postgres', q{
UPDATE fork_heap
SET payload = repeat('b', current_setting('block_size')::int / 4)
WHERE id = 500;
});
my $cleared = paused_cut();
my $cleared_hash =
  check_page('fork_heap', 2, $cleared, 'WAL-logged VM clear is captured');
isnt($cleared_hash, $vm_hash, 'clearing VM changes the retained image');
is(page_hash('fork_heap', 2, $baseline),
	$vm_hash, 'the earlier all-visible map remains unchanged');
is(page_hash('fork_heap', 0, $baseline),
	$main_hash, 'VM images do not replace historical heap images');
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

$primary->safe_psql(
	'postgres', q{
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) fork_heap;
VACUUM (FREEZE) new_vm;
});
my $visible = paused_cut();
check_page('fork_heap', 2, $visible, 'setting VM bits is captured');
my $created_hash = check_page('new_vm', 2, $visible,
	'VM first created after baseline is retained');
is( $storage->safe_psql(
		'postgres',
		'SELECT NOT fork_exists FROM ' . fetch_sql('new_vm', 2, $baseline, 0)
	),
	't',
	'the earlier absent VM does not become present retroactively');
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

# Vacuum marks empty tail pages visible, then truncates the heap and clears
# their bits without reducing the one-page VM file or registering its block.
# Checksums and wal_log_hints are off so an FPI cannot hide a missing capture.
$primary->safe_psql(
	'postgres', q{
DELETE FROM fork_heap WHERE id > 7;
VACUUM (DISABLE_PAGE_SKIPPING) fork_heap;
});
my $truncated = paused_cut();
my $new_size = $storage->safe_psql('postgres',
	'SELECT nblocks FROM ' . fetch_sql('fork_heap', 0, $truncated, 0));
cmp_ok($new_size, '<', $old_size, 'fixture actually truncates the heap');
is( $storage->safe_psql(
		'postgres',
		'SELECT nblocks FROM ' . fetch_sql('fork_heap', 2, $truncated, 0)),
	'1',
	'VM truncation clears tail bits without changing the file size');
my $truncated_hash = check_page('fork_heap', 2, $truncated,
	'truncation records the changed VM page despite having no block reference'
);
is(page_hash('fork_heap', 2, $cleared),
	$cleared_hash, 'truncation preserves older cleared-bit history');
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

$primary->safe_psql(
	'postgres', q{
INSERT INTO fork_heap
  SELECT i, repeat('c', current_setting('block_size')::int / 16)
  FROM generate_series(501, 700) i;
});
my $grown = paused_cut();
check_page('fork_heap', 2, $grown,
	'reextension cannot resurrect the truncated VM tail');
is( $storage->safe_psql(
		'postgres', qq{
SELECT count(*) FROM pg_visibility_map('fork_heap')
WHERE blkno >= $new_size AND all_visible
}),
	'0',
	'reextended heap pages are not prematurely all-visible');
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

$primary->safe_psql('postgres', 'DROP TABLE fork_heap');
my $dropped = paused_cut();
for my $fork (0, 2)
{
	is( $storage->safe_psql(
			'postgres',
			'SELECT NOT fork_exists AND nblocks = 0 FROM '
			  . fetch_sql('fork_heap', $fork, $dropped, 0)),
		't',
		"DROP removes fork $fork at the same cut");
}
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

$storage->stop('immediate');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('fork-restart')");
$storage->start;
$primary->wait_for_catchup($storage);
for my $case (
	[ 0, $baseline, $main_hash, 'heap baseline' ],
	[ 2, $baseline, $vm_hash, 'VM baseline' ],
	[ 2, $cleared, $cleared_hash, 'cleared VM' ],
	[ 2, $truncated, $truncated_hash, 'truncated VM' ])
{
	is(page_hash('fork_heap', $case->[0], $case->[1]),
		$case->[2], "restart preserves $case->[3] after DROP");
}
is(page_hash('new_vm', 2, $visible),
	$created_hash,
	'restart preserves the other relation\'s newly created VM');
my ($ret, $out, $err) = $storage->psql('postgres',
	'SELECT * FROM ' . fetch_sql('new_vm', 1, $visible, 0));
ok($ret != 0 && $err =~ /not retained/,
	'FSM remains explicitly unsupported, not silently served as VM');
$storage->stop;
$primary->stop;
done_testing();
