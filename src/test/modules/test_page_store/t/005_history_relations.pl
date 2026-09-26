# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Retain several physical relations at one cut.  Identical block numbers
# must not alias, and a multi-relation DROP must publish one complete cut.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('relations_writer');
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
CREATE TABLE history_a (id int PRIMARY KEY, payload text);
CREATE TABLE history_b (id int, payload text);
ALTER TABLE history_a ALTER COLUMN payload SET STORAGE PLAIN;
ALTER TABLE history_b ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO history_a
  SELECT i, repeat(md5(i::text), 16) FROM generate_series(1, 100) i;
INSERT INTO history_b
  SELECT i, repeat(md5((-i)::text), 16) FROM generate_series(1, 200) i;
VACUUM (FREEZE) history_a;
VACUUM (FREEZE) history_b;
SELECT pg_create_physical_replication_slot('relations_storage', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $spc = $primary->safe_psql('postgres',
	q{SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'});
my $db = $primary->safe_psql('postgres',
	q{SELECT oid FROM pg_database WHERE datname = current_database()});
my @names = qw(history_a history_b history_a_pkey);
my %locators = map {
	$_ => $primary->safe_psql('postgres', "SELECT pg_relation_filenode('$_')")
} @names;
$primary->backup('relations_seed');

my $storage = PostgreSQL::Test::Cluster->new('relations_storage');
$storage->init_from_backup($primary, 'relations_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'relations_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 1024
});
$storage->start;

sub pause_after_catchup
{
	$primary->wait_for_catchup($storage);
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
	$storage->poll_query_until('postgres',
		"SELECT pg_get_wal_replay_pause_state() = 'paused'")
	  or die 'storage did not pause';
	return $storage->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');
}

sub fetch_sql
{
	my ($name, $lsn, $block, $count) = @_;
	return "test_page_store_fetch($spc, $db, $locators{$name}, 0, "
	  . "$block, $count, '$sysid', $tli, '$lsn')";
}

sub image_hash
{
	my ($name, $lsn) = @_;
	my $size = $storage->safe_psql('postgres',
		'SELECT nblocks FROM ' . fetch_sql($name, $lsn, 0, 0));
	die "fixture must fit in a single batch: $name has $size blocks"
	  if $size < 1 || $size > 64;
	return $storage->safe_psql('postgres',
		'SELECT md5(pages) FROM ' . fetch_sql($name, $lsn, 0, $size));
}

sub check_raw_images
{
	my ($name, $lsn, $description) = @_;
	my $size = $storage->safe_psql('postgres',
		'SELECT nblocks FROM ' . fetch_sql($name, $lsn, 0, 0));
	is( image_hash($name, $lsn),
		$storage->safe_psql(
			'postgres', qq{
SELECT md5(string_agg(page, ''::bytea ORDER BY block))
FROM generate_series(0, $size - 1) block,
     LATERAL test_page_store_read('$name', '$sysid', $tli, '$lsn', block)
}),
		"$description: $name");
}

my $cut = pause_after_catchup();
for my $case (
	[ q{ARRAY[]::regclass[]}, qr/between 1 and 16/, 'empty registry' ],
	[ q{ARRAY[NULL]::regclass[]}, qr/null relation/, 'null relation' ],
	[
		q{ARRAY['history_a','history_a']::regclass[]},
		qr/duplicate relation/,
		'duplicate locator'
	])
{
	my ($ret, $out, $err) = $storage->psql('postgres',
		"SELECT test_page_store_retain_relations($case->[0])");
	ok($ret != 0 && $err =~ $case->[1], "$case->[2] is rejected");
}
is( $storage->safe_psql(
		'postgres', q{
SELECT test_page_store_retain_relations(
  ARRAY['history_a','history_b','history_a_pkey']::regclass[])
}),
	$cut,
	'all selected main forks share the same baseline');
my %baseline = map { $_ => image_hash($_, $cut) } @names;
for my $name (@names)
{
	check_raw_images($name, $cut,
		'baseline matches ordinary standby buffers');
}
isnt($baseline{history_a}, $baseline{history_b},
	'fixture relations have different contents');
my ($ret, $out, $err) = $storage->psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['history_b']::regclass[])
});
ok( $ret != 0 && $err =~ /already initialized/,
	'a live registry cannot be replaced');

$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->safe_psql(
	'postgres', q{
BEGIN;
UPDATE history_a SET id = id + 1000, payload = repeat('a', 512) WHERE id = 1;
UPDATE history_b SET payload = repeat('b', 512) WHERE id = 1;
COMMIT;
});
my $changed_cut = pause_after_catchup();
for my $name (@names)
{
	isnt(image_hash($name, $changed_cut),
		$baseline{$name}, "redo changed $name at the later cut");
	check_raw_images($name, $changed_cut, 'new images match ordinary redo');
	is(image_hash($name, $cut),
		$baseline{$name},
		"old $name survives changes to all registered relations");
}

# Truncation is per physical file, not a global high-water mark.  The
# unchanged relation must keep both its size and its exact page contents.
my $b_hash = image_hash('history_b', $changed_cut);
my $a_size = $storage->safe_psql('postgres',
	'SELECT nblocks FROM ' . fetch_sql('history_a', $changed_cut, 0, 0));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->safe_psql(
	'postgres', q{
DELETE FROM history_a;
VACUUM history_a;
});
my $small_cut = pause_after_catchup();
cmp_ok(
	$storage->safe_psql(
		'postgres',
		'SELECT nblocks FROM ' . fetch_sql('history_a', $small_cut, 0, 0)),
	'<', $a_size,
	'VACUUM truncated only the selected heap');
is(image_hash('history_b', $small_cut),
	$b_hash,
	'truncating another relation does not change the retained page range');
is(image_hash('history_a', $cut),
	$baseline{history_a}, 'pre-truncation view still has the original heap');

# A single commit drops both heaps and the index.  Stop after recording the
# first deletion, before the others: no part of this new cut may be served.
if ($injection_points)
{
	$storage->safe_psql('postgres',
		"SELECT injection_points_attach('test-page-store-after-history-drop', 'wait')"
	);
}
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->safe_psql('postgres', 'DROP TABLE history_a, history_b');
SKIP:
{
	skip 'Injection points are not available', 5 unless $injection_points;
	$storage->poll_query_until(
		'postgres', q{
SELECT EXISTS (SELECT FROM pg_stat_activity
WHERE backend_type = 'startup' AND wait_event = 'test-page-store-after-history-drop')
}) or die 'redo did not reach partial metadata publication';
	my $in_progress = $storage->safe_psql('postgres',
		'SELECT replaying_lsn FROM test_page_store_history_status()');
	for my $name (@names)
	{
		is(image_hash($name, $cut),
			$baseline{$name},
			"old $name remains readable during multi-relation DROP");
	}
	for my $name (qw(history_a history_b))
	{
		($ret, $out, $err) = $storage->psql('postgres',
			'SELECT * FROM ' . fetch_sql($name, $in_progress, 0, 0));
		ok( $ret != 0 && $err =~ /record boundary is not retained/,
			"partially collected cut is unavailable for $name");
	}
	$storage->safe_psql(
		'postgres', q{
SELECT injection_points_detach('test-page-store-after-history-drop');
SELECT injection_points_wakeup('test-page-store-after-history-drop');
});
}
my $dropped_cut = pause_after_catchup();
for my $name (@names)
{
	is( $storage->safe_psql(
			'postgres',
			'SELECT NOT fork_exists AND nblocks = 0 FROM '
			  . fetch_sql($name, $dropped_cut, 0, 0)),
		't',
		"complete DROP cut marks $name absent");
	is(image_hash($name, $cut),
		$baseline{$name}, "DROP keeps the baseline images of $name");
}
$storage->stop;
$primary->stop;
done_testing();
