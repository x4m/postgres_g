# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Keep historical main-fork pages while ordinary standby redo moves on.
# The storage deliberately forgets all views after restart: this is not yet
# a durable page journal.  No request may fall back to a newer local page.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('history_writer');
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
CREATE TABLE history_heap (id int, payload text);
ALTER TABLE history_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO history_heap
  SELECT i, repeat(md5(i::text), 3) FROM generate_series(1, 1000) i;
VACUUM (FREEZE, ANALYZE) history_heap;
SELECT pg_create_physical_replication_slot('history_storage', true);
SELECT pg_create_physical_replication_slot('history_compute', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my $signature_sql = q{
SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
FROM history_heap
};
my $signature = $primary->safe_psql('postgres', $signature_sql);
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my ($spc, $db, $rel) = split '/', $primary->safe_psql(
	'postgres', q{
SELECT (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')::text || '/' ||
       (SELECT oid FROM pg_database WHERE datname = current_database())::text || '/' ||
       pg_relation_filenode('history_heap')::text
});
my $relpath = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('history_heap')");
$primary->backup('history_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('history-cut')");

my $storage = PostgreSQL::Test::Cluster->new('history_storage');
$storage->init_from_backup($primary, 'history_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'history_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 2048
});
$storage->start;
$primary->wait_for_catchup($storage);
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not reach the baseline';
my $cut = $storage->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');

sub fetch_sql
{
	my ($lsn, $block, $count, $fork, $wait) = @_;
	$fork //= 0;
	return
		"test_page_store_fetch($spc, $db, $rel, $fork, $block, $count, "
	  . "'$sysid', $tli, '$lsn'"
	  . ($wait ? ', true' : '') . ')';
}

sub fetch_hash
{
	my ($lsn, $block) = @_;
	return $storage->safe_psql('postgres',
		'SELECT md5(pages) FROM ' . fetch_sql($lsn, $block, 1));
}

sub pause_after_catchup
{
	$primary->wait_for_catchup($storage);
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
	$storage->poll_query_until('postgres',
		"SELECT pg_get_wal_replay_pause_state() = 'paused'")
	  or die 'storage did not pause';
	return $storage->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');
}

sub resume_storage
{
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
}

my ($ret, $out, $err) =
  $storage->psql('postgres', 'SELECT * FROM ' . fetch_sql($cut, 0, 1));
ok( $ret != 0 && $err =~ /page history is not initialized/,
	'history mode never silently falls back before a baseline exists');
is( $storage->safe_psql(
		'postgres', "SELECT test_page_store_retain('history_heap')"),
	$cut,
	'baseline is retained at the exact paused record boundary');
my $old_hash = fetch_hash($cut, 0);
my $old_size = $storage->safe_psql('postgres',
	'SELECT nblocks FROM ' . fetch_sql($cut, 0, 0));
my $old_tail_hash = fetch_hash($cut, $old_size - 1);
($ret, $out, $err) =
  $storage->psql('postgres', 'SELECT * FROM ' . fetch_sql($cut, 0, 0, 1));
ok($ret != 0 && $err =~ /not retained/,
	'unsupported forks fail rather than using present-day metadata');
my $inside_cut = $primary->safe_psql('postgres', "SELECT '$cut'::pg_lsn - 1");
($ret, $out, $err) =
  $storage->psql('postgres', 'SELECT * FROM ' . fetch_sql($inside_cut, 0, 0));
ok( $ret != 0 && $err =~ /record boundary is not retained/,
	'a position older than the baseline is not retained');
($ret, $out, $err) =
  $storage->psql('postgres', "SELECT test_page_store_retain('history_heap')");
ok($ret != 0 && $err =~ /already initialized/,
	'another initialization cannot overwrite a retained view');

my $compute = PostgreSQL::Test::Cluster->new('history_compute');
# An end LSN identifies the boundary before the next record, not its start.
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('after-history-cut')");
$compute->init_from_backup($primary, 'history_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'history_compute'
recovery_target_lsn = '$cut'
recovery_target_inclusive = false
recovery_target_action = 'pause'
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.tablespace = $spc
test_page_store.database = $db
test_page_store.relfilenumber = $rel
max_parallel_workers_per_gather = 0
});
$compute->start;
$compute->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'compute did not reach the baseline';

sub evict_heap
{
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('history_heap')"
	) or die 'could not evict history_heap';
	is( $compute->safe_psql(
			'postgres', qq{
SELECT count(*) FROM pg_buffercache
WHERE reldatabase = $db AND reltablespace = $spc AND relfilenode = $rel
}),
		'0',
		'compute has no cached target buffers');
}

evict_heap();
my $local_file = $compute->data_dir . '/' . $relpath;
my $saved_file = $local_file . '.held-for-history-test';
rename($local_file, $saved_file) or die "could not move $local_file: $!";
ok(!-e $local_file, 'selected compute relation has no local file');

# The larger tuple cannot fit back onto the originally full heap page, so
# this update changes two heap blocks in one WAL record.  Stop after copying
# just one image: the whole new cut must be invisible, the old cut readable.
if ($injection_points)
{
	$storage->safe_psql('postgres',
		"SELECT injection_points_attach('test-page-store-after-history-page', 'wait')"
	);
}
resume_storage();
my ($original_block) = $primary->safe_psql('postgres',
	'SELECT ctid FROM history_heap WHERE id = 1') =~ /^\((\d+),/;
my ($changed_block) = $primary->safe_psql(
	'postgres', q{
UPDATE history_heap
SET payload = repeat('x', current_setting('block_size')::int / 4)
WHERE id = 1 RETURNING ctid
}) =~ /^\((\d+),/;
isnt($changed_block, $original_block,
	'fixture UPDATE really moves the tuple to another heap page');
SKIP:
{
	skip 'Injection points are not available', 5 unless $injection_points;
	$storage->poll_query_until(
		'postgres', q{
SELECT EXISTS (SELECT FROM pg_stat_activity
WHERE backend_type = 'startup' AND wait_event = 'test-page-store-after-history-page')
}) or die 'redo did not reach pre-publication wait';
	is(fetch_hash($cut, 0),
		$old_hash,
		'unpublished page images do not overwrite the previous version');
	is($compute->safe_psql('postgres', $signature_sql),
		$signature,
		'ordinary SQL can read the old cut during multi-page publication');
	my $in_progress = $storage->safe_psql('postgres',
		'SELECT replaying_lsn FROM test_page_store_history_status()');
	($ret, $out, $err) = $storage->psql('postgres',
		'SELECT * FROM ' . fetch_sql($in_progress, 0, 1));
	ok( $ret != 0 && $err =~ /record boundary is not retained/,
		'the exact in-flight WAL record boundary is still unavailable');
	my $waiter = $storage->background_psql('postgres');
	my $wait_query =
	  'SELECT md5(pages) FROM ' . fetch_sql($in_progress, 0, 1, 0, 1);
	$waiter->query_until(qr/wait_started/,
		"\\echo wait_started\n$wait_query;\n");
	ok( $storage->poll_query_until(
			'postgres', q{
SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event = 'TestPageStoreHistory')
}),
		'optional wait blocks until the entire record is published');
	$storage->safe_psql(
		'postgres', q{
SELECT injection_points_detach('test-page-store-after-history-page');
SELECT injection_points_wakeup('test-page-store-after-history-page');
});
	is( $waiter->query_safe(''),
		fetch_hash($in_progress, 0),
		'waiting request returns exactly the newly published version');
	$waiter->quit;
}
my $changed_cut = pause_after_catchup();
isnt(fetch_hash($changed_cut, 0),
	$old_hash, 'later cut contains the changed heap page');
is(fetch_hash($cut, 0), $old_hash,
	'earlier page image survives later replay');
is( $storage->safe_psql(
		'postgres', qq{
SELECT pages >= 2 + $old_size FROM test_page_store_history_status()
}),
	't',
	'multi-page update retained more than one changed page image');

# A normal independent raw-page oracle at the new cut, before checksums are
# enabled, proves that retained images came from real redo rather than SQL.
is( fetch_hash($changed_cut, 0),
	$storage->safe_psql(
		'postgres', qq{
SELECT md5(page) FROM test_page_store_read('history_heap', '$sysid',
  $tli, '$changed_cut', 0)
}),
	'new image matches the independently copied shared-buffer page');
evict_heap();
resume_storage();
is($compute->safe_psql('postgres', $signature_sql),
	$signature,
	'compute still reads its older complete SQL result while storage runs');

# Delete the tail and vacuum it away; later extension must not resurrect old
# images for blocks that existed before the truncation.
$primary->safe_psql(
	'postgres', q{
DELETE FROM history_heap WHERE id > 50 OR id = 1;
VACUUM history_heap;
});
my $small_cut = pause_after_catchup();
my $small_size = $storage->safe_psql('postgres',
	'SELECT nblocks FROM ' . fetch_sql($small_cut, 0, 0));
cmp_ok($small_size, '<', $old_size,
	'VACUUM actually truncated the main fork');
is( $storage->safe_psql(
		'postgres', 'SELECT nblocks FROM ' . fetch_sql($cut, 0, 0)),
	$old_size,
	'old cut retains its pre-truncation size');
is(fetch_hash($cut, $old_size - 1),
	$old_tail_hash, 'old tail remains readable after truncation');
resume_storage();
$primary->safe_psql(
	'postgres', q{
INSERT INTO history_heap
  SELECT i, repeat(md5((-i)::text), 3) FROM generate_series(1001, 1800) i;
});
my $extended_cut = pause_after_catchup();
my $extended_size = $storage->safe_psql('postgres',
	'SELECT nblocks FROM ' . fetch_sql($extended_cut, 0, 0));
cmp_ok($extended_size, '>', $small_size,
	'fixture reuses truncated block numbers');
isnt(
	fetch_hash($extended_cut, $small_size),
	fetch_hash($cut, $small_size),
	'reextended block has new contents, not its pre-truncation image');

resume_storage();
$primary->safe_psql('postgres', 'DROP TABLE history_heap');
my $dropped_cut = pause_after_catchup();
is( $storage->safe_psql(
		'postgres',
		'SELECT NOT fork_exists AND nblocks = 0 FROM '
		  . fetch_sql($dropped_cut, 0, 0)),
	't',
	'drop is represented independently of delayed physical unlink');
is(fetch_hash($cut, 0), $old_hash, 'drop does not discard an older view');
evict_heap();
is($compute->safe_psql('postgres', $signature_sql),
	$signature, 'old compute reads all rows after storage has replayed DROP');

# Restart is an explicit view loss.  Do not accidentally satisfy the request
# from whatever relation happens to occupy the physical address afterward.
$storage->stop('immediate');
$storage->append_conf('postgresql.conf',
	"test_page_store.history_pages = 2\n");
$storage->start;
($ret, $out, $err) =
  $storage->psql('postgres', 'SELECT * FROM ' . fetch_sql($cut, 0, 1));
ok( $ret != 0 && $err =~ /page history is not initialized/,
	'restart revokes volatile history rather than serving current pages');
evict_heap();
($ret, $out, $err) = $compute->psql('postgres', $signature_sql);
ok( $ret != 0 && $err =~ /page history is not initialized/,
	'compute fails closed after the retained view is lost');

rename($saved_file, $local_file) or die "could not restore $local_file: $!";
$compute->stop;

# A very small new history must keep old views when it runs out of space.
$primary->safe_psql(
	'postgres', q{
CREATE TABLE bounded_heap (id int);
INSERT INTO bounded_heap VALUES (1);
});
$cut = pause_after_catchup();
$rel = $primary->safe_psql('postgres',
	"SELECT pg_relation_filenode('bounded_heap')");
is( $storage->safe_psql(
		'postgres', "SELECT test_page_store_retain('bounded_heap')"),
	$cut,
	'new startup can establish a different bounded history');
$old_hash = fetch_hash($cut, 0);
resume_storage();
$primary->safe_psql(
	'postgres', q{
UPDATE bounded_heap SET id = 2;
UPDATE bounded_heap SET id = 3;
});
my $exhausted_cut = pause_after_catchup();
is( $storage->safe_psql(
		'postgres', q{
SELECT NOT accepting AND stop_reason = 'page capacity exhausted'
FROM test_page_store_history_status()
}),
	't',
	'capacity exhaustion stops publication without stopping ordinary redo');
is(fetch_hash($cut, 0),
	$old_hash,
	'capacity exhaustion does not overwrite previously retained images');
($ret, $out, $err) = $storage->psql('postgres',
	'SELECT * FROM ' . fetch_sql($exhausted_cut, 0, 1));
ok( $ret != 0
	  && $err =~ /record boundary is not retained/
	  && $err =~ /page capacity exhausted/,
	'an unretained new cut reports capacity exhaustion, not latest-page data'
);
$storage->stop;
$primary->stop;
done_testing();
