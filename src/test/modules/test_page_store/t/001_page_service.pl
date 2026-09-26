# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Exercise the frozen-replay page contract and show why crash-equivalent
# redo is not sufficient for reading back a live compute's evicted pages.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('compute');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq(
autovacuum = off
checkpoint_timeout = '1h'
# Exercise heap redo, not a full-page image containing the original cmin.
# This is a diagnostic test configuration, not a deployment recommendation.
full_page_writes = off
));
$primary->start;
$primary->safe_psql('postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pageinspect;
CREATE TABLE page_test (id integer PRIMARY KEY, payload text);
INSERT INTO page_test VALUES (0, 'seed');
CREATE SEQUENCE seq_test CACHE 1;
CREATE ROLE page_reader;
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
$primary->backup('seed');

my $storage = PostgreSQL::Test::Cluster->new('storage');
$storage->init_from_backup($primary, 'seed', has_streaming => 1);
$storage->start;

my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');

sub replay_through
{
	my ($lsn) = @_;
	$storage->poll_query_until('postgres',
		"SELECT pg_last_wal_replay_lsn() >= '$lsn'::pg_lsn")
	  or die "storage did not replay through $lsn";
}

sub pause_storage
{
	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
	$storage->poll_query_until('postgres',
		"SELECT pg_get_wal_replay_pause_state() = 'paused'")
	  or die 'storage did not pause';
	return $storage->safe_psql('postgres',
		'SELECT pg_last_wal_replay_lsn()');
}

sub rejected
{
	my ($node, $sql, $pattern, $name) = @_;
	my ($ret, $out, $err) = $node->psql('postgres', $sql);
	ok($ret != 0 && $err =~ $pattern, $name)
	  or diag("ret=$ret, out=$out, err=$err");
}

# Keep the inserting transaction alive while the ordinary standby replays it.
my $writer = $primary->background_psql('postgres');
$writer->query_safe(q{
BEGIN;
INSERT INTO page_test VALUES (1, 'first command');
INSERT INTO page_test VALUES (2, 'second command');
});
my $tid = $writer->query_safe('SELECT ctid FROM page_test WHERE id = 2');
my ($blk, $off) = $tid =~ /^\((\d+),(\d+)\)$/;
defined($off) or die "unexpected TID: $tid";
my $cmin = $writer->query_safe('SELECT cmin::text FROM page_test WHERE id = 2');
cmp_ok($cmin, '>', 0, 'fixture has a nonzero live command ID');

is($primary->safe_psql('postgres', "SELECT nextval('seq_test')"),
	'1', 'fixture allocated the first sequence value');
my $needed = $primary->safe_psql('postgres',
	'SELECT pg_current_wal_insert_lsn()');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
replay_through($needed);
my $cut = pause_storage();

sub read_sql
{
	my ($id, $timeline, $lsn, $block) = @_;
	return "test_page_store_read('page_test', '$id', $timeline, '$lsn', $block)";
}

my $read = read_sql($sysid, $tli, $cut, $blk);
is($storage->safe_psql('postgres',
	"SELECT octet_length(page) FROM $read"),
	$storage->safe_psql('postgres', 'SHOW block_size'),
	'page service returns a complete block');
is($storage->safe_psql('postgres', qq{
SELECT page = get_raw_page('page_test', $blk) FROM $read
}), 't', 'page service returns the actual replayed page');
is($storage->safe_psql('postgres', qq{
SELECT tablespace = (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND relfilenumber = pg_relation_filenode('page_test')
  AND nblocks = pg_relation_size('page_test') / current_setting('block_size')::int
FROM $read
}), 't', 'physical identity and size accompany the page at the same cut');

my $replayed_cmin = $storage->safe_psql('postgres', qq{
SELECT t_field3 FROM $read, LATERAL heap_page_items(page) WHERE lp = $off
});
is($replayed_cmin, '0', 'ordinary heap redo replaces the command ID');
isnt($replayed_cmin, $cmin,
	'replayed page is not an exact replacement for the live compute page');
is($storage->safe_psql('postgres', 'SELECT count(*) FROM page_test'),
	'1', 'replayed uncommitted tuples are correctly hidden from standby SQL');
is($primary->safe_psql('postgres', 'SELECT last_value FROM seq_test'),
	'1', 'live sequence page retains the current value');
is($storage->safe_psql('postgres', 'SELECT last_value FROM seq_test'),
	'33', 'sequence redo contains the prelogged future value');

rejected($primary, "SELECT * FROM $read", qr/requires a standby/,
	'primary cannot serve a frozen recovery cut');
rejected($storage,
	'SELECT * FROM ' . read_sql('0', $tli, $cut, $blk),
	qr/system identifier does not match/, 'wrong cluster identity is rejected');
rejected($storage,
	'SELECT * FROM ' . read_sql($sysid, $tli + 1, $cut, $blk),
	qr/requested replay cut is not available/, 'wrong timeline is rejected');
rejected($storage,
	'SELECT * FROM ' . read_sql($sysid, $tli, '0/1', $blk),
	qr/requested replay cut is not available/, 'different replay LSN is rejected');
rejected($storage,
	'SELECT * FROM ' . read_sql($sysid, $tli, $cut, -1),
	qr/invalid block number/, 'negative block number is rejected');
rejected($storage,
	'SELECT * FROM ' . read_sql($sysid, $tli, $cut, 100000),
	qr/outside the relation/, 'missing block is not returned as a zero page');
rejected($storage, "SET ROLE page_reader; SELECT * FROM $read",
	qr/permission denied/, 'raw page access is not granted to ordinary users');

$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'not paused'")
	  or die 'storage did not resume';
rejected($storage, "SELECT * FROM $read", qr/requires paused recovery/,
	'unfrozen storage cannot silently serve latest');

$writer->query_safe('COMMIT');
$writer->quit;
$primary->safe_psql('postgres',
	"INSERT INTO page_test VALUES (3, 'after original cut')");
$needed = $primary->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
replay_through($needed);
my $new_cut = pause_storage();
isnt($new_cut, $cut, 'second frozen cut is different');
rejected($storage, "SELECT * FROM $read", qr/requested replay cut is not available/,
	'old read token is rejected after replay advances');
$read = read_sql($sysid, $tli, $new_cut, $blk);
is($storage->safe_psql('postgres',
	"SELECT count(*) FROM $read, LATERAL heap_page_items(page) WHERE lp_flags = 1"),
	'4', 'new cut returns the newly replayed contents');

SKIP:
{
	skip 'Injection points are not available', 2 unless $injection_points;

	# A content lock protects the copy, not the surrounding replay view.
	# Resume and pause recovery while a request is between the two checks.
	$storage->safe_psql('postgres',
		"SELECT injection_points_attach('test-page-store-after-read', 'wait')");
	my $reader = $storage->background_psql('postgres', on_error_stop => 0);
	my $pid = $reader->query_safe('SELECT pg_backend_pid()');
	$reader->query_until(qr/read_started/, qq{
\\echo read_started
SELECT octet_length(page) FROM $read;
});
	$storage->poll_query_until('postgres', qq{
SELECT wait_event = 'test-page-store-after-read'
FROM pg_stat_activity WHERE pid = $pid
}) or die 'page reader did not reach the after-read wait';

	$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
	$primary->safe_psql('postgres',
		"INSERT INTO page_test VALUES (4, 'during page read')");
	$needed = $primary->safe_psql('postgres',
		'SELECT pg_current_wal_insert_lsn()');
	$primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
	replay_through($needed);
	my $later_cut = pause_storage();
	isnt($later_cut, $new_cut, 'replay advanced during the page request');
	$storage->safe_psql('postgres',
		"SELECT injection_points_wakeup('test-page-store-after-read')");
	my ($out, $ret) = $reader->query('');
	ok($ret != 0 && $reader->{stderr} =~ /requested replay cut is not available/,
		'page read is rejected when the cut changes during the request')
	  or diag("ret=$ret, out=$out, err=$reader->{stderr}");
	$reader->quit;
	$storage->safe_psql('postgres',
		"SELECT injection_points_detach('test-page-store-after-read')");
}

$storage->promote;
rejected($storage, "SELECT * FROM $read", qr/requires a standby/,
	'promotion invalidates the page-service view');

$storage->stop;
$primary->stop;
done_testing();
