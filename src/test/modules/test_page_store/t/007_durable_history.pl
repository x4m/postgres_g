# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Published historical cuts survive storage restart.  A crash during append
# must not expose a partial record or overwrite the older compute's pages.
use strict;
use warnings FATAL => 'all';

use Fcntl qw(SEEK_END);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('journal_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
wal_consistency_checking = 'heap,heap2'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION pageinspect;
CREATE TABLE journal_heap (id int, payload text);
CREATE TABLE journal_other (id int);
INSERT INTO journal_other VALUES (42);
ALTER TABLE journal_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO journal_heap
  SELECT i, repeat(md5(i::text), 3) FROM generate_series(1, 100) i;
VACUUM (FREEZE) journal_heap;
SELECT pg_create_physical_replication_slot('journal_storage', true);
SELECT pg_create_physical_replication_slot('journal_compute', true);
});
my $injection_points = check_pg_config('#define USE_INJECTION_POINTS 1')
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection_points;
my $signature_sql = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM journal_heap
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
       pg_relation_filenode('journal_heap')::text
});
my $relpath = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('journal_heap')");
my $other_rel = $primary->safe_psql('postgres',
	"SELECT pg_relation_filenode('journal_other')");
$primary->backup('journal_seed');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('journal-baseline')");

my $storage = PostgreSQL::Test::Cluster->new('journal_storage');
$storage->init_from_backup($primary, 'journal_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
fsync = on
primary_slot_name = 'journal_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 4096
test_page_store.history_durable = true
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
	my ($lsn, $count, $which_rel) = @_;
	$which_rel //= $rel;
	return "test_page_store_fetch($spc, $db, $which_rel, 0, 0, $count, "
	  . "'$sysid', $tli, '$lsn')";
}

sub page_hash
{
	my ($lsn) = @_;
	return $storage->safe_psql('postgres',
		'SELECT md5(pages) FROM ' . fetch_sql($lsn, 1));
}

my $cut = pause_after_catchup();
is( $storage->safe_psql(
		'postgres', q{SELECT test_page_store_retain_relations(
            ARRAY['journal_heap','journal_other']::regclass[])}),
	$cut,
	'durable baseline is retained at the paused cut');
my $baseline_hash = page_hash($cut);
my $other_hash = $storage->safe_psql('postgres',
	'SELECT md5(pages) FROM ' . fetch_sql($cut, 1, $other_rel));
my $journal = $storage->data_dir . '/test_page_store.history';
ok(-s $journal, 'baseline has a persistent journal');
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('after-journal-baseline')");

my $compute = PostgreSQL::Test::Cluster->new('journal_compute');
$compute->init_from_backup($primary, 'journal_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'journal_compute'
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
  or die 'compute did not reach its frozen cut';

sub evict_compute
{
	$compute->poll_query_until('postgres',
		"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('journal_heap')"
	) or die 'could not evict journal_heap';
	is( $compute->safe_psql(
			'postgres', qq{
SELECT count(*) FROM pg_buffercache WHERE reldatabase = $db
AND reltablespace = $spc AND relfilenode = $rel AND relforknumber = 0
}),
		'0',
		'old compute has no cached main pages');
}

evict_compute();
my $local = $compute->data_dir . '/' . $relpath;
rename($local, "$local.held-for-journal-test")
  or die "could not move $local aside: $!";
is($compute->safe_psql('postgres', $signature_sql),
	$signature,
	'old compute reads the retained baseline without a local main file');
$primary->safe_psql('postgres',
	"UPDATE journal_heap SET payload = 'changed before restart' WHERE id = 1"
);
my $changed = pause_after_catchup();
my $changed_hash = page_hash($changed);
isnt($changed_hash, $baseline_hash,
	'a later durable cut has different contents');

# No new restartpoint: startup must replay WAL already covered by the journal
# without appending those frames a second time or changing historical pages.
$storage->stop('immediate');
$storage->start;
$primary->wait_for_catchup($storage);
is(page_hash($cut), $baseline_hash, 'baseline survives immediate restart');
is(page_hash($changed), $changed_hash,
	'later published cut survives restart');
is( $storage->safe_psql(
		'postgres', 'SELECT md5(pages) FROM ' . fetch_sql($cut, 1, $other_rel)
	),
	$other_hash,
	'restart preserves the second relation identity and pages');
evict_compute();
is($compute->safe_psql('postgres', $signature_sql),
	$signature,
	'old compute refetches its exact SQL result after storage restart');

SKIP:
{
	skip 'Injection points are not available', 10 unless $injection_points;
	for my $case (
		[ 'test-page-store-before-history-footer', 'incomplete frame', 1 ],
		[
			'test-page-store-after-history-sync',
			'synced unpublished frame',
			2
		])
	{
		my ($point, $label, $id) = @$case;
		$storage->safe_psql('postgres',
			"SELECT injection_points_attach('$point', 'wait')");
		$primary->safe_psql('postgres',
			"UPDATE journal_heap SET payload = '$label' WHERE id = $id");
		$storage->poll_query_until(
			'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity
WHERE backend_type = 'startup' AND wait_event = '$point')
}) or die "storage did not reach $point";
		my $in_progress = $storage->safe_psql('postgres',
			'SELECT replaying_lsn FROM test_page_store_history_status()');
		my $in_progress_hash = $storage->safe_psql('postgres',
			"SELECT md5(get_raw_page('journal_heap', 0))");
		my ($ret, $out, $err) = $storage->psql('postgres',
			'SELECT * FROM ' . fetch_sql($in_progress, 1));
		ok($ret != 0 && $err =~ /record boundary is not retained/,
			"$label is not prematurely published");
		is(page_hash($cut), $baseline_hash,
			"$label leaves the published baseline unchanged");
		my $log_start = -s $storage->logfile;
		$storage->stop('immediate');
		$storage->start;
		$primary->wait_for_catchup($storage);

		if ($id == 1)
		{
			ok( $storage->log_contains(
					qr/removed incomplete page history journal tail/,
					$log_start),
				'restart discards only the incomplete tail');
		}
		else
		{
			ok( !$storage->log_contains(
					qr/removed incomplete page history journal tail/,
					$log_start),
				'restart preserves a complete synced frame');
		}
		is(page_hash($in_progress), $in_progress_hash,
			"$label cut has the exact pre-crash page after recovery");
		is(page_hash($cut), $baseline_hash,
			"$label recovery preserves the old cut");
	}
}

# A restartpoint after DROP must not erase the older view or resurrect the
# file in the new one.  This also makes losing the journal unrecoverable by
# simply replaying the current restartpoint forward.
$primary->safe_psql('postgres',
	'DROP TABLE journal_heap, journal_other; CHECKPOINT;');
my $dropped = pause_after_catchup();
$storage->safe_psql('postgres', 'CHECKPOINT');
$storage->stop('immediate');
$storage->start;
$primary->wait_for_catchup($storage);
is( $storage->safe_psql(
		'postgres',
		'SELECT NOT fork_exists AND nblocks = 0 FROM '
		  . fetch_sql($dropped, 0)),
	't',
	'DROP metadata survives a later restartpoint');
is( $storage->safe_psql(
		'postgres',
		'SELECT NOT fork_exists AND nblocks = 0 FROM '
		  . fetch_sql($dropped, 0, $other_rel)),
	't',
	'second relation DROP metadata survives the same restartpoint');
evict_compute();
is($compute->safe_psql('postgres', $signature_sql),
	$signature,
	'old compute still reads its rows after DROP and storage restartpoint');
ok(!-e $local, 'restart tests did not recreate the compute main file');
$compute->stop;

# A complete frame with a bad checksum is not an incomplete append.  Refuse
# to serve history instead of silently forgetting a previously published cut.
$storage->stop('immediate');
open(my $fh, '+<', $journal) or die "could not open $journal: $!";
binmode($fh);
seek($fh, -1, SEEK_END) or die "could not seek $journal: $!";
read($fh, my $byte, 1) == 1 or die "could not read journal footer: $!";
seek($fh, -1, SEEK_END) or die "could not seek $journal: $!";
print $fh chr(ord($byte) ^ 1);
close($fh) or die "could not close $journal: $!";
my $log_start = -s $storage->logfile;
ok(!$storage->start(fail_ok => 1),
	'corrupt complete frame prevents recovery');
ok( $storage->log_contains(
		qr/page history journal checksum mismatch/, $log_start),
	'corrupt complete frame has an explicit checksum error');
$primary->stop;
done_testing();
