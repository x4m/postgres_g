# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A following compute's local metadata can be checkpointed into a new seed.
# Replace the whole compute, not just its buffers, using that seed and storage.
use strict;
use warnings FATAL => 'all';

use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('seed_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = on
wal_log_hints = off
max_prepared_transactions = 10
track_commit_timestamp = on
wal_consistency_checking = 'heap,heap2,btree'
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION amcheck;
CREATE TABLE seed_heap (id int PRIMARY KEY, payload text);
ALTER TABLE seed_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO seed_heap
  SELECT i, repeat(md5(i::text), 4) FROM generate_series(1, 500) i;
VACUUM (FREEZE, ANALYZE) seed_heap;
SELECT pg_create_physical_replication_slot('seed_storage', true);
SELECT pg_create_physical_replication_slot('seed_compute', true);
});
my @names = qw(seed_heap seed_heap_pkey);
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
$primary->backup('storage_seed');
my $storage = PostgreSQL::Test::Cluster->new('seed_storage');
$storage->init_from_backup($primary, 'storage_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'seed_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 16384
test_page_store.history_durable = true
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
  ARRAY['seed_heap','seed_heap_pkey']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$primary->backup('first_compute');
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('first-compute-seed-ready')");
my $compute = PostgreSQL::Test::Cluster->new('seed_compute');
$compute->init_from_backup($primary, 'first_compute', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'seed_compute'
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

# Keep the original files OUTSIDE PGDATA: a backup must not include them under
# a renamed filename and accidentally remain dependent on a full data copy.
my @remote_paths;
for my $name (@names)
{
	for my $suffix ('', '_vm', '_fsm')
	{
		my $relative = $paths{$name} . $suffix;
		my $path = $compute->data_dir . '/' . $relative;
		next unless -f $path;
		my $saved = $compute->backup_dir . "/$name$suffix.original";
		rename($path, $saved)
		  or die "could not move $path outside PGDATA: $!";
		push @remote_paths, $relative;
	}
}
is(scalar(grep { !/_fsm$/ } @remote_paths),
	3, 'selected heap, index and VM are remote-only');
ok( scalar(grep { $_ eq "$paths{seed_heap}_fsm" } @remote_paths),
	'disposable heap FSM is also removed from the seed');
$compute->start;

sub catchup
{
	my ($node) = @_;
	my $lsn = $primary->lsn('insert');
	$primary->wait_for_catchup($storage, 'replay', $lsn);
	$node->safe_psql('postgres',
		"WAIT FOR LSN '$lsn' WITH (MODE 'standby_replay', timeout '30s')");
}

catchup($compute);
my $old_mapped_index = $primary->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class_oid_index')");

# These states all postdate the original seed.  Losing the original PGDATA
# must not revert them, nor make recovery depend on WAL before the new seed.
my $committed = $primary->safe_psql(
	'postgres', q{
BEGIN;
SELECT pg_current_xact_id();
INSERT INTO seed_heap VALUES (1001, 'committed before seed');
COMMIT;
});
my $commit_time = $primary->safe_psql('postgres',
	"SELECT pg_xact_commit_timestamp('$committed'::xid)");
my $aborted = $primary->safe_psql(
	'postgres', q{
BEGIN;
SELECT pg_current_xact_id();
INSERT INTO seed_heap VALUES (1002, 'aborted before seed');
ROLLBACK;
});
my $subaborted = $primary->safe_psql(
	'postgres', q{
BEGIN;
SAVEPOINT s;
INSERT INTO seed_heap VALUES (1003, 'aborted subtransaction') RETURNING xmin;
ROLLBACK TO s;
COMMIT;
});
$primary->safe_psql(
	'postgres', q{
BEGIN;
INSERT INTO seed_heap VALUES (2001, 'prepared then committed');
PREPARE TRANSACTION 'seed_commit';
BEGIN;
INSERT INTO seed_heap VALUES (2002, 'prepared then aborted');
PREPARE TRANSACTION 'seed_abort';
CREATE ROLE seed_metadata_role;
SELECT pg_replication_origin_create('seed_origin');
SELECT pg_replication_origin_advance('seed_origin', '0/1234');
REINDEX SYSTEM postgres;
});
my $new_mapped_index = $primary->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class_oid_index')");
isnt($new_mapped_index, $old_mapped_index,
	'fixture changed a mapped catalog index after the original seed');
my $locker1 = $primary->background_psql('postgres');
my $locker2 = $primary->background_psql('postgres');
$locker1->query_safe(
	'BEGIN; SELECT id FROM seed_heap WHERE id = 1 FOR KEY SHARE');
$locker2->query_safe(
	'BEGIN; SELECT id FROM seed_heap WHERE id = 1 FOR KEY SHARE');
my $multi = $primary->safe_psql('postgres',
	'SELECT xmax::text FROM seed_heap WHERE id = 1');
my $members_query = "SELECT string_agg(xid::text || ':' || mode, ',' "
  . "ORDER BY xid::text) FROM pg_get_multixact_members('$multi')";
my $members = $primary->safe_psql('postgres', $members_query);
like($members, qr/\d+:keysh,\d+:keysh/, 'fixture has a two-member multixact');
$primary->safe_psql('postgres', 'CHECKPOINT');
catchup($compute);
$compute->safe_psql('postgres', 'CHECKPOINT');
my @twophase = glob($compute->data_dir . '/pg_twophase/*');
is(scalar @twophase, 2, 'prepared state was checkpointed into real files');

# The seed's WAL dependency must outlive its donor.  A replica slot that
# advances with normal replay would not protect recovery from this old seed.
$storage->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('retained_seed', true)");
my $seed_path = $storage->backup_dir . '/compute_metadata';
$storage->command_ok(
	[
		'pg_basebackup',
		'--dbname' => $compute->connstr('postgres'),
		'--pgdata' => $seed_path,
		'--checkpoint' => 'fast',
		'--wal-method' => 'stream'
	],
	'publish a metadata seed with real checkpoint and WAL');
$storage->command_ok(
	[ 'pg_verifybackup', $seed_path ],
	'metadata seed files and required WAL pass ordinary backup verification');
is(scalar(grep { -e "$seed_path/$_" } @remote_paths),
	0, 'seed has no selected main, VM or FSM files');
my $manifest = decode_json(slurp_file("$seed_path/backup_manifest"));
my $seed_start = $manifest->{'WAL-Ranges'}[0]->{'Start-LSN'};
is( $storage->safe_psql(
		'postgres', qq{
SELECT restart_lsn <= '$seed_start'::pg_lsn
FROM pg_replication_slots WHERE slot_name = 'retained_seed'
}),
	't',
	'storage retains WAL from at least the seed recovery start');

# Preserve the old directory for diagnosis, but make the replacement incapable
# of using it.  Its only inputs are the published seed and the storage node.
$compute->stop('immediate');
my $lost_data = $compute->data_dir . '.lost';
rename($compute->data_dir, $lost_data) or die "could not move old PGDATA: $!";
$primary->safe_psql(
	'postgres', q{
UPDATE seed_heap SET payload = 'changed after seed' WHERE id % 7 = 0;
INSERT INTO seed_heap VALUES (3001, 'inserted after seed');
});
# Put the new restartpoint beyond the seed's segment.  Without its dedicated
# slot, storage would recycle the WAL needed between the seed and current data.
for my $i (1 .. 2)
{
	$primary->safe_psql(
		'postgres', qq{
SELECT pg_switch_wal();
SELECT pg_create_restore_point('after-seed-switch-$i');
});
}
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$storage->safe_psql('postgres', 'CHECKPOINT');
$storage->stop('immediate');
$storage->start;
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
my $seed_wal =
  $primary->safe_psql('postgres', "SELECT pg_walfile_name('$seed_start')");
ok( -f $storage->data_dir . "/pg_wal/$seed_wal",
	'seed WAL survives storage restartpoint and restart');
$storage->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('replacement_compute', true)"
);
my $replacement = PostgreSQL::Test::Cluster->new('seed_replacement');
$replacement->init_from_backup($storage, 'compute_metadata',
	has_streaming => 1);
$replacement->append_conf('postgresql.conf',
	"primary_slot_name = 'replacement_compute'");
$replacement->start;
catchup($replacement);

sub same_rows
{
	my ($description) = @_;
	my $digest = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM seed_heap
};
	my $expected = $primary->safe_psql(
		'postgres', q{
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
} . $digest);
	is( $replacement->safe_psql(
			'postgres',
			'SET enable_seqscan = off; SET enable_bitmapscan = off;'
			  . $digest),
		$expected,
		"$description: all values and physical TIDs match");
	$replacement->safe_psql('postgres',
		"SELECT bt_index_check('seed_heap_pkey', true)");
	pass("$description: amcheck agrees with heap");
}

same_rows('replacement from metadata seed');
is( $replacement->safe_psql(
		'postgres', qq{
SELECT pg_xact_status('$committed'), pg_xact_status('$aborted'),
       pg_xact_status('$subaborted')
}),
	'committed|aborted|aborted',
	'CLOG statuses survive replacement');
is( $replacement->safe_psql(
		'postgres', "SELECT pg_xact_commit_timestamp('$committed'::xid)"),
	$commit_time,
	'commit timestamp survives replacement');
is($replacement->safe_psql('postgres', $members_query),
	$members, 'multixact members survive replacement');
# pg_prepared_xacts hides in-redo entries on a standby.  Check the retained
# state files here and the visible outcomes when COMMIT/ABORT PREPARED replay.
my @restored_twophase = glob($replacement->data_dir . '/pg_twophase/*');
is(scalar @restored_twophase,
	2, 'both prepared state files survive replacement');
is( $replacement->safe_psql(
		'postgres',
		"SELECT remote_lsn = '0/1234'::pg_lsn FROM pg_replication_origin_status WHERE external_id = 'seed_origin'"
	),
	't',
	'replication origin progress survives replacement');
is( $replacement->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_roles WHERE rolname = 'seed_metadata_role'"),
	'1',
	'catalogs and their rebuilt mapped indexes survive replacement');
is( $replacement->safe_psql(
		'postgres', "SELECT pg_relation_filenode('pg_class_oid_index')"),
	$new_mapped_index,
	'replacement uses the new physical catalog mapping');
is( $replacement->safe_psql(
		'postgres', 'SELECT count(*) FROM pg_replication_slots'),
	'0',
	'donor replication slots are not mistaken for replacement slots');
is(scalar(grep { -e $replacement->data_dir . "/$_" } @remote_paths),
	0, 'replacement did not recreate selected local files');

$primary->safe_psql('postgres',
	"COMMIT PREPARED 'seed_commit'; ROLLBACK PREPARED 'seed_abort'");
$locker1->query_safe('COMMIT');
$locker2->query_safe('COMMIT');
$locker1->quit;
$locker2->quit;
catchup($replacement);
same_rows('prepared outcomes replayed after replacement');
@restored_twophase = glob($replacement->data_dir . '/pg_twophase/*');
is(scalar @restored_twophase, 0, 'both prepared transactions resolved');
is( $replacement->safe_psql(
		'postgres',
		q{SELECT string_agg(id::text, ',' ORDER BY id) FROM seed_heap WHERE id >= 1000}
	),
	'1001,2001,3001',
	'only committed transactions are visible');

$replacement->stop('immediate');
$replacement->start;
catchup($replacement);
same_rows('replacement can itself restart');
$replacement->stop;
$storage->stop;
$primary->stop;
done_testing();
