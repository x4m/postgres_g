# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Recover a new writer from a metadata seed and a fenced WAL prefix.  Page
# storage is deliberately ahead of that prefix; neither its latest pages
# nor any files from the lost writer are valid recovery inputs.
use strict;
use warnings FATAL => 'all';

use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'injection points are not supported'
  unless $ENV{enable_injection_points} eq 'yes';

my $source = PostgreSQL::Test::Cluster->new('activation_source');
$source->init(
	allows_streaming => 1,
	extra => [ '--wal-segsize=1', '--no-data-checksums' ]);
$source->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
shared_buffers = '16MB'
checkpoint_timeout = '1h'
wal_consistency_checking = 'heap,heap2,btree'
});
$source->start;
$source->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION amcheck;
CREATE TABLE activation_heap (id integer PRIMARY KEY, payload text);
INSERT INTO activation_heap SELECT i, md5(i::text) FROM generate_series(1, 500) i;
VACUUM (FREEZE, ANALYZE) activation_heap;
SELECT pg_create_physical_replication_slot('activation_sender', true);
});
my $sysid = $source->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $db = $source->safe_psql('postgres',
	'SELECT oid FROM pg_database WHERE datname = current_database()');
my $spc = $source->safe_psql('postgres',
	"SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'");
my @names = qw(activation_heap activation_heap_pkey);
my @nodes =
  map { $source->safe_psql('postgres', "SELECT pg_relation_filenode('$_')") }
  @names;
my @paths =
  map { $source->safe_psql('postgres', "SELECT pg_relation_filepath('$_')") }
  @names;
my @files = map {
	my $path = $_;
	map { "$path$_" } ('', '_vm', '_fsm')
} @paths;
my $locators = join ', ', map { "$spc/$db/$_" } @nodes;
my $anchor = $source->safe_psql(
	'postgres', q{
SELECT restart_lsn - ((restart_lsn - '0/0'::pg_lsn)::bigint % 1048576)::numeric
FROM pg_replication_slots WHERE slot_name = 'activation_sender'
});
$source->backup('storage_seed');
my $storage = PostgreSQL::Test::Cluster->new('activation_storage');
$storage->init_from_backup($source, 'storage_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 8192
test_page_store.history_durable = true
});
$storage->start;
$source->safe_psql('postgres',
	"SELECT pg_create_restore_point('activation-baseline')");
$source->wait_for_catchup($storage, 'replay', $source->lsn('insert'));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $baseline = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['activation_heap','activation_heap_pkey']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
my $config = qq{
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.physical_service = true
test_page_store.transport_slots = 4
test_page_store.replay_lsn = '$baseline'
test_page_store.replay_tli = 1
test_page_store.locators = '$locators'
test_page_store.follow = true
test_page_store.request_timeout = '30s'
};
$source->backup('donor_seed');
my $donor = PostgreSQL::Test::Cluster->new('activation_donor');
$donor->init_from_backup($source, 'donor_seed', has_streaming => 1);
$donor->append_conf('postgresql.conf', $config);

for my $file (@files)
{
	next unless -f $donor->data_dir . "/$file";
	(my $saved = $file) =~ s{/}{_}g;
	rename($donor->data_dir . "/$file",
		$donor->backup_dir . "/$saved.original")
	  or die "move selected donor file: $!";
}
$donor->start;
$source->safe_psql('postgres',
	"SELECT pg_create_restore_point('activation-donor-ready')");
$source->wait_for_catchup($donor, 'replay', $source->lsn('insert'));

sub new_inbox
{
	my ($name) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init(allows_streaming => 1, force_initdb => 1);
	$node->append_conf(
		'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store = true
fsync = on
autovacuum = off
});
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION test_page_store; CREATE EXTENSION injection_points'
	);
	return $node;
}

my $parent = new_inbox('activation_parent_inbox');
my $seed = $parent->backup_dir . '/metadata_seed';
$donor->command_ok(
	[
		'pg_basebackup', '--dbname',
		$donor->connstr('postgres'), '--pgdata',
		$seed, '--checkpoint=fast',
		'--wal-method=stream'
	],
	'publish independent metadata seed');
$donor->command_ok([ 'pg_verifybackup', $seed ],
	'metadata seed passes verification');
is(scalar(grep { -e "$seed/$_" } @files),
	0, 'seed has no selected relation files');
$donor->stop;
$source->stop;
$conninfo = $parent->connstr('postgres');
$conninfo =~ s/'/''/g;
$source->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store_conninfo = '$conninfo'
test_page_store.wal_store_start_lsn = '$anchor'
test_page_store.wal_store_slot = 'activation_sender'
});
$source->start;
$source->safe_psql(
	'postgres', q{
UPDATE activation_heap SET payload = 'durable' WHERE id % 7 = 0;
INSERT INTO activation_heap SELECT i, md5(i::text) FROM generate_series(501, 900) i;
DELETE FROM activation_heap WHERE id % 11 = 0;
CHECKPOINT;
});
my $digest = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload, ',' ORDER BY id, ctid))
FROM activation_heap
};
my $expected = $source->safe_psql('postgres', $digest);

# Local flush makes this commit available to the ordinary page/redo standby,
# but the independent inbox must not publish it.  Do not wake its wait point.
my $point = 'test-wal-store-before-data-sync';
$parent->safe_psql('postgres',
	"SELECT injection_points_attach('$point', 'wait')");
my ($out, $err) = ('', '');
my $pending = IPC::Run::start(
	[
		'psql', '-XAt', '--dbname', $source->connstr('postgres'),
		'-c',
		"UPDATE activation_heap SET payload = 'not durable' WHERE id = 1"
	],
	'>' => \$out,
	'2>' => \$err);
$parent->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE wait_event = '$point'")
  or die 'parent inbox did not stop before fsync';
ok( $storage->poll_query_until(
		'postgres',
		"SELECT payload = 'not durable' FROM activation_heap WHERE id = 1"),
	'page storage has replayed the unacknowledged commit');
isnt($storage->safe_psql('postgres', $digest),
	$expected, 'latest storage state differs from the durable writer state');
my $ahead =
  $storage->safe_psql('postgres', 'SELECT pg_last_wal_replay_lsn()');
$source->stop('immediate');
$pending->finish;
isnt(($pending->full_results)[0],
	0, 'old writer never reports the pending commit as successful');
rename($source->data_dir, $source->data_dir . '.lost')
  or die "isolate source: $!";
$parent->stop('immediate');
$parent->start;
my $bundle = $parent->archive_dir;
rmdir($bundle) or die "remove empty bundle directory: $!";
$parent->command_ok(
	[
		'test_wal_fetch', '--fence', $parent->connstr('postgres'),
		$sysid, 1, 1, $bundle
	],
	'fence and export only the durable parent prefix');
my $manifest = slurp_file("$bundle/wal-inbox-manifest");
my ($end) = $manifest =~ /^record_end=(\S+)$/m;
is( $storage->safe_psql(
		'postgres', "SELECT '$ahead'::pg_lsn > '$end'::pg_lsn"),
	't',
	'storage is ahead of the branch point');
$parent->stop;

my $bad = PostgreSQL::Test::Cluster->new('activation_without_inbox');
$bad->init_from_backup($parent, 'metadata_seed');
$bad->append_conf(
	'postgresql.conf', q{
test_page_store.writer_overlay_pages = 2048
test_page_store.writer_after_recovery_tli = 2
});
ok(!$bad->start(fail_ok => 1),
	'writer activation refuses a missing WAL inbox');
like(
	slurp_file($bad->logfile),
	qr/recovered writer requires an independent WAL inbox/,
	'missing durability is diagnosed before recovery');

my $child_inbox = new_inbox('activation_child_inbox');
my $child = PostgreSQL::Test::Cluster->new('activation_child');
$child->init_from_backup($parent, 'metadata_seed');
# The seed came from a standby.  This replacement must finish at archive EOF,
# not keep waiting for another record as its donor did.
unlink($child->data_dir . '/standby.signal')
  or die "remove seed standby signal: $!";
$child->enable_restoring($parent, 0);
$conninfo = $child_inbox->connstr('postgres');
$conninfo =~ s/'/''/g;
$anchor = $storage->safe_psql('postgres',
	"SELECT '$end'::pg_lsn - ((('$end'::pg_lsn - '0/0'::pg_lsn)::bigint % 1048576)::numeric)"
);
$child->append_conf(
	'postgresql.conf', qq{
primary_conninfo = ''
primary_slot_name = ''
hot_standby = off
recovery_target_timeline = '1'
test_page_store.writer_overlay_pages = 2048
test_page_store.writer_after_recovery_tli = 2
test_page_store.wal_store_conninfo = '$conninfo'
test_page_store.wal_store_start_lsn = '$anchor'
test_page_store.wal_store_slot = 'child_sender'
test_page_store.wal_store_create_slot = true
test_page_store.wal_store_epoch = 3
});
$child->start;
ok( $child->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()'),
	'fresh remote compute opens as a writer on the child timeline');
is($child->safe_psql('postgres', $digest),
	$expected,
	'new writer uses the durable parent cut, not latest storage pages');
is( $child->safe_psql(
		'postgres',
		"SELECT completed = '$end'::pg_lsn FROM test_page_store_compute_status()"
	),
	't',
	'writer baseline is the last complete durable record');
is( $child->safe_psql(
		'postgres',
		'SELECT recovery_calls > 0 AND flushed >= requested FROM test_page_store_wal_status()'
	),
	't',
	'end-of-recovery WAL reached the independent child inbox');
my $history = slurp_file($child->data_dir . '/pg_wal/00000002.history');
my ($branch) = $history =~ /^1\s+(\S+)/m;
is( $child->safe_psql(
		'postgres', "SELECT '$branch'::pg_lsn = '$end'::pg_lsn"),
	't',
	'child history branches at the exact durable cut');
is(scalar(grep { -e $child->data_dir . "/$_" } @files),
	0, 'new writer did not materialize selected relation files');

# New transactions must preserve their own runtime images across eviction;
# they cannot fetch a redo image from either the old or new timeline instead.
my $session = $child->background_psql('postgres');
$session->query_safe(
	q{
BEGIN;
INSERT INTO activation_heap VALUES (1001, 'before cursor');
DECLARE old_view CURSOR FOR SELECT id, payload FROM activation_heap WHERE id >= 1001 ORDER BY id;
INSERT INTO activation_heap VALUES (1002, 'after cursor');
UPDATE activation_heap SET payload = 'after update' WHERE id = 1001;
SAVEPOINT s;
UPDATE activation_heap SET payload = 'aborted' WHERE id = 1001;
ROLLBACK TO s;
});
my $runtime_sql = q{
SELECT string_agg(ctid::text || ':' || cmin::text || ':' || cmax::text || ':' ||
id::text || ':' || payload, ',' ORDER BY id) FROM activation_heap WHERE id >= 1001
};
my $runtime = $session->query_safe($runtime_sql);
my $nodes = join ', ', @nodes;
my $buffers =
  "FROM pg_buffercache WHERE reldatabase = $db AND reltablespace = $spc AND relfilenode IN ($nodes)";
$child->safe_psql('postgres', 'CHECKPOINT');
$child->poll_query_until('postgres',
	"SELECT coalesce(bool_and((pg_buffercache_evict(bufferid)).buffer_evicted), true) $buffers"
) or die 'could not evict the selected child buffers';
is($child->safe_psql('postgres', "SELECT count(*) $buffers"),
	'0', 'all selected buffers really evicted');
is($session->query_safe($runtime_sql),
	$runtime,
	'new writer preserves values, TIDs and command IDs after eviction');
is( $session->query_safe('FETCH ALL FROM old_view'),
	'1001|before cursor',
	'old cursor still sees its command boundary');
$session->query_safe('COMMIT');
$session->quit;
# A too-new image can appear harmless until the new writer allocates XIDs.
# The immediate post-recovery read above is not a sufficient oracle.
is($child->safe_psql('postgres', $digest . ' WHERE id <= 900'),
	$expected,
	'new transactions cannot make storage-only parent changes visible');
$child->safe_psql('postgres',
	"SELECT bt_index_check('activation_heap_pkey', true)");
pass('amcheck agrees with the new writable heap');
is( $child->safe_psql(
		'postgres',
		'SELECT submitted > 0 FROM test_page_store_transport_status()'),
	't',
	'recovery and baseline reads really used physical page service');
is(scalar(grep { -e $child->data_dir . "/$_" } @files),
	0, 'writes and eviction still have no local relation-file fallback');
$child->stop;
ok( -e $child->data_dir . '/test_page_store.writer_epoch',
	'old overlay guard is not removed by clean shutdown');
$child_inbox->stop;
$storage->stop;
done_testing();
