# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Lose the writable compute while page/redo storage is offline.  Recover
# storage and a replacement compute from the independent inbox and metadata
# seed, without any selected relation files or WAL from the lost compute.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $writer = PostgreSQL::Test::Cluster->new('inbox_recovery_writer');
$writer->init(
	allows_streaming => 1,
	extra => [ '--wal-segsize=1', '--no-data-checksums' ]);
$writer->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
shared_buffers = '16MB'
wal_consistency_checking = 'heap,heap2,btree'
});
$writer->start;
$writer->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION amcheck;
CREATE TABLE recovered_heap (id integer PRIMARY KEY, payload text);
INSERT INTO recovered_heap SELECT i, md5(i::text) FROM generate_series(1, 500) i;
VACUUM (FREEZE, ANALYZE) recovered_heap;
SELECT pg_create_physical_replication_slot('inbox_source', true);
});
my $sysid = $writer->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $writer->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $segsize = $writer->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $anchor = $writer->safe_psql(
	'postgres', qq{
SELECT restart_lsn - ((restart_lsn - '0/0'::pg_lsn)::bigint % $segsize)::numeric
FROM pg_replication_slots WHERE slot_name = 'inbox_source'
});
my $db = $writer->safe_psql('postgres',
	'SELECT oid FROM pg_database WHERE datname = current_database()');
my $spc = $writer->safe_psql('postgres',
	"SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'");
my @names = qw(recovered_heap recovered_heap_pkey);
my @paths =
  map { $writer->safe_psql('postgres', "SELECT pg_relation_filepath('$_')") }
  @names;
my @filenodes =
  map { $writer->safe_psql('postgres', "SELECT pg_relation_filenode('$_')") }
  @names;
my $locators = join ', ', map { "$spc/$db/$_" } @filenodes;

$writer->backup('initial');
my $storage = PostgreSQL::Test::Cluster->new('inbox_redo_storage');
$storage->init_from_backup($writer, 'initial', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 8192
test_page_store.history_durable = true
});
$storage->start;
$writer->safe_psql('postgres',
	"SELECT pg_create_restore_point('inbox-storage-ready')");
$writer->wait_for_catchup($storage, 'replay', $writer->lsn('insert'));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql(
	'postgres', q{
SELECT test_page_store_retain_relations(ARRAY['recovered_heap','recovered_heap_pkey']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

my $store_conn = $storage->connstr('postgres');
$store_conn =~ s/'/''/g;
my $config = qq{
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$store_conn'
test_page_store.physical_service = true
test_page_store.transport_slots = 4
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = $tli
test_page_store.locators = '$locators'
test_page_store.request_timeout = '30s'
};

# A normal backup from a following compute really omits the selected data,
# rather than editing a full backup and invalidating its manifest afterwards.
$writer->backup('donor');
my $donor = PostgreSQL::Test::Cluster->new('inbox_seed_donor');
$donor->init_from_backup($writer, 'donor', has_streaming => 1);
$donor->append_conf('postgresql.conf',
	$config . "test_page_store.follow = true\n");

sub remove_selected_files
{
	my ($node) = @_;
	for my $path (@paths)
	{
		for my $suffix ('', '_vm', '_fsm')
		{
			my $file = $node->data_dir . "/$path$suffix";
			next unless -f $file;
			(my $saved = "$path$suffix") =~ s{/}{_}g;
			rename($file, $node->backup_dir . "/$saved.original")
			  or die "move $file: $!";
		}
	}
}

remove_selected_files($donor);
$donor->start;
$writer->safe_psql('postgres',
	"SELECT pg_create_restore_point('inbox-donor-ready')");
$writer->wait_for_catchup($donor, 'replay', $writer->lsn('insert'));

my $inbox = PostgreSQL::Test::Cluster->new('recovery_inbox');
$inbox->init(allows_streaming => 1, force_initdb => 1);
$inbox->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store = true
fsync = on
autovacuum = off
});
$inbox->start;
$inbox->safe_psql('postgres', 'CREATE EXTENSION test_page_store');
my $seed = $inbox->backup_dir . '/metadata_seed';
$donor->command_ok(
	[
		'pg_basebackup', '--dbname',
		$donor->connstr('postgres'), '--pgdata',
		$seed, '--checkpoint=fast',
		'--wal-method=stream'
	],
	'publish independent metadata seed without selected data');
$donor->command_ok([ 'pg_verifybackup', $seed ],
	'metadata seed passes ordinary backup verification');
my @selected = map {
	my $p = $_;
	map { "$p$_" } ('', '_vm', '_fsm')
} @paths;
is(scalar(grep { -e "$seed/$_" } @selected),
	0, 'metadata seed contains none of the selected relation files');
$donor->stop;

# All user data is still at the retained baseline when this writer epoch starts.
$writer->stop;
remove_selected_files($writer);
my $inbox_conn = $inbox->connstr('postgres');
$inbox_conn =~ s/'/''/g;
$writer->append_conf(
	'postgresql.conf', $config . qq{
test_page_store.writer_overlay_pages = 2048
test_page_store.wal_store_conninfo = '$inbox_conn'
test_page_store.wal_store_slot = 'inbox_source'
test_page_store.wal_store_start_lsn = '$anchor'
});
$writer->start;
$writer->safe_psql(
	'postgres', q{
UPDATE recovered_heap SET payload = payload;
SELECT bt_index_check('recovered_heap_pkey', true);
CHECKPOINT;
});
$writer->wait_for_catchup($storage, 'replay', $writer->lsn('insert'));
$storage->stop('immediate');

# The small working set is warm; only the inbox is available for these
# commits.  No page/redo storage can already contain their result.  This does
# not promise that arbitrary cold reads can proceed while page storage is down.
$writer->safe_psql(
	'postgres', q{
BEGIN;
UPDATE recovered_heap SET payload = 'durable update' WHERE id % 7 = 0;
INSERT INTO recovered_heap SELECT i, md5(i::text) FROM generate_series(501, 900) i;
SAVEPOINT s;
UPDATE recovered_heap SET payload = 'aborted update' WHERE id = 1;
ROLLBACK TO s;
DELETE FROM recovered_heap WHERE id % 11 = 0;
COMMIT;
BEGIN;
INSERT INTO recovered_heap VALUES (9999, 'aborted insertion');
ROLLBACK;
CREATE ROLE restored_catalog_role;
SELECT pg_switch_wal();
SELECT pg_create_restore_point('inbox-recovery-target');
CHECKPOINT;
});
my $digest_sql = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload, ',' ORDER BY id, ctid))
FROM recovered_heap
};
my $expected = $writer->safe_psql('postgres', $digest_sql);
# A later read can generate unflushed hint WAL.  Check confirmed durability,
# not an insertion position or a request that could still be outstanding.
my $target = $writer->safe_psql('postgres',
	'SELECT flushed FROM test_page_store_wal_status()');
is(scalar(grep { -e $writer->data_dir . "/$_" } @selected),
	0, 'committed writer data has no local relation-file fallback');
$writer->stop('immediate');
rename($writer->data_dir, $writer->data_dir . '.lost')
  or die "isolate lost writer: $!";

# Export to a new directory, fencing the old sender epoch.  A transient
# failure leaves no published manifest and must not be mistaken for WAL EOF.
my $bundle = $inbox->archive_dir;
rmdir($bundle) or die "remove empty archive directory: $!";
my @fetch = ('test_wal_fetch', $inbox->connstr('postgres'), $sysid, $tli);
$inbox->command_ok(
	[
		'test_wal_fetch', '--fence', $inbox->connstr('postgres'),
		$sysid, $tli, 1, $bundle
	],
	'export acknowledged WAL without the lost writer');
my $manifest = slurp_file("$bundle/wal-inbox-manifest");
like($manifest, qr/^epoch=2$/m, 'bundle records the successful fence');
like($manifest, qr/^segment_size=$segsize$/m,
	'bundle uses the source WAL segment size');
my ($frontier) = $manifest =~ /^end=(\S+)$/m;
is( $inbox->safe_psql(
		'postgres', "SELECT '$frontier'::pg_lsn >= '$target'::pg_lsn"),
	't',
	'export includes all WAL acknowledged to compute');
my @segments = grep { /\/[0-9A-F]{24}$/ } glob("$bundle/*");
cmp_ok(scalar(@segments), '>', 1, 'export spans multiple real segments');
is(scalar(grep { -s $_ != $segsize } @segments),
	0, 'every exported file has recovery-compatible segment size');

sub lsn_number
{
	my ($lsn) = @_;
	my ($high, $low) = map { hex($_) } split m{/}, $lsn;
	return $high * 4294967296 + $low;
}

# The old directory is used only as an independent byte oracle here.  Neither
# the export command nor either recovery configuration names it.
my ($export_start) = $manifest =~ /^start=(\S+)$/m;
my $remaining = lsn_number($frontier) - lsn_number($export_start);
my ($matches, $padding_zero) = (1, 1);
for my $segment (sort @segments)
{
	my ($name) = $segment =~ m{/([^/]+)$};
	my $count = $remaining > $segsize ? $segsize : $remaining;
	my $bytes = slurp_file($segment);
	my $source = slurp_file($writer->data_dir . ".lost/pg_wal/$name");
	$matches      &&= substr($bytes, 0, $count) eq substr($source, 0, $count);
	$padding_zero &&= substr($bytes, $count) eq "\0" x ($segsize - $count);
	$remaining -= $count;
}
ok($matches && $remaining == 0,
	'all exported WAL bytes match the original stream')
  or die 'WAL bundle is not suitable for recovery';
ok($padding_zero,
	'unacknowledged suffix of the final segment is zero padding');
my $stale = $bundle . '.stale';
$inbox->command_fails_like(
	[ @fetch, 1, $stale ],
	qr/epoch does not match/,
	'stale reader epoch cannot export the fenced stream');
ok(!-e "$stale/wal-inbox-manifest",
	'failed export has no published manifest');
$inbox->command_fails_like(
	[ @fetch, 2, $bundle ],
	qr/could not create new WAL bundle/,
	'export refuses to overwrite an existing bundle');
$inbox->stop;

# Redo storage was offline for all changed rows.  Bring it back using only the
# exported inbox bytes, not an incoming connection to either compute or inbox.
$storage->enable_restoring($inbox, 1);
my $recovery_config = q{
primary_conninfo = ''
primary_slot_name = ''
recovery_target_timeline = '1'
recovery_target_name = 'inbox-recovery-target'
recovery_target_action = 'pause'
};
$storage->append_conf('postgresql.conf', $recovery_config);
$storage->start;
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not reach the named WAL target';
is($storage->safe_psql('postgres', $digest_sql),
	$expected,
	'offline redo storage reconstructs committed values and physical TIDs');

my $replacement = PostgreSQL::Test::Cluster->new('inbox_replacement');
$replacement->init_from_backup($inbox, 'metadata_seed', has_restoring => 1);
$replacement->append_conf('postgresql.conf', $recovery_config);
$replacement->start;
$replacement->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'replacement did not reach the named WAL target';
is( $replacement->safe_psql('postgres', $digest_sql),
	$expected,
	'replacement reconstructs all values and physical TIDs from metadata seed and storage'
);
is( $replacement->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_roles WHERE rolname = 'restored_catalog_role'"
	),
	'1',
	'catalog changes after the seed also replay');
$replacement->safe_psql('postgres',
	"SELECT bt_index_check('recovered_heap_pkey', true)");
pass('amcheck agrees with the recovered remote heap');
is(scalar(grep { -e $replacement->data_dir . "/$_" } @selected),
	0, 'replacement still has no selected main, VM or FSM files');
is( $replacement->safe_psql(
		'postgres',
		'SELECT submitted > 0 FROM test_page_store_transport_status()'),
	't',
	'replacement really fetched pages through the physical service');
is($replacement->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	't',
	'writer activation is not silently substituted for read-only recovery');
$replacement->stop;
$storage->stop;
done_testing();
