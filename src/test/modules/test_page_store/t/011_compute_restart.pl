# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Bootstrap and restart a following compute without its selected local files.
# Retention starts before the compute seed checkpoint, not after it.
use strict;
use warnings FATAL => 'all';

use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('restart_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
fsync = on
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
CREATE EXTENSION amcheck;
CREATE TABLE restart_heap (id int, payload text);
ALTER TABLE restart_heap ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO restart_heap
  SELECT i, repeat(md5(i::text), 4) FROM generate_series(1, 1000) i;
CREATE UNIQUE INDEX restart_idx ON restart_heap(id) INCLUDE (payload);
VACUUM (FREEZE, ANALYZE) restart_heap;
SELECT pg_create_physical_replication_slot('restart_storage', true);
SELECT pg_create_physical_replication_slot('restart_compute', true);
});
my @names = qw(restart_heap restart_idx);
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
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
my $storage = PostgreSQL::Test::Cluster->new('restart_storage');
$storage->init_from_backup($primary, 'storage_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'restart_storage'
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
  ARRAY['restart_heap','restart_idx']::regclass[])
});
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

# This ordering makes all the seed's redo available from retained history.
$primary->backup('compute_seed');
my $seed_redo = $primary->safe_psql('postgres',
	'SELECT redo_lsn FROM pg_control_checkpoint()');
is($primary->safe_psql('postgres', "SELECT '$seed_redo'::pg_lsn > '$cut'"),
	't', 'compute checkpoint REDO follows the history baseline');
$primary->safe_psql(
	'postgres', q{
UPDATE restart_heap SET payload = 'after seed' WHERE id % 3 = 0;
INSERT INTO restart_heap
  SELECT i, repeat(md5(i::text), 4) FROM generate_series(1001, 1500) i;
});
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));

sub boundary_sql
{
	my ($start) = @_;
	return "SELECT test_page_store_history_before('$sysid', $tli, $start)";
}

is($storage->safe_psql('postgres', boundary_sql("'$cut'::pg_lsn")),
	$cut, 'an exact retained boundary is unchanged');
is($storage->safe_psql('postgres', boundary_sql("'$cut'::pg_lsn + 1")),
	$cut,
	'a non-boundary position resolves to the preceding completed record');
my $before = $storage->safe_psql('postgres', boundary_sql("'$seed_redo'"));
is( $storage->safe_psql(
		'postgres',
		"SELECT '$before'::pg_lsn <= '$seed_redo' AND '$before'::pg_lsn >= '$cut'"
	),
	't',
	'seed recovery boundary is inside retained history, not in the future');
my ($stdout, $stderr);
isnt(
	$storage->psql(
		'postgres', boundary_sql("'0/1'"),
		stdout => \$stdout,
		stderr => \$stderr),
	0,
	'missing history before the baseline is refused');
like(
	$stderr,
	qr/recovery start is outside retained page history/,
	'failure identifies the missing recovery boundary');

my $compute = PostgreSQL::Test::Cluster->new('restart_compute');
$compute->init_from_backup($primary, 'compute_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
primary_slot_name = 'restart_compute'
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
my @local_files;
for my $name (@names)
{
	for my $suffix ('', '_vm')
	{
		my $path = $compute->data_dir . '/' . $paths{$name} . $suffix;
		next unless -f $path;
		rename($path, "$path.held-for-restart-test")
		  or die "could not move $path aside: $!";
		push @local_files, $path;
	}
}
is(scalar @local_files, 3,
	'heap, B-tree and VM files removed before startup');

sub catchup
{
	my $lsn = $primary->lsn('insert');
	$primary->wait_for_catchup($storage, 'replay', $lsn);
	$primary->wait_for_catchup($compute, 'replay', $lsn);
}

my $digest = q{
SELECT md5(string_agg(ctid::text || ':' || id::text || ':' || payload,
                     ',' ORDER BY id, ctid)) FROM restart_heap
};
my $index_settings = q{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
};

sub same_rows
{
	my ($description) = @_;
	my $expected = $primary->safe_psql(
		'postgres', q{
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
} . $digest);
	is($compute->safe_psql('postgres', $index_settings . $digest),
		$expected, "$description: all values and physical TIDs match");
	$compute->safe_psql('postgres',
		"SELECT bt_index_check('restart_idx', true)");
	pass("$description: amcheck agrees with the heap");
	is(scalar(grep { -e $_ } @local_files),
		0, "$description: no selected local files recreated");
}

$compute->start;
catchup();
same_rows('first startup');
is( $compute->safe_psql(
		'postgres',
		'SELECT active AND skipped > 0 FROM test_page_store_compute_status()'
	),
	't',
	'startup used cached-only redo instead of local selected files');

# No new restartpoint: recover again from the seed's REDO location, while
# storage is already ahead and its older metadata/pages must still be used.
$compute->stop('immediate');
$primary->safe_psql('postgres',
	"UPDATE restart_heap SET payload = 'while compute stopped' WHERE id % 5 = 0"
);
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$compute->start;
catchup();
same_rows('crash before a new restartpoint');

# A transaction crossing the restartpoint must remain invisible until commit,
# independently of which version of each remote page is fetched.
my $writer = $primary->background_psql('postgres');
$writer->query_safe(
	q{
BEGIN;
INSERT INTO restart_heap VALUES (2001, 'not committed at checkpoint');
});
$primary->safe_psql('postgres', 'CHECKPOINT');
catchup();
my $restart_redo = $primary->safe_psql('postgres',
	'SELECT redo_lsn FROM pg_control_checkpoint()');
$compute->safe_psql('postgres', 'CHECKPOINT');
is( $compute->safe_psql(
		'postgres', 'SELECT redo_lsn FROM pg_control_checkpoint()'),
	$restart_redo,
	'compute has a new restartpoint covered by history');
is( $primary->safe_psql(
		'postgres', "SELECT '$restart_redo'::pg_lsn > '$seed_redo'"),
	't',
	'restartpoint advanced past the seed REDO location');
$compute->stop('immediate');
$storage->stop('immediate');
$storage->start;
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$compute->start;
catchup();
same_rows('both nodes crashed after a new restartpoint');
is( $compute->safe_psql(
		'postgres', 'SELECT count(*) FROM restart_heap WHERE id = 2001'),
	'0',
	'transaction spanning restartpoint is still invisible');
$writer->query_safe('COMMIT');
$writer->quit;
catchup();
same_rows('commit after restart');
is( $compute->safe_psql(
		'postgres', 'SELECT payload FROM restart_heap WHERE id = 2001'),
	'not committed at checkpoint',
	'subsequent commit makes the row visible');

$primary->safe_psql('postgres', 'VACUUM (FREEZE, ANALYZE) restart_heap');
catchup();
$compute->stop('fast');
$compute->start;
catchup();
same_rows('clean restart');
my $plan = decode_json(
	$compute->safe_psql(
		'postgres', $index_settings . q{
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT id, payload FROM restart_heap ORDER BY id
}))->[0]->{Plan};
is($plan->{'Node Type'}, 'Index Only Scan', 'restart retains the IOS path');
is($plan->{'Heap Fetches'},
	0, 'restart restores all-visible metadata and pages');

$compute->stop;
$storage->stop;
$primary->stop;
done_testing();
