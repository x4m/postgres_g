# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A failed cold VM read during redo must release its handed-out AIO handle
# before startup exits.  SQL ERROR cleanup alone cannot cover a FATAL exit.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'injection points are not supported'
  unless $ENV{enable_injection_points} eq 'yes';

my $primary = PostgreSQL::Test::Cluster->new('failure_writer');
$primary->init(allows_streaming => 1, extra => ['--no-data-checksums']);
$primary->append_conf(
	'postgresql.conf', q{
autovacuum = off
checkpoint_timeout = '1h'
full_page_writes = off
wal_log_hints = off
});
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION injection_points;
CREATE TABLE failure_heap (id int, payload int);
INSERT INTO failure_heap SELECT i, i FROM generate_series(1, 1000) i;
VACUUM (FREEZE) failure_heap;
});
my $path = $primary->safe_psql('postgres',
	"SELECT pg_relation_filepath('failure_heap')");
my ($spc, $db, $rel) = split /\|/, $primary->safe_psql(
	'postgres', q{
SELECT (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
       (SELECT oid FROM pg_database WHERE datname = current_database()),
       pg_relation_filenode('failure_heap')
});
$primary->backup('storage_seed');
my $storage = PostgreSQL::Test::Cluster->new('failure_storage');
$storage->init_from_backup($primary, 'storage_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 1024
});
$storage->start;
$primary->wait_for_catchup($storage);
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $cut = $storage->safe_psql('postgres',
	"SELECT test_page_store_retain('failure_heap')");
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

$primary->backup('compute_seed');
my $compute = PostgreSQL::Test::Cluster->new('failure_compute');
$compute->init_from_backup($primary, 'compute_seed', has_streaming => 1);
my $conninfo = $storage->connstr('postgres');
$conninfo =~ s/'/''/g;
$compute->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'test_page_store'
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$conninfo'
test_page_store.physical_service = true
test_page_store.transport_slots = 1
test_page_store.follow = true
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = 1
test_page_store.locators = '$spc/$db/$rel'
test_page_store.request_timeout = '5s'
});
for my $suffix ('', '_vm', '_fsm')
{
	my $file = $compute->data_dir . "/$path$suffix";
	next unless -f $file;
	rename($file, "$file.original") or die "move selected file: $!";
}
$compute->start;
$primary->wait_for_catchup($compute);
$compute->poll_query_until('postgres',
	"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('failure_heap')"
) or die 'could not evict selected buffers';
is( $compute->safe_psql(
		'postgres', qq{
SELECT count(*) FROM pg_buffercache WHERE reldatabase = $db
AND reltablespace = $spc AND relfilenode = $rel
}),
	'0',
	'heap and VM are cold before redo');

# Truncating the heap clears tail bits in a VM page that itself remains in
# the relation.  Unlike a VM block reference, this read cannot be skipped.
# Stop exactly after AIO acquisition, then make the real page request fail.
my $point = 'test-page-store-before-remote-vm-read';
$compute->safe_psql('postgres',
	"SELECT injection_points_attach('$point', 'wait')");
$primary->safe_psql('postgres',
	'DELETE FROM failure_heap WHERE id > 1; VACUUM failure_heap');
ok( $compute->poll_query_until(
		'postgres', qq{
SELECT count(*) = 1 FROM pg_stat_activity
WHERE backend_type = 'startup' AND wait_event = '$point'
}),
	'startup owns an AIO handle for the cold remote read');
$storage->stop;
$compute->safe_psql(
	'postgres', qq{
SELECT injection_points_detach('$point');
SELECT injection_points_wakeup('$point');
});
$compute->wait_for_log(qr/database system is shut down/);
my $log = slurp_file($compute->logfile);
like(
	$log,
	qr/FATAL:.*page transport request failed/,
	'startup reports the failed storage request');
like(
	$log,
	qr/startup process.*exited with exit code 1/,
	'startup exits normally after its FATAL error');
unlike(
	$log,
	qr/TRAP:|PANIC:|terminated by signal|pgaio_shutdown/,
	'no secondary assertion or signal during AIO cleanup');
$primary->stop;
done_testing();
