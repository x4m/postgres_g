# Copyright (c) 2026, PostgreSQL Global Development Group
#
# The outbound transport must not need an incoming connection to compute,
# and must remain alive while checkpointer performs the shutdown checkpoint.
use strict;
use warnings FATAL => 'all';

use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use TestPageStore;

plan skip_all => 'injection points are not supported'
  unless $ENV{enable_injection_points} eq 'yes';

my $writer = PostgreSQL::Test::Cluster->new('early_wal_writer');
$writer->init(allows_streaming => 1, extra => ['--wal-segsize=1']);
$writer->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
shared_buffers = '16MB'
checkpoint_timeout = '1h'
});
$writer->start;
$writer->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION injection_points;
CREATE TABLE inbox_rows (id integer PRIMARY KEY);
INSERT INTO inbox_rows VALUES (1);
SELECT pg_create_physical_replication_slot('inbox_source', true);
});
my $segsize = $writer->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $retained = $writer->safe_psql('postgres',
	"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'inbox_source'"
);

sub lsn_number
{
	my ($lsn) = @_;
	my ($high, $low) = map { hex($_) } split m{/}, $lsn;
	return $high * 4294967296 + $low;
}

my $start = lsn_number($retained);
$start -= $start % $segsize;
my $start_lsn =
  sprintf('%X/%X', int($start / 4294967296), $start % 4294967296);
$writer->stop;

my $store = PostgreSQL::Test::Cluster->new('early_wal_inbox');
$store->init(allows_streaming => 1);
$store->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store = true
fsync = on
autovacuum = off
shared_buffers = '16MB'
});
$store->start;
$store->safe_psql('postgres',
	'CREATE EXTENSION test_page_store; CREATE EXTENSION injection_points');
my $conninfo = $store->connstr('postgres');
$conninfo =~ s/'/''/g;
$writer->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store_conninfo = '$conninfo'
test_page_store.wal_store_slot = 'inbox_source'
test_page_store.wal_store_start_lsn = '$start_lsn'
test_page_store.wal_flush_timeout = '60s'
});
$writer->start;
$writer->safe_psql('postgres', 'INSERT INTO inbox_rows VALUES (2)');
is( $writer->safe_psql(
		'postgres',
		"SELECT active FROM pg_replication_slots WHERE slot_name = 'inbox_source'"
	),
	't',
	'sender owns the slot retaining the complete test epoch');
is( $writer->safe_psql(
		'postgres', 'SELECT count(*) FROM pg_stat_replication'),
	'0',
	'no incoming WAL sender can implement the durability barrier');
is( $store->safe_psql(
		'postgres', q{
SELECT datid IS NULL FROM pg_stat_activity
WHERE application_name = 'test WAL sender'
}),
	't',
	'outbound libpq connection is a physical service session');

my $point = 'test-wal-store-before-data-sync';

sub pause_storage
{
	$store->safe_psql('postgres',
		"SELECT injection_points_attach('$point', 'wait')");
}

sub wait_storage
{
	$store->poll_query_until(
		'postgres', qq{
SELECT EXISTS (SELECT FROM pg_stat_activity
WHERE application_name = 'test WAL sender' AND wait_event = '$point')
}) or die 'WAL inbox did not reach its fsync wait';
}

sub resume_storage
{
	$store->safe_psql(
		'postgres', qq{
SELECT injection_points_detach('$point');
SELECT injection_points_wakeup('$point');
});
}

pause_storage();
my $session = $writer->background_psql('postgres');
$session->query_until(
	qr/inserting/, q{
SELECT 'inserting';
INSERT INTO inbox_rows VALUES (3);
});
wait_storage();
is( $writer->safe_psql(
		'postgres',
		'SELECT waiters > 0 AND requested > flushed FROM test_page_store_wal_status()'
	),
	't',
	'commit waits for the WAL inbox, not just local fsync');
is($writer->safe_psql('postgres', 'SELECT count(*) FROM inbox_rows'),
	'2', 'commit is not visible before remote fsync');
resume_storage();
$session->query_safe('SELECT 1');
$session->quit;
is($writer->safe_psql('postgres', 'SELECT count(*) FROM inbox_rows'),
	'3', 'commit completes after the storage acknowledgement');

# Lose a reply after storage publication but before the worker publishes the
# shared frontier.  A replacement worker must verify the identical retry.
my $confirm_point = 'test-wal-sender-before-confirm';
$writer->safe_psql('postgres',
	"SELECT injection_points_attach('$confirm_point', 'wait')");
$session = $writer->background_psql('postgres');
$session->query_until(
	qr/retrying/, q{
SELECT 'retrying';
INSERT INTO inbox_rows VALUES (4);
});
$writer->poll_query_until('postgres',
	"SELECT sender_wait_event = '$confirm_point' FROM test_page_store_wal_status()"
) or die 'sender did not pause before publishing its acknowledgement';
my $requested = $store->safe_psql('postgres',
	'SELECT flushed FROM test_page_store_wal_store_status()');
is( $writer->safe_psql(
		'postgres',
		"SELECT flushed < '$requested'::pg_lsn FROM test_page_store_wal_status()"
	),
	't',
	'storage publication alone does not update the compute frontier');
my $old_pid = $writer->safe_psql('postgres',
	'SELECT sender_pid FROM test_page_store_wal_status()');
$writer->safe_psql('postgres',
	"SELECT injection_points_detach('$confirm_point')");
# Use PostgreSQL's signal emulation on Windows, not a console signal.
is(PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'TERM', $old_pid),
	0, 'terminate the WAL sender before publishing its acknowledgement');
$writer->poll_query_until('postgres',
	"SELECT sender_pid > 0 AND sender_pid <> $old_pid FROM test_page_store_wal_status()"
) or die 'WAL sender was not restarted';
$session->query_safe('SELECT 1');
$session->quit;
is( $writer->safe_psql(
		'postgres', 'SELECT array_agg(id ORDER BY id) FROM inbox_rows'),
	'{1,2,3,4}',
	'lost ACK and sender restart preserve the committed rows');

# Cross a segment boundary with an intentionally small WAL segment.  Local
# retention is a real slot, not a wal_keep_size guess or a scheduling delay.
$writer->safe_psql('postgres',
	"SELECT pg_switch_wal(); SELECT pg_create_restore_point('after-inbox-switch')"
);
$writer->safe_psql('postgres', 'CHECKPOINT');
is( $store->safe_psql(
		'postgres',
		"SELECT flushed > '$start_lsn'::pg_lsn + $segsize FROM test_page_store_wal_store_status()"
	),
	't',
	'sender delivers WAL across a real segment boundary');

# Crash recovery requests an end-of-recovery checkpoint while incoming compute
# connections are still refused.  Hold its storage flush until observing that.
$writer->stop('immediate');
pause_storage();
# Use files, not capture pipes that a long-lived Windows child can inherit.
my $starting = IPC::Run::start(
	[
		'pg_ctl', '-D', $writer->data_dir, '-l',
		$writer->logfile, '-w', 'start'
	],
	'>' => $writer->logfile . '.pg_ctl_start',
	'2>&1');
wait_storage();
$writer->_update_pid(1);
my ($ret, $out, $err) = $writer->psql('postgres', 'SELECT 1');
ok($ret != 0 && $err =~ /not yet accepting connections/,
	'new WAL is waiting on storage before compute accepts connections');
resume_storage();
$starting->finish;
is(($starting->full_results)[0], 0, 'startup finishes after remote fsync');
is( $writer->safe_psql(
		'postgres',
		'SELECT recovery_calls > 0 FROM test_page_store_wal_status()'),
	't',
	'end-of-recovery WAL passed through the additional barrier');
is( $writer->safe_psql(
		'postgres', 'SELECT array_agg(id ORDER BY id) FROM inbox_rows'),
	'{1,2,3,4}',
	'ordinary local crash recovery preserves committed rows');

# Unlike an ordinary background worker, the sender must survive the phase
# that stops backends, so it can deliver checkpointer's final WAL record.
pause_storage();
my ($stop_out, $stop_err) = ('', '');
my $stopping = IPC::Run::start(
	[ 'pg_ctl', '-D', $writer->data_dir, '-m', 'fast', '-w', 'stop' ],
	'>' => \$stop_out,
	'2>' => \$stop_err);
wait_storage();
ok(-e $writer->data_dir . '/postmaster.pid',
	'shutdown is still waiting while final WAL is not durable on storage');
resume_storage();
$stopping->finish;
is(($stopping->full_results)[0], 0,
	'late WAL worker allows orderly shutdown');
$writer->_update_pid(0);
my ($control) = run_command([ 'pg_controldata', $writer->data_dir ]);
like(
	$control,
	qr/^Database cluster state:\s+shut down$/m,
	'shutdown completed its checkpoint, not a timeout or crash');
my ($checkpoint) = $control =~ /^Latest checkpoint location:\s+(\S+)$/m;
die 'checkpoint location missing from pg_controldata output'
  unless defined($checkpoint);
my $frontier = $store->safe_psql('postgres',
	'SELECT flushed FROM test_page_store_wal_store_status()');
cmp_ok(lsn_number($frontier), '>', lsn_number($checkpoint),
	'inbox contains the shutdown checkpoint WAL');

# Verify bytes, not merely callback counters or matching LSN arithmetic.
my $expected = '';
for (my $lsn = $start; $lsn < lsn_number($frontier); $lsn += $segsize)
{
	my $file = sprintf('%08X%08X%08X',
		1,
		int($lsn / 4294967296),
		int(($lsn % 4294967296) / $segsize));
	$expected .= substr(
		read_binary_file($writer->data_dir . '/pg_wal/' . $file),
		0,
		($lsn + $segsize > lsn_number($frontier))
		? lsn_number($frontier) - $lsn
		: $segsize);
}
is(read_binary_file($store->data_dir . '/test_page_store.wal/bytes'),
	$expected,
	'complete stored WAL matches local WAL through final checkpoint');
$store->stop;
done_testing();
