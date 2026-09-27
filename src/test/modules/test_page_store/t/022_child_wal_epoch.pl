# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A fresh compute seed has no physical slots.  End-of-recovery WAL and the
# new timeline's history must reach an independent inbox before SQL opens.
# This covers the WAL side of activation, using ordinary local data files.
use strict;
use warnings FATAL => 'all';

use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use TestPageStore;

plan skip_all => 'injection points are not supported'
  unless $ENV{enable_injection_points} eq 'yes';

sub number
{
	my ($lsn) = @_;
	my ($high, $low) = map { hex($_) } split m{/}, $lsn;
	return $high * 4294967296 + $low;
}

sub segment_start
{
	my ($lsn, $size) = @_;
	my $value = number($lsn);
	$value -= $value % $size;
	return sprintf('%X/%X', int($value / 4294967296), $value % 4294967296);
}

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

sub export_inbox
{
	my ($node, $sysid, $tli, $epoch) = @_;
	my $directory = $node->archive_dir;
	rmdir($directory) or die "remove empty archive directory: $!";
	$node->command_ok(
		[
			'test_wal_fetch', '--fence',
			$node->connstr('postgres'), $sysid,
			$tli, $epoch,
			$directory
		],
		"fence and export timeline $tli without its original compute");
	my %manifest = map { split /=/, $_, 2 }
	  split /\n/, slurp_file("$directory/wal-inbox-manifest");
	return \%manifest;
}

my $source = PostgreSQL::Test::Cluster->new('parent_compute');
$source->init(allows_streaming => 1, extra => ['--wal-segsize=1']);
$source->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
shared_buffers = '16MB'
});
$source->start;
$source->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE TABLE epoch_rows (id integer PRIMARY KEY);
INSERT INTO epoch_rows VALUES (1);
SELECT pg_create_physical_replication_slot('parent_sender', true);
});
my $sysid = $source->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $segsize = $source->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $anchor = segment_start(
	$source->safe_psql(
		'postgres',
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'parent_sender'"
	),
	$segsize);
$source->backup('parent_seed');
$source->stop;
my $parent_inbox = new_inbox('parent_inbox');
my $conninfo = $parent_inbox->connstr('postgres');
$conninfo =~ s/'/''/g;
$source->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store_conninfo = '$conninfo'
test_page_store.wal_store_slot = 'parent_sender'
test_page_store.wal_store_start_lsn = '$anchor'
});
$source->start;
$source->safe_psql('postgres',
	'INSERT INTO epoch_rows VALUES (2); CHECKPOINT');
$source->stop('immediate');
rename($source->data_dir, $source->data_dir . '.lost')
  or die "isolate old compute: $!";
my $parent = export_inbox($parent_inbox, $sysid, 1, 1);
my $parent_bytes =
  read_binary_file($parent_inbox->data_dir . '/test_page_store.wal/bytes');
my $parent_control =
  read_binary_file($parent_inbox->data_dir . '/test_page_store.wal/control');
$parent_inbox->stop;

# Use a separate empty namespace, not a reset of the old authority metadata.
my $inbox = new_inbox('child_inbox');
my $point = 'test-wal-store-before-initialize-publish';
$inbox->safe_psql('postgres',
	"SELECT injection_points_attach('$point', 'wait')");
my $child = PostgreSQL::Test::Cluster->new('child_compute');
$child->init_from_backup($source, 'parent_seed');
$child->enable_restoring($parent_inbox, 0);
$conninfo = $inbox->connstr('postgres');
$conninfo =~ s/'/''/g;
$anchor = segment_start($parent->{record_end}, $segsize);
$child->append_conf(
	'postgresql.conf', qq{
hot_standby = off
recovery_target_timeline = '1'
shared_preload_libraries = 'test_page_store'
test_page_store.replay_tli = 2
test_page_store.wal_store_conninfo = '$conninfo'
test_page_store.wal_store_slot = 'child_sender'
test_page_store.wal_store_create_slot = true
test_page_store.wal_store_start_lsn = '$anchor'
test_page_store.wal_store_epoch = 3
});
ok( !-d $child->data_dir . '/pg_replslot/child_sender',
	'fresh seed does not contain a sender slot');
# Use files, not capture pipes that a long-lived Windows child can inherit.
my $starting = IPC::Run::start(
	[
		'pg_ctl', '-D', $child->data_dir, '-l', $child->logfile, '-w',
		'start'
	],
	'>' => $child->logfile . '.pg_ctl_start',
	'2>&1');
$inbox->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_activity WHERE wait_event = '$point'")
  or die 'child did not reach inbox initialization';
$child->_update_pid(1);
my $history_path = $child->data_dir . '/pg_wal/00000002.history';
my $history = slurp_file($history_path);
my ($branch) = $history =~ /^1\s+(\S+)/m;
is( number($branch),
	number($parent->{record_end}),
	'new timeline branches at the exported parent record boundary');
is( slurp_file($inbox->data_dir . '/test_page_store.wal/history'),
	$history,
	'core-generated history reaches storage before inbox initialization is published'
);
ok(!-e $inbox->data_dir . '/test_page_store.wal/control',
	'initialization has not yet published an acknowledgement authority');
my ($ret, $sql_out, $sql_err) = $child->psql('postgres', 'SELECT 1');
ok( $ret != 0 && $sql_err =~ /not (?:yet )?accepting connections/,
	'compute cannot serve SQL while new timeline storage is not ready'
) or diag("unexpected connection result: $ret $sql_out $sql_err");
$inbox->safe_psql('postgres',
	"SELECT injection_points_detach('$point'); SELECT injection_points_wakeup('$point')"
);
$starting->finish;
is(($starting->full_results)[0],
	0, 'pg_ctl completes successfully for the new compute');
# pg_ctl also accepts PM_STATUS_STANDBY with hot_standby=off.  Wait for SQL
# readiness explicitly rather than relying on the speed of the final flush.
ok( $child->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()'),
	'new compute opens after storage initialization and WAL flush');
is( $child->safe_psql(
		'postgres',
		'SELECT recovery_calls > 0 AND flushed >= requested FROM test_page_store_wal_status()'
	),
	't',
	'end-of-recovery WAL is durable in the child inbox before SQL opens');
is( $child->safe_psql(
		'postgres',
		"SELECT slot_type = 'physical' AND active AND NOT temporary AND restart_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name = 'child_sender'"
	),
	't',
	'early sender creates and owns a persistent retention slot');
is( $child->safe_psql(
		'postgres', 'SELECT array_agg(id ORDER BY id) FROM epoch_rows'),
	'{1,2}',
	'child includes the parent commit made after the seed');
my $sender_pid = $child->safe_psql('postgres',
	'SELECT sender_pid FROM test_page_store_wal_status()');
kill 'TERM', $sender_pid or die "terminate child sender: $!";
$child->poll_query_until('postgres',
	"SELECT sender_pid > 0 AND sender_pid <> $sender_pid FROM test_page_store_wal_status()"
) or die 'child sender did not restart';
$child->safe_psql('postgres', 'INSERT INTO epoch_rows VALUES (3)');
pass(
	'restarted sender can retry initialization with the same retained history'
);
$child->backup('child_seed');
$child->safe_psql('postgres',
	'INSERT INTO epoch_rows VALUES (4); CHECKPOINT');
$child->stop('immediate');
rename($child->data_dir, $child->data_dir . '.lost')
  or die "isolate child compute: $!";

# Restart storage before export: history must be durable, not connection-local.
$inbox->stop('immediate');
$inbox->start;
my $child_manifest = export_inbox($inbox, $sysid, 2, 3);
is($child_manifest->{history_size},
	length($history), 'manifest identifies the retained history file');
is(slurp_file($inbox->archive_dir . '/00000002.history'),
	$history, 'export recovers the exact history without the child PGDATA');
$inbox->stop;

my $restored = PostgreSQL::Test::Cluster->new('child_epoch_recovery');
$restored->init_from_backup($child, 'child_seed');
$restored->enable_restoring($inbox, 0);
$restored->append_conf(
	'postgresql.conf', q{
test_page_store.wal_store_conninfo = ''
recovery_target_timeline = '2'
});
# Require the history to come from the bundle, not the seed's pg_wal.
unlink($restored->data_dir . '/pg_wal/00000002.history')
  or die "remove seed's timeline history: $!";
$restored->start;
$restored->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die 'child archive recovery did not finish';
is( $restored->safe_psql(
		'postgres', 'SELECT array_agg(id ORDER BY id) FROM epoch_rows'),
	'{1,2,3,4}',
	'ordinary recovery replays the acknowledged child epoch');
is(slurp_file($restored->data_dir . '/pg_wal/00000002.history'),
	$history, 'recovery restored the child history from the exported bundle');
$restored->stop;
is( read_binary_file($parent_inbox->data_dir . '/test_page_store.wal/bytes'),
	$parent_bytes,
	'child activation never replaces the parent WAL prefix');
is( read_binary_file(
		$parent_inbox->data_dir . '/test_page_store.wal/control'),
	$parent_control,
	'old inbox remains at its fenced epoch');

# Control still has a valid CRC, so lost or changed history must be checked too.
my $stored_history = $inbox->data_dir . '/test_page_store.wal/history';
open(my $file, '+<', $stored_history) or die "open history: $!";
binmode $file;
print $file substr($history, 0, 1) ^ "\x01";
close $file or die "close history: $!";
my $log_start = -s $inbox->logfile;
ok(!$inbox->start(fail_ok => 1),
	'damaged retained history prevents storage restart');
ok( $inbox->log_contains(qr/invalid test WAL history checksum/, $log_start),
	'history corruption is reported instead of accepting a new stream');

done_testing();
