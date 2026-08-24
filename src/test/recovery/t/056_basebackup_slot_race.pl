# Copyright (c) 2026, PostgreSQL Global Development Group

# Demonstrate WAL recycling before pg_basebackup creates its requested slot.
#
# The injection point stops BASE_BACKUP after choosing its startpoint but
# before sending it to the client.  The test recycles that WAL and then lets
# pg_basebackup create the slot and start its WAL streamer.  The test passes
# when pg_basebackup fails with the expected missing-WAL error.

use strict;
use warnings FATAL => 'all';
use File::Path qw(rmtree);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Small WAL segments make recycling cheap.
my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1, extra => [ '--wal-segsize', '1' ]);
$node->append_conf(
	'postgresql.conf', q[
wal_keep_size = 0
min_wal_size = 2MB
max_wal_size = 4MB
checkpoint_timeout = 1h
]);
$node->start;

# injection_points may not be installed under installcheck.
if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# Stop BASE_BACKUP before it sends the selected startpoint to the client.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('basebackup-before-send-startpoint', 'wait');"
);

my $backupdir = $node->backup_dir . '/basebackup_race';
my ($bb_stdout, $bb_stderr) = ('', '');
my $bb_timeout =
  IPC::Run::timeout(3 * $PostgreSQL::Test::Utils::timeout_default);
my $bb = IPC::Run::start(
	[
		'pg_basebackup',
		'--pgdata' => $backupdir,
		'--wal-method' => 'stream',
		'--slot' => 'basebackup_race',
		'--create-slot',
		'--checkpoint' => 'fast',
		'--no-sync',
		'-d' => $node->connstr('postgres')
	],
	'>' => \$bb_stdout,
	'2>' => \$bb_stderr,
	$bb_timeout);

$node->wait_for_event('walsender', 'basebackup-before-send-startpoint');

# The client cannot create the slot before it receives the startpoint.
is( $node->safe_psql(
		'postgres', 'SELECT count(*) FROM pg_replication_slots;'),
	'0',
	'no replication slot exists while startpoint is unprotected');

# do_pg_backup_start() used the current checkpoint's REDO pointer.
my $startpoint_wal = $node->safe_psql('postgres',
	'SELECT pg_walfile_name(redo_lsn) FROM pg_control_checkpoint();');
note "backup startpoint is in WAL segment $startpoint_wal";

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_ls_waldir() WHERE name = '$startpoint_wal';"
	),
	'1',
	'WAL segment containing the backup startpoint exists while waiting');

# Nothing retains the startpoint while BASE_BACKUP waits.
$node->advance_wal(10);
$node->safe_psql('postgres', 'CHECKPOINT;');
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_ls_waldir() WHERE name = '$startpoint_wal';"
	),
	'0',
	'WAL segment containing the backup startpoint was recycled before slot '
	  . 'creation'
);

# Let pg_basebackup create the slot and request the recycled WAL.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('basebackup-before-send-startpoint');");
$node->safe_psql('postgres',
	"SELECT injection_points_detach('basebackup-before-send-startpoint');");

$bb->finish;
note "pg_basebackup stderr:\n$bb_stderr";

isnt($bb->result(0), 0, 'pg_basebackup failed as expected (bug reproduced)')
  or diag "pg_basebackup stdout: $bb_stdout\npg_basebackup stderr: $bb_stderr";
like(
	$bb_stderr,
	qr/requested WAL segment [0-9A-F]+ has already been removed/,
	'WAL streamer failed because the startpoint segment was removed')
  or diag "pg_basebackup stderr: $bb_stderr";

# The WAL streamer created the slot before noticing the missing segment.
rmtree($backupdir);
$node->safe_psql('postgres',
	"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
	  . "WHERE slot_name = 'basebackup_race';"
);

done_testing();
