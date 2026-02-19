
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Test that PITR to recovery_target_timeline='latest' does not use a parent
# timeline's WAL segment for the segment containing the switch point, when the
# child timeline's segment is not in the archive.
#
# Scenario: Primary (TL1) archives WAL.  Standby is promoted, creating TL2.
# Archive has TL1 segment 3 (past switch point) and TL2 history, but NOT TL2
# segment 3.  Without the fix, recovery would use TL1 segment 3 and end on the
# wrong timeline.  With the fix, it skips TL1 and waits for TL2 (never arrives).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

# Initialize primary with WAL archiving.
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1, has_archiving => 1);
$node_primary->append_conf('postgresql.conf', qq(wal_keep_size = 0));
$node_primary->start;

$node_primary->safe_psql('postgres', 'CREATE TABLE t (i int); INSERT INTO t VALUES (1)');
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create standby, promote to TL2.
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name, has_streaming => 1);
$node_standby->start;
$node_primary->wait_for_catchup($node_standby);

$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (2)');
$node_primary->wait_for_catchup($node_standby);

my $lsn_before_promote = $node_standby->safe_psql('postgres',
	'SELECT pg_last_wal_replay_lsn()');
$node_standby->promote;
$node_standby->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die "Timed out waiting for promotion";

# Old primary continues on TL1, archives segment 3 with divergent data.
$node_primary->safe_psql('postgres', 'INSERT INTO t VALUES (-1)');
$node_primary->safe_psql('postgres',
	'INSERT INTO t SELECT g FROM generate_series(1, 10000) g');
my $old_walfile = $node_primary->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_lsn())');
$node_primary->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_primary->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$old_walfile' FROM pg_stat_archiver")
  or die "Timed out waiting for old primary to archive";

# New primary archives history and TL2 segment 3.
$node_standby->enable_archiving;
$node_standby->reload;
my $new_walfile = $node_standby->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_lsn())');
$node_standby->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node_standby->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$new_walfile' FROM pg_stat_archiver")
  or die "Timed out waiting for new primary to archive";

# Merge: copy TL2 history to old primary archive.  Do NOT copy TL2 segment 3.
my $old_archive = $node_primary->archive_dir;
my $new_archive = $node_standby->archive_dir;
my $wal_segment_size = int($node_primary->safe_psql('postgres',
	"SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'"));
my ($lsn_hi, $lsn_lo) = $lsn_before_promote =~ /^(\d+)\/([0-9a-fA-F]+)$/i;
my $switch_point_seg = int((hex($lsn_lo) + ($lsn_hi ? (hex($lsn_hi) << 32) : 0))
	/ $wal_segment_size);

opendir(my $dh, $new_archive) or die "Cannot open $new_archive: $!";
while (my $f = readdir($dh))
{
	if ($f =~ /\.history$/)
	{
		copy("$new_archive/$f", "$old_archive/$f") or die "Could not copy $f: $!";
	}
	elsif ($f =~ /^00000002([0-9a-fA-F]{8})([0-9a-fA-F]{8})$/)
	{
		my $segno = (hex($1) << 32) | hex($2);
		next if $segno == $switch_point_seg;
		copy("$new_archive/$f", "$old_archive/$f") or die "Could not copy $f: $!";
	}
}
closedir($dh);

# PITR from archive only (no streaming), recovery_target_timeline='latest'.
my $node_rec = PostgreSQL::Test::Cluster->new('recovering');
$node_rec->init_from_backup($node_primary, $backup_name,
	standby => 1, has_restoring => 1);
$node_rec->enable_restoring($node_primary, 1);
$node_rec->append_conf('postgresql.conf', qq(
recovery_target_timeline = 'latest'
));

$node_rec->start;

# With the fix: we skip TL1 for segment 3, don't find TL2, wait.  Node stays in
# recovery.  Without the fix: we use TL1, replay divergent WAL, get prev-link
# errors or end on wrong timeline.
$node_rec->poll_query_until('postgres', 'SELECT pg_is_in_recovery()', 't')
  or die "Expected node to stay in recovery (waiting for TL2 segment)";

# With the fix: we skip TL1 for segment 3, never restore it.  Without the fix:
# we would use TL1 segment 3 (wrong timeline).
my $log_content = slurp_file($node_rec->logfile);
ok($node_rec->safe_psql('postgres', 'SELECT pg_is_in_recovery()') eq 't',
	'node stays in recovery waiting for TL2 segment');
unlike($log_content, qr/restored log file "000000010000000000000003"/,
	'did not use TL1 segment 3 (would be wrong timeline)');

done_testing();
