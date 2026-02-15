# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test multixact SLRU truncation near wraparound with standby replay.
# Creates an old multixact (mx 1) on heap, then pg_resetwal to advance next
# multixact near wraparound.  VACUUM triggers truncation (TRUNCATE_ID WAL).
# Standby must replay TRUNCATE_ID followed by CREATE_ID without crashing
# (fixes bug where latest_page_number was incorrectly reset during truncation
# replay).
#
# Uses backup_fs_cold + archive recovery (PITR) because the cold copy preserves
# mx 1 (no autovacuum truncation before backup), but its checkpoint has
# wal_level=minimal so streaming is impossible.  Archive all WAL and replay.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node_primary = PostgreSQL::Test::Cluster->new('main');
$node_primary->init(
	has_archiving => 1,
	allows_streaming => 'physical',
	auth_extra => [ '--create-role' => 'repl_role' ]);
$node_primary->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_slru'");
$node_primary->append_conf('postgresql.conf', qq[
vacuum_multixact_freeze_min_age = 0
vacuum_multixact_freeze_table_age = 0
log_min_messages = debug1
]);

my $node_pgdata = $node_primary->data_dir;

# Create old multixact (mx 1) on heap before pg_resetwal
$node_primary->start;
$node_primary->safe_psql('postgres', q(CREATE EXTENSION test_slru));
$node_primary->safe_psql('postgres', q{
	CREATE TABLE mx_trunc_tab (id int);
	INSERT INTO mx_trunc_tab VALUES (1);
});
# FOR SHARE creates multixact 1 on heap xmax
$node_primary->safe_psql('postgres', q{
	BEGIN;
	SELECT * FROM mx_trunc_tab FOR SHARE;
	COMMIT;
});
# Create multixact 2 so mx 1's "next offset" is set (needed for GetMultiXactIdMembers)
$node_primary->safe_psql('postgres', q{SELECT test_create_multixact();});

$node_primary->stop;

# Advance next multixact near wraparound; keep oldest=1 so mx 1 stays valid
my $next_mx = 2**31;        # 2147483648, at wraparound boundary
command_ok(
	[
		'pg_resetwal',
		'--wal-level' => 'replica',
		'--multixact-ids' => sprintf('%u,1', $next_mx),
		$node_pgdata
	],
	"set next multixact to $next_mx (near wraparound), oldest to 1");

# Extract values from pg_resetwal --dry-run for SLRU fixup
my $out = (run_command([ 'pg_resetwal', '--dry-run', $node_primary->data_dir ]))[0];
$out =~ /^Database block size: *(\d+)$/m or die "pg_resetwal output missing Database block size";
my $blcksz = $1;
# SLRU_PAGES_PER_SEGMENT is a compile-time constant (32) in slru.h; pg_resetwal doesn't output it
my $slru_pages_per_segment = 32;

# Create segment for next multixact; segment 0 with mx 1 stays for truncation
my $multixact_offsets_per_page = $blcksz / 8;
my $segno =
  int($next_mx / $multixact_offsets_per_page / $slru_pages_per_segment);
my $slru_file = sprintf('%s/pg_multixact/offsets/%04X', $node_pgdata, $segno);
open my $fh, ">", $slru_file
  or die "could not open \"$slru_file\": $!";
binmode $fh;
my $bytes_per_seg = $slru_pages_per_segment * $blcksz;
syswrite($fh, "\0" x $bytes_per_seg) == $bytes_per_seg
  or die "could not write to \"$slru_file\": $!";
close $fh;

# Cold copy preserves pg_resetwal state (mx 1 intact); checkpoint has
# wal_level=minimal so use archive recovery instead of streaming.
$node_primary->backup_fs_cold('mx_backup');

# Start primary; it will archive WAL with wal_level=replica from config
$node_primary->start;

# mx 1 must be readable before truncation (segment 0 still exists)
is( $node_primary->safe_psql('postgres', q{SELECT test_read_multixact('1');}),
	'',
	"multixact 1 readable before truncation");

# Advance all databases' datminmxid so system-wide minimum allows truncation.
# template0 has datallowconn=false by default; allow connections so vacuumdb
# --all includes it (vacuumdb skips databases with datallowconn=false).
# vacuum_multixact_freeze_min_age=0 makes MultiXactCutoff=nextMXID (2^31)
# which exists (from template0 FOR SHARE), so truncation can succeed.
$node_primary->safe_psql('postgres', q{ALTER DATABASE template0 WITH ALLOW_CONNECTIONS true});
$node_primary->safe_psql('template0',
	q{SELECT * FROM pg_catalog.pg_class LIMIT 1 FOR SHARE});
# Vacuum all databases (including template0) so every relation's relminmxid advances past 1
$node_primary->command_ok([ 'vacuumdb', '--all', '--freeze', '--port', $node_primary->port ],
	'vacuumdb --all --freeze');
$node_primary->safe_psql('postgres', q{ALTER DATABASE template0 WITH ALLOW_CONNECTIONS false});

# CREATE_ID WAL records must follow TRUNCATE_ID - stresses latest_page_number fix.
# Create enough multixacts to cross an offset page boundary (next-page bug): the
# first multixact (2^31) is at entry 0 of its page, so we need offsets_per_page
# more to fill the page, and one more to trigger allocation of the next page.
my $multixacts_to_next_page = $multixact_offsets_per_page + 1;
foreach my $i (1 .. $multixacts_to_next_page)
{
	$node_primary->safe_psql('postgres', q{SELECT test_create_multixact();});
}

# Force archive so standby can replay
$node_primary->safe_psql('postgres', q{SELECT pg_switch_wal()});

# Standby from cold backup, replay via archive (no streaming)
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, 'mx_backup',
	has_restoring => 1,
	has_streaming => 0,
	standby => 1);
$node_standby->append_conf('postgresql.conf',
	"log_min_messages = debug1\nwal_retrieve_retry_interval = '100ms'\nmax_connections = 100");
$node_standby->start;

my $primary_lsn = $node_primary->lsn('flush');
$node_standby->poll_query_until('postgres',
	qq{SELECT '$primary_lsn'::pg_lsn <= pg_last_wal_replay_lsn()})
	or die "Timed out waiting for standby to replay";

# Standby must replay truncation (from archive)
my $standby_log = $node_standby->log_content();
ok( $standby_log =~ /replaying multixact truncation/,
	"standby replayed multixact TRUNCATE_ID (truncation near wraparound)");

# Multixact that crossed offset page boundary must be readable (next-page bug)
my $multi_at_page_boundary = $next_mx + $multixact_offsets_per_page;
is( $node_standby->safe_psql('postgres', qq{SELECT test_read_multixact('$multi_at_page_boundary');}),
	'',
	"multixact at offset page boundary readable on standby (next-page replay)");

# New multixacts must be readable on standby
my $first_new_multi = $node_primary->safe_psql('postgres',
	q{SELECT test_create_multixact();});
$node_primary->safe_psql('postgres', q{SELECT pg_switch_wal()});
my $final_lsn = $node_primary->lsn('flush');
$node_standby->poll_query_until('postgres',
	qq{SELECT '$final_lsn'::pg_lsn <= pg_last_wal_replay_lsn()})
	or die "Timed out waiting for standby to replay";
is( $node_standby->safe_psql('postgres', qq{SELECT test_read_multixact('$first_new_multi');}),
	'',
	"new multixact readable on standby after truncation replay");

$node_standby->stop;
$node_primary->stop;

done_testing();
