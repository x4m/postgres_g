# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test multixact SLRU truncation replay on standby.
#
# Reproduces the bug fixed by commit 4a36c89f165: during TRUNCATE_ID replay,
# latest_page_number was reset to MultiXactIdToOffsetPage(endTruncOff).  This
# broke the init-next-page check in RecordNewMultiXact, which compares
# latest_page_number == pageno.  If a CREATE_ID that crosses a page boundary
# is replayed AFTER a TRUNCATE_ID whose endTruncOff is on a different page,
# the init check doesn't fire, the next page isn't initialized, and
# SimpleLruReadPage fails with FATAL.
#
# The test uses test_multixact_write_truncate_wal() to inject a TRUNCATE_ID
# WAL record with endTruncOff on page 0, which the standby replays before
# the page-crossing CREATE_ID on page 1.  This simulates the real-world
# scenario where truncation runs concurrently with multixact creation.
#
# Build with -DDEMO_SIMULATE_OLD_MULTIXACT_BEHAVIOR to skip the pre-zeroing
# of the next page (simulating WAL from older minor versions).  Without
# that flag the test is a no-op regression check since current WAL always
# pre-zeros the next page.

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
log_min_messages = debug1
]);

$node_primary->start;
$node_primary->safe_psql('postgres', q(CREATE EXTENSION test_slru));

# MULTIXACT_OFFSETS_PER_PAGE = BLCKSZ/4 = 2048 (for 8kB blocks).
#
# Scenario:
#   1. Create 2046 multixacts (multis 1..2046).  nextMXact = 2047, page 0.
#   2. Take backup.
#   3. Create 2048 MORE multixacts (2047..4094).  Multi 2047 crosses page 0→1.
#   4. Inject TRUNCATE_ID with endTruncOff = 10 (page 0).
#   5. Create 1 more multixact (4095), which is the last entry on page 1
#      (entry 2047), crossing to page 2.
#
# On the standby:
#   - StartupMultiXact sets latest_page_number = page(2047) = 0
#   - CREATE_ID(2047) crosses page 0→1: init check fires (0==0), zeros page 1,
#     latest_page_number updated to 1 by SimpleLruZeroPage
#   - CREATE_IDs for 2048..4094 on page 1 (no crossings)
#   - TRUNCATE_ID(endTruncOff=10): latest_page_number reset to page(10) = 0
#   - CREATE_ID(4095): pageno=1, next_pageno=2
#     Init check: latest_page_number(0) != pageno(1) → SKIP
#     RecordNewMultiXact tries SimpleLruReadPage(page 2) → FATAL
#
# With the fix (not resetting latest_page_number in TRUNCATE_ID replay):
#   - latest_page_number stays 1 after the page 0→1 crossing
#   - Init check at CREATE_ID(4095): latest_page_number(1) == pageno(1) → fires
#   - Page 2 is initialized → replay succeeds

# Create 2046 multixacts → multis 1..2046, nextMXact = 2047 (page 0 entry 2047)
$node_primary->safe_psql('postgres', q{SELECT test_create_multixacts(2046);});

# Take backup.  nextMXact = 2047, StartupMultiXact will set
# latest_page_number = page(2047) = 0 on the standby.
$node_primary->backup('mx_backup');

# Create 2048 more → multis 2047..4094, nextMXact = 4095 (page 1 entry 2047).
# Multi 2047 crosses page 0→1.  Its init-next-page check fires (before
# TRUNCATE_ID), zeroing page 1 and setting latest_page_number = 1.
$node_primary->safe_psql('postgres', q{SELECT test_create_multixacts(2048);});

# Inject TRUNCATE_ID with endTruncOff = 10 (page 0).
# On the standby this resets latest_page_number from 1 back to 0.
$node_primary->safe_psql('postgres',
	q{SELECT test_multixact_write_truncate_wal('10'::xid)});

# Create 1 more multixact: multi 4095 (page 1, entry 2047).  This crosses to
# page 2.  On the standby, latest_page_number is now 0 (from TRUNCATE_ID),
# but pageno = 1.  The init check: 0 != 1 → skipped.
# SimpleLruReadPage(page 2) fails → FATAL.
$node_primary->safe_psql('postgres', q{SELECT test_create_multixact();});
$node_primary->safe_psql('postgres', q{SELECT pg_switch_wal()});

# Start standby from backup
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, 'mx_backup',
	has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
	"log_min_messages = debug1\nwal_retrieve_retry_interval = '100ms'");
$node_standby->start;

my $primary_lsn = $node_primary->lsn('flush');
my $replayed = $node_standby->poll_query_until('postgres',
	qq{SELECT '$primary_lsn'::pg_lsn <= pg_last_wal_replay_lsn()});

# With fix: standby replays successfully.
# Without fix: startup process crashes with FATAL during CREATE_ID replay.
ok( $replayed, "standby replayed TRUNCATE_ID + page-crossing CREATE_ID" );

if ($replayed)
{
	my $standby_log = $node_standby->log_content();
	ok( $standby_log =~ /replaying multixact truncation/,
		"standby log shows multixact truncation replay");
	$node_standby->stop;
}
$node_primary->stop;

done_testing();
