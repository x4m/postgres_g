# Copyright (c) 2025 PostgreSQL Global Development Group
#
# Test cumulative vacuum stats system using TAP
#
# In short, this test validates the correctness and stability of cumulative
# vacuum statistics accounting around freezing, visibility, and revision
# tracking across VACUUM and backend operations.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan tests => 10;

#------------------------------------------------------------------------------
# Test cluster setup
#------------------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('vacuum_extending_freeze_test');
$node->init;

# Configure the server for aggressive freezing behavior used by the test
$node->append_conf('postgresql.conf', q{
	log_min_messages = notice
    vacuum_freeze_min_age = 0
    vacuum_freeze_table_age = 0
});

$node->start();

#------------------------------------------------------------------------------
# Database creation and initialization
#------------------------------------------------------------------------------

$node->safe_psql('postgres', q{
	CREATE DATABASE statistic_vacuum_database_regression;
});

# Main test database name
my $dbname = 'statistic_vacuum_database_regression';

# Enable necessary settings and force the stats collector to flush next
$node->safe_psql($dbname, q{
    SET track_functions = 'all';
    SELECT pg_stat_force_next_flush();
});

#------------------------------------------------------------------------------
# Timing parameters for polling loops
#------------------------------------------------------------------------------

my $timeout    = 30;     # overall wait timeout in seconds
my $interval   = 0.015;  # poll interval in seconds (15 ms)
my $start_time = time();
my $updated    = 0;

# Polls statistics until the named columns exceed the provided
# baseline values or until timeout.
#
# run_vacuum is a boolean (0 or 1) means we need to fetch frozen and visible pages
# from pg_class table, otherwise we need to fetch frozen and visible pages from pg_stat_all_tables table.
# Returns: 1 if the condition is met before timeout, 0 otherwise.
sub wait_for_vacuum_stats {
    my (%args) = @_;
    my $run_vacuum = ($args{run_vacuum} or 0);
    my $result_query;
    my $sql;

    my $start = time();
    while ((time() - $start) < $timeout) {

        if ($run_vacuum) {
            $node->safe_psql($dbname, 'VACUUM vestat');

            $sql = "
            SELECT relallfrozen > 0
                AND relallvisible > 0
                FROM pg_class c
                WHERE c.relname = 'vestat'";
        }
        else {
            $sql = "
            SELECT rev_all_frozen_pages > 0
                AND rev_all_visible_pages > 0
                FROM pg_stat_all_tables
                WHERE relname = 'vestat'";
        }

        $result_query = $node->safe_psql($dbname, $sql);

        return 1 if (defined $result_query && $result_query eq 't');

        # sub-second sleep
        sleep($interval);
    }

    return 0;
}

#------------------------------------------------------------------------------
# Variables to hold vacuum statistics snapshots for comparisons
#------------------------------------------------------------------------------

my $relallvisible = 0;
my $relallfrozen = 0;

my $relallvisible_prev = 0;
my $relallfrozen_prev = 0;

my $rev_all_frozen_pages = 0;
my $rev_all_visible_pages = 0;

my $res;

#------------------------------------------------------------------------------
# fetch_vacuum_stats
#
# Loads current values of the relevant vacuum counters for the test table
# into the package-level variables above so tests can compare later.
#------------------------------------------------------------------------------

sub fetch_vacuum_stats {
    my $base_statistics = $node->safe_psql(
        $dbname,
        "SELECT c.relallvisible, c.relallfrozen,
                rev_all_visible_pages, rev_all_frozen_pages
           FROM pg_class c
           LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
          WHERE c.relname = 'vestat';"
    );

    $base_statistics =~ s/\s*\|\s*/ /g;   # transform " | " into space
    ($relallvisible, $relallfrozen, $rev_all_visible_pages, $rev_all_frozen_pages)
        = split /\s+/, $base_statistics;
}

#------------------------------------------------------------------------------
# Test 1: Create test table, populate it and run an initial vacuum to force freezing
#------------------------------------------------------------------------------

$node->safe_psql($dbname, q{
	SELECT pg_stat_force_next_flush();
	CREATE TABLE vestat (x int)
		WITH (autovacuum_enabled = off, fillfactor = 70);
	INSERT INTO vestat SELECT x FROM generate_series(1, 5000) AS g(x);
	ANALYZE vestat;
});

# Poll the stats view until the expected deltas appear or timeout.
$updated = wait_for_vacuum_stats(run_vacuum => 1);

ok($updated,
   'vacuum stats updated after vacuuming the table (relallfrozen and relallvisible advanced)')
  or diag "Timeout waiting for pg_stats_vacuum_tables to update after $timeout seconds during vacuum";

#------------------------------------------------------------------------------
# Snapshot current statistics for later comparison
#------------------------------------------------------------------------------

fetch_vacuum_stats();

#------------------------------------------------------------------------------
# Verify initial statistics after vacuum
#------------------------------------------------------------------------------
ok($relallfrozen > $relallfrozen_prev, 'relallfrozen has increased');
ok($relallvisible > $relallvisible_prev, 'relallvisible has increased');
ok($rev_all_frozen_pages == 0, 'rev_all_frozen_pages stay the same');
ok($rev_all_visible_pages == 0, 'rev_all_visible_pages stay the same');

#------------------------------------------------------------------------------
# Test 2: Trigger backend updates
# Backend activity should reset per-page visibility/freeze marks and increment revision counters
#------------------------------------------------------------------------------
$relallfrozen_prev = $relallfrozen;
$relallvisible_prev = $relallvisible;

$node->safe_psql($dbname, q{
    UPDATE vestat SET x = x + 1001;
});

$node->safe_psql($dbname, 'SELECT pg_stat_force_next_flush()');

# Poll until stats update or timeout.
$updated = wait_for_vacuum_stats(run_vacuum => 0);
ok($updated,
   'vacuum stats updated after backend tuple updates (rev_all_frozen_pages and rev_all_visible_pages advanced)')
  or diag "Timeout waiting for pg_stats_vacuum_* update after $timeout seconds";

#------------------------------------------------------------------------------
# Snapshot current statistics for later comparison
#------------------------------------------------------------------------------

fetch_vacuum_stats();

#------------------------------------------------------------------------------
# Check updated statistics after backend activity
#------------------------------------------------------------------------------

ok($relallfrozen == $relallfrozen_prev, 'relallfrozen stay the same');
ok($relallvisible == $relallvisible_prev, 'relallvisible stay the same');
ok($rev_all_frozen_pages > 0, 'rev_all_frozen_pages has increased');
ok($rev_all_visible_pages > 0, 'rev_all_visible_pages has increased');

#------------------------------------------------------------------------------
# Cleanup
#------------------------------------------------------------------------------

$node->safe_psql('postgres', q{
	DROP DATABASE statistic_vacuum_database_regression;
});

$node->stop;
done_testing();
