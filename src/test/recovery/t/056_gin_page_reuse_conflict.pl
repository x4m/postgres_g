# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Recycling a deleted GIN index page must raise a recovery conflict on a hot
# standby, as recycling a deleted nbtree or GiST page does.
#
# GIN decides a deleted page is reusable from the primary's horizon alone, and
# writes no WAL when it hands the page out.  A standby snapshot is not in that
# horizon, and nothing tells the standby to cancel, so a standby scan holding a
# reference to the page goes on to read whatever the primary put there instead.
#
# Park a standby scan on a posting tree leaf it is holding, hand that page to
# another key on the primary, and ask the resumed scan and a sequential scan the
# same question under the same snapshot.  They disagree.
#
# VACUUM prunes the deleted heap tuples in the same pass that empties the index
# pages, and that pruning would cancel the standby query on its own.  So the
# snapshot is opened after the DELETE commits, past the pruning horizon.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', qq[
autovacuum = off
# Cancel a conflicting standby query at once, so the test does not have to wait.
max_standby_streaming_delay = 0
]);
$node_primary->start;

# The modules may not be installed when this is run under installcheck.
if (!$node_primary->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}
if (!$node_primary->check_extension('pageinspect'))
{
	plan skip_all => 'Extension pageinspect not installed';
}

my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf(
	'postgresql.conf', qq[
# The whole point is that the primary must not learn about standby snapshots.
hot_standby_feedback = off
]);
$node_standby->start;

my $test_db = "test_gin_reuse";
$node_primary->safe_psql('postgres', "CREATE DATABASE $test_db");
$node_primary->safe_psql($test_db, "CREATE EXTENSION pageinspect");
$node_primary->wait_for_replay_catchup($node_standby);

my $point = 'gin-entry-load-more-items-pinned';

$node_primary->safe_psql(
	$test_db, qq[
CREATE EXTENSION injection_points;
CREATE TABLE w (id int, a int[]) WITH (autovacuum_enabled = off);
INSERT INTO w SELECT g, '{1}'::int[] FROM generate_series(1, 120000) g;
CREATE INDEX w_idx ON w USING gin (a) WITH (fastupdate = off);
]);

# The scan pauses on the second leaf, so that is the page to delete and reuse.
# Empty it before the standby takes its snapshot, so pruning is not what
# cancels the standby query later.
$node_primary->safe_psql(
	$test_db, qq[
CREATE TABLE w_victim AS
SELECT blk FROM (
  SELECT b AS blk,
         (SELECT min(t)
            FROM gin_leafpage_items(get_raw_page('w_idx', b)) i,
                 LATERAL unnest(i.tids) t) AS lo
    FROM generate_series(0, (pg_relation_size('w_idx') /
                             current_setting('block_size')::int)::int - 1) b
   WHERE (gin_page_opaque_info(get_raw_page('w_idx', b))).flags
           = '{data,leaf,compressed}'
) s
ORDER BY lo OFFSET 1 LIMIT 1;

DELETE FROM w WHERE ctid IN (
  SELECT t
    FROM w_victim v,
         gin_leafpage_items(get_raw_page('w_idx', v.blk)) i,
         LATERAL unnest(i.tids) t);
]);

# Put the standby's snapshot clear of the pruning horizon.
$node_primary->safe_psql($test_db, "SELECT pg_current_xact_id()") for (1 .. 3);
$node_primary->wait_for_replay_catchup($node_standby);

$node_standby->safe_psql($test_db,
	"SELECT injection_points_attach('$point', 'wait')");

my $psql_w = $node_standby->background_psql($test_db, on_error_stop => 0);

# Take the snapshot with a sequential scan, so it does not touch the GIN index.
$psql_w->query_safe(
	"BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM w;");

# Fire the index scan and return while it is still running.
$psql_w->query_until(
	qr/scanning/, qq[
\\echo scanning
SET enable_seqscan = off;
SELECT count(*) FROM w WHERE a \@> '{1}';
]);

$node_standby->poll_query_until($test_db,
	"SELECT count(*) > 0 FROM pg_stat_activity WHERE wait_event = '$point'")
  or die "timed out waiting for the standby scan to pause";

# Delete the pinned page and put it in the FSM.
$node_primary->safe_psql(
	$test_db, qq[
VACUUM (INDEX_CLEANUP ON) w;
SELECT pg_current_xact_id();
VACUUM (INDEX_CLEANUP ON) w;
]);
my $deleted = $node_primary->safe_psql($test_db,
	"SELECT (gin_page_opaque_info(get_raw_page('w_idx', v.blk))).flags \@> '{deleted}' FROM w_victim v"
);
is($deleted, 't', "the page the standby is pinning was deleted");

# Hand it to a different key, which takes it back out of the FSM silently.
$node_primary->safe_psql($test_db,
	"INSERT INTO w SELECT 3000000 + g, '{2}'::int[] FROM generate_series(1, 120000) g"
);
my $reused = $node_primary->safe_psql($test_db,
	"SELECT NOT ((gin_page_opaque_info(get_raw_page('w_idx', v.blk))).flags \@> '{deleted}') FROM w_victim v"
);
is($reused, 't', "the page was handed to another key while pinned");

$node_primary->wait_for_replay_catchup($node_standby);

# Replaying page reuse must cancel the scan before it can read the new page.
$node_standby->safe_psql(
	$test_db, "SELECT injection_points_detach('$point')");

$psql_w->query("ROLLBACK; SELECT 1;");
like($psql_w->{stderr}, qr/canceling statement due to conflict with recovery/,
	"page reuse canceled the standby scan");

eval { $psql_w->quit };

$node_standby->stop();
$node_primary->stop();

done_testing();
