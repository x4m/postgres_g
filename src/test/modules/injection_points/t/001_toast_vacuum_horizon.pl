# Copyright (c) 2026, PostgreSQL Global Development Group

# Reproduce "missing chunk number 0 for toast value", caused by a horizon
# race between the VACUUM of a main relation and the VACUUM of its TOAST
# table.
#
# Timeline:
#  * A transaction in the SAME database holds the per-database data horizon
#    back, so the main relation's VACUUM keeps a deleted-but-RECENTLY_DEAD
#    tuple (together with its TOAST pointer).
#  * That transaction commits while VACUUM is paused (injection point) right
#    before it processes the TOAST table.  With no same-database transaction
#    left to hold it back, the TOAST table's VACUUM computes a newer horizon
#    and removes the external TOAST chunks of that still-RECENTLY_DEAD tuple.
#  * A transaction in ANOTHER database keeps the cluster-wide snapshot xmin
#    low (GetSnapshotData() does not filter by database).  A later CREATE
#    INDEX on the main relation therefore still classifies the tuple as
#    RECENTLY_DEAD, tries to detoast its now-missing value, and fails.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# A separate database whose open transaction holds the cluster-wide snapshot
# xmin back without affecting the per-database data horizon.
$node->safe_psql('postgres', 'CREATE DATABASE other;');

# Main relation with an external (uncompressed) TOAST value.
$node->safe_psql(
	'postgres', qq{
	CREATE TABLE tbl (i int, t text);
	ALTER TABLE tbl ALTER COLUMN t SET STORAGE EXTERNAL;
	ALTER TABLE tbl SET (autovacuum_enabled = false);
	INSERT INTO tbl (i, t) VALUES (1, repeat('1234567890', 250));
});

# Session in the SAME database holding the data horizon back, so the main
# relation's VACUUM keeps the deleted tuple as RECENTLY_DEAD.
my $same_db = $node->background_psql('postgres');
$same_db->query_safe('BEGIN; SELECT txid_current();');

# Session in ANOTHER database keeping the cluster-wide snapshot xmin low.
my $other_db = $node->background_psql('other');
$other_db->query_safe('BEGIN; SELECT txid_current();');

# Create the dead tuple.
$node->safe_psql('postgres', 'DELETE FROM tbl WHERE i = 1;');

# Pause VACUUM right before it processes the TOAST table.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('vacuum-before-toast', 'wait');");

# Start VACUUM asynchronously; it blocks at the injection point after
# vacuuming the main relation.
my $vacuum = $node->background_psql('postgres');
$vacuum->query_until(qr/start/, "\\echo start\nVACUUM (VERBOSE) tbl;\n");

# Wait until VACUUM is parked at the injection point.
$node->wait_for_event('client backend', 'vacuum-before-toast');

# The same-database holder commits: nothing in this database holds the horizon
# back now, so the upcoming TOAST VACUUM removes the external chunks.
$same_db->query_safe('COMMIT;');

# Let VACUUM proceed to vacuum the TOAST table.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('vacuum-before-toast');");
$node->safe_psql('postgres',
	"SELECT injection_points_detach('vacuum-before-toast');");

# Wait for VACUUM to finish.
$vacuum->query_until(qr/done/, "\\echo done\n");

# The other-database transaction is still open, keeping the snapshot xmin low,
# so CREATE INDEX still treats the tuple as RECENTLY_DEAD and detoasts it.
#
# Correct behavior: CREATE INDEX must succeed.  On unpatched code it instead
# fails with
#     ERROR:  missing chunk number 0 for toast value NNNN in pg_toast_NNNN
# which is reported as SQLSTATE XX001 (ERRCODE_DATA_CORRUPTED, "data
# corrupted").  This test is written to assert the correct behavior, so it
# FAILS on current code and documents the unfixed bug; it will pass once
# detoasting RECENTLY_DEAD tuples during an index build no longer errors out.
my ($stdout, $stderr) = ('', '');
my $ret = $node->psql(
	'postgres', 'CREATE INDEX tbl_t_idx ON tbl (t);',
	stdout => \$stdout,
	stderr => \$stderr);

is($ret, 0, 'CREATE INDEX succeeds despite the heap/TOAST horizon race');
unlike(
	$stderr,
	qr/missing chunk number 0 for toast value/,
	'CREATE INDEX does not raise XX001 data corruption');

$same_db->quit;
$other_db->quit;
$vacuum->quit;
$node->stop;

done_testing();
