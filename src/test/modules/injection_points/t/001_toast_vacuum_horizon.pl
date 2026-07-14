# Copyright (c) 2026, PostgreSQL Global Development Group

# A relation and its TOAST table are vacuumed with separately computed
# removal horizons.  A same-database transaction can keep a deleted tuple
# RECENTLY_DEAD during the main relation's vacuum, then commit before the
# TOAST table is vacuumed, so the TOAST vacuum removes chunks the main
# relation still references.  A transaction in another database keeps the
# cluster-wide snapshot xmin low, so a later CREATE INDEX still reads the
# tuple, detoasts it, and fails with "missing chunk number 0 for toast
# value".  A "vacuum-before-toast" injection point splits the two vacuums.

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

# A second database; its open transaction (below) keeps the cluster-wide
# snapshot xmin low without affecting the per-database data horizon.
$node->safe_psql('postgres', 'CREATE DATABASE other;');

# The TOAST value must be stored externally (uncompressed) to be detoasted.
$node->safe_psql(
	'postgres', qq{
	CREATE TABLE tbl (i int, t text);
	ALTER TABLE tbl ALTER COLUMN t SET STORAGE EXTERNAL;
	ALTER TABLE tbl SET (autovacuum_enabled = false);
	INSERT INTO tbl (i, t) VALUES (1, repeat('1234567890', 250));
});

# Same-database holder: keeps the main relation's vacuum from removing the
# deleted tuple, so it stays RECENTLY_DEAD.
my $same_db = $node->background_psql('postgres');
$same_db->query_safe('BEGIN; SELECT txid_current();');

# Other-database holder: keeps the cluster-wide snapshot xmin low.
my $other_db = $node->background_psql('other');
$other_db->query_safe('BEGIN; SELECT txid_current();');

$node->safe_psql('postgres', 'DELETE FROM tbl WHERE i = 1;');

# Park VACUUM after the main relation, before its TOAST table.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('vacuum-before-toast', 'wait');");
my $vacuum = $node->background_psql('postgres');
$vacuum->query_until(qr/start/, "\\echo start\nVACUUM (VERBOSE) tbl;\n");
$node->wait_for_event('client backend', 'vacuum-before-toast');

# Drop the same-database holder, then let the TOAST vacuum advance its
# horizon and remove the external chunks.
$same_db->query_safe('COMMIT;');
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('vacuum-before-toast');");
$node->safe_psql('postgres',
	"SELECT injection_points_detach('vacuum-before-toast');");
$vacuum->query_until(qr/done/, "\\echo done\n");

# CREATE INDEX must succeed.  On unpatched code it instead fails with
# SQLSTATE XX001 ("missing chunk number 0 for toast value"), so this test
# currently fails and documents the unfixed bug.
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
