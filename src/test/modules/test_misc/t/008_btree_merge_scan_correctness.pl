# Copyright (c) 2025, PostgreSQL Global Development Group

# This test verifies scan correctness during B-tree page merge operations.
# It demonstrates the race condition where moving tuples between pages during
# merge can cause forward scans to see duplicates and backward scans to miss tuples.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Check if injection points are available 
if (!defined($ENV{enable_injection_points}) || $ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('btree_merge_scan_test');
$node->init;
$node->append_conf('postgresql.conf', 
	'shared_preload_libraries = \'injection_points\'');
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Create test table and index with conditions that should trigger merge
$node->safe_psql('postgres', q{
	CREATE TABLE btree_test (
		id INTEGER,
		data TEXT
	);
	
	-- Insert data to create multiple pages (more data for multi-page index)
	INSERT INTO btree_test 
	SELECT i, 'data_' || i 
	FROM generate_series(1, 5000) i;
	
	-- Create index with low fillfactor and high mergefactor to encourage merging
	CREATE INDEX btree_test_idx ON btree_test (id) 
	WITH (fillfactor = 30, mergefactor = 50);
});

# Check index OID for debugging our injection point
my $index_oid = $node->safe_psql('postgres', q{
	SELECT oid FROM pg_class WHERE relname = 'btree_test_idx';
});
note("Index OID: $index_oid (injection point triggers for OID > 16384)");

# Make index sparse to create merge candidates  
$node->safe_psql('postgres', q{
	-- Delete 95% of rows to make pages very sparse but with enough remaining data
	DELETE FROM btree_test WHERE id % 20 != 0;
});

# Force index usage by disabling seqscan
$node->safe_psql('postgres', q{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
});

# Create result tables
$node->safe_psql('postgres', q{
	CREATE TABLE forward_results (id INTEGER);
	CREATE TABLE backward_results (id INTEGER);
});

# Set up injection point to pause scans between pages
$node->safe_psql('postgres', q{
	SELECT injection_points_attach('btree-scan-between-pages', 'wait');
});

# Launch background scans that will hit injection points
my $forward_scan = $node->background_psql('postgres');
my $backward_scan = $node->background_psql('postgres');

# Start queries without waiting for completion (they'll hang at injection point)
$forward_scan->query_until(qr/starting forward scan/, q{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	\echo starting forward scan
	INSERT INTO forward_results (id) 
	SELECT id FROM btree_test ORDER BY id;
	\echo forward scan completed
});

$backward_scan->query_until(qr/starting backward scan/, q{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	\echo starting backward scan
	INSERT INTO backward_results (id)
	SELECT id FROM btree_test ORDER BY id DESC;
	\echo backward scan completed
});

# Give scans time to start and pause at injection point
sleep(1);

# Run VACUUM while scans are paused - this may trigger page merge
$node->safe_psql('postgres', q{
	VACUUM btree_test;
});

# Release waiting scans
$node->safe_psql('postgres', q{
	SELECT injection_points_detach('btree-scan-between-pages');
	SELECT injection_points_wakeup('btree-scan-between-pages');
	SELECT injection_points_wakeup('btree-scan-between-pages');
});

$forward_scan->quit;
$backward_scan->quit;

# Analyze results for scan correctness issues
my $expected_count = $node->safe_psql('postgres', 
	'SELECT count(*) FROM btree_test');

my $forward_count = $node->safe_psql('postgres',
	'SELECT count(*) FROM forward_results');

my $backward_count = $node->safe_psql('postgres', 
	'SELECT count(*) FROM backward_results');

# Report results
note("Expected rows: $expected_count");
note("Forward scan rows: $forward_count");
note("Backward scan rows: $backward_count");

# Test assertions - these should fail with page merge, showing the race condition
is($forward_count, $expected_count, 'Forward scan returns correct count');
is($backward_count, $expected_count, 'Backward scan returns correct count');

$node->stop('fast');

done_testing();