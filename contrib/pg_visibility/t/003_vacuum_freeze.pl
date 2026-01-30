
# Copyright (c) 2026-2026, PostgreSQL Global Development Group

# Check that vacuum phase I does not need to modify the heap buffer. 
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize the primary node
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');


# From this point, autovacuum worker will wait at startup.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('autovacuum-worker-start', 'wait');");

# Create a sample table and run vacuum
$node->safe_psql("postgres",
		"CREATE EXTENSION pg_visibility;\n"
      . "CREATE EXTENSION pageinspect;\n"
	  . "create table test_vac_unmodified_heap(a int);\n"
      . "insert into test_vac_unmodified_heap values (1);\n"
	  . "vacuum (freeze) test_vac_unmodified_heap;");

my $result = $node->safe_psql('postgres', qq(select pg_visibility_map_summary('test_vac_unmodified_heap');));
like($result, qr/(1,1)/, 'pg_visibility_map_summary returned as expected');


#		truncating the VM ensures that the next vacuum will need to set it
$node->safe_psql("postgres",
		"CHECKPOINT;\n"
      . "select pg_truncate_visibility_map('test_vac_unmodified_heap');\n");

$result = $node->safe_psql('postgres', qq(
	select pg_visibility_map_summary('test_vac_unmodified_heap');));
like($result, qr/(0,0)/, 'page_header returned as expected');


# though the VM is truncated, the heap page-level visibility hint,
# PD_ALL_VISIBLE should still be set

$result = $node->safe_psql('postgres', qq(
	SELECT 'page flags is: '||(flags & x'0004'::int)::text FROM page_header(get_raw_page('test_vac_unmodified_heap', 0));));
like($result, qr/page flags is: 4/, 'page_header returned as expected');


# vacuum sets the VM
$node->safe_psql("postgres",
		"vacuum test_vac_unmodified_heap;\n");

$result = $node->safe_psql('postgres', qq(
	select pg_visibility_map_summary('test_vac_unmodified_heap');));
like($result, qr/(1,1)/, 'page_header returned as expected');

# Release injection point.
$node->safe_psql('postgres',
	"SELECT injection_points_detach('autovacuum-worker-start');");

done_testing();
