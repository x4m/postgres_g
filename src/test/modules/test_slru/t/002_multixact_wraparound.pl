# Copyright (c) 2024-2025, PostgreSQL Global Development Group

# Test multixact wraparound

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my ($node, $result);

$node = PostgreSQL::Test::Cluster->new('mike');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'test_slru'");

# Set the cluster's next multitransaction to 0xFFFFFFF0.
my $node_pgdata = $node->data_dir;
command_ok(
	[
		'pg_resetwal',
		'--multixact-ids' => '0xFFFFFFF0,0xFFFFFFF0',
		$node_pgdata
	],
	"set the cluster's next multitransaction to 0xFFFFFFF0");
command_ok(
	[
		'dd', 'if=/dev/zero', "of=$node_pgdata/pg_multixact/offsets/FFFF",
		'bs=4', 'count=65536'
	],
	"init SLRU file");

command_ok([ 'rm', "$node_pgdata/pg_multixact/offsets/0000", ],
	"drop old SLRU file");

$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION test_slru));

# Consume multixids to wrap around
foreach my $i (1 .. 32)
{
	$node->safe_psql('postgres', q{SELECT test_create_multixact();});
}

$node->stop;

done_testing();
