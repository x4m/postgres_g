# Copyright (c) 2024-2025, PostgreSQL Global Development Group

# Test multixact wraparound

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node;

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

# Initialize SLRU file with zeros (65536 entries * 4 bytes = 262144 bytes)
my $slru_file = "$node_pgdata/pg_multixact/offsets/FFFF";
open my $fh, ">", $slru_file
	or die "could not open \"$slru_file\": $!";
binmode $fh;
# Write 65536 entries of 4 bytes each (all zeros)
syswrite($fh, "\0" x 262144) == 262144
	or die "could not write to \"$slru_file\": $!";
close $fh;

# Remove old SLRU file if it exists
if (-f "$node_pgdata/pg_multixact/offsets/0000")
{
	unlink("$node_pgdata/pg_multixact/offsets/0000")
		or die "could not unlink \"$node_pgdata/pg_multixact/offsets/0000\": $!";
}

$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION test_slru));

# Consume multixids to wrap around. We start at 0xFFFFFFF0, so after
# creating 32 multixacts we should have wrapped around past FirstMultiXactId.
# Capture all multixact IDs to verify they're all readable after wraparound.
my @multixact_ids;
foreach my $i (1 .. 32)
{
	my $multi = $node->safe_psql('postgres', q{SELECT test_create_multixact();});
	push @multixact_ids, $multi;
}

# Verify that wraparound occurred (last_multi should be less than first_multi
# or very close to FirstMultiXactId)
my $first_multi = $multixact_ids[0];
my $last_multi = $multixact_ids[-1];
ok($last_multi < $first_multi || $last_multi < 0x10000,
	"multixact wraparound occurred (first: $first_multi, last: $last_multi)");

# Verify that all multixacts created during wraparound are still readable
foreach my $i (0 .. $#multixact_ids)
{
	my $multi = $multixact_ids[$i];
	my $timed_out = 0;
	$node->safe_psql(
		'postgres',
		qq{SELECT test_read_multixact('$multi'::xid);},
		timeout => $PostgreSQL::Test::Utils::timeout_default,
		timed_out => \$timed_out);
	ok($timed_out == 0, "multixact $i (ID: $multi) is readable after wraparound");
}

$node->stop;

done_testing();
