use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ------------------------------------------------------------
# Workload generating dead tuples and PRUNE WAL
# ------------------------------------------------------------
sub generate_prune_workload
{
    my ($node) = @_;

    $node->safe_psql('postgres', q{
        CREATE TABLE t_prune (
            id int,
            val text
        ) WITH (fillfactor = 100);
    });

    $node->safe_psql('postgres', q{
        SET vacuum_freeze_min_age = 0;
        SET vacuum_freeze_table_age = 0;
    });

    # Insert many tuples
    $node->safe_psql('postgres', q{
        INSERT INTO t_prune
        SELECT g, 'x'
        FROM generate_series(1,300000) g;
    });

    # Delete 80% of records
    $node->safe_psql('postgres', q{
        DELETE FROM t_prune
		WHERE id % 550 <= 540;
    });

    # VACUUM cycles to trigger PRUNE
    for my $i (1..3)
    {
        $node->safe_psql('postgres', q{ VACUUM FREEZE t_prune; });
    }
}

# ------------------------------------------------------------
# WAL analyzer
# ------------------------------------------------------------
sub collect_prune_stats
{
    my ($node) = @_;

	my $wal_dir = $node->data_dir . "/pg_wal";

	print "wal_dir=" . $wal_dir ."\n";

    # Find the first WAL segment
    my @wal_files = sort glob("$wal_dir/[0-9A-F]*");
    my $start_seg = $wal_files[0];

    die "No WAL files found" unless defined $start_seg;

	# Run pg_waldump on all segments
	my $cmd = "pg_waldump --rmgr=Heap2 -p $wal_dir $start_seg 2>/dev/null";

    my @lines = `$cmd`;

print "lines=" . @lines . "\n";

    my $records = 0;
    my $bytes   = 0;

    foreach my $line (@lines)
    {
        next unless $line =~ /PRUNE_VACUUM_SCAN/;
        $records++;
        if ($line =~ /len \(rec\/tot\):\s*\d+\/\s*(\d+)/)
        {
            $bytes += $1;
        }
    }
	print "records=" . $records . "; bytes=" . $bytes ."\n";
    return ($records, $bytes);
}


# ------------------------------------------------------------
# Run test on a fresh cluster
# ------------------------------------------------------------
sub run_cluster_test
{
    my ($name, $compression) = @_;

    my $node = PostgreSQL::Test::Cluster->new($name);

    $node->init;

    $node->append_conf('postgresql.conf', qq{
		wal_level = replica
		autovacuum = off
		wal_prune_dfor_compression = $compression
	});

    $node->start;

    generate_prune_workload($node);

    $node->stop;

    return collect_prune_stats($node);
}

# ------------------------------------------------------------
# Cluster 1: compression OFF
# ------------------------------------------------------------
my ($off_count, $off_bytes) = run_cluster_test(
    "prune_dfor_off",
    "off"
);

note("Compression OFF: $off_count records, $off_bytes bytes");

# ------------------------------------------------------------
# Cluster 2: compression ON
# ------------------------------------------------------------
my ($on_count, $on_bytes) = run_cluster_test(
    "prune_dfor_on",
    "on"
);

note("Compression ON: $on_count records, $on_bytes bytes");

# ------------------------------------------------------------
# Compression ratio
# ------------------------------------------------------------
my $ratio = "N/A";

if ($on_bytes > 0)
{
    $ratio = sprintf("%.2f", $off_bytes / $on_bytes);
}

note("Compression ratio (uncompressed/compressed): $ratio");
note("Numerator   (uncompressed bytes): $off_bytes");
note("Denominator (compressed bytes):   $on_bytes");

# ------------------------------------------------------------
# Expect compression benefit
# ------------------------------------------------------------
cmp_ok(
    $ratio, '>=', 5,
    'DFOR compression should reduce the size of WAL by at least 5 times.'
);

done_testing();