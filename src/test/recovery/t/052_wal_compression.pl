
# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Test whole-record WAL compression via wal_compression and
# wal_compression_threshold.  Exercises compression during replication
# (walsender path) and decompression during crash recovery (startup path).
#

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Determine which compression methods are compiled in, preferring the
# faster hardware-accelerated codecs first for quicker failure feedback.
my @methods = ();
push @methods, 'zstd' if check_pg_config('#define HAVE_LIBZSTD 1');
push @methods, 'lz4'  if check_pg_config('#define HAVE_LIBLZ4 1');
push @methods, 'pglz';

# Test whole-record WAL compression with a specific method.
#
# Creates a primary with the method enabled and a low threshold so that
# every non-trivial WAL record is compressed.  A streaming standby is
# used to verify that compressed WAL is transmitted and decoded correctly.
# Then the primary is stopped immediately (simulating a crash) and
# restarted to verify that startup recovery can decompress WAL records.
sub test_wal_compression
{
	my ($method) = @_;

	note "testing wal_compression = $method";

	my $primary = PostgreSQL::Test::Cluster->new("primary_$method");
	$primary->init(allows_streaming => 1);

	# Use the minimum threshold so virtually every record gets compressed,
	# and leave wal_compression_buffer at its default (set by Cluster.pm).
	$primary->append_conf(
		'postgresql.conf',
		"wal_compression = '$method'\n"
		  . "wal_compression_threshold = 32\n");
	$primary->start;

	my $backup_name = "backup_$method";
	$primary->backup($backup_name);

	my $standby = PostgreSQL::Test::Cluster->new("standby_$method");
	$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
	$standby->start;

	# Generate WAL with records that exceed the compression threshold.
	# Each row's WAL record includes 200-byte payload well above 32 bytes.
	$primary->safe_psql(
		'postgres',
		"CREATE TABLE t AS
		 SELECT g, repeat('x', 200) AS d
		 FROM generate_series(1, 100) AS g");

	$primary->wait_for_replay_catchup($standby);

	is( $standby->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'100',
		"compressed WAL replicated via streaming ($method)");

	# Insert another batch that will be recovered after the simulated crash.
	$primary->safe_psql(
		'postgres',
		"INSERT INTO t
		 SELECT g, repeat('y', 200)
		 FROM generate_series(101, 200) AS g");

	# Stop without a clean shutdown.  On restart PostgreSQL will replay WAL
	# from the last checkpoint, exercising XLogDecompressRecordIfNeeded for
	# every compressed record generated since that checkpoint.
	$primary->stop('immediate');
	$primary->start;

	is( $primary->safe_psql('postgres', 'SELECT count(*) FROM t'),
		'200',
		"crash recovery replays compressed WAL ($method)");

	$primary->stop;
	$standby->stop;
}

foreach my $method (@methods)
{
	test_wal_compression($method);
}

done_testing();
