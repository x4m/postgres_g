# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('write-combining');
$node->init();

$node->append_conf(
	'postgresql.conf', qq(
shared_buffers = '128MB'
max_wal_size = '4GB'
checkpoint_timeout = '1h'
checkpoint_completion_target = 0.0
autovacuum = off
io_combine_limit = '128kB'
eager_clean_max_batch_size = 16
bgwriter_lru_maxpages = 0
fsync = off
synchronous_commit = off
));

$node->start();

my $block_size = $node->safe_psql('postgres',
	"SELECT current_setting('block_size')::int");

test_copy_from_combines_writes($node, $block_size);

$node->stop();

done_testing();

sub io_stat_writes
{
	my ($node, $backend_type, $context) = @_;

	my $result = $node->safe_psql(
		'postgres', qq(
SELECT COALESCE(sum(writes), 0)::bigint,
       COALESCE(sum(write_bytes), 0)::bigint,
       COALESCE((sum(write_bytes) / NULLIF(sum(writes), 0))::bigint, 0)
FROM pg_stat_io
WHERE backend_type = '$backend_type'
  AND object = 'relation'
  AND context = '$context';
));

	return split /\|/, $result;
}

sub assert_combined_writes
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $label, $backend_type, $context, $block_size) = @_;
	my ($writes, $write_bytes, $avg_write_bytes) =
	  io_stat_writes($node, $backend_type, $context);

	note "$label: writes=$writes write_bytes=$write_bytes avg_write_bytes=$avg_write_bytes";
	ok($writes > 0, "$label wrote buffers");
	ok($avg_write_bytes > $block_size, "$label combined writes");
}

sub test_copy_from_combines_writes
{
	my ($node, $block_size) = @_;

	$node->safe_psql(
		'postgres', qq(
	CREATE UNLOGGED TABLE wc_copy (id int, payload text);
	CHECKPOINT;
	));

	# Feed the rows to "COPY ... FROM STDIN" inline on psql's stdin.
	my $rows = 200000;
	my $payload = '0' x 200;
	my $copy_data = '';
	$copy_data .= "$_\t$payload\n" for (1 .. $rows);

	$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");
	$node->safe_psql('postgres',
		"COPY wc_copy FROM STDIN;\n" . $copy_data . "\\.\n");
	$node->safe_psql('postgres', 'SELECT pg_stat_force_next_flush()');

	assert_combined_writes($node, 'copy from', 'client backend', 'bulkwrite',
		$block_size);
	is($node->safe_psql('postgres', "SELECT count(*) FROM wc_copy"),
		$rows, 'copy from inserted rows');
}
