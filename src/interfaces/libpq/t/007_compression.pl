# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'Zstandard is not supported by this build'
  unless check_pg_config('#define USE_ZSTD 1');

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

my @commands = (
	'-c', 'SELECT 1',
	'-c', 'SELECT g FROM generate_series(1, 2000) AS g',
	'-c', q{COPY (SELECT repeat('copy data ', 20) FROM generate_series(1, 100)) TO STDOUT},
	'-c', 'SELECT 2');

my ($plain_stdout, $plain_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=off', @commands ]);
is($plain_stderr, '', 'uncompressed connection produced no errors');

my ($compressed_stdout, $compressed_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=zstd', @commands ]);
is($compressed_stderr, '', 'compressed connection produced no errors');
ok($compressed_stdout eq $plain_stdout,
	'compressed queries and COPY OUT produce the same output');

my (undef, $invalid_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=invalid',
	  '-c', 'SELECT 1' ]);
like($invalid_stderr, qr/invalid compression value: "invalid"/,
	'invalid compression value is rejected');

$node->stop('fast');
done_testing();
