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
$node->append_conf('postgresql.conf', 'protocol_compression = zstd');
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

my $copy_file = $node->basedir . '/copy.data';
append_to_file($copy_file,
	join('', map { "$_\tcopy data $_\n" } 1 .. 2000));
my ($copy_in_stdout, $copy_in_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=zstd',
	  '-c', 'CREATE TABLE copy_in_test (id integer, value text)',
	  '-c', "\\copy copy_in_test FROM '$copy_file'",
	  '-c', 'SELECT count(*), min(id), max(id) FROM copy_in_test' ]);
is($copy_in_stderr, '', 'compressed COPY IN produced no errors');
like($copy_in_stdout, qr/2000\|1\|2000/,
	'compressed COPY IN loaded all rows');

my $dump_file = $node->basedir . '/copy_in_test.dump';
$node->command_ok(
	[ 'pg_dump', '--format=custom', '--file', $dump_file,
	  '--table=copy_in_test', $node->connstr('postgres') ],
	'created archive for compressed pg_restore');
$node->safe_psql('postgres', 'DROP TABLE copy_in_test');
$node->command_ok(
	[ 'pg_restore', '--dbname',
	  $node->connstr('postgres') . ' compression=zstd', $dump_file ],
	'pg_restore uses compressed COPY IN');
is($node->safe_psql('postgres',
	'SELECT count(*), min(id), max(id) FROM copy_in_test'),
	'2000|1|2000', 'compressed pg_restore loaded all rows');

my (undef, $invalid_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=invalid',
	  '-c', 'SELECT 1' ]);
like($invalid_stderr, qr/invalid compression value: "invalid"/,
	'invalid compression value is rejected');

$node->safe_psql('postgres', "ALTER SYSTEM SET protocol_compression = 'off'");
$node->reload;
my ($preferred_stdout, $preferred_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=prefer',
	  '-c', 'SELECT 1' ]);
is($preferred_stderr, '', 'preferred compression falls back without errors');
is($preferred_stdout, '1', 'preferred compression fallback returns output');
my (undef, $disabled_stderr) = run_command(
	[ 'psql', '-XAt', '--dbname', $node->connstr('postgres') . ' compression=zstd',
	  '-c', 'SELECT 1' ]);
like($disabled_stderr, qr/does not support protocol compression method "zstd"/,
	'server can reject protocol compression');

$node->stop('fast');
done_testing();
