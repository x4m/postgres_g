
# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

my $src_db = 'empty_excl_src';
my $dst_db = 'empty_excl_dst';
my $dumpdir = "$tempdir/empty_excl_dump";

$node->safe_psql(
	'postgres',
	qq{CREATE DATABASE $src_db;
	   \\c $src_db
	   CREATE TABLE keep_data(id int);
	   CREATE TABLE skip_data(id int);
	   INSERT INTO keep_data VALUES (1), (2);
	   INSERT INTO skip_data VALUES (10), (20), (30);});

# Flag without --exclude-table-data must fail.
$node->command_fails(
	[
		'pg_dump',
		'--no-sync',
		'--format' => 'directory',
		'--file' => "$tempdir/bad_dump",
		'--create-table-data-placeholders',
		$node->connstr($src_db),
	],
	'create-table-data-placeholders requires exclude-table-data');

# Flag requires directory output format.
$node->command_fails_like(
	[
		'pg_dump',
		'--no-sync',
		'--format' => 'custom',
		'--file' => "$tempdir/bad_custom.dump",
		'--exclude-table-data' => 'skip_data',
		'--create-table-data-placeholders',
		$node->connstr($src_db),
	],
	qr/create-table-data-placeholders.*only supported by the directory format/,
	'create-table-data-placeholders requires directory format');

# Flag requires COPY-format data, not INSERT output.
my @incompatible_opts = (
	{ label => 'inserts', extra => [ '--inserts' ] },
	{ label => 'column-inserts', extra => [ '--column-inserts' ] },
	{ label => 'rows-per-insert', extra => [ '--rows-per-insert' => 10 ] },
);
for my $case (@incompatible_opts)
{
	$node->command_fails_like(
		[
			'pg_dump',
			'--no-sync',
			'--format' => 'directory',
			'--file' => "$tempdir/bad_$case->{label}",
			'--exclude-table-data' => 'skip_data',
			'--create-table-data-placeholders',
			@{ $case->{extra} },
			$node->connstr($src_db),
		],
		qr/create-table-data-placeholders.*cannot be used with/,
		"create-table-data-placeholders rejects $case->{label}");
}

# Flag is incompatible with schema-only and no-data dumps.
$node->command_fails(
	[
		'pg_dump',
		'--no-sync',
		'--format' => 'directory',
		'--file' => "$tempdir/bad_schema_only",
		'--exclude-table-data' => 'skip_data',
		'--create-table-data-placeholders',
		'--schema-only',
		$node->connstr($src_db),
	],
	'create-table-data-placeholders rejects schema-only');

$node->command_fails(
	[
		'pg_dump',
		'--no-sync',
		'--format' => 'directory',
		'--file' => "$tempdir/bad_no_data",
		'--exclude-table-data' => 'skip_data',
		'--create-table-data-placeholders',
		'--no-data',
		$node->connstr($src_db),
	],
	'create-table-data-placeholders rejects no-data');

$node->command_ok(
	[
		'pg_dump',
		'--no-sync',
		'--format' => 'directory',
		'--compress' => 'none',
		'--file' => $dumpdir,
		'--exclude-table-data' => 'skip_data',
		'--create-table-data-placeholders',
		$node->connstr($src_db),
	],
	'directory dump with table data placeholders for excluded tables');

$node->command_like(
	[ 'pg_restore', '--list', $dumpdir ],
	qr/TABLE DATA public skip_data/,
	'TOC lists TABLE DATA for excluded table');

my ($stdout, $stderr) = run_command([ 'pg_restore', '--list', $dumpdir ]);
my $skip_dumpid;
foreach my $line (split /\n/, $stdout)
{
	if ($line =~ /TABLE DATA public skip_data/ && $line =~ /^(\d+);/)
	{
		$skip_dumpid = $1;
		last;
	}
}
ok(defined $skip_dumpid, 'found dump ID for excluded table');
like(
	slurp_file("$dumpdir/${skip_dumpid}.dat"),
	qr/^\\\.\n/,
	'excluded table data file contains only COPY end marker')
  if defined $skip_dumpid;

my @datfiles = grep { $_ !~ /\/toc\.dat$/ } glob("$dumpdir/*.dat");
cmp_ok(scalar(@datfiles), '==', 2, 'two table data files in dump');

my ($keep_dat) = grep { $_ ne "$dumpdir/${skip_dumpid}.dat" } @datfiles;
ok(defined $keep_dat && -s $keep_dat > 0,
	'included table has a non-empty data file')
  if defined $skip_dumpid;

$node->safe_psql('postgres', "CREATE DATABASE $dst_db");

$node->command_ok(
	[
		'pg_restore',
		'--dbname' => $node->connstr($dst_db),
		$dumpdir,
	],
	'restore dump with table data placeholder file');

is(
	$node->safe_psql($dst_db, 'SELECT count(*) FROM keep_data'),
	'2',
	'included table data restored');
is(
	$node->safe_psql($dst_db, 'SELECT count(*) FROM skip_data'),
	'0',
	'excluded table restored with no rows');

# Sequences and materialized views keep stock exclude behavior with the flag.
my $mixed_db = 'empty_excl_mixed';
my $mixed_dumpdir = "$tempdir/empty_excl_mixed_dump";

$node->safe_psql(
	'postgres',
	qq{CREATE DATABASE $mixed_db;
	   \\c $mixed_db
	   CREATE TABLE mv_base(id int);
	   INSERT INTO mv_base VALUES (1), (2);
	   CREATE SEQUENCE excluded_seq START 100;
	   SELECT nextval('excluded_seq');
	   CREATE MATERIALIZED VIEW excluded_mv AS SELECT * FROM mv_base;
	   REFRESH MATERIALIZED VIEW excluded_mv;});

$node->command_ok(
	[
		'pg_dump',
		'--no-sync',
		'--format' => 'directory',
		'--compress' => 'none',
		'--file' => $mixed_dumpdir,
		'--exclude-table-data' => 'excluded_seq',
		'--exclude-table-data' => 'excluded_mv',
		'--create-table-data-placeholders',
		$node->connstr($mixed_db),
	],
	'directory dump with placeholders does not affect seq or mat view exclude');

my ($mixed_list) = run_command([ 'pg_restore', '--list', $mixed_dumpdir ]);
unlike(
	$mixed_list,
	qr/SEQUENCE SET public excluded_seq/,
	'TOC omits SEQUENCE SET for excluded sequence with placeholders flag');
unlike(
	$mixed_list,
	qr/MATERIALIZED VIEW DATA public excluded_mv/,
	'TOC omits MATERIALIZED VIEW DATA for excluded mat view with placeholders flag');

my $mixed_dst = 'empty_excl_mixed_dst';
$node->safe_psql('postgres', "CREATE DATABASE $mixed_dst");

$node->command_ok(
	[
		'pg_restore',
		'--dbname' => $node->connstr($mixed_dst),
		$mixed_dumpdir,
	],
	'restore mixed dump with excluded seq and mat view');

is(
	$node->safe_psql($mixed_dst, q{SELECT nextval('excluded_seq')}),
	'100',
	'excluded sequence not restored from source SEQUENCE SET');
is(
	$node->safe_psql($mixed_dst,
		q{SELECT relispopulated FROM pg_class WHERE relname = 'excluded_mv'}),
	'f',
	'excluded materialized view restored unpopulated');

done_testing();
