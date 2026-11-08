# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise the restricted physical-import path, not an RCE-resistance claim.
use strict;
use warnings FATAL => 'all';

use Digest::SHA;
use File::Path qw(make_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'physical import prototype needs Unix sockets'
  if $windows_os;

command_checks_all(
	[
		'pg_dump', '--binary-upgrade',
		'--no-data', '--format=plain',
		'--binary-upgrade-import-owner=imported_owner'
	],
	1,
	[],
	[qr/--binary-upgrade-import-owner requires an archive format/],
	'import schema archives cannot be emitted as a plain SQL script');

my $provider = PostgreSQL::Test::Utils::tempdir();
make_path("$provider/extension");
append_to_file(
	"$provider/extension/import_fixture.control", qq(
default_version = '1.0'
relocatable = true
));
append_to_file(
	"$provider/extension/import_fixture--1.0.sql", q{
CREATE FUNCTION provider_answer() RETURNS text
  LANGUAGE sql SECURITY DEFINER AS $$SELECT 'provider'::text$$;
CREATE TYPE provider_enum AS ENUM ('provider');
SELECT provider_answer();
});
my $extension_path = $provider;
$extension_path =~ s/\\/\\\\/g if $windows_os;

my $old = PostgreSQL::Test::Cluster->new('import_old');
my $new = PostgreSQL::Test::Cluster->new('import_new');
$old->init;
$new->init;
for my $node ($old, $new)
{
	$node->append_conf('postgresql.conf',
		"extension_control_path = '\$system:$extension_path'\n");
}
$old->start;
$old->safe_psql('postgres', 'CREATE DATABASE customer');
$old->safe_psql(
	'customer', q{
CREATE ROLE source_admin SUPERUSER LOGIN;
GRANT pg_read_server_files TO source_admin;
CREATE TABLE payload(id integer PRIMARY KEY, body text);
INSERT INTO payload
  SELECT 1, string_agg(md5(i::text), '') FROM generate_series(1, 1200) i;
ALTER TABLE payload OWNER TO source_admin;
GRANT SELECT ON payload TO PUBLIC;
CREATE SEQUENCE imported_seq START 42;
SELECT nextval('imported_seq');
CREATE FUNCTION ordinary_function() RETURNS integer
  LANGUAGE sql AS $$SELECT 42$$;
CREATE FUNCTION ordinary_set() RETURNS SETOF integer
  LANGUAGE sql STABLE AS $$SELECT 42$$;
CREATE FUNCTION unsafe_definer() RETURNS integer
  LANGUAGE sql SECURITY DEFINER AS $$SELECT 99$$;
CREATE FUNCTION unsafe_native(integer) RETURNS integer
  LANGUAGE internal AS 'int4inc';
CREATE FUNCTION unsafe_leakproof(integer) RETURNS integer
  LANGUAGE sql LEAKPROOF AS $$SELECT $1$$;
CREATE EXTENSION import_fixture;
CREATE OR REPLACE FUNCTION provider_answer() RETURNS text
  LANGUAGE sql SECURITY DEFINER AS $$SELECT 'source'::text$$;
CREATE FUNCTION forged_member() RETURNS integer
  LANGUAGE sql SECURITY DEFINER AS $$SELECT 101$$;
ALTER EXTENSION import_fixture ADD FUNCTION forged_member();
});

my $heap =
  $old->safe_psql('customer', q{SELECT pg_relation_filepath('payload')});
my $index =
  $old->safe_psql('customer', q{SELECT pg_relation_filepath('payload_pkey')});
my $payload =
  $old->safe_psql('customer', q{SELECT length(body), md5(body) FROM payload});

# These would prevent ordinary -b startup/connections.  The importer must
# override configuration and suppress role/database settings and login hooks.
$old->safe_psql(
	'customer', q{
ALTER ROLE CURRENT_USER SET local_preload_libraries = 'import_missing_local';
ALTER DATABASE customer SET session_preload_libraries = 'import_missing_session';
CREATE FUNCTION reject_login() RETURNS event_trigger
  LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'source login hook ran'; END$$;
CREATE EVENT TRIGGER reject_login ON login EXECUTE FUNCTION reject_login();
});
$old->stop;
$old->start(options => '-b -c binary_upgrade_import=on');
is( $old->safe_psql('customer', 'SELECT 1'),
	'1',
	'import source suppresses login hook and role/database preload settings');
my ($call_result, $call_stdout, $call_stderr) =
  $old->psql('customer', 'SELECT ordinary_function()');
isnt($call_result, 0, 'source invoker SQL function cannot execute');
like(
	$call_stderr,
	qr/function \d+ is not allowed during physical import/,
	'function lookup fence also covers a scalar SQL inlining candidate');
($call_result, $call_stdout, $call_stderr) =
  $old->psql('customer', 'SELECT * FROM ordinary_set()');
isnt($call_result, 0, 'source set-returning SQL function cannot execute');
like(
	$call_stderr,
	qr/function \d+ is not allowed during physical import/,
	'function lookup fence also covers SQL inlining in FROM');
($call_result, $call_stdout, $call_stderr) =
  $old->psql('customer', 'SET binary_upgrade_import = off');
isnt($call_result, 0, 'SQL cannot turn off the import fence');
like(
	$call_stderr,
	qr/cannot be changed without restarting the server/,
	'import mode is a postmaster setting');
$old->stop;
$old->append_conf('postgresql.conf',
	"shared_preload_libraries = 'import_missing_shared'\n");
$old->append_conf('postgresql.auto.conf',
	"shared_preload_libraries = 'import_missing_auto'\n");

# Neither arbitrary files in PGDATA nor unknown SLRU entries may be copied.
append_to_file($old->data_dir . '/not_a_relation', "untrusted\n");
append_to_file($old->data_dir . '/pg_xact/not_a_segment', "untrusted\n");
my $heap_hash = file_hash($old->data_dir . "/$heap");
my $index_hash = file_hash($old->data_dir . "/$index");

chdir $PostgreSQL::Test::Utils::tmp_check;
my @upgrade = (
	'pg_upgrade', '--no-sync', '--retain', '--import',
	'--old-datadir' => $old->data_dir,
	'--new-datadir' => $new->data_dir,
	'--old-bindir' => $old->config_data('--bindir'),
	'--new-bindir' => $new->config_data('--bindir'),
	'--socketdir' => $new->host,
	'--old-port' => $old->port,
	'--new-port' => $new->port);

command_checks_all(\@upgrade, 1,
	[qr/extension "import_fixture" requires --import-extension/],
	[], 'unapproved source extension is rejected');
command_ok([ @upgrade, '--import-extension=import_fixture' ],
	'import reconstructs catalogs and transfers physical relations');

is(file_hash($new->data_dir . "/$heap"),
	$heap_hash, 'heap file is transferred byte for byte');
is(file_hash($new->data_dir . "/$index"),
	$index_hash, 'index file is transferred byte for byte');
ok(!-e $new->data_dir . '/not_a_relation', 'unknown PGDATA file is absent');
ok(!-e $new->data_dir . '/pg_xact/not_a_segment',
	'unknown SLRU file is absent');

$new->start;
is( $new->safe_psql(
		'customer', q{SELECT length(body), md5(body) FROM payload}),
	$payload,
	'heap and external TOAST data are readable');
is( $new->safe_psql(
		'customer', q{
SET enable_seqscan = off;
SELECT id FROM payload WHERE id = 1;
}),
	'1',
	'transferred index supports lookup');
is($new->safe_psql('customer', q{SELECT nextval('imported_seq')}),
	'43', 'sequence state is retained');
is($new->safe_psql('customer', q{SELECT ordinary_function()}),
	'42', 'invoker SQL function is retained');
is( $new->safe_psql(
		'customer', q{
SELECT count(*) FROM pg_proc WHERE proname IN
  ('unsafe_definer', 'unsafe_native', 'unsafe_leakproof', 'forged_member');
}),
	'0',
	'unsafe functions and forged extension member are absent');
is($new->safe_psql('customer', q{SELECT provider_answer()}),
	'provider', 'extension SQL came from the provider package');
is( $new->safe_psql(
		'customer', q{
SELECT prosecdef FROM pg_proc WHERE proname = 'provider_answer';
}),
	't',
	'fresh provider SECURITY DEFINER function is retained');
is($new->safe_psql('customer', q{SELECT count(*) FROM pg_event_trigger}),
	'0', 'login event trigger is absent');
is( $new->safe_psql(
		'customer', q{
SELECT count(*) FROM pg_roles WHERE rolname = 'source_admin';
}),
	'0',
	'source superuser is absent');
is( $new->safe_psql(
		'customer', q{
SELECT rolcanlogin OR rolsuper OR rolcreaterole OR rolcreatedb OR
       rolreplication OR rolbypassrls FROM pg_roles WHERE rolname = 'imported_owner';
}),
	'f',
	'import owner has no login or administrative attributes');
is( $new->safe_psql(
		'customer', q{
SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'payload'::regclass;
}),
	'imported_owner',
	'relation ownership is normalized');
is( $new->safe_psql(
		'customer', q{
SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname = 'ordinary_function';
}),
	'imported_owner',
	'function ownership is normalized');
is($new->safe_psql('customer', q{SELECT count(*) FROM pg_db_role_setting}),
	'0', 'role and database settings are absent');
is( $new->safe_psql(
		'customer', q{
SELECT relacl IS NULL FROM pg_class WHERE oid = 'payload'::regclass;
}),
	't',
	'source ACL is absent');
$new->stop;

# Metadata preflight failures precede destructive target changes. A restore
# dependency failure below deliberately leaves a disposable partial target.
my $bad = PostgreSQL::Test::Cluster->new('unsupported_old');
my $empty = PostgreSQL::Test::Cluster->new('unsupported_new');
$bad->init;
$empty->init;
$bad->start;
$bad->safe_psql('postgres', 'CREATE DATABASE customer');
$bad->safe_psql(
	'customer', q{
CREATE TYPE user_enum AS ENUM ('a');
CREATE TABLE unsupported(v user_enum);
});
$bad->stop;
my @unsupported = (
	'pg_upgrade', '--no-sync', '--import',
	'--old-datadir' => $bad->data_dir,
	'--new-datadir' => $empty->data_dir,
	'--old-bindir' => $bad->config_data('--bindir'),
	'--new-bindir' => $empty->config_data('--bindir'),
	'--socketdir' => $empty->host,
	'--old-port' => $bad->port,
	'--new-port' => $empty->port);
command_checks_all(\@unsupported, 1,
	[qr/import prototype requires heap storage and built-in column types/],
	[], 'user column type is rejected before transfer');

append_to_file($bad->data_dir . '/pg_twophase/00000001', 'untrusted');
command_checks_all(\@unsupported, 1,
	[qr/requires an empty pg_twophase directory/],
	[], 'two-phase state is rejected before source startup');
unlink $bad->data_dir . '/pg_twophase/00000001' or die $!;

append_to_file($bad->data_dir . '/recovery.signal', '');
command_fails(\@unsupported, 'source requiring recovery is rejected');
my @logs =
  glob($empty->data_dir . '/pg_upgrade_output.d/*/log/pg_upgrade_server.log');
like(
	join('', map { slurp_file($_) } @logs),
	qr/physical import requires a clean shutdown without recovery files/,
	'recovery rejection is the intended startup fence');
unlink $bad->data_dir . '/recovery.signal' or die $!;

$bad->start;
$bad->stop('immediate');
command_checks_all(\@unsupported, 1,
	[qr/source cluster was not shut down cleanly/],
	[], 'crash recovery is refused before starting the source');

# Normal startup here recovers our own test fixture, outside import mode.
$bad->start;
$bad->safe_psql(
	'customer', q{
DROP TABLE unsupported;
DROP TYPE user_enum;
CREATE TABLE extension_data(v integer);
ALTER EXTENSION plpgsql ADD TABLE extension_data;
});
$bad->stop;
command_checks_all(\@unsupported, 1,
	[qr/extension-owned relations are not supported/],
	[], 'claimed extension data is refused even for an approved extension');

$bad->start;
$bad->safe_psql(
	'customer', q{
ALTER EXTENSION plpgsql DROP TABLE extension_data;
DROP TABLE extension_data;
CREATE FUNCTION required_definer() RETURNS integer
  LANGUAGE sql SECURITY DEFINER AS $$SELECT 1$$;
CREATE TABLE dependent(v integer DEFAULT required_definer());
});
$bad->stop;
command_fails(\@unsupported,
	'dependency on an omitted definer function fails restoration');
my @restore_logs =
  glob($empty->data_dir . '/pg_upgrade_output.d/*/log/pg_upgrade_dump_*.log');
like(
	join('', map { slurp_file($_) } @restore_logs),
	qr/function public.required_definer\(\) does not exist/,
	'restore failed for the omitted dependency');

command_checks_all(
	[ 'pg_upgrade', '--import-extension=anything' ], 1,
	[qr/--import-extension requires --import/], [],
	'extension approval requires import mode');
command_checks_all(
	[ @unsupported, '--link' ],
	1, [qr/--import requires copy, clone or copy-file-range mode/],
	[], 'link mode is rejected');

done_testing();

sub file_hash
{
	my ($path) = @_;
	open my $fh, '<:raw', $path or die "open $path: $!";
	my $hash = Digest::SHA->new(256)->addfile($fh)->hexdigest;
	close $fh;
	return $hash;
}
