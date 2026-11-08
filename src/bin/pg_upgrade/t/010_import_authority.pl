# Copyright (c) 2026, PostgreSQL Global Development Group

# Controlled TAP/SQL fixtures for the import authority boundary. These are
# neither hostile native-code programs nor a container-escape test harness.
use strict;
use warnings FATAL => 'all';

use File::Path qw(make_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'physical import prototype needs Unix sockets'
  if $windows_os;

my $provider = PostgreSQL::Test::Utils::tempdir();
make_path("$provider/extension");
append_to_file(
	"$provider/extension/repl_mon.control", q{
default_version = '1.0'
relocatable = true
});
append_to_file(
	"$provider/extension/repl_mon--1.0.sql", q{
CREATE TABLE service_private(v integer);
INSERT INTO service_private VALUES (876);
CREATE TYPE service_private_type AS ENUM ('service');
});

my $old = PostgreSQL::Test::Cluster->new('authority_old');
my $new = PostgreSQL::Test::Cluster->new('authority_new');
$old->init;
$new->init;
$old->append_conf('postgresql.conf',
	"extension_control_path = '\$system:$provider'\n");
$old->start;
$old->safe_psql(
	'postgres', q{
CREATE ROLE imported_owner NOSUPERUSER;
CREATE ROLE source_escape SUPERUSER NOLOGIN;
GRANT source_escape TO imported_owner WITH INHERIT FALSE, SET TRUE;
CREATE DATABASE customer TEMPLATE template0;
CREATE EXTENSION repl_mon;
});
$old->safe_psql(
	'template1', q{
CREATE TABLE template_marker(v integer);
ALTER DATABASE template0 ALLOW_CONNECTIONS true;
});
$old->safe_psql('template0', q{CREATE TABLE template_zero_marker(v integer)});

$old->safe_psql(
	'customer', q{
CREATE TABLE payload(id integer PRIMARY KEY, body text);
INSERT INTO payload VALUES (1, 'customer data');
CREATE FUNCTION give_me_su() RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
  ALTER ROLE imported_owner SUPERUSER;
  RETURN 1;
END$$;
CREATE FUNCTION definer_payload() RETURNS integer LANGUAGE plpgsql
  SECURITY DEFINER AS $$BEGIN ALTER ROLE imported_owner SUPERUSER; RETURN 1; END$$;
CREATE FUNCTION native_alias() RETURNS language_handler
  AS '$libdir/plpgsql', 'plpgsql_call_handler' LANGUAGE c;
CREATE FUNCTION forged_member() RETURNS integer LANGUAGE sql
  SECURITY DEFINER AS $$SELECT 1$$;
ALTER EXTENSION plpgsql ADD FUNCTION forged_member();
});

# A payload that cannot grant SU in the positive control would be a weak oracle.
is($old->safe_psql('customer', 'SELECT give_me_su()'),
	'1', 'positive control executes the SQL payload as source superuser');
is( $old->safe_psql(
		'template1', q{
SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner';
}),
	't',
	'positive control really granted SU');
$old->safe_psql('template1', 'ALTER ROLE imported_owner NOSUPERUSER');

$old->safe_psql(
	'customer', q{
CREATE FUNCTION login_payload() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN ALTER ROLE imported_owner SUPERUSER; END$$;
CREATE FUNCTION ddl_payload() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN ALTER ROLE imported_owner SUPERUSER; END$$;
CREATE EVENT TRIGGER ddl_payload ON ddl_command_start EXECUTE FUNCTION ddl_payload();
CREATE TABLE ddl_control(v integer);
CREATE EVENT TRIGGER login_payload ON login EXECUTE FUNCTION login_payload();
});
is( $old->safe_psql(
		'template1', q{
SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner';
}),
	't',
	'DDL trigger positive control granted SU');
$old->safe_psql('template1', 'ALTER ROLE imported_owner NOSUPERUSER');
$old->safe_psql('customer', 'SELECT 1');
is( $old->safe_psql(
		'template1', q{
SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner';
}),
	't',
	'login trigger positive control granted SU');
$old->safe_psql(
	'template1', q{
ALTER ROLE imported_owner NOSUPERUSER;
ALTER DATABASE customer IS_TEMPLATE true;
ALTER DATABASE postgres RENAME TO returned_service;
});
$old->stop;
$old->append_conf('postgresql.conf',
	"shared_preload_libraries = 'not_a_provider_library'\n");
$old->append_conf('postgresql.auto.conf',
	"session_preload_libraries = 'not_a_provider_library'\n");
append_to_file($old->data_dir . '/not_a_database_file', 'untrusted');

chdir $PostgreSQL::Test::Utils::tmp_check;
command_ok(upgrade_args($old, $new),
	'import ignores service databases and suppresses SQL payloads');
$new->start;
is( $new->safe_psql('customer', 'SELECT body FROM payload WHERE id = 1'),
	'customer data',
	'customer data survives despite source template flag');
is( $new->safe_psql(
		'customer', q{
SELECT count(*) FROM pg_proc WHERE proname IN ('definer_payload', 'native_alias', 'forged_member');
}),
	'0',
	'definer/native/forged extension definitions were omitted');
is($new->safe_psql('customer', 'SELECT count(*) FROM pg_event_trigger'),
	'0', 'login and DDL triggers were omitted');
is( $new->safe_psql(
		'postgres', q{
SELECT count(*) FROM pg_roles WHERE rolsuper AND rolname <> current_user;
}),
	'0',
	'no unexpected superuser after importing attack fixtures');
is( $new->safe_psql(
		'postgres', q{
SELECT count(*) FROM pg_roles WHERE rolname = 'source_escape';
}),
	'0',
	'source SET-only path to a superuser role was not imported');
for my $db ('postgres', 'template1')
{
	is( $new->safe_psql(
			$db, q{
SELECT count(*) FROM pg_class WHERE relname IN
  ('service_private', 'template_marker', 'template_zero_marker');
}),
		'0',
		"$db contains no returned service objects");
	is( $new->safe_psql(
			$db,
			q{SELECT count(*) FROM pg_extension WHERE extname = 'repl_mon'}),
		'0',
		"$db did not import the source service extension");
}
is( $new->safe_psql(
		'postgres', q{
SELECT datallowconn FROM pg_database WHERE datname = 'template0';
}),
	'f',
	'fresh template0 keeps provider connection policy');
is( $new->safe_psql(
		'customer', q{
SELECT datistemplate FROM pg_database WHERE datname = current_database();
}),
	'f',
	'untrusted template flag is not restored on the user database');
ok( !-e $new->data_dir . '/not_a_database_file',
	'unrecognized PGDATA file is absent');
is( $new->safe_psql(
		'postgres', q{
SELECT rolcanlogin FROM pg_roles WHERE rolname = 'import_maintenance';
}),
	'f',
	'job login is disabled after import');

# The trusted test controller enables the bounded job login for direct checks.
# This models arbitrary SQL emitted by a compromised extractor, bypassing
# pg_dump's object filtering but not the target's authority checks.
$new->safe_psql('postgres', q{ALTER ROLE import_maintenance LOGIN});
my $root_literal =
  $new->safe_psql('postgres', 'SELECT quote_literal(current_user)');
denied(
	'SELECT give_me_su()',
	qr/42501:.*(?:permission denied|must be superuser)/s,
	'imported invoker payload cannot grant SU under customer authority');
is( $new->safe_psql(
		'postgres', q{
SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner';
}),
	'f',
	'failed SQL payload left target ownership role unprivileged');
$new->safe_psql(
	'customer', q{
CREATE SCHEMA provider_private;
CREATE TABLE provider_private.internal_state(v integer);
CREATE FUNCTION provider_private.upgrade_wrapper() RETURNS void LANGUAGE sql
  SECURITY DEFINER AS $$SELECT pg_catalog.binary_upgrade_set_next_pg_authid_oid(10)$$;
ALTER EXTENSION plpgsql ADD FUNCTION provider_private.upgrade_wrapper();
GRANT USAGE ON SCHEMA provider_private TO import_maintenance;
});
my $provider_oid = $new->safe_psql('customer',
	q{SELECT 'provider_private.internal_state'::regclass::oid});
$new->stop;
$new->start(options =>
	  '-b -c binary_upgrade_import=on -c binary_upgrade_import_trusted_extensions=on'
);

my ($rc, $out, $err) = maintenance(
	q{
SELECT current_user, session_user;
SET ROLE imported_owner;
SELECT current_user;
RESET ROLE;
RESET SESSION AUTHORIZATION;
SELECT current_user, session_user;
SELECT pg_catalog.binary_upgrade_set_next_heap_pg_class_oid(500000);
SELECT pg_catalog.binary_upgrade_set_next_heap_relfilenode(500000);
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid(500001);
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid(500002);
CREATE TABLE ordinary_maintenance(v integer);
DROP TABLE ordinary_maintenance;
});
is($rc, 0, 'maintenance can perform ordinary owner-level DDL');
is( $out,
	"import_maintenance|import_maintenance\nimported_owner\nimport_maintenance|import_maintenance\n\n\n\n",
	'role and session reset stay within the authenticated customer ceiling');

my @attempts = (
	[
		"SET ROLE $root_literal",
		qr/42501:/,
		'SET ROLE cannot reach provider root'
	],
	[
		"SET SESSION AUTHORIZATION $root_literal",
		qr/42501:/,
		'session authorization cannot reach provider root'
	],
	[
		'ALTER ROLE imported_owner SUPERUSER',
		qr/42501:/,
		'cannot grant SUPERUSER'
	],
	[
		'ALTER ROLE import_maintenance CREATEROLE',
		qr/42501:/,
		'cannot grant CREATEROLE'
	],
	[
		'ALTER ROLE import_maintenance REPLICATION',
		qr/42501:/,
		'cannot grant REPLICATION'
	],
	[
		'ALTER ROLE import_maintenance BYPASSRLS',
		qr/42501:/,
		'cannot grant BYPASSRLS'
	],
	[
		'GRANT pg_execute_server_program TO import_maintenance',
		qr/42501:/,
		'cannot acquire server program privilege'
	],
	[
		'UPDATE pg_catalog.pg_authid SET rolsuper = true WHERE rolname = current_user',
		qr/42501:/,
		'cannot update protected role catalog'
	],
	[
		q{CREATE FUNCTION bad_native() RETURNS integer AS '/not/a/provider/library', 'payload' LANGUAGE c},
		qr/42501:/,
		'C declaration fails by permission before library loading'
	],
	[
		q{CREATE FUNCTION bad_internal(integer) RETURNS integer AS 'int4inc' LANGUAGE internal},
		qr/42501:/,
		'internal-language declaration is denied'
	],
	[
		q{CREATE FUNCTION bad_definer() RETURNS integer LANGUAGE sql SECURITY DEFINER AS $$SELECT 1$$},
		qr/42501:.*source SECURITY DEFINER/s,
		'source definer declaration is denied'
	],
	[
		q{ALTER FUNCTION give_me_su() SECURITY DEFINER},
		qr/42501:.*source SECURITY DEFINER/s,
		'ALTER cannot reintroduce source definer authority'
	],
	[
		q{CREATE EVENT TRIGGER bad_login ON login EXECUTE FUNCTION login_payload()},
		qr/42501:/,
		'cannot register a login trigger'
	],
	[
		q{ALTER EXTENSION plpgsql ADD FUNCTION give_me_su()},
		qr/42501:/,
		'cannot forge provider extension membership'
	],
	[
		q{CREATE EXTENSION any_source_selected_extension},
		qr/42501:.*import coordinator/s,
		'source SQL cannot install an extension'
	],
	[
		q{SELECT pg_catalog.binary_upgrade_set_next_pg_authid_oid(10)},
		qr/42501:.*upgrade helper/s,
		'role OID override is not a maintenance capability'
	],
	[
		q{SELECT pg_catalog.binary_upgrade_set_next_heap_pg_class_oid(1259)},
		qr/42501:.*non-system object identifier/s,
		'cannot assign a system relation OID'
	],
	[
		q{SELECT pg_catalog.binary_upgrade_set_missing_value('pg_authid'::regclass, 'rolsuper', 't')},
		qr/42501:.*upgrade helper/s,
		'generic missing-value catalog helper is denied'
	],
	[
		q{SELECT pg_catalog.binary_upgrade_import_set_relation_frozen('pg_authid'::regclass, '3', '1')},
		qr/42501:.*outside the import maintenance boundary/s,
		'frozen metadata helper refuses system catalogs'
	],
	[
		qq{SELECT pg_catalog.binary_upgrade_import_set_relation_frozen('$provider_oid', '3', '1')},
		qr/42501:.*outside the import maintenance boundary/s,
		'frozen metadata helper refuses provider-owned user-range OIDs'
	],
	[
		q{SELECT provider_private.upgrade_wrapper()},
		qr/42501:.*upgrade helper/s,
		'helper checks authenticated ceiling through a provider definer'
	],
	[
		q{SET binary_upgrade_import = off},
		qr/55P02:/,
		'cannot disable import mode from SQL'
	],);
for my $attempt (@attempts)
{
	denied(@$attempt);
}
# A rejected operation must not first acquire an exclusive lock on provider
# data. Hold a reader lock so a misplaced ownership check gives 55P03 instead.
my $blocker = $new->background_psql('customer');
$blocker->query_safe(
	'BEGIN; SELECT count(*) FROM provider_private.internal_state');
denied(
	"SET lock_timeout = '100ms'; SELECT pg_catalog.binary_upgrade_import_set_relation_frozen('$provider_oid', '3', '1')",
	qr/42501:.*outside the import maintenance boundary/s,
	'ownership is checked before locking a provider relation');
$blocker->query_safe('ROLLBACK');
$blocker->quit;

is( $new->safe_psql(
		'postgres', q{
SELECT count(*) FROM pg_roles WHERE rolsuper AND rolname <> current_user;
}),
	'0',
	'direct SQL attempts left no unexpected superuser');
$new->stop;

# Force an immutable function call during index expression processing. A
# failure must be at the intended fence, not an unrelated startup problem.
my $bad = PostgreSQL::Test::Cluster->new('expression_old');
my $empty = PostgreSQL::Test::Cluster->new('expression_new');
$bad->init;
$empty->init;
$bad->start;
$bad->safe_psql(
	'postgres', q{
CREATE ROLE imported_owner NOSUPERUSER;
CREATE DATABASE customer TEMPLATE template0;
});
$bad->safe_psql(
	'customer', q{
CREATE FUNCTION volatile_payload() RETURNS integer LANGUAGE plpgsql AS $$
BEGIN ALTER ROLE imported_owner SUPERUSER; RETURN 1; END$$;
CREATE FUNCTION immutable_payload() RETURNS integer LANGUAGE sql IMMUTABLE
  AS $$SELECT public.volatile_payload()$$;
CREATE TABLE payload(id integer);
CREATE INDEX expression_payload ON payload ((immutable_payload()));
});
is( $bad->safe_psql(
		'postgres',
		q{SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner'}),
	't',
	'immutable expression positive control executed during source DDL');
$bad->safe_psql('postgres', 'ALTER ROLE imported_owner NOSUPERUSER');
$bad->stop;
command_fails(upgrade_args($bad, $empty),
	'immutable SQL callback fails closed during import');
my @restore_logs =
  glob($empty->data_dir . '/pg_upgrade_output.d/*/log/pg_upgrade_dump_*.log');
like(
	join('', map { slurp_file($_) } @restore_logs),
	qr/function \d+ is not allowed during physical import/,
	'immutable callback failure is the intended execution fence');
$empty->start;
is( $empty->safe_psql(
		'postgres',
		q{SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner'}),
	'f',
	'failed expression restore did not grant SU');
$empty->stop;

done_testing();

sub maintenance
{
	my ($sql) = @_;
	return $new->psql('customer', $sql,
		extra_params =>
		  [ '--username=import_maintenance', '--set=VERBOSITY=verbose' ]);
}

sub denied
{
	my ($sql, $pattern, $label) = @_;
	my ($result, $stdout, $stderr) = maintenance($sql);
	isnt($result, 0, $label);
	like($stderr, $pattern, "$label: intended error");
}

sub upgrade_args
{
	my ($source, $target) = @_;
	return [
		'pg_upgrade', '--import', '--copy', '--no-sync', '--retain',
		'--old-datadir' => $source->data_dir,
		'--new-datadir' => $target->data_dir,
		'--old-bindir' => $source->config_data('--bindir'),
		'--new-bindir' => $target->config_data('--bindir'),
		'--socketdir' => $target->host,
		'--old-port' => $source->port,
		'--new-port' => $target->port,
	];
}
