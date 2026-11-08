# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise catalog reconstruction independently of authority-test fixtures.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'physical import prototype needs Unix sockets'
  if $windows_os;

my $old = PostgreSQL::Test::Cluster->new('edges_old');
my $new = PostgreSQL::Test::Cluster->new('edges_new');
$old->init;
$new->init;
$old->start;
my $quoted_db = q{customer "quote"; --};
$old->safe_psql(
	'postgres', q{
CREATE ROLE imported_owner NOSUPERUSER;
CREATE DATABASE customer TEMPLATE template0;
CREATE DATABASE "customer ""quote""; --" TEMPLATE template0;
});
$old->safe_psql(
	$quoted_db, q{
CREATE TABLE payload(v text);
INSERT INTO payload VALUES ('quoted database survived');
});
$old->safe_psql(
	'customer', q{
CREATE TABLE payload(v integer);
INSERT INTO payload VALUES (42);
CREATE VIEW role_projection AS
  SELECT rolname, rolsuper FROM pg_catalog.pg_authid;
CREATE RULE change_role AS ON UPDATE TO role_projection DO INSTEAD
  UPDATE pg_catalog.pg_authid SET rolsuper = NEW.rolsuper
  WHERE rolname = OLD.rolname;
UPDATE role_projection SET rolsuper = true WHERE rolname = 'imported_owner';
});
is( $old->safe_psql(
		'postgres',
		q{SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner'}),
	't',
	'view/rule positive control uses the source owner authority');
$old->safe_psql('postgres', 'ALTER ROLE imported_owner NOSUPERUSER');
$old->stop;

chdir $PostgreSQL::Test::Utils::tmp_check;
command_ok(upgrade_args($old, $new),
	'import handles a quoted database name and normalized view/rule owners');
$new->start;
is( $new->safe_psql($quoted_db, 'SELECT v FROM payload'),
	'quoted database survived',
	'database metadata and restore connection preserve identifier quoting');
is($new->safe_psql('customer', 'SELECT v FROM payload'),
	'42', 'ordinary customer data survives');
$new->safe_psql('postgres', 'ALTER ROLE import_maintenance LOGIN');
my ($rc, $out, $err) = $new->psql(
	'customer',
	q{UPDATE role_projection SET rolsuper = true WHERE rolname = 'imported_owner'},
	extra_params =>
	  [ '--username=import_maintenance', '--set=VERBOSITY=verbose' ]);
isnt($rc, 0, 'view/rule cannot retain source superuser authority');
like(
	$err,
	qr/42501:.*permission denied for table pg_authid/s,
	'view/rule fails at the protected catalog permission check');
is( $new->safe_psql(
		'postgres',
		q{SELECT rolsuper FROM pg_roles WHERE rolname = 'imported_owner'}),
	'f',
	'view/rule left the normalized owner unprivileged');
$new->stop;

# With no customer databases, none of the source service contents is needed.
my $service = PostgreSQL::Test::Cluster->new('services_only_old');
my $fresh = PostgreSQL::Test::Cluster->new('services_only_new');
$service->init;
$fresh->init;
$service->start;
for my $db ('postgres', 'template1')
{
	$service->safe_psql($db,
		q{CREATE TABLE service_marker(v integer); INSERT INTO service_marker VALUES (1)}
	);
}
$service->stop;
command_ok(upgrade_args($service, $fresh),
	'import accepts a cluster with no customer databases');
$fresh->start;
is( $fresh->safe_psql(
		'postgres',
		q{SELECT string_agg(datname, ',' ORDER BY datname) FROM pg_database}),
	'postgres,template0,template1',
	'only fresh service databases remain');
for my $db ('postgres', 'template1')
{
	is( $fresh->safe_psql(
			$db,
			q{SELECT count(*) FROM pg_class WHERE relname = 'service_marker'}
		),
		'0',
		"$db contains no source service table");
}
$fresh->stop;

done_testing();

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
