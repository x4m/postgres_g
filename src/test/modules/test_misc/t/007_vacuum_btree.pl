# Copyright (c) 2024, PostgreSQL Global Development Group

# This test verifies that B-tree vacuum can restart read stream.
# To do so we need to insert some data during vacuum. So we wait in injection point
# after first vacuum scan. During this wait we insert some data forcing page split.
# this split will trigger relation extension and subsequent read_stream_reset().

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize postgres
my $psql_err = '';
my $psql_out = '';
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;

# This ensures autovacuum do not run
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# From this point, vacuum worker will wait at startup.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('nbtree-vacuum-2', 'wait');");

my $psql_session = $node->background_psql('postgres');

$psql_session->query_until(
	qr/starting_bg_psql/,
		q(\echo starting_bg_psql
		create table a as select random() r from generate_series(1,100) x;
		create index on a(r);
		delete from a;
		vacuum a;
	));

#print $node->safe_psql('postgres','select * from pg_stat_activity');

# Wait until an vacuum worker starts.
$node->wait_for_event('client backend', 'nbtree-vacuum-2');

$node->safe_psql('postgres',
	"SELECT injection_points_attach('nbtree-vacuum-1', 'wait');");

# Here's the key point of a test: during vacuum we add some page splits.
# This will force vacuum into doing another scan thus reseting read stream.
$node->safe_psql('postgres',
	"insert into a select x from generate_series(1,3000) x;");

$node->safe_psql('postgres',
	"SELECT injection_points_detach('nbtree-vacuum-2');");
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('nbtree-vacuum-2');");

# Observe that second scan is reached.
$node->wait_for_event('client backend', 'nbtree-vacuum-1');

$node->safe_psql('postgres',
	"SELECT injection_points_detach('nbtree-vacuum-1');");
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('nbtree-vacuum-1');");

ok($psql_session->quit);

done_testing();
