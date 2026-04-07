# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Cached plans for queries that scan set-returning functions returning a named
# composite type must be invalidated when ALTER TYPE adds attributes, even when
# the function's pg_proc row is not updated (typed table follows the type).
#
# The PREPARE uses "SELECT p FROM planinv_srf() p" (returning the row as a
# composite value) so that the plan's result type is planinv_ct OID rather
# than an expanded column list.  This means ADD ATTRIBUTE does not change the
# result type OID and the replanned prepared statement returns the new
# attribute without a "cached plan must not change result type" error.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('composite_srf_plan_inval');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q(
CREATE TYPE planinv_ct AS (a int, b int);
CREATE TABLE planinv_tbl OF planinv_ct;
INSERT INTO planinv_tbl VALUES (1, 2);
CREATE FUNCTION planinv_srf() RETURNS SETOF planinv_ct
  LANGUAGE sql STABLE AS $$ SELECT * FROM planinv_tbl $$;
));

# Keep one persistent session so that the prepared statement survives
# across the ALTER TYPE that happens in a separate connection below.
my $session = $node->background_psql('postgres');

$session->query_safe(
	'PREPARE planinv_p AS SELECT p FROM planinv_srf() p');

chomp(my $before = $session->query_safe('EXECUTE planinv_p'));
is($before, '(1,2)', 'prepared SRF scan before ALTER TYPE');

# ALTER TYPE in a separate connection (simulates a concurrent migration).
$node->safe_psql('postgres', q(
ALTER TYPE planinv_ct ADD ATTRIBUTE c int CASCADE;
UPDATE planinv_tbl SET c = 99;
));

chomp(my $after = $session->query_safe('EXECUTE planinv_p'));
is($after, '(1,2,99)',
	'prepared statement replans after composite widens without replacing function'
);

$session->query_safe('DEALLOCATE planinv_p');
ok($session->quit);

$node->safe_psql(
	'postgres', q(
DROP FUNCTION planinv_srf();
DROP TABLE planinv_tbl;
DROP TYPE planinv_ct;
));

$node->stop;
done_testing();
