# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A cached plan for a query that scans a set-returning function (SRF)
# returning a named composite type must be invalidated when ALTER TYPE adds
# an attribute to that type.
#
# Before the fix, the composite type's underlying relation OID (typrelid) was
# not recorded in the plan's invalItems.  ALTER TYPE sent a relcache
# invalidation for typrelid, but since it was absent from invalItems the outer
# FunctionScan node was never marked stale.  Its TupleDesc still described
# only the original two columns, so the new third column was silently dropped:
# the prepared statement returned (1,2) instead of (1,2,99).
#
# With the fix the outer plan is correctly invalidated and replanned, and the
# prepared statement returns (1,2,99).
#
# The ALTER TYPE fires in a separate connection to simulate a concurrent
# migration.  A persistent background_psql session holds the prepared
# statement so it survives across the ALTER TYPE.

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
CREATE TABLE planinv_tbl (a int, b int);
INSERT INTO planinv_tbl VALUES (1, 2);
-- SECURITY DEFINER prevents inlining so the outer plan holds only the proc
-- OID in invalItems, not the table OID.  Without the fix the plan is never
-- invalidated and the new column is silently dropped from the result.
CREATE FUNCTION planinv_srf() RETURNS SETOF planinv_ct
  LANGUAGE sql STABLE SECURITY DEFINER AS $$ SELECT * FROM planinv_tbl $$;
));

# Keep one persistent session so the prepared statement survives across the
# ALTER TYPE that happens in a separate connection below.
my $session = $node->background_psql('postgres');

# Use the composite-row form: the result type is planinv_ct OID, which is
# stable across ADD ATTRIBUTE.  This lets the replanned statement return the
# new column without a "cached plan must not change result type" error.
$session->query_safe(
	'PREPARE planinv_p AS SELECT p FROM planinv_srf() p');

chomp(my $before = $session->query_safe('EXECUTE planinv_p'));
is($before, '(1,2)', 'prepared SRF scan before ALTER TYPE');

# ALTER TYPE in a separate connection (simulates a concurrent migration).
# ALTER TABLE exposes the new column in the underlying storage.
$node->safe_psql('postgres', q(
ALTER TYPE planinv_ct ADD ATTRIBUTE c int;
ALTER TABLE planinv_tbl ADD COLUMN c int DEFAULT 99;
));

# Without the fix the plan is not invalidated and the stale FunctionScan
# TupleDesc drops column c, returning (1,2).  With the fix the plan is
# replanned and all three columns are returned.
chomp(my $after = $session->query_safe('EXECUTE planinv_p'));
is($after, '(1,2,99)',
	'prepared statement replans after composite widens: new column returned');

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
