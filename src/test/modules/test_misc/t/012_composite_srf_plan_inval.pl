# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A cached plan for a query that scans a set-returning function (SRF)
# returning a named composite type must be invalidated when ALTER TYPE adds
# an attribute to that type.
#
# Before the fix, the composite type's underlying relation OID (typrelid) was
# not recorded in the plan's invalItems.  ALTER TYPE sent a relcache
# invalidation for typrelid, but since it was absent from invalItems the plan
# was never marked stale.  A subsequent EXECUTE silently returned stale data,
# omitting the newly added column.
#
# With the fix the plan is correctly invalidated.  EXECUTE then raises
# "cached plan must not change result type" instead of returning wrong data;
# after re-preparing the query returns all columns correctly.
#
# SECURITY DEFINER prevents the planner from inlining the function body into
# the outer plan.  Without inlining the outer plan holds only the proc OID in
# its invalItems, not the underlying table OID; therefore only the typrelid
# dependency added by the fix triggers invalidation of the outer plan.

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
-- SECURITY DEFINER prevents inlining so the outer plan only has the proc OID
-- in invalItems, not the table OID.
CREATE FUNCTION planinv_srf() RETURNS SETOF planinv_ct
  LANGUAGE sql STABLE SECURITY DEFINER AS $$ SELECT * FROM planinv_tbl $$;
));

# Keep one persistent session so the prepared statement survives
# across the ALTER TYPE that happens in a separate connection below.
my $session = $node->background_psql('postgres');

$session->query_safe(
	'PREPARE planinv_p AS SELECT * FROM planinv_srf()');

chomp(my $before = $session->query_safe('EXECUTE planinv_p'));
is($before, '1|2', 'prepared SRF scan before ALTER TYPE');

# ALTER TYPE in a separate connection (simulates a concurrent migration).
# ALTER TABLE adds the column to the underlying storage so the function body
# can actually return a value for the new attribute.
$node->safe_psql('postgres', q(
ALTER TYPE planinv_ct ADD ATTRIBUTE c int;
ALTER TABLE planinv_tbl ADD COLUMN c int DEFAULT 99;
));

# The plan is now stale.  EXECUTE must raise an error rather than silently
# returning the old two-column result without the new 'c' attribute.
eval { $session->query_safe('EXECUTE planinv_p') };
like(
	$@,
	qr/cached plan must not change result type/,
	'stale plan detected: error raised instead of returning wrong data');

# Re-prepare and verify that all three columns are returned correctly.
$session->query_safe('DEALLOCATE planinv_p');
$session->query_safe(
	'PREPARE planinv_p AS SELECT * FROM planinv_srf()');
chomp(my $after = $session->query_safe('EXECUTE planinv_p'));
is($after, '1|2|99',
	'correct three-column result after re-preparing the statement');

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
