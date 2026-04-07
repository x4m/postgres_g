# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Demonstrate the production scenario reported in bug #19382:
#
#   A PL/pgSQL function (lock_cluster) calls a SETOF SQL function
#   (as_cluster_with_labels) that returns a named composite type.  The SQL
#   function is declared SECURITY DEFINER so the planner cannot inline it;
#   the outer plan therefore carries an RTE_FUNCTION node whose result type
#   depends on the composite.
#
# Before the fix: ALTER TYPE ADD ATTRIBUTE in a concurrent backend left the
# outer plan stale.  The next call from backend A produced:
#
#   ERROR:  structure of query does not match function result type
#   DETAIL:  Number of returned columns (N) does not match expected
#            column count (N+1).
#   CONTEXT:  PL/pgSQL function lock_cluster(...) at RETURN QUERY
#
# After the fix: the composite type's underlying relation OID is recorded in
# the plan's relationOids list, so the relcache invalidation sent by ALTER
# TYPE reaches the plan and triggers a replan.  The next call succeeds and
# returns the new attribute.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('composite_plpgsql_caller');
$node->init;
$node->start;

# ---------- schema setup ----------
$node->safe_psql(
	'postgres', q(
CREATE TYPE cluster_t AS (id int, name text);

-- Typed table: ALTER TYPE ... ADD ATTRIBUTE ... CASCADE will rewrite it,
-- allowing as_cluster_with_labels to see the new column via SELECT *.
CREATE TABLE clusters OF cluster_t;
INSERT INTO clusters VALUES (1, 'primary');

-- SECURITY DEFINER prevents the planner from inlining the function body
-- into lock_cluster's plan.  Without inlining, the outer plan only sees an
-- RTE_FUNCTION node; before the fix its relationOids did not include the
-- composite type's relid, so ALTER TYPE invalidation never reached it.
CREATE FUNCTION as_cluster_with_labels(v text)
  RETURNS SETOF cluster_t LANGUAGE sql STABLE SECURITY DEFINER AS
  $$ SELECT * FROM clusters WHERE name = v $$;

-- PL/pgSQL caller.  Its RETURN QUERY plan is compiled and cached after the
-- first call.  The fix ensures this plan is invalidated when the composite
-- type changes, so the second call replans and returns the new attribute.
CREATE FUNCTION lock_cluster(v text) RETURNS SETOF cluster_t LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT cl.* FROM as_cluster_with_labels(v) cl;
END;
$$;
));

# ---------- backend A: warm up the plan ----------
# Keep a persistent session so the cached plan survives between calls.
my $session_a = $node->background_psql('postgres');

chomp(my $before =
	  $session_a->query_safe(q(SELECT * FROM lock_cluster('primary'))));
is($before, "1|primary",
	'backend A: lock_cluster works before migration');

# ---------- backend B: migration (ALTER TYPE ADD ATTRIBUTE) ----------
# This simulates a schema migration run by a separate service or tool.
# The relcache invalidation is broadcast to all backends; the fix ensures
# it reaches the cached plan inside lock_cluster on backend A.
$node->safe_psql(
	'postgres', q(
ALTER TYPE cluster_t ADD ATTRIBUTE label text CASCADE;
UPDATE clusters SET label = 'prod-label';
));

# ---------- backend A: second call must succeed with new attribute ----------
chomp(my $after =
	  $session_a->query_safe(q(SELECT * FROM lock_cluster('primary'))));
is( $after,
	"1|primary|prod-label",
	'backend A: lock_cluster returns new attribute after ALTER TYPE in backend B'
);

ok($session_a->quit);

$node->stop;
done_testing();
