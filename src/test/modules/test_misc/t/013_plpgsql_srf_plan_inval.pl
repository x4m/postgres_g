# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A PL/pgSQL function that calls a SECURITY DEFINER SQL set-returning function
# (SRF) returning a named composite type must not raise "structure of query
# does not match function result type" after ALTER TYPE ADD ATTRIBUTE widens
# the composite in a concurrent session.
#
# The error occurred because the outer PL/pgSQL SPI plan was never invalidated
# (typrelid was absent from its invalItems), while the inner SQL SRF was
# correctly replanned via its table relcache dependency and began returning the
# new wider row.  The mismatch between the stale SPI plan's expected column
# count and the SRF's actual output triggered the error.
#
# With the fix the outer SPI plan is invalidated via the typrelid dependency
# and replanned before the next execution, so the call succeeds and returns
# the new attribute.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('plpgsql_srf_plan_inval');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q(
CREATE TYPE ct AS (a int, b int);
CREATE TABLE t (a int, b int);
INSERT INTO t VALUES (1, 2);

-- SECURITY DEFINER prevents inlining so the outer SPI plan holds only the
-- proc OID, not the table OID.  Without the fix the SPI plan is not
-- invalidated by ALTER TYPE.
CREATE FUNCTION inner_srf() RETURNS SETOF ct
  LANGUAGE sql STABLE SECURITY DEFINER AS $$ SELECT * FROM t $$;

-- PL/pgSQL caller: its RETURN QUERY SPI plan is the one that goes stale.
CREATE FUNCTION outer_fn() RETURNS SETOF ct LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT r.* FROM inner_srf() r;
END; $$;
));

# Backend A: warm up the plan cache.
my $session_a = $node->background_psql('postgres');
chomp(my $before = $session_a->query_safe(q(SELECT * FROM outer_fn())));
is($before, '1|2', 'backend A: outer_fn works before ALTER TYPE');

# Backend B: concurrent migration adds a new attribute.
$node->safe_psql('postgres', q(
ALTER TYPE ct ADD ATTRIBUTE c int;
ALTER TABLE t ADD COLUMN c int DEFAULT 99;
));

# Backend A: without the fix, the stale SPI plan raised:
#   ERROR: structure of query does not match function result type
#   DETAIL: Number of returned columns (2) does not match expected column
#           count (3).
# With the fix the plan is invalidated and the call returns all three columns.
chomp(my $after = $session_a->query_safe(q(SELECT * FROM outer_fn())));
is($after, '1|2|99',
	'backend A: outer_fn returns new column after ALTER TYPE in backend B');

ok($session_a->quit);

$node->safe_psql(
	'postgres', q(
DROP FUNCTION outer_fn();
DROP FUNCTION inner_srf();
DROP TABLE t;
DROP TYPE ct;
));

$node->stop;
done_testing();
