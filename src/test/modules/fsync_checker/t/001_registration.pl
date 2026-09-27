# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('smgr');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'fsync_checker'
smgr_chain = 'fsync_checker, md'
});
$node->start;
is($node->safe_psql('postgres', q{
CREATE TABLE t AS SELECT generate_series(1, 100) AS i;
CHECKPOINT;
SELECT sum(i) FROM t;
}), '5050', 'modifier chain preserves ordinary storage operations');
$node->stop;

# The last element must supply storage.  A malformed list must not silently
# fall back to md, and an overlong list must be rejected before copying IDs.
for my $case (
	['', qr/smgr_chain must not be empty/],
	['md,', qr/invalid list syntax/],
	['md, md', qr/not a modifier/],
	['fsync_checker', qr/last element/],
	[join(', ', ('fsync_checker') x 15, 'md'), qr/between 1 and 15 entries/])
{
	my ($chain, $pattern) = @$case;
	my $offset = -s $node->logfile;

	# pg_ctl also supplies the restricted process token needed on Windows.
	$node->append_conf('postgresql.conf', "smgr_chain = '$chain'\n");
	my $started = $node->start(fail_ok => 1);
	ok(!$started, "invalid chain prevents startup: $chain");
	ok($node->log_contains($pattern, $offset),
		"invalid chain has the expected diagnostic: $chain");
	$node->stop if $started;
}

done_testing();
