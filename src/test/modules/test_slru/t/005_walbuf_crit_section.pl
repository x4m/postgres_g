# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Detection test for "Get rid of WALBufMappingLock" (bc22dc0e0dd).
#
# Kirill Reshke and Andrey Borodin showed that WaitEventSetWait() must not be
# called inside a critical section.  With
#
#   Assert(CritSectionCount == 0);
#
# added to WaitEventSetWait() (see waiteventset.c), the next WAL inserter that
# hits AdvanceXLInsertBuffer()'s InitializedUpToCondVar wait trips the assert.
#
# injection_points_stall_wal_buffer_init() forces that wait deterministically.
#
# NOT intended for upstream commit.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);

use Test::More;

if (($ENV{enable_injection_points} // '') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('detect');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'injection_points'
wal_writer_delay = 10s
});
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));

if ($node->safe_psql('postgres', q(SELECT injection_points_cassert_enabled())) ne 't')
{
	plan skip_all => 'needs --enable-cassert build with CritSectionCount assert in WaitEventSetWait';
}

# Ensure detection assert is enabled (corruption test 004 disables it).
$node->safe_psql('postgres',
	q{SELECT injection_points_walbuf_crit_section_assert(true)});

$node->safe_psql(
	'postgres', q{
	CREATE TABLE t (id int, pad bytea) WITH (fillfactor = 100);
	ALTER TABLE t ALTER COLUMN pad SET STORAGE PLAIN;
});

my $logfile = $node->logfile;

my $p1 = $node->background_psql('postgres', on_error_stop => 0);
$p1->query_until(
	qr/p1_go/, q{
	\echo p1_go
	SELECT injection_points_stall_wal_buffer_init();
	INSERT INTO t
	SELECT g, decode(repeat('ab', 3900), 'hex')
	FROM generate_series(1, 6) g;
});

# Backend should abort on Assert(CritSectionCount == 0) in WaitEventSetWait.
my $deadline = time() + 30;
my $found = 0;
while (time() < $deadline)
{
	open my $fh, '<', $logfile or die "open $logfile: $!";
	while (my $line = <$fh>)
	{
		if ($line =~ /CritSectionCount == 0/)
		{
			$found = 1;
			last;
		}
	}
	close $fh;
	last if $found;
	usleep(200_000);
}

ok($found, 'stall+insert hit WaitEventSetWait inside critical section (assert fired)');
eval { $p1->quit; };
$node->stop('immediate', fail_ok => 1);

done_testing();
