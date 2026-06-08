# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Reproducer for data-ahead-of-WAL corruption with "Get rid of WALBufMappingLock"
# (bc22dc0e0dd, reverted by c13070a27b6).
#
# AdvanceXLInsertBuffer() waits on InitializedUpToCondVar inside a critical
# section.  On postmaster death WaitLatch exits and releases locks instead of
# PANIC, so a dirty heap page can be flushed without its WAL record.
#
# Same stall trigger as t/005_walbuf_crit_section.pl, but this test needs the
# backend to survive on WalBufferInit until kill -9.  Skip on --enable-cassert
# builds that include Assert(CritSectionCount == 0) in WaitEventSetWait().
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
if ($windows_os)
{
	plan skip_all => 'Kill9 works unpredictably on Windows';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'injection_points'
autovacuum = off
checkpoint_timeout = 1h
max_wal_size = 8GB
wal_writer_delay = 10s
});
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION pageinspect));

# Allow WalBufferInit wait inside critical section (005 keeps assert enabled).
$node->safe_psql('postgres',
	q{SELECT injection_points_walbuf_crit_section_assert(false)});

my $backup_name = 'backup';
$node->backup($backup_name);
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, $backup_name, has_streaming => 1);
$standby->start;

$node->safe_psql(
	'postgres', q{
	CREATE TABLE t (id int, pad bytea) WITH (autovacuum_enabled = off, fillfactor = 100);
	ALTER TABLE t ALTER COLUMN pad SET STORAGE PLAIN;
	INSERT INTO t SELECT g, '' FROM generate_series(1, 10) g;
	CHECKPOINT;
});
$node->wait_for_catchup($standby);

my $committed_raw = $node->safe_psql('postgres',
	q{SELECT count(*) FROM heap_page_items(get_raw_page('t', 0))});
note "raw line pointers on block 0 before the run: $committed_raw";

# --- p1: stall + INSERT (same trigger as 005) --------------------------------
my $heap_blocks = $node->safe_psql('postgres',
	q{SELECT pg_relation_size('t') / current_setting('block_size')::int});
note "heap blocks in t before insert: $heap_blocks";
my $buf1 = $heap_blocks - 1;

# Open p2 before the WAL gap exists; send flush only after p1 is stuck.
my $p2 = $node->background_psql('postgres', on_error_stop => 0);

my $p1 = $node->background_psql('postgres', on_error_stop => 0);
$p1->query_until(
	qr/p1_go/, q{
	\echo p1_go
	SELECT injection_points_stall_wal_buffer_init();
	INSERT INTO t
	SELECT 999999 + g, decode(repeat('ab', 3900), 'hex')
	FROM generate_series(1, 6) g;
});

# After stall, any backend that writes WAL blocks in WalBufferInit, including
# monitoring queries that touch pgstat.  Give p1 time to reach the CV wait.
usleep(500_000);
note "p1 expected on WalBufferInit inside AdvanceXLInsertBuffer(), buf1=$buf1";

$p2->query_until(
	qr/p2_go/, qq{
	\\echo p2_go
	SELECT injection_points_flush_buffer('t'::regclass, $buf1);
});
usleep(500_000);

# --- kill postmaster; p1 exits on PM death, p2 flushes dirty page --------------
my $pmpid = $node->{_pid};
my @cluster_pids = ($pmpid);
push @cluster_pids, grep { /^\d+$/ } split /\s+/, `pgrep -P $pmpid`;

$node->kill9;
sleep(3);

foreach my $i (1 .. 100)
{
	my @alive = grep { kill 0, $_ } @cluster_pids;
	last unless @alive;
	kill 'KILL', @alive;
	usleep(100_000);
}
eval { $p1->quit; };
eval { $p2->quit; };

$node->poll_start;

my $primary_raw = $node->safe_psql('postgres',
	qq{SELECT count(*) FROM heap_page_items(get_raw_page('t', $buf1))});
my $standby_raw = $standby->safe_psql('postgres',
	qq{SELECT count(*) FROM heap_page_items(get_raw_page('t', $buf1))});

note "raw line pointers on block $buf1 after recovery: primary=$primary_raw standby=$standby_raw (before=$committed_raw)";

cmp_ok($primary_raw, '>', $standby_raw,
	'primary has extra raw tuples on buf1 (data-ahead-of-WAL corruption)');
cmp_ok($primary_raw, '>', $committed_raw,
	'never-WAL-logged insert left a phantom tuple on buf1');

$node->stop;
$standby->stop;

done_testing();
