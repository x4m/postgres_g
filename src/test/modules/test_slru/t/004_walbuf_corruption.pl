# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Reproducer for the data corruption that forced the revert of
# "Get rid of WALBufMappingLock" (bc22dc0e0dd, reverted by c13070a27b6).
#
# That commit replaced WALBufMappingLock with a condition variable
# (XLogCtl->InitializedUpToCondVar) that is waited on *inside a critical
# section* in AdvanceXLInsertBuffer().  When WaitLatch()/WaitEventSetWaitBlock()
# faces postmaster death it exits releasing all locks instead of PANIC, so a
# backend frozen on that CV drops its buffer content lock mid-insert.  A
# checkpoint can then flush the modified data page to disk while the WAL record
# describing the change never reaches disk -- data ahead of WAL.
#
# Kirill Reshke described the exact scenario but could not trigger it
# deterministically without injection points (pgsql-hackers, 2025-08-14):
#
#   1) p1 locks a heap buffer, enters a crit section, MarkBufferDirty, and
#      hangs inside XLogInsert -> GetXLogBuffer -> AdvanceXLInsertBuffer on the
#      CV.
#   2) CHECKPOINT (p2) tries to flush the dirty buffer, waits on its content
#      lock.
#   3) postmaster killed -9.
#   4) p1 wakes in WaitLatch, sees postmaster death, exits releasing all locks.
#   5) p2 acquires the buffer lock and flushes it to disk.
#   6) the xlog record for that change never made it to disk.
#
# We force p1 to block on the real InitializedUpToCondVar by freezing a third
# backend (p0) with the postmaster-death-SAFE injection wait, right after it
# reserves a WAL page (INJECTION_POINT "wal-buffer-reserved" in
# AdvanceXLInsertBuffer()).  p0 holds no buffer lock (it only emits a logical
# WAL message), so the checkpointer is free to block on p1's data buffer.
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
});
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION pageinspect));

# Streaming standby: it only ever sees changes that were actually WAL-logged,
# so it is our "WAL-consistent" ground truth.
my $backup_name = 'backup';
$node->backup($backup_name);
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, $backup_name, has_streaming => 1);
$standby->start;

# One small table whose only data page (block 0) is buf1.  Fill it with a few
# committed rows, then checkpoint so block 0 is clean and the redo point is
# recent.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE t (id int) WITH (autovacuum_enabled = off);
	INSERT INTO t SELECT g FROM generate_series(1, 10) g;
	CHECKPOINT;
});
$node->wait_for_catchup($standby);

my $committed_raw = $node->safe_psql('postgres',
	q{SELECT count(*) FROM heap_page_items(get_raw_page('t', 0))});
note "raw line pointers on block 0 before the run: $committed_raw";

# --- p0: freeze WAL-buffer initialization -------------------------------------
# Load the stop point in this backend (must happen outside a critical section),
# then emit a logical WAL message large enough to reserve a fresh WAL page.
# The backend freezes after reserving the page, before advancing
# InitializedUpTo -- so InitializedUpTo is stuck and p0 holds no buffer lock.
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('wal-buffer-reserved', 'wait')});

my $p0 = $node->background_psql('postgres', on_error_stop => 0);
$p0->query_until(
	qr/p0_go/, q{
	\echo p0_go
	SELECT injection_points_load('wal-buffer-reserved');
	SELECT pg_logical_emit_message(false, 'p0', repeat('x', 32768));
});
$node->wait_for_event('client backend', 'wal-buffer-reserved');
note "p0 frozen after reserving a WAL page";

# NB: do NOT issue any WAL-writing statement now.  With InitializedUpTo stalled
# behind p0, every backend that needs to write WAL blocks on
# InitializedUpToCondVar.  We also don't need to detach the injection point:
# it is load-gated (INJECTION_POINT_CACHED only fires in a backend that called
# injection_points_load), and only p0 did that, so p1 cannot freeze there.

# --- p1: the victim -----------------------------------------------------------
# INSERT a distinctive row.  heap_insert() locks block 0 (buf1), MarkBufferDirty,
# enters the crit section, then calls XLogInsert -> AdvanceXLInsertBuffer, which
# blocks on InitializedUpToCondVar because InitializedUpTo is stuck behind p0.
# p1 is now sleeping on the CV while holding buf1's content lock.
my $p1 = $node->background_psql('postgres', on_error_stop => 0);
$p1->query_until(
	qr/p1_go/, q{
	\echo p1_go
	INSERT INTO t VALUES (999999);
});
$node->wait_for_event('client backend', 'WalBufferInit');
note "p1 blocked on InitializedUpToCondVar while holding buf1";

# --- p2: the flusher ----------------------------------------------------------
# We cannot use CHECKPOINT here: CreateCheckPoint() first grabs all WAL
# insertion locks exclusively to fix the redo point, and frozen p0 holds one,
# so the checkpointer would block on WALInsert and never reach the buffer flush.
# Instead, a tiny helper takes a SHARE content lock on buf1 (blocking behind
# p1) and flushes just that page.  This path needs no WAL insertion lock.
# LWLockAcquire() ignores postmaster death, so the instant p1 releases buf1
# (by exiting on postmaster death) p2 flushes the page -- whose change was
# never WAL-logged.
my $p2 = $node->background_psql('postgres', on_error_stop => 0);
$p2->query_until(
	qr/p2_go/, q{
	\echo p2_go
	SELECT injection_points_flush_buffer('t'::regclass, 0);
});

# Wait until p2 is actually blocked on buf1's content lock.
my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
my $p2_waiting = 0;
while (time() < $deadline)
{
	$p2_waiting = $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_stat_activity
		WHERE backend_type = 'client backend' AND wait_event = 'BufferContent'});
	last if $p2_waiting eq '1';
	usleep(100_000);
}
is($p2_waiting, '1', 'flusher p2 is blocked on buf1 content lock');

# Capture the postmaster and all of its children before the crash.
my $pmpid = $node->{_pid};
my @cluster_pids = ($pmpid);
push @cluster_pids, grep { /^\d+$/ } split /\s+/, `pgrep -P $pmpid`;

# --- the crash ----------------------------------------------------------------
# Kill ONLY the postmaster.  p1, sleeping on InitializedUpToCondVar, detects
# postmaster death in WaitLatch and exits, releasing buf1's content lock
# instead of PANICking.  The checkpointer then acquires buf1 and flushes the
# data page -- whose change was never WAL-logged.
$node->kill9;

# Give p1 time to exit and the checkpointer time to acquire buf1 and flush it
# to disk before we take down the rest of the cluster.
sleep(3);

# Now kill the whole cluster by PID (p0 is frozen on the safe injection wait
# and will not exit on its own; the checkpointer/others may also linger).
foreach my $i (1 .. 100)
{
	my @alive = grep { kill 0, $_ } @cluster_pids;
	last unless @alive;
	kill 'KILL', @alive;
	usleep(100_000);
}
eval { $p0->quit; };
eval { $p1->quit; };
eval { $p2->quit; };

# --- recovery + corruption check ----------------------------------------------
$node->poll_start;

my $primary_raw = $node->safe_psql('postgres',
	q{SELECT count(*) FROM heap_page_items(get_raw_page('t', 0))});
my $standby_raw = $standby->safe_psql('postgres',
	q{SELECT count(*) FROM heap_page_items(get_raw_page('t', 0))});

note "raw line pointers on block 0 after recovery: primary=$primary_raw standby=$standby_raw";

# The standby (WAL-consistent) must not have grown.  If the primary has more
# raw tuples than the standby, a data-page change reached the primary's disk
# without a WAL record -- the corruption we are demonstrating.
is($primary_raw, $standby_raw,
	'primary and standby agree on block 0 (no data-ahead-of-WAL corruption)');
is($primary_raw, $committed_raw,
	'no phantom (never-WAL-logged) tuple on the primary');

$node->stop;
$standby->stop;

done_testing();
