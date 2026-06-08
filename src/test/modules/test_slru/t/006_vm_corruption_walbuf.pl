# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# VM corruption reproduced through the *real* condition-variable wait that
# "Get rid of WALBufMappingLock" (commit bc22dc0e0dd) added to
# AdvanceXLInsertBuffer() -- not through an artificial injection point.
#
# This crosses injection_points_stall_wal_buffer_init(), which makes the next
# WAL inserter block on InitializedUpToCondVar (WalBufferInit), with Andrey
# Borodin's "VM corruption on standby" reproducer (pgsql-hackers, Aug 2025).
#
# Scenario:
#   1. A row is frozen, so heap block 0 is ALL_VISIBLE/ALL_FROZEN in the VM, on
#      both primary and standby.
#   2. The WAL buffer init is stalled, then an INSERT targets block 0.  Inside
#      heap_insert()'s critical section it clears the VM bit (releasing the VM
#      buffer's content lock) and then blocks for good on the WalBufferInit
#      condition variable in AdvanceXLInsertBuffer() -- the exact place the
#      reverted commit introduced.  The insert's WAL is therefore never written.
#   3. The dirty heap and VM pages are flushed to disk directly.  A real
#      CHECKPOINT would do this in the small window after the stalled backend
#      exits on postmaster death, but before the checkpointer dies too.
#   4. kill -9 the cluster.  After recovery the primary has PD_ALL_VISIBLE and
#      VM bits clear, but the standby still has ALL_VISIBLE/ALL_FROZEN because
#      those clears were never WAL-logged -- VM corruption, observable as
#      deleted rows resurfacing on the standby.
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

sub poll_start_after_kill9
{
	my ($node) = @_;

	for (my $attempts = 0;
		 $attempts < 10 * $PostgreSQL::Test::Utils::timeout_default;
		 $attempts++)
	{
		return if $node->start(fail_ok => 1);

		usleep(100_000);
		$node->stop('fast', fail_ok => 1);
	}

	$node->start;
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'injection_points'
autovacuum = off
wal_writer_delay = 10s
checkpoint_timeout = 1h
max_wal_size = 8GB
});
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION injection_points));
$node->safe_psql('postgres', q(CREATE EXTENSION pg_visibility));

my $backup_name = 'backup';
$node->backup($backup_name);
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, $backup_name, has_streaming => 1);
$standby->start;

# A frozen row: heap block 0 becomes ALL_VISIBLE/ALL_FROZEN in the VM.  The
# index only exists to later demonstrate deleted rows resurfacing on the
# standby via an index-only scan.
$node->safe_psql(
	'postgres', q{
	CREATE TABLE x (i int) WITH (autovacuum_enabled = off);
	CREATE INDEX ON x (i);
	INSERT INTO x VALUES (1);
	VACUUM FREEZE x;
});
# Make the standby learn the frozen VM, then checkpoint so the next write to
# block 0 emits a full-page image (a record comfortably larger than the WAL gap
# the stall leaves, guaranteeing it crosses into the stalled page).
$node->wait_for_catchup($standby);
$node->safe_psql('postgres', q{CHECKPOINT});

my $frozen_before = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_visibility_map('x') WHERE all_frozen});
note "all_frozen VM blocks before the run: $frozen_before";

# A dedicated flusher session.  Once the WAL buffer is stalled, *any* WAL write
# blocks -- including the incidental catalog hint-bit FPIs a backend emits while
# parsing/planning a query.  So warm this session (and the shared catalog hint
# bits) by flushing once now, before the stall; afterwards the same call writes
# the VM page out without touching WAL.
my $flusher = $node->background_psql('postgres', on_error_stop => 0);
$flusher->query_safe(q{SELECT injection_points_flush_vm_buffer('x'::regclass, 0)});

# p1: stall WAL buffer init, then INSERT into block 0.  heap_insert() clears the
# VM bit, then hangs in AdvanceXLInsertBuffer() on the WalBufferInit CV.
my $p1 = $node->background_psql('postgres', on_error_stop => 0);
$p1->query_until(
	qr/p1_go/, q{
	\echo p1_go
	SELECT injection_points_stall_wal_buffer_init();
	INSERT INTO x VALUES (1);
});

$node->wait_for_event('client backend', 'WalBufferInit');
note 'p1 stalled on WalBufferInit inside heap_insert() critical section';

# The heap page is still content-locked by p1, but its PD_ALL_VISIBLE bit is
# already clear.  The VM page is dirty too.  Write both pages directly with no
# WAL (smgrwrite, not FlushBuffer).  This deterministically models the pages
# escaping to disk after p1 releases locks on postmaster death.
$flusher->query_safe(q{SELECT injection_points_flush_heap_buffer_raw('x'::regclass, 0)});
$flusher->query_safe(q{SELECT injection_points_flush_vm_buffer('x'::regclass, 0)});

# Remember every cluster process so we can hard-kill them all by PID.
my $pmpid = $node->{_pid};
my @cluster_pids = ($pmpid);
push @cluster_pids, grep { /^\d+$/ } split /\s+/, `pgrep -P $pmpid`;

$node->kill9;
$node->stop('immediate', fail_ok => 1);

foreach my $i (1 .. 100)
{
	my @alive = grep { kill 0, $_ } @cluster_pids;
	last unless @alive;
	kill 'KILL', @alive;
	usleep(100_000);
}
eval { $p1->quit; };
eval { $flusher->quit; };

poll_start_after_kill9($node);

my $frozen_after = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_visibility_map('x') WHERE all_frozen});
note "all_frozen VM blocks on primary after recovery: $frozen_after (was $frozen_before)";

# Drive activity that exposes the divergence: with block 0 no longer all-frozen
# on the primary but still all-frozen on the standby, the all-frozen clear for
# the following changes is not propagated, so the standby keeps a stale frozen
# bit over rows that were inserted and deleted.
$node->safe_psql(
	'postgres', q{
	INSERT INTO x VALUES (1);
	INSERT INTO x VALUES (2);
	DELETE FROM x WHERE i = 2;
});
$node->wait_for_catchup($standby);

my $corrupted_all_frozen =
  $standby->safe_psql('postgres', q{SELECT pg_check_frozen('x')});
my $corrupted_all_visible =
  $standby->safe_psql('postgres', q{SELECT pg_check_visible('x')});
my $observed_deleted_data = $standby->safe_psql('postgres',
	q{SET enable_seqscan TO false; SELECT * FROM x WHERE i = 2});

note "pg_check_frozen:  $corrupted_all_frozen";
note "pg_check_visible: $corrupted_all_visible";
note "deleted row seen on standby: $observed_deleted_data";

isnt($corrupted_all_frozen, '', 'pg_check_frozen() observes corruption');
isnt($corrupted_all_visible, '', 'pg_check_visible() observes corruption');
is($observed_deleted_data, '2', 'deleted data returned by standby select');

$node->stop;
$standby->stop;

done_testing();
