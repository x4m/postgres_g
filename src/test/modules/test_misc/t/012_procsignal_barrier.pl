
# Copyright (c) 2026, PostgreSQL Global Development Group

# Test for the race condition between ProcSignalInit and EmitProcSignalBarrier.
#
# When a new backend calls ProcSignalInit, it reads the current global barrier
# generation and stores it in its slot before setting pss_pid.  If
# EmitProcSignalBarrier runs between those two steps it skips the new backend
# (pss_pid is still 0) but increments the global generation.  Once pss_pid is
# set, WaitForProcSignalBarrier finds the slot with a stale generation and
# can wait for a long time, because the new backend never received SIGUSR1 for
# this barrier.
#
# Commit 67c20979 introduced a call to WaitForProcSignalBarrier() in the
# startup process (UpdateLogicalDecodingStatusEndOfRecovery), exposing this
# latent race as a hang during server startup observed on the buildfarm.
#
# The fix in ProcSignalInit re-reads the global barrier generation after
# setting pss_pid.  If the global generation has advanced, ProcSignalInit
# updates its local generation and broadcasts on the condition variable,
# allowing any concurrent WaitForProcSignalBarrier to proceed without waiting.
#
# Injection points used:
#  "procsignal-init-before-pid-set"  -- in ProcSignalInit, just before pss_pid
#                                       is written; pausing here lets a barrier
#                                       be emitted while pss_pid is still 0.
#  "procsignal-barrier-before-wait"  -- in injection_points_wait_for_barrier(),
#                                       between the emit and the WaitFor call,
#                                       letting us release the paused backend
#                                       before WaitForProcSignalBarrier runs.
#
# IMPORTANT: because "procsignal-init-before-pid-set" fires inside ProcSignalInit
# of every connecting backend, attaching it globally would block the polling
# connections used by wait_for_event.  Instead, all management and polling is
# done through two long-lived sessions ($sess_monitor and $sess_waiter) that
# connect BEFORE the injection point is attached, so they are not affected.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Record log file position so we can check for error messages later.
my $log_offset = -s $node->logfile;

# -------------------------------------------------------------------------
# Connect the helper sessions BEFORE attaching any injection points.
# These sessions' backends have already passed ProcSignalInit, so they will
# not be blocked by the "procsignal-init-before-pid-set" injection point.
# We use them for all polling and management operations to avoid creating
# new connections (which would also hit the injection point).
# -------------------------------------------------------------------------
my $sess_monitor = $node->background_psql('postgres');
my $sess_waiter  = $node->background_psql('postgres');

# -------------------------------------------------------------------------
# Set up the race: pause a connecting backend just before it sets pss_pid.
# -------------------------------------------------------------------------

# Attach the injection point via the already-connected monitor session.
$sess_monitor->query_safe(
	"SELECT injection_points_attach('procsignal-init-before-pid-set', 'wait')"
);

# Start a new background session with wait=>0 so that background_psql does
# not block waiting for ReadyForQuery.  The new backend will pause inside
# ProcSignalInit before setting pss_pid.
my $sess_new = $node->background_psql('postgres', wait => 0);

# Poll for the new backend using sess_monitor (already connected -- no new
# connections that would also hit the injection point).  Use a PL/pgSQL
# loop so everything happens within a single, already-open session.
$sess_monitor->query_safe(
	"DO \$\$
BEGIN
    LOOP
        EXIT WHEN EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE backend_type = 'client backend'
              AND wait_event = 'procsignal-init-before-pid-set'
        );
        PERFORM pg_sleep(0.05);
    END LOOP;
END\$\$"
);
note "new backend paused at procsignal-init-before-pid-set (pss_pid still 0)";

# Detach the injection point so that future connections are not paused.
# The new backend stays blocked on the condition variable until woken up.
$sess_monitor->query_safe(
	"SELECT injection_points_detach('procsignal-init-before-pid-set')");

# -------------------------------------------------------------------------
# Attach the injection point that fires between the emit and the WaitFor.
# -------------------------------------------------------------------------
$sess_monitor->query_safe(
	"SELECT injection_points_attach('procsignal-barrier-before-wait', 'wait')"
);

# Emit the barrier now, while the new backend still has pss_pid = 0.
# EmitProcSignalBarrier's SIGUSR1-sending loop will therefore skip it.
my $gen =
  $sess_monitor->query_safe('SELECT injection_points_emit_barrier()');
chomp $gen;
note "emitted barrier generation $gen with new backend's pss_pid still 0";

# Start injection_points_wait_for_barrier($gen) in the waiter session.
# It fires "procsignal-barrier-before-wait" before the actual wait, giving
# us a window to release the paused backend first.
$sess_waiter->query_until(
	qr/procsignal_wait_started/,
	"\\echo procsignal_wait_started\n"
	  . "SELECT injection_points_wait_for_barrier($gen);\n");

# Wait for sess_waiter to reach the injection point.
$sess_monitor->query_safe(
	"DO \$\$
BEGIN
    LOOP
        EXIT WHEN EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE backend_type = 'client backend'
              AND wait_event = 'procsignal-barrier-before-wait'
        );
        PERFORM pg_sleep(0.05);
    END LOOP;
END\$\$"
);
note
  "waiter paused at procsignal-barrier-before-wait (WaitForProcSignalBarrier not called yet)";

# Release the paused new backend.  Its ProcSignalInit will now:
#   - set pss_pid (making the slot visible)
#   - re-read the global barrier generation
#   - if it has advanced (fix): update local gen and broadcast the CV
#   - if not (no fix): leave local gen stale
$sess_monitor->query_safe(
	"SELECT injection_points_wakeup('procsignal-init-before-pid-set')");

# Wait for the new backend to finish connecting.
$sess_new->wait_connect();
note "new backend connected (pss_pid now set)";

# Release the waiter.  WaitForProcSignalBarrier($gen) will now run.
#
# With the fix:    the new backend already updated its local gen in
#                  ProcSignalInit, so WaitForProcSignalBarrier returns
#                  immediately.
#
# Without the fix: the new backend has a stale local gen, and
#                  WaitForProcSignalBarrier blocks until it receives
#                  SIGUSR1 for a future barrier.
$sess_monitor->query_safe(
	"SELECT injection_points_detach('procsignal-barrier-before-wait')");
$sess_monitor->query_safe(
	"SELECT injection_points_wakeup('procsignal-barrier-before-wait')");

# To ensure the test always completes (even without the fix), emit a rescue
# barrier.  When sess_new runs SELECT 1, it calls CHECK_FOR_INTERRUPTS which
# processes the rescue barrier and updates the local generation, unblocking
# any WaitForProcSignalBarrier that is waiting for it.
$sess_monitor->query_safe('SELECT injection_points_emit_barrier()');
$sess_new->query_safe("SELECT 1");

# Wait for the waiter's blocking query to finish.
$sess_waiter->query_safe("SELECT 1");
ok(1, "WaitForProcSignalBarrier completed for barrier generation $gen");

# With the fix, WaitForProcSignalBarrier returns without ever hitting the
# 5-second log message.
$node->log_check(
	"no 5-second ProcSignalBarrier stall (fix is effective)",
	$log_offset,
	log_unlike => [qr/still waiting for backend.*ProcSignalBarrier/],
);

$sess_new->quit;
$sess_waiter->quit;
$sess_monitor->quit;

# -------------------------------------------------------------------------
# Sanity check: a normal (un-paused) connection does not stall
# WaitForProcSignalBarrier.
# -------------------------------------------------------------------------
my $gen2 =
  $node->safe_psql('postgres', 'SELECT injection_points_emit_barrier()');
chomp $gen2;
$node->safe_psql('postgres',
	"SELECT injection_points_wait_for_barrier($gen2)");
ok(1,
	"WaitForProcSignalBarrier completes normally for barrier generation $gen2"
);

done_testing();
