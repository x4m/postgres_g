# Copyright (c) 2026, PostgreSQL Global Development Group

# Regression test for a postmaster shutdown race.  A fast or smart shutdown
# that arrives while the postmaster is still reinitializing after a crash --
# FatalError set, before the startup process reports
# PMSIGNAL_RECOVERY_STARTED -- used to hang forever in PM_WAIT_BACKENDS: the
# auxiliary processes relaunched for crash recovery only react to crash-style
# SIGQUIT, but the shutdown path signalled them with SIGTERM, which the
# checkpointer ignores.
#
# The bug lives in a window that ordinary SQL-based injection points cannot
# reach:
#
#   * There is no SQL connection during crash-restart reinitialization, so the
#     parked startup process cannot be released with injection_points_wakeup().
#   * The crash resets shared memory, so a point attached through SQL before
#     the crash is gone by the time the replacement startup process runs.
#
# The filesystem control added to the injection_points module solves both: the
# marker directory is rescanned whenever shared memory is (re)created, so the
# point is attached in the replacement shared memory after the crash, and the
# parked process is released by removing its PID file.

use strict;
use warnings FATAL => 'all';

use Errno;
use Time::HiRes qw(usleep);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $point = 'recovery-before-signal-recovery-started';

# Both shutdown modes that used to hang go through the same buggy path, so run
# the same scenario for each: fast is SIGINT, smart is SIGTERM.
test_shutdown_during_crash_restart('fast', 'INT');
test_shutdown_during_crash_restart('smart', 'TERM');

done_testing();

sub test_shutdown_during_crash_restart
{
	my ($mode, $signal) = @_;

	my $node = PostgreSQL::Test::Cluster->new("crash_shutdown_$mode");
	$node->init;
	$node->append_conf('postgresql.conf', <<'EOF');
shared_preload_libraries = 'injection_points'
restart_after_crash = on
EOF

	$node->start;

	# Request that the wait point be attached at the next shared-memory
	# initialization, after the server has started but before triggering crash
	# restart.  The directory is therefore first seen while shared memory is
	# being recreated, when no SQL connection exists.
	my $inj_root = $node->data_dir . '/pg_injection_points';
	my $pdir = "$inj_root/$point";
	mkdir $inj_root or die "could not create $inj_root: $!";
	mkdir $pdir or die "could not create $pdir: $!";

	# The postmaster survives crash restart, so this PID is stable for the
	# rest of the scenario.
	my $pidfile = $node->data_dir . '/postmaster.pid';
	my $pmpid = slurp_pid($pidfile);
	ok($pmpid > 0, "$mode: obtained postmaster pid");

	# Force a crash-and-restart cycle by SIGKILL'ing an auxiliary process.
	# Killing the checkpointer needs no long-lived client connection kept
	# alive, and routing the signal through "pg_ctl kill" keeps this portable
	# to Windows.
	my $ckpt = $node->safe_psql('postgres',
		"SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer'"
	);
	PostgreSQL::Test::Utils::system_or_bail('pg_ctl', 'kill', 'KILL', $ckpt);

	# Wait, through the filesystem only, until the replacement startup process
	# is parked at the injection point just before PMSIGNAL_RECOVERY_STARTED.
	# It publishes a file named after its PID inside the point directory.
	my $waiter;
	foreach my $i (1 .. 10 * $PostgreSQL::Test::Utils::timeout_default)
	{
		if (opendir(my $dh, $pdir))
		{
			($waiter) = grep { /^\d+\z/ } readdir($dh);
			closedir($dh);
			last if defined $waiter;
		}
		usleep(100_000);
	}
	if (!ok(defined $waiter,
			"$mode: startup process reached the pre-recovery-signal injection point"
		))
	{
		# Without a parked startup process the rest of the sequence would test
		# a different scenario; end this one, leaving no server behind.
		$node->stop('immediate', fail_ok => 1);
		return;
	}

	# Request the shutdown while the startup process is still parked.  Send
	# the signal asynchronously via pg_ctl kill, because a blocking pg_ctl
	# stop would never return on the unfixed code.
	my $logstart = -s $node->logfile;
	PostgreSQL::Test::Utils::system_or_bail('pg_ctl', 'kill', $signal,
		$pmpid);

	# Release the startup process strictly after the postmaster has taken the
	# shutdown request.  This ordering is what reproduces the bug: the
	# postmaster handles the shutdown with FatalError still set, and only then
	# does the startup process proceed to signal PMSIGNAL_RECOVERY_STARTED
	# (which the postmaster now ignores because a shutdown is pending).
	# Releasing earlier would let FatalError be cleared first, and the test
	# could pass on buggy code.
	$node->wait_for_log(qr/received $mode shutdown request/, $logstart);
	if (!unlink("$pdir/$waiter") && !$!{ENOENT})
	{
		die "could not remove waiter file: $!";
	}

	# The postmaster must now exit within a bounded time; it removes
	# postmaster.pid as it goes.  On the unfixed code it stays in
	# PM_WAIT_BACKENDS forever, waiting on the SIGTERM-ignoring checkpointer,
	# and the file remains.
	my $stopped = 0;
	foreach my $i (1 .. 10 * $PostgreSQL::Test::Utils::timeout_default)
	{
		if (!-f $pidfile)
		{
			$stopped = 1;
			last;
		}
		usleep(100_000);
	}
	ok($stopped,
		"$mode: postmaster shuts down when requested during crash restart");

	# Never leave a hung server behind, even when the assertion above failed:
	# an immediate shutdown SIGQUITs the children, which the checkpointer does
	# honor.
	$node->stop('immediate', fail_ok => 1);
	return;
}

# Read the first line (the PID) of a postmaster.pid file.
sub slurp_pid
{
	my ($path) = @_;
	open(my $fh, '<', $path) or die "could not open $path: $!";
	my $line = <$fh>;
	close($fh);
	chomp $line if defined $line;
	return defined $line ? $line + 0 : 0;
}
