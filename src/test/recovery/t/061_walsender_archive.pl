# Copyright (c) 2026, PostgreSQL Global Development Group

# A physical sender can fetch old WAL without putting it back in the normal
# WAL namespace. Exercise primary and cascading senders, and cleanup after
# a failed restore, normal streaming, and crash restart.
use strict;
use warnings FATAL => 'all';

use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $helper = "$FindBin::RealBin/walsender_restore";
my $have_injection_points = $ENV{enable_injection_points} eq 'yes';

# This is a directory count, not a count of normally named WAL segments.
my $scratch_count = q{SELECT count(*) FROM pg_ls_dir('pg_wal') AS f
  WHERE f LIKE 'xlogtemp.%.walsender'};

for my $cascading (0, 1)
{
	my $label = $cascading ? 'cascading' : 'primary';
	my $primary = PostgreSQL::Test::Cluster->new("${label}_primary");
	$primary->init(
		allows_streaming => 1,
		has_archiving => 1,
		extra => ['--wal-segsize=1']);
	$primary->append_conf(
		'postgresql.conf', qq(
min_wal_size = '2MB'
max_wal_size = '4MB'
wal_keep_size = 0
checkpoint_timeout = '1h'
));
	$primary->start;
	$primary->safe_psql('postgres', 'CREATE TABLE archive_test (i int)');
	$primary->backup('base');

	my $sender = $primary;
	if ($cascading)
	{
		$sender = PostgreSQL::Test::Cluster->new('cascading_sender');
		$sender->init_from_backup($primary, 'base', has_streaming => 1);
		$sender->start;
	}

	# Two receivers also exercise independent scratch copies in the primary
	# case when injection points are available.
	my @receivers;
	for my $i (1 .. 2)
	{
		my $receiver = PostgreSQL::Test::Cluster->new("${label}_receiver_$i");
		$receiver->init_from_backup($primary, 'base', has_streaming => 1);
		$receiver->enable_streaming($sender) if $cascading;
		$receiver->append_conf(
			'postgresql.conf', qq(
restore_command = ''
wal_retrieve_retry_interval = '100ms'
));
		$receiver->start;
		push @receivers, $receiver;
	}
	$primary->safe_psql('postgres', 'INSERT INTO archive_test VALUES (1)');
	my $initial_lsn = $primary->lsn('insert');
	$primary->wait_for_catchup($sender, 'replay', $initial_lsn) if $cascading;
	for my $receiver (@receivers)
	{
		$sender->wait_for_catchup($receiver, 'replay', $initial_lsn);
		$receiver->stop;
	}
	my $old_segment = $primary->safe_psql('postgres',
		"SELECT pg_walfile_name('$initial_lsn')");

	# Produce and archive enough WAL for checkpoint/restartpoint cleanup to
	# remove the segment at which the receivers stopped.
	$primary->safe_psql('postgres', 'INSERT INTO archive_test VALUES (2)');
	$primary->advance_wal(10);
	$primary->safe_psql('postgres', 'CHECKPOINT');
	my $target_lsn = $primary->lsn('insert');
	my $archive = $primary->archive_dir;
	$primary->poll_query_until('postgres',
		"SELECT last_archived_wal > '$old_segment' FROM pg_stat_archiver")
	  or die 'archive did not advance';
	$primary->safe_psql('postgres', 'CHECKPOINT');

	if ($cascading)
	{
		$primary->wait_for_catchup($sender, 'replay',
			$primary->lsn('insert'));
		$sender->safe_psql('postgres', 'CHECKPOINT');
	}
	ok( !-e $sender->data_dir . "/pg_wal/$old_segment",
		"$label: required WAL really left pg_wal");
	ok(-f "$archive/$old_segment", "$label: required WAL is archived");

	my $restore_command = qq{"$^X" "$helper" "$archive" "%f" "%p" "%r"};
	$restore_command =~ s/\\/\\\\/g;
	$restore_command =~ s/'/''/g;
	$sender->append_conf('postgresql.conf',
		"restore_command = '$restore_command'");
	$sender->reload;

	# Configuring restore_command alone does not change existing behavior.
	my $log_offset = -s $sender->logfile;
	$receivers[0]->start;
	$sender->wait_for_log(
		qr/requested WAL segment .* has already been removed/, $log_offset);
	$receivers[0]->stop;
	is($sender->safe_psql('postgres', $scratch_count),
		'0', "$label: disabled fallback creates no scratch copies");

	# A failed size check must not leave a private directory behind.
	append_to_file("$archive/short", '');
	$sender->append_conf('postgresql.conf', 'wal_sender_restore = on');
	$sender->reload;
	$log_offset = -s $sender->logfile;
	$receivers[0]->start;
	$sender->wait_for_log(qr/archive file .* has wrong size/, $log_offset);
	$receivers[0]->stop;
	unlink("$archive/short") or die "unlink short: $!";
	$sender->poll_query_until('postgres', $scratch_count, '0')
	  or die 'failed restore left a scratch directory';

	if ($have_injection_points && !$cascading)
	{
		$sender->safe_psql('postgres', 'CREATE EXTENSION injection_points');
		$sender->safe_psql('postgres',
			"SELECT injection_points_attach('walsender-after-archive-restore', 'wait')"
		);
	}

	$_->start for @receivers;
	if ($have_injection_points && !$cascading)
	{
		$sender->poll_query_until(
			'postgres',
			q{SELECT count(*) = 2 FROM pg_stat_activity
              WHERE wait_event = 'walsender-after-archive-restore'}
		) or die 'senders did not reach archive wait point';
		is($sender->safe_psql('postgres', $scratch_count),
			'2', 'two senders have separate archive copies');
		$sender->safe_psql('postgres', 'CHECKPOINT');
		is($sender->safe_psql('postgres', $scratch_count),
			'2', 'checkpoint leaves private archive copies alone');

		# Leave real restored files behind with an immediate shutdown, then
		# verify startup removes them before either receiver reconnects.
		$_->stop for @receivers;
		$sender->stop('immediate');
		opendir(my $wal_dir, $sender->data_dir . '/pg_wal')
		  or die "open WAL directory: $!";
		my @leftovers =
		  grep { /^xlogtemp\..*\.walsender$/ } readdir($wal_dir);
		closedir($wal_dir);
		is(scalar @leftovers, 2, 'crash leaves the two private directories');
		$sender->start;
		is($sender->safe_psql('postgres', $scratch_count),
			'0', 'crash restart removes private archive directories');
		$_->start for @receivers;
	}

	for my $receiver (@receivers)
	{
		$sender->wait_for_catchup($receiver, 'replay', $target_lsn);
		is( $receiver->safe_psql(
				'postgres', 'SELECT sum(i) FROM archive_test'),
			'3',
			"$label: receiver replayed WAL delivered from archive");
	}
	ok(!-e $sender->data_dir . "/pg_wal/$old_segment",
		"$label: restored WAL was not installed under its original name");
	$sender->poll_query_until('postgres', $scratch_count, '0')
	  or die 'completed archive reads left scratch directories';

	# Verify that the same connections continue with ordinary live WAL.
	$primary->safe_psql('postgres', 'INSERT INTO archive_test VALUES (3)');
	my $live_lsn = $primary->lsn('insert');
	$primary->wait_for_catchup($sender, 'replay', $live_lsn) if $cascading;
	for my $receiver (@receivers)
	{
		$sender->wait_for_catchup($receiver, 'replay', $live_lsn);
		is( $receiver->safe_psql(
				'postgres', 'SELECT sum(i) FROM archive_test'),
			'6',
			"$label: receiver continues with live WAL");
		$receiver->stop;
	}
	$sender->stop if $cascading;
	$primary->stop;
}

done_testing();
