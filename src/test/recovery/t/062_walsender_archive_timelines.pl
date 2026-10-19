# Copyright (c) 2026, PostgreSQL Global Development Group

# Stream archived WAL across one or two timeline switches within a segment.
# Only the last timeline has a complete segment in the archive; earlier
# timelines' .partial files must not be needed.
use strict;
use warnings FATAL => 'all';

use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $helper = "$FindBin::RealBin/walsender_restore";
my $rows_query =
  q{SELECT string_agg(i::text, ',' ORDER BY i) FROM timeline_test};

for my $switches (1, 2)
{
	subtest "$switches timeline switches" => sub {
		my $sender = PostgreSQL::Test::Cluster->new("sender_$switches");
		$sender->init(
			allows_streaming => 1,
			has_archiving => 1,
			extra => ['--wal-segsize=16']);
		$sender->append_conf(
			'postgresql.conf', qq(
min_wal_size = '32MB'
max_wal_size = '64MB'
wal_keep_size = 0
checkpoint_timeout = '1h'
archive_timeout = 0
autovacuum = off
log_replication_commands = on
));
		$sender->start;
		$sender->safe_psql('postgres', 'CREATE TABLE timeline_test (i int)');
		$sender->backup('base');

		my $receiver = PostgreSQL::Test::Cluster->new("receiver_$switches");
		$receiver->init_from_backup($sender, 'base', has_streaming => 1);
		$receiver->append_conf(
			'postgresql.conf', qq(
archive_mode = off
restore_command = ''
recovery_target_timeline = 'latest'
wal_retrieve_retry_interval = '100ms'
));
		$receiver->start;
		$sender->safe_psql('postgres',
			'INSERT INTO timeline_test VALUES (0)');
		$sender->wait_for_catchup($receiver, 'replay',
			$sender->lsn('insert'));
		$receiver->stop;

		# The receiver needs this row before it can reach the first fork.
		$sender->safe_psql('postgres',
			'INSERT INTO timeline_test VALUES (1)');
		my $first_segment = $sender->safe_psql('postgres',
			'SELECT pg_walfile_name(pg_current_wal_insert_lsn())');
		my $segment_suffix = substr($first_segment, 8);

		# Reuse the sender as its own standby. An immediate stop avoids
		# completing and archiving the current segment as a clean primary
		# shutdown would. Promotion copies its valid prefix to the new timeline.
		for my $switch (1 .. $switches)
		{
			$sender->stop('immediate');
			$sender->set_standby_mode;
			$sender->start;
			$sender->promote;
			my $value = $switch + 1;
			$sender->safe_psql('postgres',
				"INSERT INTO timeline_test VALUES ($value)");
			my $segment = $sender->safe_psql('postgres',
				'SELECT pg_walfile_name(pg_current_wal_insert_lsn())');
			is( $segment,
				sprintf('%08X%s', $switch + 1, $segment_suffix),
				"promotion $switch stayed in the same segment")
			  or BAIL_OUT('timeline-switch setup crossed a segment boundary');
		}

		my $target_lsn = $sender->lsn('insert');
		my $expected_rows = join(',', 0 .. $switches + 1);
		my $archive = $sender->archive_dir;
		my $last_segment = sprintf('%08X%s', $switches + 1, $segment_suffix);

		# Archive the complete segment on the last timeline, then let
		# checkpoints recycle it. Earlier timelines ended within this segment.
		$sender->safe_psql('postgres', 'SELECT pg_switch_wal()');
		$sender->poll_query_until('postgres',
			"SELECT last_archived_wal >= '$last_segment' FROM pg_stat_archiver"
		) or BAIL_OUT('the complete descendant segment was not archived');
		is( -s "$archive/$last_segment",
			16 * 1024 * 1024,
			'archive contains a full-sized descendant segment')
		  or BAIL_OUT('the complete descendant segment is missing or short');

		for my $tli (1 .. $switches)
		{
			my $segment = sprintf('%08X%s', $tli, $segment_suffix);
			ok(!-e "$archive/$segment",
				"$segment was never archived as a full segment")
			  or BAIL_OUT('an ancestor segment was unexpectedly completed');
		}

		$sender->advance_wal(6);
		$sender->safe_psql('postgres', 'CHECKPOINT');
		for my $tli (1 .. $switches + 1)
		{
			my $segment = sprintf('%08X%s', $tli, $segment_suffix);
			ok( !-e $sender->data_dir . "/pg_wal/$segment",
				"$segment is absent from the sender's pg_wal")
			  or
			  BAIL_OUT('checkpoint did not remove a required local segment');
		}

		my $restore_command = qq{"$^X" "$helper" "$archive" "%f" "%p" "%r"};
		$restore_command =~ s/\\/\\\\/g;
		$restore_command =~ s/'/''/g;
		$sender->append_conf(
			'postgresql.conf', qq(
restore_command = '$restore_command'
wal_sender_restore = on
));
		$sender->reload;
		$sender->poll_query_until('postgres',
			"SELECT current_setting('wal_sender_restore')::boolean")
		  or BAIL_OUT('archive fallback was not enabled');
		my $sender_log_offset = -s $sender->logfile;

		$receiver->start;
		$sender->wait_for_catchup($receiver, 'replay', $target_lsn);
		is($receiver->safe_psql('postgres', $rows_query),
			$expected_rows, 'streaming replayed rows from all timelines');
		like(
			slurp_file($sender->logfile, $sender_log_offset),
			qr/restored log file "\Q$last_segment\E" from archive/,
			'the sender fetched the complete descendant segment');
		$receiver->stop;

		# Recover the same base backup directly from the archive as a control.
		# Neither path should need to fetch or rename .partial files.
		my $control =
		  PostgreSQL::Test::Cluster->new("archive_control_$switches");
		$control->init_from_backup($sender, 'base', has_restoring => 1);
		$control->append_conf(
			'postgresql.conf', qq(
archive_mode = off
primary_conninfo = ''
recovery_target_timeline = 'latest'
));
		$control->start;
		ok( $control->poll_query_until(
				'postgres',
				"SELECT pg_last_wal_replay_lsn() >= '$target_lsn'::pg_lsn"),
			'direct archive recovery reached the same target');
		is($control->safe_psql('postgres', $rows_query),
			$expected_rows,
			'direct archive recovery replayed rows from all timelines');
		like(
			slurp_file($control->logfile),
			qr/restored log file "\Q$last_segment\E" from archive/,
			'direct recovery used the complete descendant segment');
		$control->stop;
		$sender->stop;
	};
}

done_testing();
