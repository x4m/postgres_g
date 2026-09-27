# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Opt-in, bounded workload, not part of check-world.  All nodes are disposable.
# The local-files mode keeps the same storage/history/WAL-inbox topology to
# isolate the cost of remote relation access, not to model a normal cluster.
use strict;
use warnings FATAL => 'all';

use IPC::Run;
use JSON::PP;
use Time::HiRes qw(time);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $mode = $ENV{PAGE_STORE_BENCH_MODE} // 'remote';
die 'mode must be remote or local' unless $mode =~ /\A(remote|local)\z/;
my $seconds = $ENV{PAGE_STORE_BENCH_SECONDS} // 5;
my $rate = $ENV{PAGE_STORE_BENCH_RATE} // 100;
my $scale = $ENV{PAGE_STORE_BENCH_SCALE} // 1;
my $hold = $ENV{PAGE_STORE_BENCH_HOLD_SECONDS} // 0;
for my $setting (
	[ $seconds, 3, 60 ],
	[ $rate, 1, 200 ],
	[ $scale, 1, 4 ],
	[ $hold, 0, 3600 ])
{
	die 'benchmark setting is outside its bounded range'
	  unless $setting->[0] =~ /\A[0-9]+\z/
	  && $setting->[0] >= $setting->[1]
	  && $setting->[0] <= $setting->[2];
}
my %report = (
	mode => $mode,
	seconds => $seconds,
	writer_rate => $rate,
	scale => $scale,
	clients_per_node => 2,
	shared_buffers => '8MB',
	revision => $ENV{PAGE_STORE_BENCH_REVISION} // 'unspecified',
	history_durable => JSON::PP::false,
	fsync => JSON::PP::true);
IPC::Run::run([ 'pg_config', '--configure' ], '>', \$report{configure})
  or die 'pg_config failed';

my $writer = PostgreSQL::Test::Cluster->new('bench_writer');
$writer->init(
	allows_streaming => 1,
	extra => [ '--wal-segsize=1', '--no-data-checksums' ]);
my $common = q{
fsync = on
autovacuum = off
shared_buffers = '8MB'
checkpoint_timeout = '1h'
max_wal_size = '1GB'
log_statement = 'none'
log_checkpoints = on
max_parallel_workers_per_gather = 0
};
$writer->append_conf('postgresql.conf', $common);
$writer->start;
$writer->command_ok(
	[ 'pgbench', '-i', '-s', $scale, $writer->connstr('postgres') ],
	'initialize the standard pgbench schema');
$writer->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE EXTENSION pg_buffercache;
CREATE EXTENSION amcheck;
VACUUM (FREEZE, ANALYZE);
SELECT pg_create_physical_replication_slot('bench_storage', true);
SELECT pg_create_physical_replication_slot('bench_r1', true);
SELECT pg_create_physical_replication_slot('bench_r2', true);
SELECT pg_create_physical_replication_slot('bench_inbox', true);
});
my @tables =
  qw(pgbench_accounts pgbench_branches pgbench_tellers pgbench_history);
my @indexes =
  qw(pgbench_accounts_pkey pgbench_branches_pkey pgbench_tellers_pkey);
my @names = (@tables, @indexes);
my $db = $writer->safe_psql('postgres',
	'SELECT oid FROM pg_database WHERE datname = current_database()');
my $spc = $writer->safe_psql('postgres',
	"SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'");
my @nodes =
  map { $writer->safe_psql('postgres', "SELECT pg_relation_filenode('$_')") }
  @names;
my @paths =
  map { $writer->safe_psql('postgres', "SELECT pg_relation_filepath('$_')") }
  @names;
my $locators = join ', ', map { "$spc/$db/$_" } @nodes;
my $anchor = $writer->safe_psql(
	'postgres', q{
SELECT restart_lsn - ((restart_lsn - '0/0'::pg_lsn)::bigint % 1048576)::numeric
FROM pg_replication_slots WHERE slot_name = 'bench_inbox'
});
$writer->backup('storage_seed');
my $storage = PostgreSQL::Test::Cluster->new('bench_storage');
$storage->init_from_backup($writer, 'storage_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', $common . q{
shared_preload_libraries = 'test_page_store'
primary_slot_name = 'bench_storage'
test_page_store.history_pages = 131072
});
$storage->start;
$writer->wait_for_catchup($storage);
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'storage did not pause';
my $relations = join ',', map { "'$_'" } @names;
my $cut = $storage->safe_psql('postgres',
	"SELECT test_page_store_retain_relations(ARRAY[$relations]::regclass[])");
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');
$writer->backup('reader_seed');
my @readers;

for my $i (1, 2)
{
	my $reader = PostgreSQL::Test::Cluster->new("bench_r$i");
	$reader->init_from_backup($writer, 'reader_seed', has_streaming => 1);
	$reader->append_conf('postgresql.conf',
		"primary_slot_name = 'bench_r$i'\n");
	push @readers, $reader;
}
my $inbox = PostgreSQL::Test::Cluster->new('bench_inbox');
$inbox->init(allows_streaming => 1);
$inbox->append_conf(
	'postgresql.conf', $common . q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store = true
});
$inbox->start;
$inbox->safe_psql('postgres', 'CREATE EXTENSION test_page_store');
my $wal_conninfo = $inbox->connstr('postgres');
$wal_conninfo =~ s/'/''/g;
my $page_conninfo = $storage->connstr('postgres');
$page_conninfo =~ s/'/''/g;
my $remote = qq{
smgr_chain = 'test_page_store, md'
test_page_store.conninfo = '$page_conninfo'
test_page_store.physical_service = true
test_page_store.transport_slots = 8
test_page_store.replay_lsn = '$cut'
test_page_store.replay_tli = 1
test_page_store.locators = '$locators'
test_page_store.request_timeout = '60s'
};
$writer->stop;
$writer->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store_conninfo = '$wal_conninfo'
test_page_store.wal_store_start_lsn = '$anchor'
test_page_store.wal_store_slot = 'bench_inbox'
});
my @missing;
if ($mode eq 'remote')
{
	$writer->append_conf('postgresql.conf',
		$remote . "test_page_store.writer_overlay_pages = 16384\n");
	for my $reader (@readers)
	{
		$reader->append_conf('postgresql.conf',
				"shared_preload_libraries = 'test_page_store'\n"
			  . $remote
			  . "test_page_store.follow = true\n");
	}
	for my $node ($writer, @readers)
	{
		for my $path (@paths)
		{
			for my $suffix ('', '_vm', '_fsm')
			{
				my $file = $node->data_dir . "/$path$suffix";
				push @missing, $file;
				next unless -f $file;
				(my $saved = "$path$suffix") =~ s{/}{_}g;
				rename($file, $node->backup_dir . "/$saved.original")
				  or die "move selected file: $!";
			}
		}
	}
}
$writer->start;
$_->start for @readers;

sub catchup
{
	my $target = $writer->safe_psql('postgres',
		"SELECT pg_create_restore_point('benchmark-boundary')");
	$writer->safe_psql('postgres',
		"SELECT test_page_store_flush_wal('$target', false)");
	$writer->wait_for_catchup($_, 'replay', $target) for ($storage, @readers);
	return $target;
}
catchup();
$report{checkpointer_before} =
  sql_json($writer, 'SELECT * FROM pg_stat_checkpointer');
if ($mode eq 'remote')
{
	for my $reader (@readers)
	{
		$report{compute_before}{ $reader->name } =
		  sql_json($reader, 'SELECT * FROM test_page_store_compute_status()');
	}
}
$report{load_started} = time;
my @jobs;
for my $node (@readers, $writer)
{
	my $writing = $node == $writer;
	my @args = (
		'pgbench', '-n', '-s', $scale, '-c', 2, '-j', 2,
		'-T', $seconds, '-P', 1, '-M', 'prepared', '--random-seed=20260927',
		$writing ? ('-N', '--rate', $rate) : ('-S'),
		$node->connstr('postgres'));
	my $job = { node => $node, stdout => '', stderr => '', args => \@args };
	$job->{start} = time;
	$job->{handle} = IPC::Run::start(
		\@args,
		'>' => \$job->{stdout},
		'2>' => \$job->{stderr});
	push @jobs, $job;
}
for my $job (@jobs)
{
	$job->{handle}->finish;
	my $name = $job->{node}->name;
	is(($job->{handle}->full_results)[0], 0, "$name pgbench completes")
	  or diag($job->{stderr});
	my ($processed) =
	  $job->{stdout} =~ /^number of transactions actually processed: (\d+)$/m;
	my ($failed) =
	  $job->{stdout} =~ /^number of failed transactions: (\d+) /m;
	ok(defined($processed) && $processed > 0, "$name processed transactions")
	  or diag($job->{stdout});
	# Serialization/deadlock failures can leave pgbench's exit status zero.
	ok( defined($failed) && $failed == 0,
		"$name reports no failed transactions") or diag($job->{stdout});
	$job->{processed} = $processed;
	$job->{failed} = $failed;
	$report{pgbench}{$name} =
	  { map { $_ => $job->{$_} }
		  qw(args stdout stderr start processed failed) };
}
$report{load_finished} = time;
$report{checkpointer_after} =
  sql_json($writer, 'SELECT * FROM pg_stat_checkpointer');
my $drain_start = time;
$report{final_lsn} = catchup();
$report{drain_seconds} = time - $drain_start;
if ($mode eq 'remote')
{
	for my $reader (@readers)
	{
		my $before = $report{compute_before}{ $reader->name };
		my $after =
		  sql_json($reader, 'SELECT * FROM test_page_store_compute_status()');
		$report{compute_load_end}{ $reader->name } = $after;
		ok( $after->{fetches} > $before->{fetches}
			  && $after->{skipped} > $before->{skipped}
			  && $after->{cached} > $before->{cached},
			$reader->name
			  . ' used remote reads and both redo paths during pgbench');
	}
}

sub sql_json
{
	my ($node, $query) = @_;
	return decode_json(
		$node->safe_psql('postgres', "SELECT row_to_json(s) FROM ($query) s")
	);
}
$report{history} =
  sql_json($storage, 'SELECT * FROM test_page_store_history_status()');
ok($report{history}{accepting}, 'history capacity was not exhausted');
cmp_ok($report{history}{records},
	'<', 60000, 'record history retains headroom');
cmp_ok($report{history}{pages}, '<', 120000, 'page history retains headroom');
$report{wal} =
  sql_json($writer, 'SELECT * FROM test_page_store_wal_status()');
is( $writer->safe_psql(
		'postgres',
		'SELECT flushed >= requested FROM test_page_store_wal_status()'),
	't',
	'writer durability requests reached the independent inbox');
$report{history_rows} =
  $writer->safe_psql('postgres', 'SELECT count(*) FROM pgbench_history');
cmp_ok($report{history_rows}, '>', 0, 'the workload committed writes');
is( $report{history_rows},
	$report{pgbench}{ $writer->name }{processed},
	'each reported writer transaction has a committed history row');

# Force writer writeback and reload; a warm shared buffer must not hide a
# missing overlay image.  This work is deliberately outside measured pgbench.
if ($mode eq 'remote')
{
	$writer->safe_psql('postgres', 'CHECKPOINT');
	for my $name (@names)
	{
		$writer->poll_query_until('postgres',
			"SELECT buffers_skipped = 0 FROM pg_buffercache_evict_relation('$name')"
		) or die "could not evict $name";
	}
}
catchup();
for my $table (@tables)
{
	my $sql =
		"SELECT md5(string_agg(ctid::text || ':' || row_to_json(t)::text, "
	  . "E'\\n' ORDER BY ctid)) FROM $table t";
	my $expected = $storage->safe_psql('postgres', $sql);
	for my $node ($writer, @readers)
	{
		is($node->safe_psql('postgres', $sql),
			$expected,
			$node->name . " $table values and physical TIDs match storage");
	}
}
for my $node ($writer, @readers)
{
	for my $index (@indexes)
	{
		$node->safe_psql('postgres', "SELECT bt_index_check('$index', true)");
	}
	pass($node->name . ' indexes agree with their heaps');
}
if ($mode eq 'remote')
{
	for my $reader (@readers)
	{
		my $stats =
		  sql_json($reader, 'SELECT * FROM test_page_store_compute_status()');
		$report{compute}{ $reader->name } = $stats;
		ok( $stats->{fetches} > 0
			  && $stats->{skipped} > 0
			  && $stats->{cached} > 0,
			$reader->name . ' exercised remote reads and both redo paths');
	}
	$report{overlay} =
	  sql_json($writer, 'SELECT * FROM test_page_store_overlay_status()');
	ok($report{overlay}{reads} > 0 && $report{overlay}{writes} > 0,
		'writer reloaded exact runtime images');
	is(scalar(grep { -e $_ } @missing),
		0, 'all seven selected relations stay remote on all three computes');
}
open my $out, '>', "$PostgreSQL::Test::Utils::log_path/pgbench-$mode.json"
  or die "open benchmark report: $!";
print $out JSON::PP->new->pretty->canonical->encode(\%report);
close $out or die "close benchmark report: $!";

# Optional interactive phase, outside the measured workload and its checks.
# Keep the harness alive so that its normal exit cleanup still owns the nodes.
if ($hold > 0 && PostgreSQL::Test::Utils::all_tests_passing())
{
	my $ready = "$PostgreSQL::Test::Utils::log_path/pgbench-demo.json";
	my $resume = "$PostgreSQL::Test::Utils::log_path/pgbench-demo.resume";
	die "resume file already exists: $resume" if -e $resume;
	my %demo = (resume_file => $resume, expires_at => time + $hold);
	for my $node ($writer, @readers, $storage, $inbox)
	{
		$demo{nodes}{ $node->name } = {
			connstr => $node->connstr('postgres'),
			pgdata => $node->data_dir,
			logfile => $node->logfile
		};
	}
	open my $manifest, '>', "$ready.tmp" or die "open demo manifest: $!";
	print $manifest JSON::PP->new->pretty->canonical->encode(\%demo);
	close $manifest or die "close demo manifest: $!";
	rename("$ready.tmp", $ready) or die "publish demo manifest: $!";
	note "Live demo: $ready; create $resume to stop early";
	while (time < $demo{expires_at} && !-e $resume)
	{
		sleep 1;
	}
}
$writer->stop;
$_->stop for @readers;
$storage->stop;
$inbox->stop;
done_testing();
