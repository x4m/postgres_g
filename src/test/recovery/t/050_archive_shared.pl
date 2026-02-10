# Copyright (c) 2025, PostgreSQL Global Development Group

# Test archive_mode=shared for coordinated WAL archiving between primary and standby
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Path qw(rmtree);

# Initialize primary node with archiving
my $archive_dir = PostgreSQL::Test::Utils::tempdir();
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(has_archiving => 1, allows_streaming => 1);
$primary->append_conf('postgresql.conf', "
archive_mode = shared
archive_command = 'cp %p \"$archive_dir\"/%f'
wal_keep_size = 128MB
");
$primary->start;

# Create a test table and generate some WAL
$primary->safe_psql('postgres', 'CREATE TABLE test_table (id int, data text);');
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'data' || i FROM generate_series(1, 500) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'data' || i FROM generate_series(501, 1000) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait for archiver to archive segments
$primary->poll_query_until('postgres',
	"SELECT archived_count > 0 FROM pg_stat_archiver")
	or die "Timed out waiting for archiver to start";

my $archived_count = () = glob("$archive_dir/*");
ok($archived_count > 0, "primary has archived WAL files to shared archive");
note("Primary archived $archived_count files");

# Take backup for standby
my $backup_name = 'standby_backup';
$primary->backup($backup_name);

# Exclude possible race condition when backup WAL is last archived
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'data' || i FROM generate_series(501, 1000) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Set up standby with archive_mode=shared
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', "
archive_mode = shared
archive_command = 'cp %p \"$archive_dir\"/%f'
wal_receiver_status_interval = 1s
");
$standby->start;

# Wait for standby to catch up
$primary->wait_for_catchup($standby);

# Generate more WAL on primary (these are new segments not yet archived)
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'data' || i FROM generate_series(1001, 1500) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'data' || i FROM generate_series(1501, 2000) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait for standby to receive the new WAL
$primary->wait_for_catchup($standby);

# Check that standby has .ready or .done files for the newly received segments.
# Normally they should be .ready (not yet archived by primary), but in rare cases
# the archiver could be very fast and an archive report sent immediately, creating
# .done files instead. Both are correct behavior - the key is that files exist.
my $standby_archive_status = $standby->data_dir . '/pg_wal/archive_status';
my $status_count = 0;
if (opendir(my $dh, $standby_archive_status))
{
	my @files = grep { /\.(ready|done)$/ } readdir($dh);
	$status_count = scalar(@files);
	my $ready_count = scalar(grep { /\.ready$/ } @files);
	my $done_count = scalar(grep { /\.done$/ } @files);
	note("Standby has $ready_count .ready files and $done_count .done files");
	closedir($dh);
}
cmp_ok($status_count, '>', 0, "standby creates archive status files for received WAL");

# Generate more WAL and wait for archiving on primary
my $initial_archived = $primary->safe_psql('postgres', 'SELECT archived_count FROM pg_stat_archiver');
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'more-data' || i FROM generate_series(2001, 2500) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');
$primary->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'more-data2' || i FROM generate_series(2501, 3000) i;");
$primary->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait for primary to archive the new segments
$primary->poll_query_until('postgres',
	"SELECT archived_count > $initial_archived FROM pg_stat_archiver")
	or die "Timed out waiting for primary to archive new segments";

# Wait for standby to catch up (archive status is sent during replication)
$primary->wait_for_catchup($standby);

# Wait for primary to send archival status updates and standby to process them
# The standby should mark segments as .done after receiving archive status from primary
my $done_count = 0;
for (my $i = 0; $i < $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	$done_count = 0;
	if (opendir(my $dh, $standby_archive_status))
	{
		$done_count = scalar(grep { /\.done$/ } readdir($dh));
		closedir($dh);
	}
	last if $done_count > 0;
	sleep(1);
}
ok($done_count > 0, "standby marked segments as .done after primary's archival report");
note("Standby has $done_count .done files");

###############################################################################
# Test 2: Standby promotion - verify archiver activates
###############################################################################

# Before promotion, verify archiver is not running on standby (shared mode during recovery)
# In shared mode, the standby's archiver should not be archiving during recovery
my $archived_before = $standby->safe_psql('postgres', 
	"SELECT archived_count FROM pg_stat_archiver");
is($archived_before, '0', 
	"archiver not active on standby before promotion (archived_count=0)");

# Verify standby is still in recovery before promoting
my $in_recovery = $standby->safe_psql('postgres', "SELECT pg_is_in_recovery();");
is($in_recovery, 't', "standby is in recovery before promotion");

# Promote the standby
$standby->promote;
$standby->poll_query_until('postgres', "SELECT NOT pg_is_in_recovery();");

# Generate WAL on new primary (former standby)
$standby->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'post-promotion' || i FROM generate_series(2001, 2500) i;");
$standby->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait for archiver to activate and archive the new WAL
# Check pg_stat_archiver to verify archiving is happening
$standby->poll_query_until('postgres',
	"SELECT archived_count > 0 FROM pg_stat_archiver")
	or die "Timed out waiting for promoted standby to start archiving";
pass("promoted standby started archiving");

# Verify data integrity
my $count = $standby->safe_psql('postgres', 'SELECT COUNT(*) FROM test_table;');
ok($count >= 2500, "promoted standby has all data (got $count rows)");

###############################################################################
# Test 3: Cascading replication
###############################################################################

# Take a backup from the promoted standby (now the new primary)
my $promoted_backup = 'promoted_backup';
$standby->backup($promoted_backup);

# Set up second-level standby (cascading from first standby, now promoted)
my $standby2 = PostgreSQL::Test::Cluster->new('standby2');
$standby2->init_from_backup($standby, $promoted_backup, has_streaming => 1);
$standby2->append_conf('postgresql.conf', "
archive_mode = shared
archive_command = 'cp %p \"$archive_dir\"/%f'
wal_receiver_status_interval = 1s
");
$standby2->start;

# Generate WAL on promoted standby (now primary for standby2)
my $cascading_archived_before = $standby->safe_psql('postgres', 'SELECT archived_count FROM pg_stat_archiver');
$standby->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'cascading' || i FROM generate_series(2501, 3000) i;");
$standby->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait for the promoted standby (acting as primary) to archive the new segment
$standby->poll_query_until('postgres',
	"SELECT archived_count > $cascading_archived_before FROM pg_stat_archiver")
	or die "Timed out waiting for primary to archive segment in cascading test";

# Wait for cascading standby to catch up
$standby->wait_for_catchup($standby2);

# Wait for cascading standby to receive archive status and mark segments as .done
my $standby2_archive_status = $standby2->data_dir . '/pg_wal/archive_status';
my $standby2_done_count = 0;
for (my $i = 0; $i < $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	$standby2_done_count = 0;
	if (opendir(my $dh, $standby2_archive_status))
	{
		$standby2_done_count = scalar(grep { /\.done$/ } readdir($dh));
		closedir($dh);
	}
	last if $standby2_done_count > 0;
	sleep(1);
}
ok($standby2_done_count > 0, "cascading standby marks segments as .done");
note("Cascading standby has $standby2_done_count .done files");

# Verify cascading standby has all data
my $standby2_count = $standby2->safe_psql('postgres', 'SELECT COUNT(*) FROM test_table;');
ok($standby2_count >= 3000, "cascading standby has all data (got $standby2_count rows)");

###############################################################################
# Test 4: Multiple standbys from same primary
###############################################################################

# Create third standby from promoted standby (current primary)
my $standby3 = PostgreSQL::Test::Cluster->new('standby3');
my $backup2 = 'multi_standby_backup';
$standby->backup($backup2);
$standby3->init_from_backup($standby, $backup2, has_streaming => 1);
$standby3->append_conf('postgresql.conf', "
archive_mode = shared
archive_command = 'cp %p \"$archive_dir\"/%f'
wal_receiver_status_interval = 1s
");
$standby3->start;

# Generate WAL and ensure both standbys receive it
my $standby_archived_before = $standby->safe_psql('postgres', 'SELECT archived_count FROM pg_stat_archiver');
$standby->safe_psql('postgres', "INSERT INTO test_table SELECT i, 'multi' || i FROM generate_series(3001, 3500) i;");
$standby->safe_psql('postgres', 'SELECT pg_switch_wal();');

# Wait for the promoted standby (acting as primary) to archive the new segment
$standby->poll_query_until('postgres',
	"SELECT archived_count > $standby_archived_before FROM pg_stat_archiver")
	or die "Timed out waiting for primary to archive segment in multi-standby test";

$standby->wait_for_catchup($standby2);
$standby->wait_for_catchup($standby3);

# Verify both standbys eventually mark segments as .done
my $standby3_archive_status = $standby3->data_dir . '/pg_wal/archive_status';

for (my $i = 0; $i < $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	$standby2_done_count = 0;
	if (opendir(my $dh, $standby2_archive_status))
	{
		$standby2_done_count = scalar(grep { /\.done$/ } readdir($dh));
		closedir($dh);
	}
	last if $standby2_done_count > 0;
	sleep(1);
}

my $standby3_done_count = 0;
for (my $i = 0; $i < $PostgreSQL::Test::Utils::timeout_default; $i++)
{
	$standby3_done_count = 0;
	if (opendir(my $dh, $standby3_archive_status))
	{
		$standby3_done_count = scalar(grep { /\.done$/ } readdir($dh));
		closedir($dh);
	}
	last if $standby3_done_count > 0;
	sleep(1);
}

ok($standby2_done_count > 0, "standby2 marks segments as .done");
ok($standby3_done_count > 0, "standby3 marks segments as .done");
note("standby2 has $standby2_done_count .done files, standby3 has $standby3_done_count .done files");

# Verify both standbys have all data
$standby2_count = $standby2->safe_psql('postgres', 'SELECT COUNT(*) FROM test_table;');
my $standby3_count = $standby3->safe_psql('postgres', 'SELECT COUNT(*) FROM test_table;');
ok($standby2_count >= 3500, "standby2 has all data (got $standby2_count rows)");
ok($standby3_count >= 3500, "standby3 has all data (got $standby3_count rows)");

done_testing();
