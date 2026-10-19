# Copyright (c) 2026, PostgreSQL Global Development Group

# Externally allocated timeline IDs, including gaps and concurrent requests.
use strict;
use warnings FATAL => 'all';
use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;
my $injection = $ENV{enable_injection_points} eq 'yes'
  && $primary->check_extension('injection_points');
$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points')
  if $injection;
$primary->safe_psql(
	'postgres', q{
CREATE TABLE data (i int);
INSERT INTO data VALUES (1);
CREATE ROLE promoter;
GRANT EXECUTE ON FUNCTION pg_promote(boolean, integer) TO promoter;
});
$primary->backup('backup');

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
my $follower = PostgreSQL::Test::Cluster->new('follower');
$follower->init_from_backup($primary, 'backup', has_streaming => 1);
$follower->append_conf('postgresql.conf',
	"primary_conninfo = '" . $standby->connstr . "'\n");

# Log every archive probe and report the file missing.  Promotion must not
# fetch a prospective timeline or the parent's history after choosing the ID.
my $perl = $^X;
$perl =~ s!\\!/!g if $windows_os;
for my $node ($standby, $follower)
{
	$node->append_conf(
		'postgresql.conf', qq{
restore_command = '"$perl" "$FindBin::RealBin/restore_probe" "%f"'
archive_mode = on
archive_command = ''
});
	$node->start;
}
$primary->wait_for_replay_catchup($standby);
$standby->wait_for_catchup($follower);

is( $standby->safe_psql(
		'postgres', q{
SELECT has_function_privilege('promoter', 'pg_promote(boolean,integer)',
                             'EXECUTE')
   AND NOT has_function_privilege('promoter',
                                  'pg_promote(boolean,integer,bigint)',
                                  'EXECUTE')
}),
	't',
	'existing grant does not authorize explicit timeline allocation');

for my $id (-1, 0, 1, 4294967296)
{
	my ($out, $err);
	is( $standby->psql(
			'postgres', "SELECT pg_promote(true, 60, $id)",
			stdout => \$out,
			stderr => \$err),
		3,
		"reject timeline $id");
	like($err, qr/new_timeline.*must be between/, 'range check');
}

my $collision = $standby->data_dir . '/pg_wal/000000630000000000000000';
append_to_file($collision, 'existing WAL');
my ($out, $err);
is( $standby->psql(
		'postgres', 'SELECT pg_promote(true, 60, 99)',
		stdout => \$out,
		stderr => \$err),
	3,
	'reject existing local WAL');
like($err, qr/timeline ID 99 is already present/, 'local conflict reported');
unlink($collision) or die "unlink $collision: $!";

$primary->stop;

if ($injection)
{
	$standby->safe_psql('postgres',
		"SELECT injection_points_attach('promotion-after-wal-segment-cleanup', 'wait')"
	);
}
is( $standby->safe_psql(
		'postgres',
		'SELECT pg_promote(wait => false, wait_seconds => 60, new_timeline => 42)'
	),
	't',
	'accept explicit promotion');

if ($injection)
{
	$standby->wait_for_event('startup',
		'promotion-after-wal-segment-cleanup');
	is($standby->safe_psql('postgres', 'SELECT pg_promote(false, 60, 42)'),
		't', 'same request can be repeated');
	is( $standby->psql(
			'postgres', 'SELECT pg_promote(false, 60, 43)',
			stdout => \$out,
			stderr => \$err),
		3,
		'reject conflicting request');
	like(
		$err,
		qr/cannot change timeline/,
		'conflict does not replace request');
	is( $standby->safe_psql(
			'postgres', 'SELECT pg_promote(wait_seconds => 1)'),
		'f',
		'legacy call waits for existing request and times out');
	$standby->safe_psql('postgres',
		"SELECT injection_points_wakeup('promotion-after-wal-segment-cleanup')"
	);
}
$standby->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die 'promotion did not complete';
is( $standby->safe_psql(
		'postgres',
		'SELECT left(pg_walfile_name(pg_current_wal_insert_lsn()), 8)'),
	'0000002A',
	'requested timeline used despite wait timeout');
ok(-f $standby->data_dir . '/pg_wal/0000002A.history', 'new history exists');

sub no_history_fetch_after_selection
{
	my ($node, $id) = @_;
	my $log = slurp_file($node->logfile);
	my ($after) = $log =~ /selected new timeline ID: $id\r?\n(.*)/s;
	ok(defined($after), "timeline $id selection logged");
	unlike(
		$after // '',
		qr/restore probe: .*\.history/,
		'no parent history fetch after timeline selection');
}
no_history_fetch_after_selection($standby, 42);

$standby->safe_psql('postgres', 'INSERT INTO data VALUES (2)');
$standby->wait_for_replay_catchup($follower);
is($follower->safe_psql('postgres', 'SELECT sum(i) FROM data'),
	3, 'streaming follower crosses a gap in timeline IDs');

is( $follower->psql(
		'postgres', 'SELECT pg_promote(true, 60, 41)',
		stdout => \$out,
		stderr => \$err),
	3,
	'reject ID below replay timeline');
like($err, qr/must be greater than timeline ID 42/, 'monotonicity checked');

# Use the unsigned half of the TLI range and a parent other than timeline 1.
$standby->stop;
is( $follower->safe_psql(
		'postgres', 'SELECT pg_promote(true, 300, 2147483648)'),
	't',
	'promote with an ID above signed int32 range');
like(slurp_file($follower->data_dir . '/pg_wal/80000000.history'),
	qr/^42\t/m, 'parent timeline retained in new history');
no_history_fetch_after_selection($follower, 2147483648);
$follower->safe_psql('postgres', 'INSERT INTO data VALUES (3)');
$follower->restart;
is($follower->safe_psql('postgres', 'SELECT sum(i) FROM data'),
	6, 'restart after explicit promotion preserves data');

# The original API and pg_ctl remain usable without a timeline argument.
# Reuse the fenced original primary as an isolated standby.
$primary->set_standby_mode;
$primary->start;
is($primary->safe_psql('postgres', "SELECT pg_promote('true', 300)"),
	't', 'original SQL signature still promotes');
is( $primary->safe_psql(
		'postgres',
		'SELECT left(pg_walfile_name(pg_current_wal_insert_lsn()), 8)'),
	'00000002',
	'automatic timeline allocation retained');

done_testing();
