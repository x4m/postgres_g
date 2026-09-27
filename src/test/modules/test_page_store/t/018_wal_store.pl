# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A separate postmaster durably accepts an opaque, contiguous WAL stream.
# Exercise retries, fencing of old connections and crashes on both sides of
# publication.  Neither SQL objects nor the source's availability implement
# the service.  This tests a WAL inbox, not compute restart or power loss.
use strict;
use warnings FATAL => 'all';

use IO::Select;
use IO::Socket::UNIX;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'raw protocol fixture needs Unix sockets'
  if $PostgreSQL::Test::Utils::windows_os
  && !$PostgreSQL::Test::Utils::use_unix_sockets;

my $source = PostgreSQL::Test::Cluster->new('inbox_source');
$source->init(allows_streaming => 1);
$source->start;
my $sysid = $source->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $source->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $segsize = $source->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $walfile = $source->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_current_wal_insert_lsn())');
my $start = hex(substr($walfile, 8, 8)) * 4294967296 +
  hex(substr($walfile, 16, 8)) * $segsize;
$source->safe_psql('postgres',
	'CREATE TABLE inbox_rows AS SELECT i, md5(i::text) FROM generate_series(1, 5000) i; CHECKPOINT'
);
my $wal = substr(slurp_file($source->data_dir . '/pg_wal/' . $walfile), 0,
	512 * 1024);
$source->stop;

my $store = PostgreSQL::Test::Cluster->new('wal_inbox');
$store->init(allows_streaming => 1, force_initdb => 1);
$store->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store = true
test_page_store.wal_store_max_mb = 1
fsync = on
autovacuum = off
});
$store->start;
my $user = $store->safe_psql('postgres', 'SELECT current_user');
my $service_sysid = $store->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
isnt($service_sysid, $sysid,
	'the WAL service is not a physical replica of the source');
my $bytes_path = $store->data_dir . '/test_page_store.wal/bytes';
my $control_path = $store->data_dir . '/test_page_store.wal/control';

sub send_bytes
{
	my ($socket, $bytes) = @_;
	while (length $bytes)
	{
		my $n = syswrite($socket, $bytes);
		die "protocol write: $!" unless defined $n && $n > 0;
		substr($bytes, 0, $n, '');
	}
}

sub read_bytes
{
	my ($socket, $count) = @_;
	my $bytes = '';
	my $select = IO::Select->new($socket);
	while (length($bytes) < $count)
	{
		die 'protocol read timed out' unless $select->can_read(30);
		my $n = sysread($socket, my $part, $count - length($bytes));
		die 'protocol connection ended early' unless defined $n && $n > 0;
		$bytes .= $part;
	}
	return $bytes;
}

sub send_frame
{
	my ($socket, $kind, $body) = @_;
	send_bytes($socket, $kind . pack('N', length($body) + 4) . $body);
}

sub read_frame
{
	my ($socket) = @_;
	my ($kind, $length) = unpack('aN', read_bytes($socket, 5));
	die 'invalid response length' if $length < 4 || $length > 1024 * 1024;
	return ($kind, read_bytes($socket, $length - 4));
}

sub open_service
{
	my $socket = IO::Socket::UNIX->new(
		Type => SOCK_STREAM,
		Peer => $store->host . '/.s.PGSQL.' . $store->port
	) or die "protocol connect: $!";
	my $startup = pack('N', 196608)
	  . "user\0$user\0database\0does_not_exist\0replication\0true\0\0";
	send_bytes($socket, pack('N', length($startup) + 4) . $startup);
	my $pid;
	while (1)
	{
		my ($kind, $body) = read_frame($socket);
		die $body if $kind eq 'E';
		die 'raw fixture requires trust authentication'
		  if $kind eq 'R' && unpack('N', $body) != 0;
		$pid = unpack('N', $body) if $kind eq 'K';
		last if $kind eq 'Z';
	}
	send_frame($socket, 'Q', "TEST_WAL_SERVICE\0");
	my ($kind, $body) = read_frame($socket);
	die "expected CopyBoth, got $kind: $body" unless $kind eq 'W';
	($kind, $body) = read_frame($socket);
	my ($tag, $version, $pgversion, $blcksz, $identity) =
	  unpack('aNNNQ>', $body);
	die 'invalid WAL service greeting'
	  unless $kind eq 'd'
	  && length($body) == 21
	  && $tag eq 'h'
	  && $version == 1
	  && $identity eq $service_sysid;
	return ($socket, $pid);
}

sub request
{
	my ($kind, $epoch, $lsn, $tail) = @_;
	return
	  pack('aQ>NQ>Q>', $kind, $sysid, $tli, $epoch, $lsn) . ($tail // '');
}

sub exchange
{
	my ($socket, $request) = @_;
	send_frame($socket, 'd', $request);
	my ($kind, $body) = read_frame($socket);
	die "WAL service failed: $kind $body" unless $kind eq 'd';
	die 'invalid WAL response size' if length($body) < 49;
	my ($tag, $id, $timeline, $epoch, $lsn, $base, $flushed, $seg) =
	  unpack('aQ>NQ>Q>Q>Q>N', $body);
	die 'WAL response identity mismatch'
	  unless $tag eq substr($request, 0, 1)
	  && $id eq $sysid
	  && $timeline == $tli
	  && $lsn == unpack('Q>', substr($request, 21, 8))
	  && $base == $start
	  && $seg == $segsize;
	return ($flushed, $epoch, substr($body, 49));
}

sub reject
{
	my ($request, $error, $label) = @_;
	my ($socket) = open_service();
	send_frame($socket, 'd', $request);
	my ($kind, $body) = read_frame($socket);
	ok($kind eq 'E' && $body =~ $error, $label)
	  or diag("unexpected response: $kind $body");
	close $socket;
}

my ($socket, $pid) = open_service();
is( $store->safe_psql(
		'postgres',
		"SELECT datid IS NULL AND backend_type = 'walsender' FROM pg_stat_activity WHERE pid = $pid"
	),
	't',
	'WAL inbox runs without a database connection');
reject(
	request('s', 0, 0),
	qr/not initialized/,
	'an uninitialized stream has no durable frontier');
my ($frontier, $epoch) =
  exchange($socket, request('i', 1, $start, pack('N', $segsize)));
is($frontier, $start, 'empty stream has no acknowledged bytes');
is($epoch, 1, 'initial writer epoch is returned');
($frontier) =
  exchange($socket, request('a', 1, $start, substr($wal, 0, 32768)));
is($frontier, $start + 32768, 'append publishes its complete durable prefix');
($frontier) = exchange($socket,
	request('a', 1, $start + 16384, substr($wal, 16384, 49152)));
is( $frontier,
	$start + 65536,
	'matching overlap can extend the durable prefix');
($frontier) =
  exchange($socket, request('a', 1, $start, substr($wal, 0, 32768)));
is($frontier, $start + 65536, 'exact retry does not move the frontier back');
my (undef, undef, $received) =
  exchange($socket, request('r', 1, $start, pack('N', 65536)));
is( $received,
	substr($wal, 0, 65536),
	'physical read returns exactly the source WAL bytes');

for my $case (
	[ request('a', 1, $frontier + 1, 'gap'), qr/not contiguous/, 'gap' ],
	[
		request('a', 1, $start - 1, 'old'),
		qr/not contiguous/,
		'before retained start'
	],
	[
		request('a', 1, $start, substr($wal, 0, 1) ^ "\xff"),
		qr/disagrees with durable bytes/,
		'conflicting retry'
	],
	[
		request('r', 1, $frontier, pack('N', 1)),
		qr/exceeds durable frontier/,
		'unpublished read'
	],
	[
		request('r', 1, $start, pack('N', 0)),
		qr/invalid test WAL byte range/,
		'empty read'
	],
	[
		request('i', 1, $start + $segsize, pack('N', $segsize)),
		qr/initialization does not match/,
		'changed anchor'
	],
	[
		request('i', 1, $start, pack('N', 17)),
		qr/invalid test WAL store initialization/,
		'invalid segment size'
	],
	[ 'a', qr/insufficient data/, 'truncated frame' ],
	[
		pack('aQ>NQ>Q>', 's', $sysid + 1, $tli, 0, 0),
		qr/identity does not match/,
		'foreign system'
	],
	[
		pack('aQ>NQ>Q>', 's', $sysid, $tli + 1, 0, 0),
		qr/identity does not match/,
		'foreign timeline'
	])
{
	reject($case->[0], $case->[1], "$case->[2] is rejected");
}
($frontier) = exchange($socket, request('s', 0, 0));
is( $frontier,
	$start + 65536,
	'rejected requests leave durable frontier unchanged');

# The old connection remains open across fencing.  Even its harmless-looking
# retransmit must fail: checking epoch only at connection startup is wrong.
my ($new_socket) = open_service();
($frontier, $epoch) = exchange($new_socket, request('f', 1, 0));
is($epoch, 2, 'fence durably advances the epoch');
is($frontier, $start + 65536, 'fence does not invent or discard durable WAL');
send_frame($socket, 'd', request('a', 1, $start, substr($wal, 0, 8)));
my ($kind, $body) = read_frame($socket);
ok( $kind eq 'E' && $body =~ /epoch does not match/,
	'old connection cannot acknowledge a retry after fencing');
close $socket;
reject(
	request('f', 1, 0),
	qr/epoch does not match/,
	'competing fence with stale expected epoch fails');
($frontier) = exchange($new_socket,
	request('a', 2, $frontier, substr($wal, 65536, 32768)));
is($frontier, $start + 98304, 'new epoch appends without replacing old WAL');
close $new_socket;
$store->stop('immediate');
$store->start;
($socket) = open_service();
($frontier, $epoch) = exchange($socket, request('s', 0, 0));
is($epoch, 2, 'epoch survives process crash');
is($frontier, $start + 98304, 'acknowledged prefix survives process crash');
close $socket;
reject(
	request('a', 1, $frontier, 'stale'),
	qr/epoch does not match/,
	'old writer remains fenced after service restart');

SKIP:
{
	skip 'Injection points are not available', 19
	  unless $ENV{enable_injection_points} eq 'yes';
	$store->safe_psql('postgres', 'CREATE EXTENSION injection_points');
	for my $case (
		[ 'test-wal-store-before-data-sync', 0 ],
		[ 'test-wal-store-after-data-sync', 0 ],
		[ 'test-wal-store-after-publish', 1 ])
	{
		my ($point, $published) = @$case;
		$store->safe_psql('postgres',
			"SELECT injection_points_attach('$point', 'wait')");
		($socket, $pid) = open_service();
		my $append =
		  request('a', 2, $frontier, substr($wal, $frontier - $start, 16384));
		send_frame($socket, 'd', $append);
		$store->poll_query_until('postgres',
			"SELECT wait_event = '$point' FROM pg_stat_activity WHERE pid = $pid"
		) or die "WAL store did not reach $point";
		ok( !IO::Select->new($socket)->can_read(0),
			"$point: no ACK before completing the request");
		is( -s $bytes_path,
			$frontier + 16384 - $start,
			"$point: bytes really reached the data file");
		$store->stop('immediate');
		close $socket;
		$store->start;
		($socket) = open_service();
		my ($recovered, $recovered_epoch) =
		  exchange($socket, request('s', 0, 0));
		is( $recovered,
			$frontier + $published * 16384,
			"$point: only the published prefix survives");
		is( -s $bytes_path,
			$recovered - $start,
			"$point: unpublished tail is removed on startup");
		($frontier) = exchange($socket, $append);
		my (undef, undef, $tail) = exchange($socket,
			request('r', 2, $frontier - 16384, pack('N', 16384)));
		is( $tail,
			substr($wal, $frontier - $start - 16384, 16384),
			"$point: retry after lost ACK returns the exact WAL bytes");
		close $socket;
	}

	# An uncertain fence outcome cannot let the old writer regain authority.
	my $point = 'test-wal-store-after-publish';
	$store->safe_psql('postgres',
		"SELECT injection_points_attach('$point', 'wait')");
	($socket, $pid) = open_service();
	send_frame($socket, 'd', request('f', 2, 0));
	$store->poll_query_until('postgres',
		"SELECT wait_event = '$point' FROM pg_stat_activity WHERE pid = $pid")
	  or die 'fence did not reach publication';
	ok(!IO::Select->new($socket)->can_read(0), 'fence ACK has not been sent');
	$store->stop('immediate');
	close $socket;
	$store->start;
	($socket) = open_service();
	my ($after_fence, $after_epoch) = exchange($socket, request('s', 0, 0));
	is($after_epoch, 3, 'published fence survives even without its ACK');
	is($after_fence, $frontier,
		'uncertain fence preserves the durable prefix');
	$epoch = $after_epoch;
	close $socket;
	reject(
		request('a', 2, $frontier, 'stale'),
		qr/epoch does not match/,
		'writer from unacknowledged but fenced epoch is rejected');
}

# A durable control file without all of its bytes is corruption, not an empty
# or partially initialized store that a new writer can silently replace.
$store->stop;
my $saved = slurp_file($bytes_path);
truncate($bytes_path, length($saved) - 1) or die "truncate WAL: $!";
my $log_start = -s $store->logfile;
ok(!$store->start(fail_ok => 1),
	'missing acknowledged byte prevents startup');
ok($store->log_contains(qr/shorter than its durable frontier/, $log_start),
	'missing durable WAL has an explicit diagnostic');
open(my $file, '>', $bytes_path) or die "restore WAL: $!";
binmode $file;
print $file $saved;
close $file;

my $control = slurp_file($control_path);
rename($control_path, "$control_path.saved") or die "hide control: $!";
$log_start = -s $store->logfile;
ok(!$store->start(fail_ok => 1),
	'missing manifest cannot reinitialize existing WAL');
ok( $store->log_contains(
		qr/control is missing for existing bytes/, $log_start),
	'lost authority metadata has an explicit diagnostic');
rename("$control_path.saved", $control_path) or die "restore control: $!";
open($file, '+<', $control_path) or die "open control: $!";
binmode $file;
# Change the still-nonzero system identifier, not a separately checked magic
# or version field, so this specifically requires the checksum check.
seek($file, 8, 0) or die "seek control: $!";
print $file substr($control, 8, 1) ^ "\x80";
close $file;
$log_start = -s $store->logfile;
ok(!$store->start(fail_ok => 1), 'damaged manifest prevents startup');
ok($store->log_contains(qr/invalid test WAL store control/, $log_start),
	'manifest checksum is validated');

done_testing();
