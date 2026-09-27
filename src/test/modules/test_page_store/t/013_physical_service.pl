# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Exercise the binary protocol independently of its libpq SMgr consumer.
use strict;
use warnings FATAL => 'all';

use Errno qw(ECONNRESET);
use IO::Select;
use IO::Socket::UNIX;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# 011/012 exercise the libpq consumer on every platform.  This raw protocol
# fixture uses the private Unix socket so it need not implement SSPI or expose
# a trust-authenticated TCP service to other users of the test machine.
plan skip_all => 'raw protocol fixture needs Unix sockets'
  if $PostgreSQL::Test::Utils::windows_os
  && !$PostgreSQL::Test::Utils::use_unix_sockets;

my $primary = PostgreSQL::Test::Cluster->new('protocol_writer');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', 'autovacuum = off');
$primary->start;
$primary->safe_psql(
	'postgres', q{
CREATE EXTENSION test_page_store;
CREATE TABLE protocol_heap (id int, payload text);
INSERT INTO protocol_heap SELECT i, repeat(md5(i::text), 4)
  FROM generate_series(1, 500) i;
SELECT pg_create_physical_replication_slot('protocol_storage', true);
});
my $user = $primary->safe_psql('postgres', 'SELECT current_user');
my $sysid = $primary->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $primary->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $block_size = $primary->safe_psql('postgres', 'SHOW block_size');
my @locator = split /\|/, $primary->safe_psql(
	'postgres', q{
SELECT (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
       (SELECT oid FROM pg_database WHERE datname = current_database()),
       pg_relation_filenode('protocol_heap')
});
$primary->backup('protocol_seed');
my $storage = PostgreSQL::Test::Cluster->new('protocol_storage');
$storage->init_from_backup($primary, 'protocol_seed', has_streaming => 1);
$storage->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'protocol_storage'
shared_preload_libraries = 'test_page_store'
test_page_store.history_pages = 1024
test_page_store.history_durable = true
fsync = on
});
$storage->start;
$primary->safe_psql('postgres',
	"SELECT pg_create_restore_point('protocol-baseline')");
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');
$storage->poll_query_until('postgres',
	"SELECT pg_get_wal_replay_pause_state() = 'paused'")
  or die 'not paused';
my $cut = $storage->safe_psql('postgres',
	"SELECT test_page_store_retain('protocol_heap')");
my ($high, $low) = map { hex($_) } split m{/}, $cut;
my $cut_number = $high * 4294967296 + $low;
my $fetch =
	'test_page_store_fetch('
  . join(',', @locator)
  . ",0,0,2,'$sysid',$tli,'$cut',false)";
my $expected =
  $storage->safe_psql('postgres', "SELECT encode(pages, 'hex') FROM $fetch");
my $nblocks = $storage->safe_psql('postgres', "SELECT nblocks FROM $fetch");
$storage->safe_psql('postgres', 'SELECT pg_wal_replay_resume()');

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
		Peer => $storage->host . '/.s.PGSQL.' . $storage->port
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
	send_frame($socket, 'Q', "TEST_PAGE_SERVICE\0");
	my ($kind, $body) = read_frame($socket);
	die "expected CopyBoth, got $kind: $body" unless $kind eq 'W';
	($kind, $body) = read_frame($socket);
	my ($tag, $version, $pgversion, $blcksz, $identity) =
	  unpack('aNNNQ>', $body);
	die 'invalid physical greeting'
	  unless $kind eq 'd'
	  && length($body) == 21
	  && $tag eq 'h'
	  && $version == 1
	  && $blcksz == $block_size
	  && $identity eq $sysid;
	return ($socket, $pid);
}

sub page_request
{
	my ($count, $fork, $timeline) = @_;
	return pack('aQ>NQ>N3CNN C',
		'p', 1, $timeline, $cut_number, @locator, $fork, 0, $count, 0);
}

my ($socket, $pid) = open_service();
is( $storage->safe_psql(
		'postgres',
		"SELECT datid IS NULL AND backend_type = 'walsender' FROM pg_stat_activity WHERE pid = $pid"
	),
	't',
	'page service has no database connection');
my $request = page_request(2, 0, $tli);
send_frame($socket, 'd', $request);
my ($kind, $body) = read_frame($socket);
is($kind, 'd', 'page batch uses CopyData');
is(substr($body, 0, length($request)),
	$request, 'response echoes the complete physical read identity');
is(unpack('C', substr($body, length($request), 1)),
	1, 'existing fork is reported');
is(unpack('N', substr($body, length($request) + 1, 4)),
	$nblocks, 'physical fork size matches SQL oracle');
is(unpack('H*', substr($body, length($request) + 5)),
	$expected, 'binary batch matches two exact retained pages');

my $before = pack('aQ>NQ>', 'b', 2, $tli, $cut_number);
send_frame($socket, 'd', $before);
($kind, $body) = read_frame($socket);
is( $body,
	$before . pack('Q>', $cut_number),
	'recovery predecessor uses the same physical session');
send_frame($socket, 'c', '');
($kind, $body) = read_frame($socket);
is($kind, 'c', 'CopyDone ends the page stream');
($kind, $body) = read_frame($socket);
is($body, "TEST_PAGE_SERVICE\0", 'command completion follows CopyDone');
($kind, $body) = read_frame($socket);
is($kind, 'Z', 'session returns to normal command processing');
send_frame($socket, 'Q', "IDENTIFY_SYSTEM\0");
($kind, $body) = read_frame($socket);
is($kind, 'T', 'built-in replication command still works after page service');
close $socket;

for my $case (
	[
		page_request(65, 0, $tli),
		qr/invalid physical page request/,
		'oversized batch'
	],
	[ page_request(0, 1, $tli), qr/not retained/, 'unsupported fork' ],
	[ page_request(0, 0, $tli + 1), qr/not retained/, 'different timeline' ],
	[ 'p', qr/insufficient data/, 'truncated request' ])
{
	($socket) = open_service();
	send_frame($socket, 'd', $case->[0]);
	($kind, $body) = read_frame($socket);
	is($kind, 'E', "$case->[2] is rejected");
	like($body, $case->[1], "$case->[2] has an explicit error");
	close $socket;
}

# pq_getmessage drops an oversized frame before reading its body.  This is a
# connection failure, not an ErrorResponse for a parsed request.
($socket) = open_service();
send_bytes($socket, 'd' . pack('N', 69));
die 'oversized frame did not close the connection'
  unless IO::Select->new($socket)->can_read(30);
my $eof = sysread($socket, my $unused, 1);
# Windows can report a connection reset rather than an orderly EOF.
ok(defined($eof) ? $eof == 0 : $! == ECONNRESET,
	'oversized frame closes the connection before reading its body')
  or diag("read after oversized frame: $!");
like(
	slurp_file($storage->logfile),
	qr/invalid message length/,
	'oversized frame is diagnosed in the server log');
close $socket;

# A waiting request must be cancellable without a transaction or SQL executor.
($socket, $pid) = open_service();
send_frame($socket, 'd',
	pack('aQ>NQ>', 'b', 3, $tli, $cut_number + 16777216));
$storage->poll_query_until('postgres',
	"SELECT wait_event = 'TestPageStoreHistory' FROM pg_stat_activity WHERE pid = $pid"
) or die 'page service did not wait for future history';
is($storage->safe_psql('postgres', "SELECT pg_cancel_backend($pid)"),
	't', 'cancel delivered to the waiting physical service');
($kind, $body) = read_frame($socket);
is($kind, 'E', 'cancel terminates the outstanding page request');
like(
	$body,
	qr/canceling statement due to user request/,
	'physical history wait checks cancellation');
($kind, $body) = read_frame($socket);
is($kind, 'Z', 'cancel returns the session to command processing');
close $socket;

# Do not let SQL functions or a database connection accidentally implement the
# physical endpoint.  Keep normal hot standby startup: the postmaster rejects
# all incoming replication connections too when hot_standby is off.
$primary->safe_psql('postgres', 'DROP EXTENSION test_page_store');
$primary->wait_for_catchup($storage, 'replay', $primary->lsn('insert'));
$storage->stop;
rename(
	$storage->data_dir . '/pg_hba.conf',
	$storage->data_dir . '/pg_hba.conf.saved') or die "rename hba: $!";
$storage->append_conf('pg_hba.conf',
	"local replication all trust\nlocal all all reject\n");
$storage->start;
my ($stdout, $stderr);
isnt(
	$storage->psql(
		'postgres', 'SELECT 1',
		stdout => \$stdout,
		stderr => \$stderr),
	0,
	'ordinary SQL connections are disabled on storage');
like(
	$stderr,
	qr/pg_hba.conf rejects connection/,
	'SQL connection fails because only physical sessions are allowed');
($socket) = open_service();
send_frame($socket, 'd', $request);
($kind, $body) = read_frame($socket);
is( unpack('H*', substr($body, length($request) + 5)),
	$expected,
	'physical page service works without SQL objects or database connections'
);

# Keep an idle service connection open.  It must not hold up fast shutdown by
# claiming to have outstanding WAL that still needs delivery.
$storage->stop;
pass('storage shuts down with an idle physical page-service connection');
close $socket;
$primary->stop;
done_testing();
