# Copyright (c) 2026, PostgreSQL Global Development Group
#
# A WAL inbox acknowledges bytes, not records.  Export prefixes ending inside
# a record header and inside a multi-page record, then let ordinary recovery
# independently choose the new timeline's branch point.  Bad complete records
# must fail export instead of being treated as an incomplete tail.
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

sub number
{
	my ($lsn) = @_;
	my ($high, $low) = map { hex($_) } split m{/}, $lsn;
	return $high * 4294967296 + $low;
}

sub lsn
{
	my ($value) = @_;
	return sprintf('%X/%X', int($value / 4294967296), $value % 4294967296);
}

my $source = PostgreSQL::Test::Cluster->new('record_source');
$source->init(allows_streaming => 1, extra => ['--wal-segsize=1']);
$source->append_conf(
	'postgresql.conf', q{
fsync = on
autovacuum = off
checkpoint_timeout = '1h'
});
$source->start;
$source->safe_psql(
	'postgres', q{
CREATE TABLE prefix_rows (id integer PRIMARY KEY, payload text);
INSERT INTO prefix_rows VALUES (1, 'seed');
SELECT pg_create_physical_replication_slot('record_source', true);
});
my $sysid = $source->safe_psql('postgres',
	'SELECT system_identifier FROM pg_control_system()');
my $tli = $source->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
my $segsize = $source->safe_psql('postgres',
	"SELECT pg_size_bytes(current_setting('wal_segment_size'))");
my $blocksize = $source->safe_psql('postgres', 'SHOW wal_block_size');
my $start = number(
	$source->safe_psql(
		'postgres',
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'record_source'"
	));
$start -= $start % $segsize;
$source->backup('before_tail');
$source->safe_psql('postgres',
	"INSERT INTO prefix_rows VALUES (2, 'committed after seed')");
my $complete = number(
	$source->safe_psql(
		'postgres', "SELECT pg_create_restore_point('last-complete')"));
my $tail_end = number(
	$source->safe_psql(
		'postgres',
		"SELECT pg_logical_emit_message(false, 'incomplete', repeat('large record payload ', 5000))"
	));
my $switch_end =
  number($source->safe_psql('postgres', 'SELECT pg_switch_wal()'));
my $next_segment = int(($switch_end + $segsize - 1) / $segsize) * $segsize;
my $post_switch = number(
	$source->safe_psql(
		'postgres', "SELECT pg_create_restore_point('post-switch')"));
$source->safe_psql('postgres', 'CHECKPOINT');
$source->stop('immediate');

my $wal = '';
for (my $pos = $start; $pos < $post_switch; $pos += $segsize)
{
	my $name = sprintf('%08X%08X%08X',
		$tli,
		int($pos / 4294967296),
		int(($pos % 4294967296) / $segsize));
	$wal .= slurp_file($source->data_dir . "/pg_wal/$name");
}
rename($source->data_dir, $source->data_dir . '.lost')
  or die "isolate original source: $!";

my $store = PostgreSQL::Test::Cluster->new('record_inbox');
$store->init(allows_streaming => 1, force_initdb => 1);
$store->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'test_page_store'
test_page_store.wal_store = true
fsync = on
autovacuum = off
});
$store->start;
my $user = $store->safe_psql('postgres', 'SELECT current_user');

# Deliberately use the byte protocol so the fixture, not the real sender,
# chooses an incomplete frontier.  All reads have a bounded wait.
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
	while (length($bytes) < $count)
	{
		die 'protocol read timed out'
		  unless IO::Select->new($socket)->can_read(30);
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
	my $body = read_bytes($socket, $length - 4);
	die "service error: $body" if $kind eq 'E';
	return ($kind, $body);
}

my $socket = IO::Socket::UNIX->new(
	Type => SOCK_STREAM,
	Peer => $store->host . '/.s.PGSQL.' . $store->port
) or die "protocol connect: $!";
my $startup = pack('N', 196608) . "user\0$user\0replication\0true\0\0";
send_bytes($socket, pack('N', length($startup) + 4) . $startup);
while (1)
{
	my ($kind, $body) = read_frame($socket);
	die 'raw fixture requires trust authentication'
	  if $kind eq 'R' && unpack('N', $body) != 0;
	last if $kind eq 'Z';
}
send_frame($socket, 'Q', "TEST_WAL_SERVICE\0");
my ($kind, $body) = read_frame($socket);
die 'expected CopyBoth' unless $kind eq 'W';
($kind, $body) = read_frame($socket);
die 'invalid service greeting'
  unless $kind eq 'd' && length($body) == 21 && substr($body, 0, 1) eq 'h';

sub exchange
{
	my ($tag, $pos, $data) = @_;
	send_frame($socket, 'd',
		pack('aQ>NQ>Q>', $tag, $sysid, $tli, 1, $pos) . $data);
	my ($kind, $body) = read_frame($socket);
	die 'invalid WAL service response'
	  unless $kind eq 'd' && length($body) == 49;
	my ($reply, $id, $timeline, $epoch, $lsn, $base, $flushed, $seg) =
	  unpack('aQ>NQ>Q>Q>Q>N', $body);
	die 'WAL service response does not match'
	  unless $reply eq $tag
	  && $id eq $sysid
	  && $timeline == $tli
	  && $epoch == 1
	  && $lsn == $pos
	  && $base == $start
	  && $seg == $segsize;
	return $flushed;
}

my $frontier = exchange('i', $start, pack('N', $segsize));

sub append_to
{
	my ($end) = @_;
	while ($frontier < $end)
	{
		my $size = $end - $frontier;
		$size = 65536 if $size > 65536;
		$frontier =
		  exchange('a', $frontier, substr($wal, $frontier - $start, $size));
	}
	is($frontier, $end,
		'fixture publishes exactly the requested byte frontier');
}

my @fetch = ('test_wal_fetch', $store->connstr('postgres'), $sysid, $tli, 1);

sub export_prefix
{
	my ($directory) = @_;
	$store->command_ok([ @fetch, $directory ], 'export bounded WAL prefix');
	my %manifest = map { split /=/, $_, 2 }
	  split /\n/, slurp_file("$directory/wal-inbox-manifest");
	is($manifest{version}, 2,
		'manifest distinguishes byte and record frontiers');
	is(number($manifest{end}), $frontier, 'byte frontier is preserved');
	return \%manifest;
}

my $empty = $store->backup_dir . '/empty';
$store->command_fails_like(
	[ @fetch, $empty ],
	qr/no complete WAL record/,
	'empty inbox is not a recovery bundle');
ok(!-e "$empty/wal-inbox-manifest", 'empty prefix has no published manifest');

append_to($complete);
my $manifest = export_prefix($store->backup_dir . '/complete');
is(number($manifest->{record_end}),
	$complete, 'exact record boundary does not lose the final record');
cmp_ok(number($manifest->{first_record}),
	'>=', $start,
	'manifest identifies the start of the validated record chain');
cmp_ok(number($manifest->{last_record}),
	'<', $complete,
	'manifest identifies the final record, not its ending LSN');

append_to($complete + 8);
$manifest = export_prefix($store->backup_dir . '/partial_header');
is(number($manifest->{record_end}),
	$complete, 'partial header cannot advance the record frontier');

# A page-aligned frontier is still inside the large logical-message record,
# as can happen when the WAL writer flushes completed WAL pages.
my $partial = $tail_end - 2 * $blocksize;
$partial -= $partial % $blocksize;
cmp_ok(
	$partial, '>',
	$complete + 2 * $blocksize,
	'fixture has a genuinely multi-page incomplete record');
append_to($partial);
my $bundle = $store->archive_dir;
rmdir($bundle) or die "remove empty archive directory: $!";
$manifest = export_prefix($bundle);
cmp_ok(number($manifest->{record_end}),
	'<', $partial, 'whole WAL pages do not imply a complete record');
cmp_ok(number($manifest->{record_end}),
	'>=', $complete, 'complete records before the tail are retained');

# Neither the original source nor its pg_wal is available.  The old backup
# predates the INSERT and incomplete record.  Normal archive recovery, with
# no named recovery target, must independently agree on the branch point.
my $restored = PostgreSQL::Test::Cluster->new('record_recovery');
$restored->init_from_backup($source, 'before_tail');
$restored->enable_restoring($store, 0);
$restored->append_conf('postgresql.conf',
	"recovery_target_timeline = '$tli'\n");
$restored->start;
# pg_ctl can return as soon as hot standby accepts read-only connections.
ok( $restored->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()'),
	'ordinary recovery completes at incomplete WAL EOF');
is( $restored->safe_psql(
		'postgres', 'SELECT id, payload FROM prefix_rows ORDER BY id'),
	"1|seed\n2|committed after seed",
	'commit before the partial record survives');
my $new_tli = $restored->safe_psql('postgres',
	'SELECT timeline_id FROM pg_control_checkpoint()');
cmp_ok($new_tli, '>', $tli, 'recovery uses a child timeline');
my $history = slurp_file(
	$restored->data_dir . '/pg_wal/' . sprintf('%08X.history', $new_tli));
my ($branch) = $history =~ /^$tli\s+(\S+)/m;
is(number($branch), number($manifest->{record_end}),
	'core recovery chooses the manifest record boundary, not the byte frontier'
);
$restored->stop;

# Header-only and short continuation fragments must be treated as an
# incomplete record, not as corrupt WAL or as readable zero padding.
my $partial_record_end = number($manifest->{record_end});
for my $extra (1, 8, 16, 24, 32)
{
	append_to($partial + $extra);
	$manifest = export_prefix($store->backup_dir . "/continuation_$extra");
	is(number($manifest->{record_end}),
		$partial_record_end,
		"$extra bytes of a continuation page do not complete the record");
}

append_to($tail_end);
$manifest = export_prefix($store->backup_dir . '/complete_tail');
is(number($manifest->{record_end}),
	$tail_end, 'complete multi-page record advances the record frontier');
append_to($switch_end);
$manifest = export_prefix($store->backup_dir . '/switch');
is(number($manifest->{record_end}),
	$next_segment,
	'switch record includes implicit segment padding in its logical end');
for my $extra (1, 24, 40, 48, 64)
{
	append_to($next_segment + $extra);
	$manifest = export_prefix($store->backup_dir . "/new_page_$extra");
	is(number($manifest->{record_end}),
		$next_segment,
		"$extra bytes of a new segment do not complete its first record");
}
append_to($post_switch);
$manifest = export_prefix($store->backup_dir . '/post_switch');
is(number($manifest->{record_end}),
	$post_switch, 'first complete record of the new segment is retained');
close $socket;

# The inbox is deliberately opaque.  Damage a complete WAL record without
# touching its valid control file, so it is the exporter that must detect the
# CRC error, not the inbox's length or control checksum checks.
$store->stop;
my $bad = index($wal, 'large record payload');
die 'test payload not found in WAL' if $bad < 0;
my $bytes_path = $store->data_dir . '/test_page_store.wal/bytes';
open(my $file, '+<', $bytes_path) or die "open stored WAL: $!";
binmode $file;
seek($file, $bad, 0) or die "seek stored WAL: $!";
print $file substr($wal, $bad, 1) ^ "\x01";
close $file or die "close stored WAL: $!";
$store->start;
my $damaged = $store->backup_dir . '/damaged';
$store->command_fails_like(
	[ @fetch, $damaged ],
	qr/incorrect resource manager data checksum/,
	'corrupt complete record is not treated as an incomplete tail');
ok(!-e "$damaged/wal-inbox-manifest",
	'damaged WAL is never published as a bundle');
$store->stop;

done_testing();
