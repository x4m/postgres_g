# Copyright (c) 2025, PostgreSQL Global Development Group

# Check that messages are passed to backends by
# pg_terminate_backend, pg_cancel_backend

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init();
$node->start;

my ($stdout, $stderr);
$node->psql('postgres',
	q[select pg_terminate_backend(pg_backend_pid(), 0, 'Have you seen my coffee cup?');],
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/Have you seen my coffee cup\?/, "expected message to be passed");

$stdout = '';
$stderr = '';
$node->psql('postgres',
	q[select pg_cancel_backend(pg_backend_pid(), 'You have to wear some ridiculous tie');],
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/You have to wear some ridiculous tie/, "expected message to be passed");

$stdout = '';
$stderr = '';
my $longstr = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
my $truncated = substr($longstr, 0, 127);
$node->psql('postgres',
	qq[select pg_terminate_backend(pg_backend_pid(), 0, '$longstr');],
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/NOTICE:  message is too long, truncated to 127/, "NOTICE message should be created");
like($stderr, qr/\Q$truncated\E/, "expected truncated message (127 chars) to be passed");
unlike($stderr, qr/\Q$longstr\E/, "full message must not be passed");

$node->stop;

done_testing();
