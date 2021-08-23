
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test for CREATE INDEX CONCURRENTLY with prepared transactions
use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;

use Test::More tests => 5;

my ($node, $result);

#
# Test set-up
#
$node = PostgresNode->new('CIC_2PC_test');
$node->init;
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->append_conf('postgresql.conf', 'lock_timeout = 5000');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int)));
$node->safe_psql('postgres', q(CREATE INDEX idx on tbl(i)));


#
# Test 1. Run 3 overlapping 2PC transactions with concurrent reindex rebuild
#
# We have two concurrent background psqls: main_h for transaction and
# reindex_h for cuncurrent reindex. Also we commit transactions from independent
# psql's sometimes.
#

my $main_in  = '';
my $main_out = '';
my $main_timer = IPC::Run::timeout(180);

my $main_h = $node->background_psql('postgres', \$main_in, \$main_out, $main_timer,
	on_error_stop => 1);
$main_in .= q(
BEGIN;
INSERT INTO tbl VALUES(0);
);
$main_h->pump_nb;

my $reindex_in  = '';
my $reindex_out = '';
my $reindex_timer = IPC::Run::timeout(180);
my $reindex_h = $node->background_psql('postgres', \$reindex_in, \$reindex_out, $reindex_timer,
	on_error_stop => 1);
$reindex_in .= q(
\echo start
REINDEX TABLE CONCURRENTLY tbl;
);
pump $reindex_h until $reindex_out =~ /start/ || $reindex_timer->is_expired;

$main_in .= q(
PREPARE TRANSACTION 'a';
);
$main_h->pump_nb;

$main_in .= q(
BEGIN;
INSERT INTO tbl VALUES(0);
);
$main_h->pump_nb;

$node->safe_psql('postgres', q(COMMIT PREPARED 'a';));

$main_in .= q(
PREPARE TRANSACTION 'b';
BEGIN;
INSERT INTO tbl VALUES(0);
);
$main_h->pump_nb;

$node->safe_psql('postgres', q(COMMIT PREPARED 'b';));

$main_in .= q(
PREPARE TRANSACTION 'c';
COMMIT PREPARED 'c';
);
$main_h->pump_nb;

$main_h->finish;
$reindex_h->finish;

$result = $node->psql('postgres', q(SELECT bt_index_check('idx',true)));
is($result, '0', 'bt_index_check checks index');


#
# Test 2. Stress CIC+2PC with pgbench
#

# Fix broken index first
$node->safe_psql('postgres', q(REINDEX TABLE CONCURRENTLY tbl;));

# Run background pgbench with CIC. We cannot mix-in this script into single pgbench:
# CIC will deadlock with itself occasionally.
my $pgbench_in  = '';
my $pgbench_out = '';
my $pgbench_timer = IPC::Run::timeout(180);
my $pgbench_h = $node->background_pgbench('postgres', \$pgbench_in, \$pgbench_out, $pgbench_timer,
	{
		'002_pgbench_concurrent_cic' =>
		  q(
			REINDEX INDEX CONCURRENTLY idx;
			SELECT bt_index_check('idx',true);
		   )
	},
	'--no-vacuum --client=1 --time=5');

# Run pgbench.
$node->pgbench(
	'--no-vacuum --client=5 --time=5',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent CID and 2PC',
	{
		'002_pgbench_concurrent_2pc' =>
		  q(
			BEGIN;
			INSERT INTO tbl VALUES(0);
			PREPARE TRANSACTION 'c:client_id';
			COMMIT PREPARED 'c:client_id';
		  )
	});

$pgbench_h->pump_nb;
$pgbench_h->finish();
$result = ($Config{osname} eq "MSWin32")
	  ? ($pgbench_h->full_results)[0]
	  : $pgbench_h->result(0);
is($result, 0, "pgbench with CIC works");

# done
$node->safe_psql('postgres', q(DROP TABLE tbl;));
$node->stop;
done_testing();

# AUX stuff, taken from 001_pgbench_with_server.pl