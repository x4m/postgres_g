
# Copyright (c) 2021, PostgreSQL Global Development Group

# Test for point-in-time-recovery (PITR) with prepared transactions
use strict;
use warnings;

use PostgresNode;
use TestLib;

use Test::More tests => 1;

my ($node, $result);

#
# Test set-up
#
$node = get_new_node('CIC_2PC_test');
$node->init;
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int)));
$node->safe_psql('postgres', q(CREATE INDEX idx on tbl(i)));


#
# Test 1: run 3 overlapping 2PC transactions with concurrent reindex rebuild
#
# We have two concurrent background psqls: main_h for transaction and
# reindex_h for cuncurrent reindex. Also we commit transactions from independent
# psql's sometimes.
#

my $main_in  = '';
my $main_out = '';
my $main_timer = IPC::Run::timeout(5);

my $main_h = $node->background_psql('postgres', \$main_in, \$main_out, $main_timer,
	on_error_stop => 1);
$main_in .= q(
BEGIN;
INSERT INTO tbl VALUES(0);
);
$main_h->pump_nb;

my $reindex_in  = '';
my $reindex_out = '';
my $reindex_timer = IPC::Run::timeout(5);
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
is($result, '', 'bt_index_check checks index');


# done
$node->stop;
done_testing();