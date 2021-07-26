
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
#$node->append_conf('postgresql.conf', 'autovacuum=off');
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

$node->safe_psql('postgres', q(CREATE TABLE tbl(i int)));
$node->safe_psql('postgres', q(CREATE INDEX idx on tbl(i)));


my $i_in  = '';
my $i_out = '';
my $i_timer = IPC::Run::timeout(5);

my $i_h = $node->background_psql('postgres', \$i_in, \$i_out, $i_timer,
	on_error_stop => 1);
$i_in .= q(
BEGIN;
INSERT INTO tbl VALUES(0);
);
$i_h->pump_nb;

my $bg_in  = '';
my $bg_out = '';
my $bg_timer = IPC::Run::timeout(5);
my $bg_h = $node->background_psql('postgres', \$bg_in, \$bg_out, $bg_timer,
	on_error_stop => 1);
$bg_in .= q(
\echo start
REINDEX TABLE CONCURRENTLY tbl;
select 'Reindex done';
);
pump $bg_h until $bg_out =~ /start/ || $bg_timer->is_expired;

$i_in .= q(
PREPARE TRANSACTION 'a';
);
$i_h->pump_nb;

$i_in .= q(
BEGIN;
INSERT INTO tbl VALUES(0);
);
$i_h->pump_nb;

$node->safe_psql('postgres', q(COMMIT PREPARED 'a';));

$i_in .= q(
PREPARE TRANSACTION 'b';
BEGIN;
INSERT INTO tbl VALUES(0);
);
$i_h->pump_nb;

$node->safe_psql('postgres', q(COMMIT PREPARED 'b';));

$i_in .= q(
PREPARE TRANSACTION 'c';
BEGIN;
INSERT INTO tbl VALUES(0);
);
$i_h->pump_nb;

$node->safe_psql('postgres', q(COMMIT PREPARED 'c';));

$i_in .= q(
PREPARE TRANSACTION 'd';
COMMIT PREPARED 'd';
);
$i_h->pump_nb;

$i_h->finish;
$bg_h->finish;

# $node->safe_psql('postgres', q(
# BEGIN;
# INSERT INTO tbl VALUES(1);
# PREPARE TRANSACTION 'b';
# ));

# $node->safe_psql('postgres', q(COMMIT PREPARED 'a'));

# $node->safe_psql('postgres', q(
# BEGIN;
# INSERT INTO tbl VALUES(2);
# PREPARE TRANSACTION 'c';
# ));

# $node->safe_psql('postgres', q(COMMIT PREPARED 'b'));
# $node->safe_psql('postgres', q(
# BEGIN;
# INSERT INTO tbl VALUES(3);
# PREPARE TRANSACTION 'd';
# ));
# $node->safe_psql('postgres', q(COMMIT PREPARED 'c'));
# $node->safe_psql('postgres', q(COMMIT PREPARED 'd'));

$result = $node->safe_psql('postgres', q(SELECT bt_index_check('idx',true)));
is($result, '', 'bt_index_check checks index');

$node->safe_psql('postgres', q(DROP TABLE tbl));