
# Copyright (c) 2022, PostgreSQL Global Development Group

# Test REINDEX CONCURRENTLY with concurrent modifications and HOT updates
use strict;
use warnings;

use Config;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More tests => 3;

my ($node, $result);

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('RC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf('postgresql.conf', 'fsync = off');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int primary key,
								c1 money default 0,c2 money default 0,
								c3 money default 0, updated_at timestamp)));
$node->safe_psql('postgres', q(CREATE INDEX idx ON tbl(i)));

#
# Stress RC with pgbench.
#
# pgbench might try to launch more than one instance of the RC
# transaction concurrently.  That would deadlock, so use an advisory
# lock to ensure only one RC runs at a time.
#
$node->pgbench(
	'--no-vacuum --client=5 --transactions=1500',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent INSERTs, UPDATES and RC',
	{
		'002_pgbench_concurrent_transaction_inserts' => q(
			BEGIN;
			INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			SELECT pg_sleep(case when random()<0.05 then 0.01 else 0 end);
			INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			COMMIT;
		  ),
		# Ensure some HOT updates happen
		'002_pgbench_concurrent_transaction_updates' => q(
			BEGIN;
			INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			SELECT pg_sleep(case when random()<0.05 then 0.01 else 0 end);
			INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
				on conflict(i) do update set updated_at = now();
			COMMIT;
		  ),
		'002_pgbench_concurrent_cic' => q(
			SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
			\if :gotlock
				REINDEX INDEX CONCURRENTLY idx;
				SELECT bt_index_check('idx',true);
				SELECT pg_advisory_unlock(42);
			\endif
		  )
	});

$node->stop;
done_testing();
