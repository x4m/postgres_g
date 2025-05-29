
# Copyright (c) 2021-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node;
my $blksize;

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('test');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'autovacuum=off');
$node->start;
$blksize = int($node->safe_psql('postgres', 'SHOW block_size;'));
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql(
	'postgres', q(
        CREATE OR REPLACE FUNCTION  random_string( INT ) RETURNS text AS $$
        SELECT string_agg(substring('0123456789bcdfghjkmnpqrstvwxyz', ceil(random() * 30)::integer, 1), '') from generate_series(1, $1);
        $$ LANGUAGE SQL;));

# Tests
invalid_entry_order_leaf_page_test();
invalid_entry_order_inner_page_test();
invalid_entry_columns_order_test();
inconsistent_with_parent_key__parent_key_corrupted_test();
inconsistent_with_parent_key__child_key_corrupted_test();
inconsistent_with_parent_key__parent_key_corrupted_posting_tree_test();

sub invalid_entry_order_leaf_page_test
{
	my $relname = "test";
	my $indexname = "test_gin_idx";

	$node->safe_psql(
		'postgres', qq(
		DROP TABLE IF EXISTS $relname;
	 	CREATE TABLE $relname (a text[]);
	 	CREATE INDEX $indexname ON $relname USING gin (a);
		INSERT INTO $relname (a) VALUES ('{aaaaa,bbbbb}');
		SELECT gin_clean_pending_list('$indexname');
	 ));
	my $relpath = relation_filepath($indexname);

	$node->stop;

	my $blkno = 1;  # root

	# produce wrong order by replacing aaaaa with ccccc
	string_replace_block(
		$relpath,
		"aaaaa",
		'"ccccc"',
		$blksize,
		$blkno
	);

	$node->start;

	my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT gin_index_check('$indexname')));
	ok($stderr =~ "index \"$indexname\" has wrong tuple order on entry tree page, block 1, offset 2, rightlink 4294967295");
}

sub invalid_entry_order_inner_page_test
{
	my $relname = "test";
	my $indexname = "test_gin_idx";

	$node->safe_psql(
		'postgres', qq(
		DROP TABLE IF EXISTS $relname;
	 	CREATE TABLE $relname (a text[]);
	 	CREATE INDEX $indexname ON $relname USING gin (a);
		INSERT INTO $relname (a) VALUES (('{' || 'pppppppppp' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'qqqqqqqqqq' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'rrrrrrrrrr' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'ssssssssss' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'tttttttttt' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'uuuuuuuuuu' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'vvvvvvvvvv' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'wwwwwwwwww' || random_string(1870) ||'}')::text[]);
		SELECT gin_clean_pending_list('$indexname');
	 ));
	my $relpath = relation_filepath($indexname);

	$node->stop;

	my $blkno = 1;  # root

	# we have rrrrrrrrr... and tttttttttt... as keys in the root, so produce wrong order by replacing rrrrrrrrrr....
	string_replace_block(
		$relpath,
		"rrrrrrrrrr",
		'"zzzzzzzzzz"',
		$blksize,
		$blkno
	);

	$node->start;

	my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT gin_index_check('$indexname')));
	ok($stderr =~ "index \"$indexname\" has wrong tuple order on entry tree page, block 1, offset 2, rightlink 4294967295");
}

sub invalid_entry_columns_order_test
{
	my $relname = "test";
	my $indexname = "test_gin_idx";

	$node->safe_psql(
		'postgres', qq(
		DROP TABLE IF EXISTS $relname;
	 	CREATE TABLE $relname (a text[],b text[]);
	 	CREATE INDEX $indexname ON $relname USING gin (a,b);
		INSERT INTO $relname (a,b) VALUES ('{aaa}','{bbb}');
		SELECT gin_clean_pending_list('$indexname');
	 ));
	my $relpath = relation_filepath($indexname);

	$node->stop;

	my $blkno = 1;  # root

	# mess column numbers
	# root items order before: (1,aaa), (2,bbb)
	# root items order after:  (2,aaa), (1,bbb)
	my $attrno_1 = pack('s', 1);
	my $attrno_2 = pack('s', 2);

	my $find = qr/($attrno_1)(.)(aaa)/s;
	my $replace = '"' . $attrno_2 . '$2$3"';
	string_replace_block(
		$relpath,
		$find,
		$replace,
		$blksize,
		$blkno
	);

	$find = qr/($attrno_2)(.)(bbb)/s;
	$replace = '"' . $attrno_1 . '$2$3"';
	string_replace_block(
		$relpath,
		$find,
		$replace,
		$blksize,
		$blkno
	);

	$node->start;

	my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT gin_index_check('$indexname')));
	ok($stderr =~ "index \"$indexname\" has wrong tuple order on entry tree page, block 1, offset 2, rightlink 4294967295");
}

sub inconsistent_with_parent_key__parent_key_corrupted_test
{
	my $relname = "test";
	my $indexname = "test_gin_idx";

	$node->safe_psql(
		'postgres', qq(
		DROP TABLE IF EXISTS $relname;
	 	CREATE TABLE $relname (a text[]);
	 	CREATE INDEX $indexname ON $relname USING gin (a);
		INSERT INTO $relname (a) VALUES (('{' || 'llllllllll' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'mmmmmmmmmm' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'nnnnnnnnnn' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'xxxxxxxxxx' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'yyyyyyyyyy' || random_string(1870) ||'}')::text[]);
		SELECT gin_clean_pending_list('$indexname');
	 ));
	my $relpath = relation_filepath($indexname);

	$node->stop;

	my $blkno = 1;  # root

	# we have nnnnnnnnnn... as parent key in the root, so replace it with something smaller then child's keys
	string_replace_block(
		$relpath,
		"nnnnnnnnnn",
		'"aaaaaaaaaa"',
		$blksize,
		$blkno
	);

	$node->start;

	my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT gin_index_check('$indexname')));
	ok($stderr =~ "index \"$indexname\" has inconsistent records on page 5 offset 3");
}

sub inconsistent_with_parent_key__child_key_corrupted_test
{
	my $relname = "test";
	my $indexname = "test_gin_idx";

	$node->safe_psql(
		'postgres', qq(
		DROP TABLE IF EXISTS $relname;
	 	CREATE TABLE $relname (a text[]);
	 	CREATE INDEX $indexname ON $relname USING gin (a);
		INSERT INTO $relname (a) VALUES (('{' || 'llllllllll' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'mmmmmmmmmm' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'nnnnnnnnnn' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'xxxxxxxxxx' || random_string(1870) ||'}')::text[]);
        INSERT INTO $relname (a) VALUES (('{' || 'yyyyyyyyyy' || random_string(1870) ||'}')::text[]);
		SELECT gin_clean_pending_list('$indexname');
	 ));
	my $relpath = relation_filepath($indexname);

	$node->stop;

	my $blkno = 5;  # leaf

	# we have nnnnnnnnnn... as parent key in the root, so replace child key with something bigger
	string_replace_block(
		$relpath,
		"nnnnnnnnnn",
		'"pppppppppp"',
		$blksize,
		$blkno
	);

	$node->start;

	my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT gin_index_check('$indexname')));
	ok($stderr =~ "index \"$indexname\" has inconsistent records on page 5 offset 3");
}

sub inconsistent_with_parent_key__parent_key_corrupted_posting_tree_test
{
	my $relname = "test";
	my $indexname = "test_gin_idx";

	$node->safe_psql(
		'postgres', qq(
		DROP TABLE IF EXISTS $relname;
		CREATE TABLE $relname (a text[]);
		INSERT INTO $relname (a) select ('{aaaaa}') from generate_series(1,10000);
		CREATE INDEX $indexname ON $relname USING gin (a);
	 ));
	my $relpath = relation_filepath($indexname);

	$node->stop;

	my $blkno = 2;  # posting tree root

	# we have a posting tree for 'aaaaa' key with the root at 2nd block
	# and two leaf pages 3 and 4. for 4th page high key is (65,52), so let's make it a little bit
	# smaller, so that there are tid's in leaf page that are larger then the new high key.
	my $find = pack('S', 65) . pack('S', 52);
	my $replace = '"' . pack('S', 64) . pack('S', 52) . '"';
	string_replace_block(
		$relpath,
		$find,
		$replace,
		$blksize,
		$blkno
	);

	$node->start;

	my ($result, $stdout, $stderr) = $node->psql('postgres', qq(SELECT gin_index_check('$indexname')));
	ok($stderr =~ "index \"$indexname\": tid exceeds parent's high key in postingTree leaf on block 4");
}


# Returns the filesystem path for the named relation.
sub relation_filepath
{
	my ($relname) = @_;

	my $pgdata = $node->data_dir;
	my $rel = $node->safe_psql('postgres',
		qq(SELECT pg_relation_filepath('$relname')));
	die "path not found for relation $relname" unless defined $rel;
	return "$pgdata/$rel";
}

sub string_replace_block {
	my ($filename, $find, $replace, $blksize, $blkno) = @_;

	my $fh;
	open($fh, '+<', $filename) or BAIL_OUT("open failed: $!");
	binmode $fh;

	my $offset = $blkno * $blksize;
	my $buffer;

	sysseek($fh, $offset, 0) or BAIL_OUT("seek failed: $!");
	sysread($fh, $buffer, $blksize) or BAIL_OUT("read failed: $!");

	$buffer =~ s/$find/$replace/gee;

	sysseek($fh, $offset, 0) or BAIL_OUT("seek failed: $!");
	syswrite($fh, $buffer) or BAIL_OUT("write failed: $!");

	close($fh) or BAIL_OUT("close failed: $!");

	return;
}

done_testing();