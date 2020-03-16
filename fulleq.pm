package Mkvcbuild;

#
# Package that generates build files for msvc build
#
# src/tools/msvc/Mkvcbuild.pm
#
use Carp;
use strict;
use warnings;




sub GenerateFulleqSql
{
	my @argtypes = ('bool', 'bytea', 'char', 'name', 'int8', 'int2', 'int4',
					'text', 'oid', 'xid', 'cid', 'oidvector', 'float4',
					'float8', 'abstime', 'reltime', 'macaddr', 'inet', 'cidr',
					'varchar', 'date', 'time', 'timestamp', 'timestamptz',
					'interval', 'timetz');

	#form extension script
	my $i;
	open($i, '<', "contrib/fulleq/fulleq.sql.in") ||
		croak "Could not read contrib/fulleq/fulleq.sql.in";
	my $template = do { local $/; <$i> };
	close($i);

	my $o;
	open($o, '>', "contrib/fulleq/fulleq--2.0.sql") ||
		croak "Could not write to contrib/fulleq/fulleq--2.0.sql";
	print $o "\\echo Use \"CREATE EXTENSION fulleq\" to load this file. \\quit\n";
	foreach my $argtype (@argtypes)
	{
		my $newtype = $template;
		$newtype =~ s/ARGTYPE/$argtype/g;
		print $o $newtype;
	}
	close($o);

	#form migration script
	$template = undef;
	open($i, '<', "contrib/fulleq/fulleq-unpackaged.sql.in") ||
		croak "Could not read contrib/fulleq/fulleq-unpackaged.sql.in";
	$template = do { local $/; <$i> };
	close($i);

	open($o, '>', "contrib/fulleq/fulleq--unpackaged--2.0.sql") ||
		croak "Could not write to contrib/fulleq/fulleq--2.0.sql";

	print $o "\\echo Use \"CREATE EXTENSION fulleq FROM unpackaged\" to load this file. \\quit\n";
	print $o "DROP OPERATOR CLASS IF EXISTS int2vector_fill_ops USING hash;\n";
	print $o "DROP OPERATOR FAMILY IF EXISTS int2vector_fill_ops USING hash;\n";
	print $o "DROP FUNCTION IF EXISTS fullhash_int2vector(int2vector);\n";
	print $o "DROP OPERATOR IF EXISTS == (int2vector, int2vector);\n";
	print $o "DROP FUNCTION IF EXISTS isfulleq_int2vector(int2vector, int2vector);\n";

	foreach my $argtype (@argtypes)
	{
		my $newtype = $template;
		$newtype =~ s/ARGTYPE/$argtype/g;
		print $o $newtype;
	}
	close($o);
}


GenerateFulleqSql();
