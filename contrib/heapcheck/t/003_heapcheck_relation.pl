use strict;
use warnings;

use PostgresNode;
use TestLib;

use Test::More tests => 1;

# This regression test demonstrates that the heapcheck_relation() function
# supplied with this contrib module correctly identifies specific kinds of
# corruption within pages.  To test this, we need a mechanism to create corrupt
# pages with predictable, repeatable corruption.  The postgres backend cannot be
# expected to help us with this, as its design is not consistent with the goal
# of intentionally corrupting pages.
#
# Instead, we create a table to corrupt, and with careful consideration of how
# postgresql lays out heap pages, we seek to offsets within the page and
# overwrite deliberately chosen bytes with specific values calculated to
# corrupt the page in expected ways.  We then verify that heapcheck_relation
# reports the corruption, and that it runs without crashing.  Note that the
# backend cannot simply be started to run queries against the corrupt table, as
# the backend will crash, at least for some of the corruption types we
# generate.
#
# Autovacuum potentially touching the table in the background makes the exact
# behavior of this test harder to reason about.  We turn it off to keep things
# simpler.  We use a "belt and suspenders" approach, turning it off for the
# system generally in postgresql.conf, and turning it off specifically for the
# test table.
#
# This test depends on the table being written to the heap file exactly as we
# expect it to be, so we take care to arrange the columns of the table, and
# insert rows of the table, that give predictable sizes and locations within
# the table page.
#
# The HeapTupleHeaderData has 23 bytes of fixed size fields before the variable
# length t_bits[] array.  We have exactly 3 columns in the table, so natts = 3,
# t_bits is 1 byte long, and t_hoff = MAXALIGN(23 + 1) = 24.
#
# We're not too fussy about which datatypes we use for the test, but we do care
# about some specific properties.  We'd like to test both fixed size and
# varlena types.  We'd like some varlena data inline and some toasted.  And
# we'd like the layout of the table such that the datums land at predictable
# offsets within the tuple.  We choose a structure without padding on all
# supported architectures:
#
# 	a BIGINT
#	b TEXT
#	c TEXT
#
# We always insert a 7-ascii character string into field 'b', which with a
# 1-byte varlena header gives an 8 byte inline value.  We always insert a long
# text string in field 'c', long enough to force toast storage.
#
# This formatting produces heap pages where each tuple is 58 bytes long, padded
# out to 64 bytes for alignment, with the first one on the page starting at
# offset 8128, as follows:
#
#    [ lp_off: 8128 lp_len:   58]
#    [ lp_off: 8064 lp_len:   58]
#    [ lp_off: 8000 lp_len:   58]
#    [ lp_off: 7936 lp_len:   58]
#    [ lp_off: 7872 lp_len:   58]
#    [ lp_off: 7808 lp_len:   58]
#               ...
#

use constant LP_OFF_BEGIN => 8128;
use constant LP_OFF_DELTA => 64;

# We choose to read and write binary copies of our table's tuples, using perl's
# pack() and unpack() functions.  Perl uses a packing code system in which:
#
#	L = "Unsigned 32-bit Long",
#	S = "Unsigned 16-bit Short",
#	C = "Unsigned 8-bit Octet",
#	c = "signed 8-bit octet",
#	q = "signed 64-bit quadword"
#	
# Each tuple in our table has a layout as follows:
#
#    xx xx xx xx            t_xmin: xxxx		offset = 0		L
#    xx xx xx xx            t_xmax: xxxx		offset = 4		L
#    xx xx xx xx          t_field3: xxxx		offset = 8		L
#    xx xx                   bi_hi: xx			offset = 12		S
#    xx xx                   bi_lo: xx			offset = 14		S
#    xx xx                ip_posid: xx			offset = 16		S
#    xx xx             t_infomask2: xx			offset = 18		S
#    xx xx              t_infomask: xx			offset = 20		S
#    xx                     t_hoff: x			offset = 22		C
#    xx                     t_bits: x			offset = 23		C
#    xx xx xx xx xx xx xx xx   'a': xxxxxxxx	offset = 24		q
#    xx xx xx xx xx xx xx xx   'b': xxxxxxxx	offset = 32		Cccccccc
#    xx xx xx xx xx xx xx xx   'c': xxxxxxxx	offset = 40		SSSS
#    xx xx xx xx xx xx xx xx      : xxxxxxxx	 ...continued	SSSS
#    xx xx                        : xx      	 ...continued	S
#	
# We could choose to read and write columns 'b' and 'c' in other ways, but
# it is convenient enough to do it this way.  We define packing code
# constants here, where they can be compared easily against the layout.

use constant HEAPTUPLE_PACK_CODE => 'LLLSSSSSCCqCcccccccSSSSSSSSS';
use constant HEAPTUPLE_PACK_LENGTH => 58;     # Total size

# Read a tuple of our table from a heap page.
#
# Takes an open filehandle to the heap file, and the offset of the tuple.
#
# Rather than returning the binary data from the file, unpacks the data into a
# perl hash with named fields.  These fields exactly match the ones understood
# by write_tuple(), below.  Returns a reference to this hash.
#
sub read_tuple ($$)
{
	my ($fh, $offset) = @_;
	my ($buffer, %tup);
	seek($fh, $offset, 0);
	sysread($fh, $buffer, HEAPTUPLE_PACK_LENGTH);
	
	@_ = unpack(HEAPTUPLE_PACK_CODE, $buffer);
	%tup = (t_xmin => shift,
			t_xmax => shift,
			t_field3 => shift,
			bi_hi => shift,
			bi_lo => shift,
			ip_posid => shift,
			t_infomask2 => shift,
			t_infomask => shift,
			t_hoff => shift,
			t_bits => shift,
			a => shift,
			b_header => shift,
			b_body1 => shift,
			b_body2 => shift,
			b_body3 => shift,
			b_body4 => shift,
			b_body5 => shift,
			b_body6 => shift,
			b_body7 => shift,
			c1 => shift,
			c2 => shift,
			c3 => shift,
			c4 => shift,
			c5 => shift,
			c6 => shift,
			c7 => shift,
			c8 => shift,
			c9 => shift);
	# Stitch together the text for column 'b'
	$tup{b} = join('', map { chr($tup{"b_body$_"}) } (1..7));
	return \%tup;
}

# Write a tuple of our table to a heap page.
#
# Takes an open filehandle to the heap file, the offset of the tuple, and a
# reference to a hash with the tuple values, as returned by read_tuple().
# Writes the tuple fields from the hash into the heap file.
#
# The purpose of this function is to write a tuple back to disk with some
# subset of fields modified.  The function does no error checking.  Use
# cautiously.
#
sub write_tuple($$$)
{
	my ($fh, $offset, $tup) = @_;
	my $buffer = pack(HEAPTUPLE_PACK_CODE,
					$tup->{t_xmin},
					$tup->{t_xmax},
					$tup->{t_field3},
					$tup->{bi_hi},
					$tup->{bi_lo},
					$tup->{ip_posid},
					$tup->{t_infomask2},
					$tup->{t_infomask},
					$tup->{t_hoff},
					$tup->{t_bits},
					$tup->{a},
					$tup->{b_header},
					$tup->{b_body1},
					$tup->{b_body2},
					$tup->{b_body3},
					$tup->{b_body4},
					$tup->{b_body5},
					$tup->{b_body6},
					$tup->{b_body7},
					$tup->{c1},
					$tup->{c2},
					$tup->{c3},
					$tup->{c4},
					$tup->{c5},
					$tup->{c6},
					$tup->{c7},
					$tup->{c8},
					$tup->{c9});
	seek($fh, $offset, 0);
	syswrite($fh, $buffer, HEAPTUPLE_PACK_LENGTH);
	return;
}

# Set umask so test directories and files are created with default permissions
umask(0077);

my ($result, $node);

# Set up the node and test table.
$node = get_new_node('test');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum=off');
$node->start;
my $pgdata = $node->data_dir;
$node->safe_psql('postgres', "CREATE EXTENSION heapcheck");

$node->safe_psql(
	'postgres', qq(
		CREATE TABLE public.test (a BIGINT, b TEXT, c TEXT);
		ALTER TABLE public.test SET (autovacuum_enabled=false);
		ALTER TABLE public.test ALTER COLUMN c SET STORAGE EXTERNAL;
	));

my $rel = $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('public.test')));
my $relpath = "$pgdata/$rel";

use constant ROWCOUNT => 12;
$node->safe_psql('postgres', qq(
	INSERT INTO public.test (a, b, c)
		VALUES (
			12345678,
			repeat('f', 7),
			repeat('w', 10000)
		);
	VACUUM FREEZE public.test
	)) for (1..ROWCOUNT);

my $relfrozenxid = $node->safe_psql('postgres',
	q(select relfrozenxid from pg_class where relname = 'test'));

$node->stop;

# Some #define constants from access/htup_details.h for use while corrupting.
use constant HEAP_HASNULL            => 0x0001;
use constant HEAP_XMIN_COMMITTED     => 0x0100;
use constant HEAP_XMIN_INVALID       => 0x0200;
use constant HEAP_XMAX_INVALID       => 0x0800;
use constant HEAP_NATTS_MASK         => 0x07FF;

# Corrupt the tuples, one type of corruption per tuple.  Some types of
# corruption cause heapcheck_relation to skip to the next tuple without
# performing any remaining checks, so we can't exercise the system properly if
# we focus all our corruption on a single tuple.
#
my $file;
open($file, '+<', $relpath);
binmode $file;
for (my $offset = LP_OFF_BEGIN, my $tupidx = 0; $tupidx < ROWCOUNT; $tupidx++, $offset -= LP_OFF_DELTA)
{
	my $tup = read_tuple($file, $offset);

	if ($tupidx == 0)
	{
		# Corruptly set xmin < relfrozenxid
		$tup->{t_xmin} = 3;
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;
	}
	elsif ($tupidx == 1)
	{
		# Corruptly set xmin < relfrozenxid, further back
		$tup->{t_xmin} = 4026531839;		# Note circularity of xid comparison
		$tup->{t_infomask} &= ~HEAP_XMIN_COMMITTED;
		$tup->{t_infomask} &= ~HEAP_XMIN_INVALID;
	}
	elsif ($tupidx == 2)
	{
		# Corruptly set xmax < relminmxid;
		$tup->{t_xmax} = 4026531839;		# Note circularity of xid comparison
		$tup->{t_infomask} &= ~HEAP_XMAX_INVALID;
	}
	elsif ($tupidx == 3)
	{
		# Corrupt the tuple t_hoff, but keep it aligned properly
		$tup->{t_hoff} += 128;
	}
	elsif ($tupidx == 4)
	{
		# Corrupt the tuple t_hoff, wrong alignment
		$tup->{t_hoff} += 3;
	}
	elsif ($tupidx == 5)
	{
		# Corrupt the tuple t_hoff, underflow but correct alignment
		$tup->{t_hoff} -= 8;
	}
	elsif ($tupidx == 6)
	{
		# Corrupt the tuple t_hoff, underflow and wrong alignment
		$tup->{t_hoff} -= 3;
	}
	elsif ($tupidx == 7)
	{
		# Corrupt the tuple to look like it has lots of attributes, not just 3
		$tup->{t_infomask2} |= HEAP_NATTS_MASK;
	}
	elsif ($tupidx == 8)
	{
		# Corrupt the tuple to look like it has lots of attributes, some of
		# them null.  This falsely creates the impression that the t_bits
		# array is longer than just one byte, but t_hoff still says otherwise.
		$tup->{t_infomask} |= HEAP_HASNULL;
		$tup->{t_infomask2} |= HEAP_NATTS_MASK;
		$tup->{t_bits} = 0xAA;
	}
	elsif ($tupidx == 9)
	{
		# Same as above, but this time t_hoff plays along
		$tup->{t_infomask} |= HEAP_HASNULL;
		$tup->{t_infomask2} |= (HEAP_NATTS_MASK & 0x40);
		$tup->{t_bits} = 0xAA;
		$tup->{t_hoff} = 32;
	}
	elsif ($tupidx == 10)
	{
		# Corrupt the bits in column 'b' 1-byte varlena header
		$tup->{b_header} = 0x80;
	}
	elsif ($tupidx == 11)
	{
		# Corrupt the bits in column 'c' toast pointer
		$tup->{c6} = 41;
		$tup->{c7} = 41;
	}
	write_tuple($file, $offset, $tup);
}
close($file);

# Run heapcheck_relation on the corrupted file
$node->start;

$result = $node->safe_psql('postgres', q(SELECT * FROM heapcheck_relation('test')));
is ($result,
"0|1|8128|1|58|||tuple xmin = 3 precedes relation relfrozenxid = $relfrozenxid
0|1|8128|1|58|||tuple xmin = 3 (interpreted as 3) not or no longer valid
0|2|8064|1|58|||tuple xmin = 4026531839 precedes relation relfrozenxid = $relfrozenxid
0|2|8064|1|58|||tuple xmin = 4026531839 (interpreted as 18446744073441116159) not or no longer valid
0|3|8000|1|58|||tuple xmax = 4026531839 precedes relation relfrozenxid = $relfrozenxid
0|4|7936|1|58|||t_hoff > lp_len (152 > 58)
0|5|7872|1|58|||t_hoff not max-aligned (27)
0|6|7808|1|58|||t_hoff < SizeofHeapTupleHeader (16 < 23)
0|7|7744|1|58|||t_hoff < SizeofHeapTupleHeader (21 < 23)
0|7|7744|1|58|||t_hoff not max-aligned (21)
0|8|7680|1|58|||relation natts < tuple natts (3 < 2047)
0|9|7616|1|58|||SizeofHeapTupleHeader + BITMAPLEN(natts) > t_hoff (23 + 256 > 24)
0|10|7552|1|58|||relation natts < tuple natts (3 < 67)
0|11|7488|1|58|2||t_hoff + offset > lp_len (24 + 429496744 > 58)
0|12|7424|1|58|2|0|final chunk number differs from expected (0 vs. 6)
0|12|7424|1|58|2|0|toasted value missing from toast table",
"Expected heapcheck_relation output");

$node->teardown_node;
$node->clean_node;

