# Copyright (c) 2026, PostgreSQL Global Development Group

package TestPageStore;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';

our @EXPORT = qw(read_binary_file);

# WAL and control files must not undergo Windows text-mode translation.
sub read_binary_file
{
	my ($path) = @_;
	open(my $file, '<:raw', $path) or die "open $path: $!";
	local $/;
	my $bytes = <$file>;
	die "read $path: $!" unless defined $bytes;
	close($file) or die "close $path: $!";
	return $bytes;
}

1;
