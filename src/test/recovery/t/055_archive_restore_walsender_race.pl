use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'injection points required'
  unless $ENV{enable_injection_points} eq 'yes';

my $inj = 'keepfile-restored-before-rename';
my $gate = "$PostgreSQL::Test::Utils::tmp_check/g$$";

my $p = PostgreSQL::Test::Cluster->new('p');
$p->init(
	has_archiving => 1, allows_streaming => 1,
	extra => ['--wal-segsize', '1']);
$p->append_conf('postgresql.conf',
	"shared_preload_libraries = 'injection_points'");
$p->start;
$p->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$p->advance_wal(2);
$p->backup('b');
$p->advance_wal(3);

my $a = $p->archive_dir;
my $w = (sort grep { /^[0-9A-F]{24}$/ } slurp_dir($a))[-1];
my $prev = substr($w, 0, 16) . sprintf('%08X', hex(substr($w, 16, 8)) - 1);
my $s = PostgreSQL::Test::Cluster->new('s');
$s->init_from_backup($p, 'b', has_restoring => 1);
system('cp', "$a/$w", $s->data_dir . "/pg_wal/$w") == 0 or die $!;
$s->append_conf('postgresql.conf', qq{
recovery_prefetch = off
shared_preload_libraries = 'injection_points'
restore_command = 'case "%f" in $w) while [ ! -f $gate ]; do sleep 0.01; done ;; esac; cp "$a/%f" "%p"'
});
$s->start;
$s->wait_for_log(qr/restored log file "\Q$prev\E" from archive/);
$s->safe_psql('postgres', "SELECT injection_points_attach('$inj','wait')");
system('touch', $gate);
$s->wait_for_event('startup', $inj);
ok(-e $s->data_dir . "/pg_wal/$w");
done_testing();
