#!/bin/bash
# Перепроверка времён построения повторами с чередованием.
#
# Поводом послужило то, что на другой паре сборок базовая гуляла между
# прогонами на 27 %. Все времена построения в P1 сняты однократно, и часть
# утверждений держится на разнице в проценты — её такой шум стирает.
#
# Счётчики обращений к страницам перепроверять не нужно: это счёт, а не время,
# и при фиксированном зерне он воспроизводится точно.
set -e
export LANG=C LC_ALL=C

N=${N:-10000000}
REP=${REP:-3}

q() { local n="$1" p="$2"; shift 2; "$HOME/bench-$n/bin/psql" -h /tmp -p "$p" -d postgres -X -q -t -A "$@"; }
now() { q "$1" "$2" -c "select extract(epoch from clock_timestamp())"; }
el() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }

build_once() {  # build_once <name> <port> [fillfactor]
  local n="$1" p="$2" ff="$3" with=""
  [ -n "$ff" ] && with=" WITH (fillfactor = $ff)"
  q "$n" "$p" -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
  q "$n" "$p" -c "CHECKPOINT" >/dev/null
  local t0 t1
  t0=$(now "$n" "$p"); q "$n" "$p" -c "CREATE INDEX ti ON t USING gist(p)$with" >/dev/null; t1=$(now "$n" "$p")
  el "$t0" "$t1"
}

echo "=== три способа построения, $N точек, чередование"
printf 'прогон|сортировка по заполнению|сортировка picksplit\n'
for i in $(seq 1 $REP); do
  A=$(build_once naive 5442)
  B=$(build_once picksplit 5443)
  printf '%s|%s|%s\n' "$i" "$A" "$B"
done

echo
echo "=== построение вставкой (опорная функция снята на кластере naive)"
q naive 5442 -c "DELETE FROM pg_amproc WHERE amprocnum=11 AND amproc::regproc::text ~ 'gist_point'" >/dev/null
"$HOME/bench-naive/bin/pg_ctl" -D /mnt/nvme/data/naive -w restart -l /mnt/nvme/data/naive/pg.log >/dev/null 2>&1
sleep 2
printf 'прогон|вставка\n'
for i in $(seq 1 $REP); do printf '%s|%s\n' "$i" "$(build_once naive 5442)"; done
q naive 5442 -c "insert into pg_amproc (oid, amprocfamily, amproclefttype, amprocrighttype, amprocnum, amproc)
   select 90004, f.oid, 'point'::regtype, 'point'::regtype, 11, 'gist_point_sortsupport'::regproc
   from pg_opfamily f join pg_am a on a.oid=f.opfmethod
   where f.opfname='point_ops' and a.amname='gist'
     and not exists (select 1 from pg_amproc ap where ap.amprocfamily=f.oid and ap.amprocnum=11)" >/dev/null
"$HOME/bench-naive/bin/pg_ctl" -D /mnt/nvme/data/naive -w restart -l /mnt/nvme/data/naive/pg.log >/dev/null 2>&1
sleep 2

echo
echo "=== размер буфера k: время построения, чередование"
printf 'прогон|k=1|k=2|k=4|k=8|k=16\n'
DATA=/mnt/nvme/data/k
for i in $(seq 1 $REP); do
  ROW=""
  for K in 1 2 4 8 16; do
    "$HOME/bench-k$K/bin/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1 || true
    "$HOME/bench-k$K/bin/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w start >/dev/null
    "$HOME/bench-k$K/bin/psql" -h /tmp -p 5444 -d postgres -X -q -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
    "$HOME/bench-k$K/bin/psql" -h /tmp -p 5444 -d postgres -X -q -c "CHECKPOINT" >/dev/null
    T0=$("$HOME/bench-k$K/bin/psql" -h /tmp -p 5444 -d postgres -X -q -t -A -c "select extract(epoch from clock_timestamp())")
    "$HOME/bench-k$K/bin/psql" -h /tmp -p 5444 -d postgres -X -q -c "CREATE INDEX ti ON t USING gist(p)" >/dev/null
    T1=$("$HOME/bench-k$K/bin/psql" -h /tmp -p 5444 -d postgres -X -q -t -A -c "select extract(epoch from clock_timestamp())")
    ROW="$ROW|$(el "$T0" "$T1")"
    "$HOME/bench-k$K/bin/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1 || true
  done
  printf '%s%s\n' "$i" "$ROW"
done
