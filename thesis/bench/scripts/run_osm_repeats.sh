#!/bin/bash
# Перепроверка OSM повторами: времена построения и подтверждение находки о том,
# что построение вставкой даёт худшее дерево на реальных данных.
#
# В P1 числа по OSM сняты однократно. Времена этому подвержены (на другой паре
# сборок разброс достигал 27 %), счётчики обращений — нет.
#
# Три способа построения меряются на одном кластере: сортированные два — на
# разных сборках, вставка — снятием опорной функции сортировки.
set -e
export LANG=C LC_ALL=C

REP=${REP:-3}
NPROBE=${NPROBE:-5000}
D_SEL=0.00005

q() { local n="$1" p="$2"; shift 2; "$HOME/bench-$n/bin/psql" -h /tmp -p "$p" -d postgres -X -q -t -A "$@"; }
now() { q "$1" "$2" -c "select extract(epoch from clock_timestamp())"; }
el() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.1f", b-a}'; }

load_osm() {  # load_osm <name> <port>
  local n="$1" p="$2"
  if [ "$(q "$n" "$p" -c "select coalesce((select count(*) from pg_class where relname='t'),0)")" != "0" ]; then
    local cnt; cnt=$(q "$n" "$p" -c "select count(*) from t")
    [ "$cnt" -gt 100000000 ] && { echo "$n: данные уже загружены ($cnt)" >&2; return; }
  fi
  q "$n" "$p" -c "DROP TABLE IF EXISTS t, osm" >/dev/null 2>&1 || true
  q "$n" "$p" -c "CREATE TABLE osm(lon float8, lat float8)" >/dev/null
  q "$n" "$p" -c "COPY osm FROM '/mnt/nvme/osm/nodes.csv' WITH (FORMAT csv)" >/dev/null
  q "$n" "$p" -c "CREATE TABLE t AS SELECT point(lon,lat) AS p FROM osm" >/dev/null
  q "$n" "$p" -c "DROP TABLE osm" >/dev/null
  q "$n" "$p" -c "VACUUM ANALYZE t" >/dev/null
  echo "$n: загружено" >&2
}

drop_sortsupport() { q "$1" "$2" -c "DELETE FROM pg_amproc WHERE amprocnum=11 AND amproc::regproc::text ~ 'gist_point'" >/dev/null
  "$HOME/bench-$1/bin/pg_ctl" -D "/mnt/nvme/data/$1" -w restart -l "/mnt/nvme/data/$1/pg.log" >/dev/null 2>&1; sleep 2; }
add_sortsupport() { q "$1" "$2" -c "insert into pg_amproc (oid, amprocfamily, amproclefttype, amprocrighttype, amprocnum, amproc)
     select 90005, f.oid, 'point'::regtype, 'point'::regtype, 11, 'gist_point_sortsupport'::regproc
     from pg_opfamily f join pg_am a on a.oid=f.opfmethod
     where f.opfname='point_ops' and a.amname='gist'
       and not exists (select 1 from pg_amproc ap where ap.amprocfamily=f.oid and ap.amprocnum=11)" >/dev/null
  "$HOME/bench-$1/bin/pg_ctl" -D "/mnt/nvme/data/$1" -w restart -l "/mnt/nvme/data/$1/pg.log" >/dev/null 2>&1; sleep 2; }

build() {  # build <name> <port> -> время
  q "$1" "$2" -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
  q "$1" "$2" -c "CHECKPOINT" >/dev/null
  local t0 t1; t0=$(now "$1" "$2"); q "$1" "$2" -c "CREATE INDEX ti ON t USING gist(p)" >/dev/null; t1=$(now "$1" "$2")
  el "$t0" "$t1"
}

load_osm naive 5442
load_osm picksplit 5443
q picksplit 5443 -f "$HOME/probe_osm.sql" >/dev/null 2>&1

printf 'прогон|способ|время построения, с|страниц\n'
for i in $(seq 1 $REP); do
  T=$(build naive 5442);      printf '%s|по заполнению|%s|%s\n' "$i" "$T" "$(q naive 5442 -c "select pg_relation_size('ti')/8192")"
  T=$(build picksplit 5443);  printf '%s|picksplit|%s|%s\n' "$i" "$T" "$(q picksplit 5443 -c "select pg_relation_size('ti')/8192")"
done

drop_sortsupport naive 5442
for i in $(seq 1 $REP); do
  T=$(build naive 5442);      printf '%s|вставка|%s|%s\n' "$i" "$T" "$(q naive 5442 -c "select pg_relation_size('ti')/8192")"
done

# качество дерева при построении вставкой: подтверждение находки
q naive 5442 -f "$HOME/probe_osm.sql" >/dev/null 2>&1
printf '\nспособ|обращений на селективный запрос\n'
printf 'вставка|%s\n' "$(q naive 5442 -c "select probe_at_data('t','ti',$NPROBE,$D_SEL)")"
add_sortsupport naive 5442
T=$(build naive 5442)
printf 'по заполнению|%s\n' "$(q naive 5442 -c "select probe_at_data('t','ti',$NPROBE,$D_SEL)")"
printf 'picksplit|%s\n' "$(q picksplit 5443 -c "select probe_at_data('t','ti',$NPROBE,$D_SEL)")"
