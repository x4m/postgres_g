#!/bin/bash
# Сборка мусора: логический обход против физического.
#
# fe280694d0d заменил обход дерева поиском в глубину на обход в порядке
# возрастания номеров страниц. Разница видна только когда индекс не помещается
# в буферный кэш: иначе оба варианта работают по памяти и порядок не важен.
# Поэтому буферный кэш намеренно мал.
#
# Первичные величины: время сборки мусора и число прочитанных страниц. Второе
# должно совпасть у обоих вариантов — обходятся одни и те же страницы,
# различается порядок. Если не совпадёт, измерение неверно.
set -e
export LANG=C LC_ALL=C

N=${N:-20000000}
DELFRAC=${DELFRAC:-5}      # удаляем каждую DELFRAC-ю строку
SHB=${SHB:-512MB}
REP=${REP:-3}

q() { local n="$1"; shift; "$HOME/bench-$n/bin/psql" -h /tmp -p 5460 -d postgres -X -q -t -A "$@"; }

start() {  # start <name>
  local D=/mnt/nvme/data/vac
  "$HOME/bench-$1/bin/pg_ctl" -D "$D" -w stop >/dev/null 2>&1 || true
  "$HOME/bench-$1/bin/pg_ctl" -D "$D" -l "$D/pg.log" -w start >/dev/null
}

D=/mnt/nvme/data/vac
if [ ! -d "$D" ]; then
  "$HOME/bench-vac-before/bin/initdb" -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = 5460"
    echo "shared_buffers = $SHB"
    echo "maintenance_work_mem = 1GB"
    echo "max_wal_size = 16GB"
    echo "autovacuum = off"
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '/tmp'"
    echo "track_io_timing = on"
  } >> "$D/postgresql.conf"
fi

start vac-before
if [ "$(q vac-before -c "select count(*) from pg_class where relname='vt'")" = "0" ]; then
  q vac-before -c "SELECT setseed(0.5);
      CREATE TABLE vt AS SELECT point(random(), random()) AS p, i FROM generate_series(1,$N) i" >/dev/null
  q vac-before -c "VACUUM ANALYZE vt" >/dev/null
fi

printf 'прогон|версия|сборка мусора, с|прочитано страниц индекса|страниц индекса\n'
for i in $(seq 1 $REP); do
  for v in vac-before vac-after; do
    start "$v"
    q "$v" -c "DROP INDEX IF EXISTS vti" >/dev/null 2>&1 || true
    q "$v" -c "CREATE INDEX vti ON vt USING gist(p)" >/dev/null
    q "$v" -c "DELETE FROM vt WHERE i % $DELFRAC = 0" >/dev/null
    q "$v" -c "CHECKPOINT" >/dev/null
    # сбросить кэш ОС нельзя без прав, но буферный кэш мал и индекс в него не влезает
    B0=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='vti'")
    T0=$(q "$v" -c "select extract(epoch from clock_timestamp())")
    q "$v" -c "VACUUM vt" >/dev/null
    T1=$(q "$v" -c "select extract(epoch from clock_timestamp())")
    sleep 1
    B1=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='vti'")
    PAGES=$(q "$v" -c "select pg_relation_size('vti')/8192")
    printf '%s|%s|%s|%s|%s\n' "$i" "$v" \
      "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')" \
      "$((B1 - B0))" "$PAGES"
    # вернуть удалённые строки для следующего прогона
    q "$v" -c "INSERT INTO vt SELECT point(random(), random()), i FROM generate_series(1,$N) i
               WHERE i % $DELFRAC = 0 AND NOT EXISTS (SELECT 1 FROM vt v2 WHERE v2.i = i)" >/dev/null 2>&1 || true
  done
done
