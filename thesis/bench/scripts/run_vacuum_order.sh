#!/bin/bash
# Сборка мусора: логический обход против физического (fe280694d0d).
#
# Разница видна только когда индекс не помещается в буферный кэш: иначе оба
# варианта работают по памяти и порядок не важен. Поэтому кэш намеренно мал.
#
# Первая версия скрипта была неверной: строки удалялись один раз, а «возврат
# удалённых» между прогонами не работал, и все замеры после первого мерили
# сборку мусора, которой нечего было делать (0,3 с). Здесь состояние таблицы
# восстанавливается копированием из неизменяемого образца перед каждым замером.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

N=${N:-10000000}
DELFRAC=${DELFRAC:-5}
SHB=${SHB:-256MB}
REP=${REP:-3}
PORT=5460
D=/mnt/nvme/data/vac

q() { local n="$1"; shift; "$HOME/bench-$n/bin/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
start() { "$HOME/bench-$1/bin/pg_ctl" 9>&- 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
          "$HOME/bench-$1/bin/pg_ctl" 9>&- 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }

if [ ! -d "$D" ]; then
  "$HOME/bench-vac-before/bin/initdb" 9>&- 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = $PORT"; echo "shared_buffers = $SHB"; echo "maintenance_work_mem = 1GB"
    echo "max_wal_size = 16GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
  } >> "$D/postgresql.conf"
fi

start vac-before
q vac-before -c "DROP TABLE IF EXISTS vt" >/dev/null 2>&1 || true
if [ "$(q vac-before -c "select count(*) from pg_class where relname='vt0'")" = "0" ]; then
  q vac-before -c "SELECT setseed(0.5);
      CREATE TABLE vt0 AS SELECT point(random(), random()) AS p, i FROM generate_series(1,$N) i" >/dev/null
  q vac-before -c "VACUUM ANALYZE vt0" >/dev/null
fi

bench_lock
bench_preflight

printf 'прогон|версия|сборка мусора, с|прочитано страниц индекса|удалено строк|страниц индекса\n'
for i in $(seq 1 $REP); do
  for v in vac-before vac-after; do
    start "$v"
    # каждый замер — на свежей копии образца
    q "$v" -c "DROP TABLE IF EXISTS vt" >/dev/null 2>&1 || true
    q "$v" -c "CREATE TABLE vt AS SELECT * FROM vt0" >/dev/null
    q "$v" -c "VACUUM ANALYZE vt" >/dev/null
    q "$v" -c "CREATE INDEX vti ON vt USING gist(p)" >/dev/null
    DEL=$(q "$v" -c "WITH d AS (DELETE FROM vt WHERE i % $DELFRAC = 0 RETURNING 1)
                     SELECT count(*) FROM d")
    q "$v" -c "CHECKPOINT" >/dev/null
    B0=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='vti'")
    T0=$(q "$v" -c "select extract(epoch from clock_timestamp())")
    q "$v" -c "VACUUM vt" >/dev/null
    T1=$(q "$v" -c "select extract(epoch from clock_timestamp())")
    sleep 1
    B1=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='vti'")
    printf '%s|%s|%s|%s|%s|%s\n' "$i" "$v" \
      "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')" \
      "$((B1 - B0))" "$DEL" "$(q "$v" -c "select pg_relation_size('vti')/8192")"
  done
done

bench_postflight
