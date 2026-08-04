#!/bin/bash
# Упреждающее чтение при сборке мусора: 69273b818b1^ против 69273b818b1
# («Use streaming read I/O in GiST vacuuming», выпуск 18).
#
# Обе версии обходят страницы в одном и том же физическом порядке и читают
# одно и то же их число. Различие — только в том, выдаются ли запросы к
# носителю по одному или пакетом, а это видно исключительно во времени и
# исключительно при холодном кэше. Поэтому:
#   * страничный кэш операционной системы сбрасывается перед каждым замером;
#   * таблица после сброса прогревается просмотром, чтобы холодными остались
#     только страницы индекса, — иначе просмотр таблицы, одинаковый у обеих
#     версий, размывает эффект (как это вышло в run_vacuum_order.sh).
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

N=${N:-20000000}
DELFRAC=${DELFRAC:-5}
SHB=${SHB:-256MB}
REP=${REP:-3}
PORT=5462
D=/mnt/nvme/data/stream

q() { local n="$1"; shift; "$HOME/bench-$n/bin/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
stop()  { "$HOME/bench-$1/bin/pg_ctl" 9>&- 9>&- -D "$D" -w stop >/dev/null 2>&1 || true; }
start() { "$HOME/bench-$1/bin/pg_ctl" 9>&- 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }

if [ ! -d "$D" ]; then
  "$HOME/bench-stream-before/bin/initdb" 9>&- 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = $PORT"; echo "shared_buffers = $SHB"; echo "maintenance_work_mem = 1GB"
    echo "max_wal_size = 32GB"; echo "autovacuum = off"
    echo "effective_io_concurrency = 16"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
  } >> "$D/postgresql.conf"
fi

stop stream-before; start stream-before
if [ "$(q stream-before -c "select count(*) from pg_class where relname='st0'")" = "0" ]; then
  q stream-before -c "SELECT setseed(0.5);
      CREATE TABLE st0 AS SELECT point(random(), random()) AS p, i FROM generate_series(1,$N) i" >/dev/null
  q stream-before -c "VACUUM ANALYZE st0" >/dev/null
fi

bench_lock
bench_preflight

printf 'прогон|версия|сборка мусора, с|прочитано страниц индекса|страниц индекса\n'
for i in $(seq 1 $REP); do
  for v in stream-before stream-after; do
    stop "$v"; start "$v"
    q "$v" -c "DROP TABLE IF EXISTS st" >/dev/null 2>&1 || true
    q "$v" -c "CREATE TABLE st AS SELECT * FROM st0" >/dev/null
    q "$v" -c "VACUUM ANALYZE st" >/dev/null
    q "$v" -c "CREATE INDEX sti ON st USING gist(p)" >/dev/null
    q "$v" -c "DELETE FROM st WHERE i % $DELFRAC = 0" >/dev/null
    q "$v" -c "CHECKPOINT" >/dev/null
    stop "$v"
    sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start "$v"
    q "$v" -c "SELECT count(*) FROM st" >/dev/null   # прогрев таблицы
    B0=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='sti'")
    T0=$(q "$v" -c "select extract(epoch from clock_timestamp())")
    q "$v" -c "VACUUM st" >/dev/null
    T1=$(q "$v" -c "select extract(epoch from clock_timestamp())")
    sleep 1
    B1=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='sti'")
    printf '%s|%s|%s|%s|%s\n' "$i" "$v" \
      "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')" \
      "$((B1 - B0))" "$(q "$v" -c "select pg_relation_size('sti')/8192")"
  done
done

bench_postflight
