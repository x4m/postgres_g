#!/bin/bash
# Возврат страниц в оборот: 7df159a620b^ против 7df159a620b.
#
# Нагрузка, ради которой изменение и делалось: новые ключи вставляются в новую
# область пространства, старые удаляются из старой. Окно шириной в WIN раундов
# движется по оси x, так что число живых строк постоянно, а число когда-либо
# вставленных растёт. Без возврата страниц индекс растёт вместе со вторым, с
# возвратом — держится около первого.
#
# Родитель уже содержит физический обход (fe280694d0d), так что пара версий
# изолирует именно возврат страниц.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

M=${M:-200000}     # строк за раунд
R=${R:-12}         # раундов
WIN=${WIN:-3}      # ширина окна в раундах
SHB=${SHB:-1GB}
PORT=5461
D=/mnt/nvme/data/reuse

q() { local n="$1"; shift; "$HOME/bench-$n/bin/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
start() { "$HOME/bench-$1/bin/pg_ctl" 9>&- 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
          "$HOME/bench-$1/bin/pg_ctl" 9>&- 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }

if [ ! -d "$D" ]; then
  "$HOME/bench-reuse-before/bin/initdb" 9>&- 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = $PORT"; echo "shared_buffers = $SHB"; echo "maintenance_work_mem = 1GB"
    echo "max_wal_size = 16GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
  } >> "$D/postgresql.conf"
fi

bench_lock
bench_preflight

printf 'версия|раунд|живых строк|страниц индекса|страниц таблицы\n'
for v in reuse-before reuse-after; do
  start "$v"
  q "$v" -c "DROP TABLE IF EXISTS rt" >/dev/null 2>&1 || true
  q "$v" -c "CREATE TABLE rt (p point, r int)" >/dev/null
  q "$v" -c "CREATE INDEX rti ON rt USING gist(p)" >/dev/null
  q "$v" -c "SELECT setseed(0.5)" >/dev/null
  for r in $(seq 1 $R); do
    # вставка в новую область: x лежит в [r, r+1)
    q "$v" -c "INSERT INTO rt SELECT point($r + random(), random()), $r
               FROM generate_series(1,$M)" >/dev/null
    # удаление из старой
    q "$v" -c "DELETE FROM rt WHERE r <= $r - $WIN" >/dev/null
    q "$v" -c "VACUUM rt" >/dev/null
    printf '%s|%s|%s|%s|%s\n' "$v" "$r" \
      "$(q "$v" -c "SELECT count(*) FROM rt")" \
      "$(q "$v" -c "SELECT pg_relation_size('rti')/8192")" \
      "$(q "$v" -c "SELECT pg_relation_size('rt')/8192")"
  done
done

bench_postflight
