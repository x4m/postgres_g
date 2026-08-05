#!/bin/bash
# Накладные расходы проверки инвариантов и её расход памяти.
#
# ДВА УТВЕРЖДЕНИЯ, КОТОРЫЕ ПРОВЕРЯЮТСЯ.
#
# 1. Обход по уровням читает каждую страницу примерно по одному разу, в отличие
#    от обхода сверху вниз, который возвращается к уже прочитанным страницам.
#    Величина — отношение числа прочитанных страниц к размеру индекса; она
#    детерминирована и должна быть близка к единице.
#
# 2. Расход памяти ограничен шириной одного уровня, а не размером индекса.
#    Величина — пиковая резидентная память обслуживающего процесса (VmHWM).
#    Если утверждение верно, при росте индекса на порядок память растёт
#    заметно медленнее.
#
# Память читается из /proc обслуживающего процесса: VmHWM — пик за время его
# жизни, поэтому достаточно прочитать её после проверки, пока процесс жив.
#
# ВАЖНАЯ ОГОВОРКА. VmHWM включает затронутые разделяемые буферы, а не только
# собственное состояние проверки. При shared_buffers = 256 МБ и индексе в 84 МБ
# пик составил 80 МБ, то есть измерялся размер индекса, а не расход проверки.
# Поэтому прогон повторяется с малым буферным кэшем (SHB=32MB): если рост
# памяти сохранится, он принадлежит проверке, если исчезнет — буферам.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

SIZES=${SIZES:-"200000 1000000 5000000"}
PORT=${PORT:-5467}
D=${D:-/mnt/nvme/data/amcost}
PGBIN=${PGBIN:-/usr/lib/postgresql/18/bin}

q() { "$PGBIN/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
stop() { "$PGBIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true; }
start() { "$PGBIN/pg_ctl" 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }

bench_lock
bench_preflight

stop; rm -f /tmp/.s.PGSQL.$PORT*; rm -rf "$D"
"$PGBIN/initdb" 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
{ echo "port = $PORT"; echo "shared_buffers = ${SHB:-256MB}"; echo "maintenance_work_mem = 512MB"
  echo "max_wal_size = 8GB"; echo "autovacuum = off"
  echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"; } >> "$D/postgresql.conf"
start
q -c "CREATE EXTENSION IF NOT EXISTS amcheck" >/dev/null
q -c "CREATE TABLE pidt(p int)" >/dev/null

printf 'строк|страниц индекса|кэш|прочитано страниц|отношение|время, с|пик памяти, МБ\n'
for n in $SIZES; do
  q -c "DROP TABLE IF EXISTS gt" >/dev/null 2>&1 || true
  q -c "SELECT setseed(0.5);
        CREATE TABLE gt AS
          SELECT ARRAY[(random()*100000)::int, (random()*100000)::int,
                       (random()*100000)::int] AS a, i
          FROM generate_series(1,$n) i" >/dev/null
  q -c "CREATE INDEX gti ON gt USING gin (a) WITH (fastupdate = off)" >/dev/null
  q -c "VACUUM ANALYZE gt" >/dev/null
  PAGES=$(q -c "SELECT pg_relation_size('gti')/8192")

  for cache in холодный тёплый; do
    if [ "$cache" = холодный ]; then
      q -c "CHECKPOINT" >/dev/null; stop
      sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      start
    else
      q -c "SELECT gin_index_check('gti')" >/dev/null 2>&1 || true   # прогрев
    fi
    B0=$(q -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0)
               FROM pg_statio_user_indexes WHERE indexrelname='gti'")
    # время самой проверки — отдельным простым запуском, без опроса
    T0=$(date +%s.%N)
    q -c "SELECT gin_index_check('gti')" >/dev/null 2>&1 || true
    T1=$(date +%s.%N)
    sleep 1
    B1=$(q -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0)
               FROM pg_statio_user_indexes WHERE indexrelname='gti'")
    READ=$((B1 - B0))

    # память — вторым запуском. Каждый -c идёт отдельной транзакцией, иначе
    # номер процесса не виден снаружи до конца всей серии операторов.
    "$PGBIN/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A \
      -c "TRUNCATE pidt" \
      -c "INSERT INTO pidt SELECT pg_backend_pid()" \
      -c "SELECT gin_index_check('gti')" \
      -c "SELECT pg_sleep(5)" >/dev/null 2>&1 &
    CHK=$!
    PID=""
    for _ in $(seq 1 40); do
      PID=$(q -c "SELECT p FROM pidt" 2>/dev/null || true)
      [ -n "$PID" ] && break
      sleep 0.25
    done
    HWM=0
    if [ -n "$PID" ]; then
      # ждать, пока проверка не сменится ожиданием сна, затем снять пик
      for _ in $(seq 1 400); do
        ST=$(q -c "SELECT query FROM pg_stat_activity WHERE pid=$PID" 2>/dev/null || true)
        case "$ST" in *pg_sleep*) break;; esac
        sleep 0.25
      done
      HWM=$(awk '/VmHWM/{print $2}' /proc/$PID/status 2>/dev/null || echo 0)
    fi
    wait $CHK 2>/dev/null || true

    printf '%s|%s|%s|%s|%s|%s|%s\n' "$n" "$PAGES" "$cache" "$READ" \
      "$(q -c "SELECT round($READ::numeric/greatest($PAGES,1),2)")" \
      "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')" \
      "$(awk -v h="$HWM" 'BEGIN{printf "%.1f", h/1024}')"
  done
done
stop
bench_postflight
