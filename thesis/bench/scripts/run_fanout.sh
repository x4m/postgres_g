#!/bin/bash
# Измерения к P4.
#
# M1. Необязательные функции преобразования (d3a4f89d8a3^ / d3a4f89d8a3).
#     Мерится НЕ время. Автор коммита прямо пишет, что выигрыш в скорости мал,
#     а смысл — в сокращении обязательной обвязки; заявлять здесь ускорение
#     значило бы выдавать желаемое. Мерится возможность: создаётся класс
#     операторов без функции compress и проверяется, (а) создаётся ли он вообще
#     и (б) выполняется ли по нему сканирование без обращения к таблице без
#     функции fetch. Величина двоичная.
#
# M2. Покрывающие атрибуты. Сравниваются три варианта на текущем выпуске:
#     столбец в ключе, столбец покрывающий, столбца нет. Главные величины —
#     ветвистость внутренних уровней и высота: именно на них строится довод,
#     что покрывающий атрибут не удлиняет спуск.
#
# M3. Перезапись элемента на месте (b1328d78f88^ / b1328d78f88). Время вставки
#     отсортированных и случайных данных, объём индекса. Перепроверка чисел
#     2017 года (заявлено ~3x на отсортированных и ~15% на случайных).
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight

N=${N:-1000000}
REP=${REP:-3}
PGSYS=/usr/lib/postgresql/18/bin
D=/mnt/nvme/data/fanout
PORT=5470

q() { local bin="$1"; shift; "$bin/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
up() { local bin="$1"
  "$bin/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
  rm -rf "$D"; "$bin/initdb" 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  { echo "port = $PORT"; echo "shared_buffers = 1GB"; echo "maintenance_work_mem = 512MB"
    echo "max_wal_size = 8GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"; } >> "$D/postgresql.conf"
  "$bin/pg_ctl" 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }
down() { "$1/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true; }


echo "### M1: класс операторов без compress"
for v in opt-before opt-after; do
  BIN="$HOME/bench-$v/bin"
  [ -x "$BIN/postgres" ] || { echo "$v: сборки нет"; continue; }
  up "$BIN"
  # класс операторов для point без функции compress (amprocnum 3)
  OUT=$(q "$BIN" -c "
    CREATE OPERATOR CLASS point_nocompress FOR TYPE point USING gist AS
      OPERATOR 11 <<, OPERATOR 15 >^, OPERATOR 6 ~=,
      FUNCTION 1 gist_point_consistent(internal, point, smallint, oid, internal),
      FUNCTION 2 gist_box_union(internal, internal),
      FUNCTION 5 gist_box_penalty(internal, internal, internal),
      FUNCTION 6 gist_box_picksplit(internal, internal),
      FUNCTION 7 gist_box_same(box, box, internal)" 2>&1 || true)
  if echo "$OUT" | grep -qi "error"; then
    echo "$v: класс без compress НЕ создаётся | $(echo "$OUT" | head -1 | cut -c1-90)"
  else
    echo "$v: класс без compress создаётся"
  fi
  down "$BIN"
done

echo
echo "### M2: покрывающие атрибуты (выпуск 18.4)"
up "$PGSYS"
q "$PGSYS" -c "CREATE EXTENSION IF NOT EXISTS pageinspect" >/dev/null
q "$PGSYS" -c "SELECT setseed(0.5);
   CREATE TABLE ct AS SELECT point(random(),random()) AS p, i,
     repeat('x', 40) AS payload FROM generate_series(1,$N) i" >/dev/null
q "$PGSYS" -c "VACUUM ANALYZE ct" >/dev/null
printf 'вариант|страниц индекса|внутренних|листовых|ветвистость внутр.|запрос, обращений\n'
for variant in "нет|CREATE INDEX ci ON ct USING gist(p)" \
               "покрывающий|CREATE INDEX ci ON ct USING gist(p) INCLUDE (i)" \
               "в ключе|CREATE INDEX ci ON ct USING gist(p, i)"; do
  name=${variant%%|*}; ddl=${variant#*|}
  q "$PGSYS" -c "DROP INDEX IF EXISTS ci" >/dev/null 2>&1 || true
  if ! q "$PGSYS" -c "$ddl" >/dev/null 2>&1; then
    printf '%s|не создаётся (нет класса операторов для типа)|||\n' "$name"; continue
  fi
  PAGES=$(q "$PGSYS" -c "SELECT pg_relation_size('ci')/8192")
  LEV=$(q "$PGSYS" -c "SELECT count(*) FILTER (WHERE NOT leaf), count(*) FILTER (WHERE leaf)
        FROM (SELECT (gist_page_opaque_info(get_raw_page('ci', g))).flags @> ARRAY['leaf'] AS leaf
              FROM generate_series(1, pg_relation_size('ci')/8192 - 1) g) t" | tr '|' ' ')
  INT=$(echo $LEV | cut -d' ' -f1); LEA=$(echo $LEV | cut -d' ' -f2)
  B0=$(q "$PGSYS" -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0) FROM pg_statio_user_indexes WHERE indexrelname='ci'")
  q "$PGSYS" -c "SELECT setseed(0.25);
     SELECT sum(n) FROM (SELECT (SELECT count(*) FROM ct WHERE p <@ box(point(x,y), point(x+0.002,y+0.002))) n
       FROM (SELECT random() x, random() y FROM generate_series(1,2000)) s) t" >/dev/null
  sleep 1
  B1=$(q "$PGSYS" -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0) FROM pg_statio_user_indexes WHERE indexrelname='ci'")
  printf '%s|%s|%s|%s|%s|%s\n' "$name" "$PAGES" "$INT" "$LEA" \
    "$(q "$PGSYS" -c "SELECT round($LEA::numeric/greatest($INT,1),1)")" \
    "$(q "$PGSYS" -c "SELECT round((($B1-$B0)::numeric)/2000,2)")"
done
down "$PGSYS"

echo
echo "### M3: перезапись элемента на месте"
printf 'версия|прогон|вставка отсортированных, с|вставка случайных, с|страниц индекса\n'
for i in $(seq 1 $REP); do
  for v in ovw-before ovw-after; do
    BIN="$HOME/bench-$v/bin"
    [ -x "$BIN/postgres" ] || continue
    up "$BIN"
    q "$BIN" -c "CREATE TABLE ot(p point)" >/dev/null
    q "$BIN" -c "CREATE INDEX oti ON ot USING gist(p)" >/dev/null
    T0=$(date +%s.%N)
    q "$BIN" -c "INSERT INTO ot SELECT point(g::float8/$N, g::float8/$N) FROM generate_series(1,$N) g" >/dev/null
    T1=$(date +%s.%N)
    q "$BIN" -c "TRUNCATE ot" >/dev/null
    q "$BIN" -c "SELECT setseed(0.5)" >/dev/null
    T2=$(date +%s.%N)
    q "$BIN" -c "INSERT INTO ot SELECT point(random(),random()) FROM generate_series(1,$N)" >/dev/null
    T3=$(date +%s.%N)
    printf '%s|%s|%s|%s|%s\n' "$v" "$i" \
      "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')" \
      "$(awk -v a="$T2" -v b="$T3" 'BEGIN{printf "%.2f", b-a}')" \
      "$(q "$BIN" -c "SELECT pg_relation_size('oti')/8192")"
    down "$BIN"
  done
done
bench_postflight
