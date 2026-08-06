#!/bin/bash
# Доделка измерений к P4.
#
# M1'. Прошлая проверка была негодной вдвойне: класс операторов строился для
#      point из функций gist_box_*, тогда как без compress ключ остаётся точкой,
#      а функции ждут прямоугольник, — то есть класс был бессмысленным и
#      создавался лишь потому, что проверка при создании этого не ловит. Здесь
#      берётся box, для которого хранимое представление и есть входное, и
#      опускаются обе функции — compress (3) и fetch (9). Проверяется не время,
#      а выбирается ли сканирование без обращения к таблице.
#      Класс НЕ объявляется умолчательным: для box умолчательный уже есть, и
#      попытка объявить второй отвергается независимо от версии. Индекс
#      строится с явным указанием класса.
#
# M2'. Вариант «столбец в ключе» с установленным btree_gist: расширение даёт
#      класс операторов для int, и наивный способ становится возможен. Теперь
#      измеряется его цена по ветвистости внутренних уровней.
#
# M3'. Перезапись элемента на месте при КРУПНОМ ключе. На point (16 байт)
#      различия не было, и правдоподобное объяснение — перемещать нечего.
#      cube размерности 16 даёт ключ в 256 байт.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight

N=${N:-500000}
REP=${REP:-3}
PGSYS=/usr/lib/postgresql/18/bin
D=/mnt/nvme/data/fanout2
PORT=5471

q() { local bin="$1"; shift; "$bin/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
up() { local bin="$1"
  "$bin/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
  rm -rf "$D"; "$bin/initdb" 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  { echo "port = $PORT"; echo "shared_buffers = 1GB"; echo "maintenance_work_mem = 512MB"
    echo "max_wal_size = 8GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"; } >> "$D/postgresql.conf"
  "$bin/pg_ctl" 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }
down() { "$1/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true; }


echo "### M1': класс операторов для box без compress и без fetch"
for v in opt-before opt-after; do
  BIN="$HOME/bench-$v/bin"
  [ -x "$BIN/postgres" ] || { echo "$v: сборки нет"; continue; }
  up "$BIN"
  CREATE=$(q "$BIN" -c "
    CREATE OPERATOR CLASS box_nocompress FOR TYPE box USING gist AS
      OPERATOR 3 &&, OPERATOR 7 @>, OPERATOR 8 <@, OPERATOR 6 ~=,
      FUNCTION 1 gist_box_consistent(internal, box, smallint, oid, internal),
      FUNCTION 2 gist_box_union(internal, internal),
      FUNCTION 5 gist_box_penalty(internal, internal, internal),
      FUNCTION 6 gist_box_picksplit(internal, internal),
      FUNCTION 7 gist_box_same(box, box, internal)" 2>&1 || true)
  if echo "$CREATE" | grep -qi error; then
    echo "$v: класс НЕ создаётся | $(echo "$CREATE" | head -1 | cut -c1-80)"
    down "$BIN"; continue
  fi
  q "$BIN" -c "CREATE TABLE bt AS SELECT box(point(random(),random()),point(random()+0.01,random()+0.01)) AS b FROM generate_series(1,50000)" >/dev/null 2>&1
  IDX=$(q "$BIN" -c "CREATE INDEX bti ON bt USING gist(b box_nocompress)" 2>&1 || true)
  if echo "$IDX" | grep -qi error; then
    echo "$v: индекс НЕ строится | $(echo "$IDX" | head -1 | cut -c1-80)"; down "$BIN"; continue
  fi
  q "$BIN" -c "VACUUM ANALYZE bt" >/dev/null
  PLAN=$(q "$BIN" -c "SET enable_seqscan=off; SET enable_bitmapscan=off;
          EXPLAIN (COSTS OFF) SELECT b FROM bt WHERE b <@ box(point(0,0),point(0.5,0.5))" 2>&1 | tr '\n' ' ')
  case "$PLAN" in
    *"Index Only Scan"*) echo "$v: класс создаётся, сканирование БЕЗ обращения к таблице доступно";;
    *"Index Scan"*)      echo "$v: класс создаётся, но сканирование с обращением к таблице";;
    *) echo "$v: класс создаётся, план: $(echo "$PLAN" | cut -c1-70)";;
  esac
  down "$BIN"
done

echo
echo "### M2': столбец в ключе против покрывающего (btree_gist, выпуск 18.4)"
up "$PGSYS"
q "$PGSYS" -c "CREATE EXTENSION IF NOT EXISTS pageinspect" >/dev/null
BG=$(q "$PGSYS" -c "CREATE EXTENSION IF NOT EXISTS btree_gist" 2>&1 || true)
echo "$BG" | grep -qi error && echo "btree_gist недоступен: $(echo "$BG"|head -1)" || echo "btree_gist установлен"
q "$PGSYS" -c "SELECT setseed(0.5);
   CREATE TABLE ct AS SELECT point(random(),random()) AS p, i FROM generate_series(1,1000000) i" >/dev/null
q "$PGSYS" -c "VACUUM ANALYZE ct" >/dev/null
printf 'вариант|страниц индекса|внутренних|листовых|ветвистость внутр.|обращений на запрос\n'
for variant in "нет|CREATE INDEX ci ON ct USING gist(p)" \
               "покрывающий|CREATE INDEX ci ON ct USING gist(p) INCLUDE (i)" \
               "в ключе|CREATE INDEX ci ON ct USING gist(p, i)"; do
  name=${variant%%|*}; ddl=${variant#*|}
  q "$PGSYS" -c "DROP INDEX IF EXISTS ci" >/dev/null 2>&1 || true
  if ! q "$PGSYS" -c "$ddl" >/dev/null 2>&1; then printf '%s|не создаётся|||\n' "$name"; continue; fi
  PAGES=$(q "$PGSYS" -c "SELECT pg_relation_size('ci')/8192")
  LEV=$(q "$PGSYS" -c "SELECT count(*) FILTER (WHERE NOT leaf), count(*) FILTER (WHERE leaf)
        FROM (SELECT (gist_page_opaque_info(get_raw_page('ci', g))).flags @> ARRAY['leaf'] AS leaf
              FROM generate_series(1, pg_relation_size('ci')/8192 - 1) g) t" | tr '|' ' ')
  INT=$(echo $LEV | cut -d' ' -f1); LEA=$(echo $LEV | cut -d' ' -f2)
  B0=$(q "$PGSYS" -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0) FROM pg_statio_user_indexes WHERE indexrelname='ci'")
  q "$PGSYS" -c "SELECT setseed(0.25);
     SELECT sum(n) FROM (SELECT (SELECT count(*) FROM ct WHERE p <@ box(point(x,y),point(x+0.002,y+0.002))) n
       FROM (SELECT random() x, random() y FROM generate_series(1,2000)) s) t" >/dev/null
  sleep 1
  B1=$(q "$PGSYS" -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0) FROM pg_statio_user_indexes WHERE indexrelname='ci'")
  printf '%s|%s|%s|%s|%s|%s\n' "$name" "$PAGES" "$INT" "$LEA" \
    "$(q "$PGSYS" -c "SELECT round($LEA::numeric/greatest($INT,1),1)")" \
    "$(q "$PGSYS" -c "SELECT round((($B1-$B0)::numeric)/2000,2)")"
done
down "$PGSYS"

echo
echo "### M3': перезапись при крупном ключе (cube, D=16, ключ 256 байт)"
printf 'версия|прогон|вставка отсортированных, с|вставка случайных, с|страниц индекса\n'
for i in $(seq 1 $REP); do
  for v in ovw-before ovw-after; do
    BIN="$HOME/bench-$v/bin"
    [ -f "$HOME/bench-$v/share/postgresql/extension/cube.control" ] || { echo "$v: cube не установлен, пропуск"; continue; }
    up "$BIN"
    q "$BIN" -c "CREATE EXTENSION cube" >/dev/null 2>&1 || { echo "$v: cube не создаётся"; down "$BIN"; continue; }
    q "$BIN" -c "CREATE TABLE cbt(c cube)" >/dev/null
    q "$BIN" -c "CREATE INDEX cbti ON cbt USING gist(c)" >/dev/null
    T0=$(date +%s.%N)
    q "$BIN" -c "INSERT INTO cbt SELECT cube(array_agg(v ORDER BY j))
                 FROM (SELECT g AS i, j, (g::float8/$N + j) AS v
                       FROM generate_series(1,$N) g, generate_series(1,16) j) s GROUP BY i" >/dev/null
    T1=$(date +%s.%N)
    q "$BIN" -c "TRUNCATE cbt; SELECT setseed(0.5)" >/dev/null
    T2=$(date +%s.%N)
    q "$BIN" -c "INSERT INTO cbt SELECT cube(array_agg(v ORDER BY j))
                 FROM (SELECT g AS i, j, random() AS v
                       FROM generate_series(1,$N) g, generate_series(1,16) j) s GROUP BY i" >/dev/null
    T3=$(date +%s.%N)
    printf '%s|%s|%s|%s|%s\n' "$v" "$i" \
      "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')" \
      "$(awk -v a="$T2" -v b="$T3" 'BEGIN{printf "%.2f", b-a}')" \
      "$(q "$BIN" -c "SELECT pg_relation_size('cbti')/8192")"
    down "$BIN"
  done
done
bench_postflight
