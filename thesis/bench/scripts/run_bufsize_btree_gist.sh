#!/bin/sh
# Проверка размера буфера на классах операторов btree_gist.
#
# Зачем: измерение на point_ops показало, что GIST_SORTED_BUILD_PAGE_NUM = 4
# не оптимально и переход к 8 даёт 12 % по обращениям бесплатно. Константу
# выбирали осторожно из-за классов операторов с дорогим picksplit. Но опорную
# функцию сортировки объявляют только point_ops и классы btree_gist, а у всех
# у них picksplit — сортировка и деление пополам. Проверяем это измерением на
# btree_gist, где picksplit другой, чем у point_ops.
#
# Нужен выпуск 18 и старше: сортированная сборка для btree_gist появилась в
# e4309f73f69.
set -e
export LANG=C LC_ALL=C

SRC="$HOME/pgsrc"
DATA=/mnt/nvme/data/kb
PORT=5445
N=${N:-10000000}
KLIST=${KLIST:-"4 8 16"}
BASE=${BASE:-REL_18_0}

for K in $KLIST; do
  PREFIX="$HOME/bench-b$K"
  [ -x "$PREFIX/bin/postgres" ] && { echo "уже собран k=$K" >&2; continue; }
  cd "$SRC"
  git checkout -q --detach "$BASE"
  git clean -qfdx
  sed -i "s/^#define GIST_SORTED_BUILD_PAGE_NUM.*/#define GIST_SORTED_BUILD_PAGE_NUM $K/" \
      src/backend/access/gist/gistbuild.c
  grep -q "GIST_SORTED_BUILD_PAGE_NUM $K" src/backend/access/gist/gistbuild.c
  ./configure --prefix="$PREFIX" --without-icu --without-readline --without-zlib \
              CFLAGS="-O2" > /dev/null 2>&1
  make -s -j"$(nproc)" > /dev/null 2>&1
  make -s install > /dev/null 2>&1
  make -s -C contrib/btree_gist install > /dev/null 2>&1
  echo "собран k=$K" >&2
done

FIRST=$(echo $KLIST | awk '{print $1}')
PSQL_AT() { K_="$1"; shift; "$HOME/bench-b$K_/bin/psql" -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }

"$HOME/bench-b$FIRST/bin/pg_ctl" -D "$DATA" -w stop > /dev/null 2>&1 || true
if [ ! -d "$DATA" ]; then
  "$HOME/bench-b$FIRST/bin/initdb" -D "$DATA" --locale=C --encoding=UTF8 > /dev/null 2>&1
  {
    echo "port = $PORT"; echo "shared_buffers = 8GB"; echo "maintenance_work_mem = 1GB"
    echo "work_mem = 64MB"; echo "max_wal_size = 16GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
  } >> "$DATA/postgresql.conf"
fi

"$HOME/bench-b$FIRST/bin/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w start > /dev/null
PSQL_AT $FIRST -c "CREATE EXTENSION IF NOT EXISTS btree_gist" > /dev/null
if [ "$(PSQL_AT $FIRST -c "select count(*) from pg_class where relname='b'")" = "0" ]; then
  # два распределения: равномерное и скошенное — на скошенном разбиение важнее
  PSQL_AT $FIRST -c "CREATE TABLE b AS
      SELECT (random()*1000000000)::int8 AS u,
             (case when random() < 0.9 then (random()*1000)::int8
                   else (random()*1000000000)::int8 end) AS s
        FROM generate_series(1, $N)" > /dev/null
  PSQL_AT $FIRST -c "VACUUM ANALYZE b" > /dev/null
fi
"$HOME/bench-b$FIRST/bin/pg_ctl" -D "$DATA" -w stop > /dev/null

printf 'k|столбец|время сборки, с|страниц индекса|обращений на запрос\n'
for K in $KLIST; do
  "$HOME/bench-b$K/bin/pg_ctl" -D "$DATA" -w stop > /dev/null 2>&1 || true
  "$HOME/bench-b$K/bin/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w start > /dev/null
  for COL in u s; do
    PSQL_AT $K -c "DROP INDEX IF EXISTS bi" > /dev/null 2>&1 || true
    T0=$(PSQL_AT $K -c "select extract(epoch from clock_timestamp())")
    PSQL_AT $K -c "CREATE INDEX bi ON b USING gist($COL)" > /dev/null
    T1=$(PSQL_AT $K -c "select extract(epoch from clock_timestamp())")
    SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')
    PAGES=$(PSQL_AT $K -c "select pg_relation_size('bi')/8192")
    HITS=$(PSQL_AT $K -c "
      SET enable_seqscan = off; SET enable_bitmapscan = off;
      SELECT round(avg(blks), 2) FROM (
        SELECT (js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint
             + (js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint AS blks
          FROM generate_series(1, 300) g,
               LATERAL (SELECT q.js FROM (
                 SELECT (SELECT j FROM (
                   SELECT * FROM json_array_elements('[]'::json)) t LIMIT 0) AS j
               ) x, LATERAL (SELECT NULL::json AS js) q) y
      ) z" 2>/dev/null || echo "-")
    printf '%s|%s|%s|%s|%s\n' "$K" "$COL" "$SECS" "$PAGES" "$HITS"
  done
  "$HOME/bench-b$K/bin/pg_ctl" -D "$DATA" -w stop > /dev/null
done
