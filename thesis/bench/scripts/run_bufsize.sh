#!/bin/sh
# Чувствительность к размеру буфера при сортированном построении.
#
# f1ea98a7975 накапливает GIST_SORTED_BUILD_PAGE_NUM страниц и применяет к ним
# picksplit рекурсивно. Константа зашита и равна 4; в статье она обозначена k.
# Собираются варианты с разными k, каталог данных общий: версия каталога у всех
# одна, различается только константа, поэтому достаточно подменять двоичные
# файлы.
#
# k = 1 ожидается близким к разбиению по заполнению страницы: выбирать точку
# разреза внутри одной страницы почти не из чего.
set -e
export LANG=C LC_ALL=C

SRC="$HOME/pgsrc"
DATA=/mnt/nvme/data/k
PORT=5444
N=${N:-10000000}
KLIST=${KLIST:-"1 2 4 8 16"}

# --- сборка вариантов
for K in $KLIST; do
  PREFIX="$HOME/bench-k$K"
  [ -x "$PREFIX/bin/postgres" ] && continue
  cd "$SRC"
  git checkout -q --detach f1ea98a7975
  git clean -qfdx
  sed -i "s/^#define GIST_SORTED_BUILD_PAGE_NUM.*/#define GIST_SORTED_BUILD_PAGE_NUM $K/" \
      src/backend/access/gist/gistbuild.c
  grep -q "GIST_SORTED_BUILD_PAGE_NUM $K" src/backend/access/gist/gistbuild.c
  ./configure --prefix="$PREFIX" --enable-debug --without-icu --without-readline \
              --without-zlib CFLAGS="-O2 -fno-omit-frame-pointer" > /dev/null 2>&1
  make -s -j"$(nproc)" > /dev/null 2>&1
  make -s install > /dev/null 2>&1
  make -s -C contrib/pageinspect install > /dev/null 2>&1
  echo "собран k=$K" >&2
done
cd "$SRC" && git checkout -q --detach f1ea98a7975 && git clean -qfdx

# --- общий каталог данных
FIRST=$(echo $KLIST | awk '{print $1}')
if [ ! -d "$DATA" ]; then
  "$HOME/bench-k$FIRST/bin/initdb" -D "$DATA" --locale=C --encoding=UTF8 > /dev/null 2>&1
  {
    echo "port = $PORT"
    echo "shared_buffers = 8GB"
    echo "maintenance_work_mem = 1GB"
    echo "work_mem = 64MB"
    echo "max_wal_size = 16GB"
    echo "autovacuum = off"
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '/tmp'"
  } >> "$DATA/postgresql.conf"
fi

# первый аргумент — вариант k, остальные передаются psql
PSQL_AT() {
  K_="$1"; shift
  "$HOME/bench-k$K_/bin/psql" -h /tmp -p $PORT -d postgres -X -q -t -A "$@"
}

# на случай, если предыдущий прогон оставил сервер запущенным
"$HOME/bench-k$FIRST/bin/pg_ctl" -D "$DATA" -w stop > /dev/null 2>&1 || true

# данные создаём один раз
"$HOME/bench-k$FIRST/bin/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w start > /dev/null
if [ "$(PSQL_AT $FIRST -c "select count(*) from pg_class where relname='t'")" = "0" ]; then
  PSQL_AT $FIRST -c "CREATE OR REPLACE FUNCTION gen_uniform(n int) RETURNS SETOF point
                     LANGUAGE sql AS \$\$ SELECT point(random(), random())
                     FROM generate_series(1, n) \$\$" > /dev/null
  PSQL_AT $FIRST -c "CREATE TABLE t AS SELECT gen_uniform($N) AS p" > /dev/null
  PSQL_AT $FIRST -c "VACUUM ANALYZE t" > /dev/null
fi
PSQL_AT $FIRST -f "$HOME/clustered.sql" > /dev/null 2>&1
  PSQL_AT $FIRST -f "$HOME/probe_index.sql" > /dev/null 2>&1
"$HOME/bench-k$FIRST/bin/pg_ctl" -D "$DATA" -w stop > /dev/null

printf 'k|время сборки, с|страниц индекса|обращений на запрос\n'
for K in $KLIST; do
  "$HOME/bench-k$K/bin/pg_ctl" -D "$DATA" -w stop > /dev/null 2>&1 || true
  "$HOME/bench-k$K/bin/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w start > /dev/null
  PSQL_AT $K -c "DROP INDEX IF EXISTS ti" > /dev/null 2>&1 || true
  T0=$(PSQL_AT $K -c "select extract(epoch from clock_timestamp())")
  PSQL_AT $K -c "CREATE INDEX ti ON t USING gist(p)" > /dev/null
  T1=$(PSQL_AT $K -c "select extract(epoch from clock_timestamp())")
  SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')
  PAGES=$(PSQL_AT $K -c "select pg_relation_size('ti')/8192")
  HITS=$(PSQL_AT $K -c "select probe_index('t','ti',500,$N)")
  printf '%s|%s|%s|%s\n' "$K" "$SECS" "$PAGES" "$HITS"
  "$HOME/bench-k$K/bin/pg_ctl" -D "$DATA" -w stop > /dev/null
done
