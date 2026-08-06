#!/bin/bash
# M1: класс операторов без compress и без fetch — проверка возможности.
#
# Класс НЕ объявляется умолчательным: для box умолчательный уже есть, и попытка
# объявить второй отвергается независимо от версии. Индекс строится с явным
# указанием класса. Мерится не время, а выбирается ли сканирование без
# обращения к таблице: именно эту возможность изменение и открывает.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

PORT=5471
D=/mnt/nvme/data/m1
bench_lock

for v in opt-before opt-after; do
  BIN="$HOME/bench-$v/bin"
  [ -x "$BIN/postgres" ] || { echo "$v: сборки нет"; continue; }
  "$BIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
  rm -rf "$D"
  "$BIN/initdb" 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = $PORT"
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '/tmp'"
    echo "shared_buffers = 256MB"
  } >> "$D/postgresql.conf"
  "$BIN/pg_ctl" 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null
  P() { "$BIN/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }

  C=$(P -c "CREATE OPERATOR CLASS box_nocompress FOR TYPE box USING gist AS
              OPERATOR 3 &&, OPERATOR 7 @>, OPERATOR 8 <@, OPERATOR 6 ~=,
              FUNCTION 1 gist_box_consistent(internal, box, smallint, oid, internal),
              FUNCTION 2 gist_box_union(internal, internal),
              FUNCTION 5 gist_box_penalty(internal, internal, internal),
              FUNCTION 6 gist_box_picksplit(internal, internal),
              FUNCTION 7 gist_box_same(box, box, internal)" 2>&1 || true)
  if echo "$C" | grep -qi error; then
    echo "$v: класс НЕ создаётся | $(echo "$C" | head -1 | cut -c1-90)"
    "$BIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
    continue
  fi

  P -c "CREATE TABLE bt AS
        SELECT box(point(random(),random()), point(random()+0.01,random()+0.01)) AS b
        FROM generate_series(1,50000)" >/dev/null 2>&1
  I=$(P -c "CREATE INDEX bti ON bt USING gist(b box_nocompress)" 2>&1 || true)
  if echo "$I" | grep -qi error; then
    echo "$v: класс создаётся, индекс НЕ строится | $(echo "$I" | head -1 | cut -c1-90)"
  else
    P -c "VACUUM ANALYZE bt" >/dev/null
    PL=$(P -c "SET enable_seqscan=off; SET enable_bitmapscan=off;
               EXPLAIN (COSTS OFF) SELECT b FROM bt WHERE b <@ box(point(0,0),point(0.5,0.5))" 2>&1 | tr '\n' ' ')
    case "$PL" in
      *"Index Only Scan"*) echo "$v: индекс строится, сканирование БЕЗ обращения к таблице";;
      *"Index Scan"*)      echo "$v: индекс строится, сканирование С обращением к таблице";;
      *) echo "$v: план — $(echo "$PL" | cut -c1-70)";;
    esac
  fi
  "$BIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
done
