#!/bin/sh
# Измерение внутристраничного индексирования (v3, пропускающие кортежи на
# странице) против базовой сборки того же коммита.
#
# Возможность включена всегда, ручек нет, поэтому сравниваются два двоичных
# файла на отдельных каталогах данных: индекс со скип-кортежами базовой сборкой
# читать нельзя.
#
# Что меряем:
#  * вставку — в 2018 году именно там был выигрыш 30–40 %;
#  * построение сортировкой и объём индекса (скип-кортежи занимают место);
#  * точечные запросы и соединение по индексу;
#  * то же на индексе, построенном вставкой, — проверка догадки о том, что
#    пропуск эффективен только при упорядоченной странице.
set -e
export LANG=C LC_ALL=C

N=${N:-5000000}
SEED=${SEED:-0.42}
NPROBE=${NPROBE:-2000}

start() {  # start <name> <port>
  D=/mnt/nvme/data/$1
  if [ ! -d "$D" ]; then
    "$HOME/bench-$1/bin/initdb" -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
    {
      echo "port = $2"; echo "shared_buffers = 8GB"; echo "maintenance_work_mem = 1GB"
      echo "work_mem = 64MB"; echo "max_wal_size = 16GB"; echo "autovacuum = off"
      echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
    } >> "$D/postgresql.conf"
  fi
  "$HOME/bench-$1/bin/pg_ctl" -D "$D" -w stop >/dev/null 2>&1 || true
  "$HOME/bench-$1/bin/pg_ctl" -D "$D" -l "$D/pg.log" -w start >/dev/null
}

psql_at() { N_="$1"; P_="$2"; shift 2; "$HOME/bench-$N_/bin/psql" -h /tmp -p "$P_" -d postgres -X -q -t -A "$@"; }

now() { psql_at "$1" "$2" -c "select extract(epoch from clock_timestamp())"; }
el()  { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }

prepare() {  # prepare <name> <port>
  psql_at "$1" "$2" -c "DROP TABLE IF EXISTS t, probes, ins" >/dev/null 2>&1 || true
  psql_at "$1" "$2" -c "SELECT setseed($SEED);
      CREATE TABLE t AS SELECT point(random(), random()) AS p FROM generate_series(1, $N)" >/dev/null
  psql_at "$1" "$2" -c "SELECT setseed($SEED);
      CREATE TABLE probes AS SELECT random() AS x, random() AS y FROM generate_series(1, $NPROBE)" >/dev/null
  psql_at "$1" "$2" -c "SELECT setseed(0.77);
      CREATE TABLE ins AS SELECT point(random(), random()) AS p FROM generate_series(1, 500000)" >/dev/null
  psql_at "$1" "$2" -c "VACUUM ANALYZE t, probes, ins" >/dev/null
}

measure() {  # measure <name> <port> <label>
  NAME="$1"; PORT="$2"; LABEL="$3"

  # --- построение сортировкой
  psql_at "$NAME" "$PORT" -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
  T0=$(now "$NAME" "$PORT")
  psql_at "$NAME" "$PORT" -c "CREATE INDEX ti ON t USING gist(p)" >/dev/null
  T1=$(now "$NAME" "$PORT")
  BUILD=$(el "$T0" "$T1")
  PAGES=$(psql_at "$NAME" "$PORT" -c "select pg_relation_size('ti')/8192")

  # --- точечные запросы
  psql_at "$NAME" "$PORT" >/dev/null <<'EOF'
CREATE OR REPLACE FUNCTION probe_run(nrows float8) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE r record; d float8 := 0.5/sqrt(nrows); t0 timestamptz; c bigint;
BEGIN
  SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;
  t0 := clock_timestamp();
  FOR r IN SELECT x, y FROM probes LOOP
    EXECUTE format('SELECT count(*) FROM t WHERE p <@ box(point(%s,%s), point(%s,%s))',
                   r.x-d, r.y-d, r.x+d, r.y+d) INTO c;
  END LOOP;
  RETURN round(extract(epoch from clock_timestamp()-t0)::numeric, 3);
END $$;
EOF
  POINT=$(psql_at "$NAME" "$PORT" -c "select probe_run($N)")

  # --- соединение по индексу
  T0=$(now "$NAME" "$PORT")
  psql_at "$NAME" "$PORT" -c "SET enable_seqscan=off; SET enable_bitmapscan=off;
      SET max_parallel_workers_per_gather=0;
      SELECT count(*) FROM probes q JOIN t ON t.p <@ box(point(q.x-0.0007,q.y-0.0007), point(q.x+0.0007,q.y+0.0007))" >/dev/null
  T1=$(now "$NAME" "$PORT")
  JOIN=$(el "$T0" "$T1")

  # --- вставка в готовый индекс
  T0=$(now "$NAME" "$PORT")
  psql_at "$NAME" "$PORT" -c "INSERT INTO t SELECT p FROM ins" >/dev/null
  T1=$(now "$NAME" "$PORT")
  INSERT=$(el "$T0" "$T1")
  PAGES2=$(psql_at "$NAME" "$PORT" -c "select pg_relation_size('ti')/8192")
  psql_at "$NAME" "$PORT" -c "DELETE FROM t WHERE ctid > (select max(ctid) from t)" >/dev/null 2>&1 || true

  printf '%s|%s|%s|%s|%s|%s|%s\n' "$LABEL" "$BUILD" "$PAGES" "$POINT" "$JOIN" "$INSERT" "$PAGES2"
}

start ip-base 5447
start ip-skip 5448
prepare ip-base 5447
prepare ip-skip 5448

printf 'сборка|построение, с|страниц|точечные, с|соединение, с|вставка 500k, с|страниц после\n'
measure ip-base 5447 базовая
measure ip-skip 5448 скип-группы
