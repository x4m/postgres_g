#!/bin/sh
# Эксперимент с управляемым размером кластера (plan/60-pvldb.md, п. 2).
#
# Гипотеза: при разбиении по заполнению страницы перекрытие нелистовых ключей
# зависит от того, как размер плотной области соотносится с ёмкостью страницы;
# разбиение через picksplit эту зависимость снимает.
#
# Метрика — число обращений к страницам индекса на точечный запрос, снятое по
# pg_statio_user_indexes. Она переносима и прямо равна сумме omega_l по уровням.
# Ключи из pageinspect в версии 15 достать нельзя: внутренние ключи
# декодируются дескриптором листовой строки.
#
# usage: run_clustering.sh <name> <port>   (name: naive | picksplit)
set -e
export LANG=C LC_ALL=C

NAME="$1"; PORT="$2"
PSQL="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A -F| -P pager=off"

N=1000000        # точек в наборе
SIGMA=0.002      # диаметр кластера
PROBES=500       # точечных запросов на замер

$PSQL -c "CREATE EXTENSION IF NOT EXISTS pageinspect" > /dev/null 2>&1 || true

# Число обращений к страницам индекса на один точечный запрос.
$PSQL > /dev/null <<'EOF'
-- Сборка предшествует появлению статистики в разделяемой памяти, поэтому
-- pg_stat_force_next_flush() недоступна, а коллектор статистики отдаёт
-- значения с задержкой. Считаем детерминированно, по EXPLAIN BUFFERS.
CREATE OR REPLACE FUNCTION probe_index(tbl text, idx text, nprobes int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
  i int; x float8; y float8; d float8 := 0.0005;
  js json; total bigint := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  FOR i IN 1 .. nprobes LOOP
    x := random(); y := random();
    EXECUTE format(
      'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) '
      'SELECT count(*) FROM %I WHERE p <@ box(point(%s,%s), point(%s,%s))',
      tbl, x - d, y - d, x + d, y + d) INTO js;
    total := total
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint, 0)
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint, 0);
  END LOOP;
  RETURN round(total::numeric / nprobes, 2);
END $$;
EOF

printf 'вариант|c|точек в кластере|кластеров|время сборки, с|страниц индекса|обращений на запрос\n'

for C in 0 0.5 1 2 4 8; do
  if [ "$C" = "0" ]; then
    GEN="SELECT gen_uniform($N) AS p"
    PER=0; NC=0
  else
    PER=$(awk -v c="$C" 'BEGIN{printf "%d", c*124}')   # 124 — измеренная ёмкость листа
    NC=$(( N / PER ))
    GEN="SELECT gen_clustered($NC, $PER, $SIGMA) AS p"
  fi

  $PSQL > /dev/null <<EOF
DROP TABLE IF EXISTS t;
CREATE TABLE t AS $GEN;
EOF

  BUILD=$($PSQL -c "\timing off" -c "
    SELECT extract(epoch from clock_timestamp());" )
  T0=$($PSQL -c "SELECT extract(epoch from clock_timestamp())")
  $PSQL -c "CREATE INDEX ti ON t USING gist(p)" > /dev/null
  T1=$($PSQL -c "SELECT extract(epoch from clock_timestamp())")
  SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')

  $PSQL -c "VACUUM ANALYZE t" > /dev/null
  PAGES=$($PSQL -c "SELECT pg_relation_size('ti')/8192")
  HITS=$($PSQL -c "SELECT probe_index('t','ti',$PROBES)")

  printf '%s|%s|%s|%s|%s|%s|%s\n' "$NAME" "$C" "$PER" "$NC" "$SECS" "$PAGES" "$HITS"
done
