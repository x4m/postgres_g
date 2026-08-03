#!/bin/sh
# КОНТРОЛЬНЫЙ ОПЫТ: выигрыш picksplit — это качество разбиения или просто
# большее число страниц?
#
# Наблюдение из переписки декабря 2021 (А. Каленик, реальные данные PostGIS):
# индекс, построенный вставкой, отвечает быстрее сортированного потому, что у
# него вдвое больше листовых страниц, а значит меньше кортежей на странице.
# Вывод оттуда же: «с методом сортировки можно просто fillfactor делить на 2 и
# получать индекс, запросы с которым выполняются так же быстро».
#
# Во всех наших измерениях стоимость запроса монотонно падала с ростом числа
# страниц, и мы это не контролировали.
#
# Устройство опыта:
#  * данные и зонды порождаются с фиксированным зерном, поэтому оба кластера
#    видят один и тот же набор и один и тот же список запросов. Прошлый прогон
#    пересоздавал данные случайно и дал 6,10 против 7,92 на одной и той же
#    конфигурации — разброс больше заявленного в статье;
#  * на сборке с разбиением по заполнению снижаем fillfactor, пока число
#    страниц не сравняется с picksplit при fillfactor по умолчанию.
#
# Если при равном числе страниц стоимость запроса сравняется — причинное
# утверждение статьи неверно, и выигрыш объясняется ветвистостью, а не
# качеством разбиения.
set -e
export LANG=C LC_ALL=C

N=${N:-10000000}
SEED=${SEED:-0.42}
NPROBE=${NPROBE:-2000}

prepare() {  # prepare <name> <port>
  P="$HOME/bench-$1/bin/psql -h /tmp -p $2 -d postgres -X -q -t -A"
  $P -c "DROP TABLE IF EXISTS t, probes" > /dev/null 2>&1 || true
  # одно зерно на оба кластера: наборы совпадают побайтово
  $P -c "SELECT setseed($SEED);
         CREATE TABLE t AS
           SELECT point(random(), random()) AS p FROM generate_series(1, $N)" > /dev/null
  $P -c "SELECT setseed($SEED);
         CREATE TABLE probes AS
           SELECT random() AS x, random() AS y FROM generate_series(1, $NPROBE)" > /dev/null
  $P -c "VACUUM ANALYZE t, probes" > /dev/null
  # зонды берутся из таблицы, а не генерируются на лету
  $P > /dev/null <<'EOF'
CREATE OR REPLACE FUNCTION probe_fixed(tbl text, nrows float8)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
  r record; js json; total bigint := 0; n int := 0; d float8 := 0.5 / sqrt(nrows);
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  FOR r IN SELECT x, y FROM probes LOOP
    EXECUTE format(
      'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) '
      'SELECT count(*) FROM %I WHERE p <@ box(point(%s,%s), point(%s,%s))',
      tbl, r.x - d, r.y - d, r.x + d, r.y + d) INTO js;
    total := total
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint, 0)
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint, 0);
    n := n + 1;
  END LOOP;
  RETURN round(total::numeric / greatest(n, 1), 2);
END $$;
EOF
}

run() {  # run <name> <port> <fillfactor|default>
  NAME="$1"; PORT="$2"; FF="$3"
  P="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A"
  $P -c "DROP INDEX IF EXISTS ti" > /dev/null 2>&1 || true
  [ "$FF" = "default" ] && WITH="" || WITH=" WITH (fillfactor = $FF)"
  T0=$($P -c "select extract(epoch from clock_timestamp())")
  $P -c "CREATE INDEX ti ON t USING gist(p)$WITH" > /dev/null
  T1=$($P -c "select extract(epoch from clock_timestamp())")
  SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')
  PAGES=$($P -c "select pg_relation_size('ti')/8192")
  HITS=$($P -c "select probe_fixed('t', $N)")
  printf '%s|%s|%s|%s|%s\n' "$NAME" "$FF" "$SECS" "$PAGES" "$HITS"
}

prepare naive 5442
prepare picksplit 5443

printf 'разбиение|fillfactor|сборка, с|страниц|обращений на запрос\n'
run picksplit 5443 default
run naive 5442 default
for FF in 80 70 65 60 50; do
  run naive 5442 $FF
done
run picksplit 5443 65
