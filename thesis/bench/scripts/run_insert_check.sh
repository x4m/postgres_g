#!/bin/bash
# Перепроверка вставки и точечных запросов повторами с чередованием.
#
# Первый прогон был однократным по каждой конфигурации. На построении индекса
# это дало ложные 28 % платы: базовая сборка гуляет на 27 % между прогонами.
# Здесь каждая величина меряется трижды, конфигурации чередуются.
set -e
export LANG=C LC_ALL=C

q() { local n="$1" p="$2"; shift 2; "$HOME/bench-$n/bin/psql" -h /tmp -p "$p" -d postgres -X -q -t -A "$@"; }
now() { q "$1" "$2" -c "select extract(epoch from clock_timestamp())"; }
el() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }

for v in ip-base:5447 ip-skip:5448; do
  n=${v%%:*}; p=${v##*:}
  q "$n" "$p" -c "DROP TABLE IF EXISTS ins" >/dev/null 2>&1 || true
  q "$n" "$p" -c "SELECT setseed(0.77);
      CREATE TABLE ins AS SELECT point(random(), random()) AS p FROM generate_series(1,500000)" >/dev/null
  q "$n" "$p" -c "VACUUM ANALYZE ins" >/dev/null
done

printf 'прогон|сборка|вставка 500k, с|точечные 2000, с\n'
for i in 1 2 3; do
  for v in ip-base:5447 ip-skip:5448; do
    n=${v%%:*}; p=${v##*:}
    # свежая таблица и индекс на каждый прогон: вставка меняет дерево
    q "$n" "$p" -c "DROP TABLE IF EXISTS bt2" >/dev/null 2>&1 || true
    q "$n" "$p" -c "CREATE TABLE bt2 AS SELECT p FROM bt" >/dev/null
    q "$n" "$p" -c "CREATE INDEX bt2i ON bt2 USING gist(p)" >/dev/null
    q "$n" "$p" -c "VACUUM ANALYZE bt2" >/dev/null
    q "$n" "$p" -c "CHECKPOINT" >/dev/null

    T0=$(now "$n" "$p")
    q "$n" "$p" -c "INSERT INTO bt2 SELECT p FROM ins" >/dev/null
    T1=$(now "$n" "$p")
    INS=$(el "$T0" "$T1")

    POINT=$(q "$n" "$p" -c "
      SET enable_seqscan=off; SET enable_bitmapscan=off;
      SELECT round(extract(epoch from (
        SELECT clock_timestamp() - t0 FROM (
          SELECT clock_timestamp() AS t0, count(*) FROM (
            SELECT (SELECT count(*) FROM bt2 WHERE p <@ box(
                      point(q.x-0.00022, q.y-0.00022), point(q.x+0.00022, q.y+0.00022)))
              FROM bigprobes q LIMIT 2000) z) y))::numeric, 3)")
    printf '%s|%s|%s|%s\n' "$i" "$n" "$INS" "$POINT"
  done
done
