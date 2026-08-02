#!/bin/sh
# Три способа построения на одном и том же наборе:
#   insert    — построение вставкой; получается снятием опорной функции 11
#               (поддержка сортировки) из семейства операторов, поэтому
#               сравнение идёт на одной и той же сборке, без различий версий;
#   naive     — сортированное построение, разбиение по заполнению страницы
#               (сборка f1ea98a7975^);
#   picksplit — сортированное построение, разбиение функцией класса операторов
#               (сборка f1ea98a7975).
#
# usage: run_three_way.sh <N> <repeats>
set -e
export LANG=C LC_ALL=C

N=${1:-1000000}
REP=${2:-3}
PROBES=300

psql_for() {  # psql_for <name> <port>
  echo "/home/x4mmm/bench-$1/bin/psql -h /tmp -p $2 -d postgres -X -q -t -A -P pager=off"
}

# Один замер: строит индекс, возвращает "время|страниц|обращений на запрос".
measure() {  # measure <psql-cmd> <label>
  P="$1"
  $P -c "DROP INDEX IF EXISTS ti" > /dev/null
  T0=$($P -c "SELECT extract(epoch from clock_timestamp())")
  $P -c "CREATE INDEX ti ON t USING gist(p)" > /dev/null
  T1=$($P -c "SELECT extract(epoch from clock_timestamp())")
  SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", b-a}')
  PAGES=$($P -c "SELECT pg_relation_size('ti')/8192")
  HITS=$($P -c "SELECT probe_index('t','ti',$PROBES,$N)")
  printf '%s|%s|%s' "$SECS" "$PAGES" "$HITS"
}

printf 'способ|прогон|время сборки, с|страниц индекса|обращений на запрос\n'

for v in naive:5442 picksplit:5443; do
  NAME=${v%%:*}; PORT=${v##*:}
  P=$(psql_for "$NAME" "$PORT")

  $P -c "DROP TABLE IF EXISTS t" > /dev/null 2>&1 || true
  $P -c "CREATE TABLE t AS SELECT gen_uniform($N) AS p" > /dev/null
  $P -c "VACUUM ANALYZE t" > /dev/null

  i=1
  while [ "$i" -le "$REP" ]; do
    printf '%s|%s|%s\n' "$NAME" "$i" "$(measure "$P")"
    i=$((i + 1))
  done

  # Построение вставкой измеряем один раз — оно не зависит от сборки.
  if [ "$NAME" = "picksplit" ]; then
    $P -c "ALTER OPERATOR FAMILY point_ops USING gist DROP FUNCTION 11 (point, point)" > /dev/null
    i=1
    while [ "$i" -le "$REP" ]; do
      printf 'insert|%s|%s\n' "$i" "$(measure "$P")"
      i=$((i + 1))
    done
    $P -c "ALTER OPERATOR FAMILY point_ops USING gist ADD FUNCTION 11 (point, point) gist_point_sortsupport(internal)" > /dev/null
  fi
done
