#!/bin/sh
# Профиль процессорного времени при поиске в GiST.
#
# Вопрос: сколько из времени сканирования уходит на собственно предикат класса
# операторов, сколько — на его вызов через fmgr, сколько — на работу со
# страницами. Индекс держится в буферном кэше целиком, поэтому ввод-вывод из
# картины исключён и остаётся только процессорная составляющая.
#
# usage: profile_scan.sh <name> <port> <N> <seconds>
set -e
export LANG=C LC_ALL=C

NAME="$1"; PORT="$2"; N="${3:-10000000}"; SECS="${4:-20}"
PSQL="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A"

# Небольшой набор, помещающийся в кэш целиком.
$PSQL -c "DROP TABLE IF EXISTS pt" >/dev/null 2>&1 || true
$PSQL -c "CREATE TABLE pt AS SELECT gen_uniform($N) AS p" >/dev/null
$PSQL -c "CREATE INDEX pti ON pt USING gist(p)" >/dev/null
$PSQL -c "VACUUM ANALYZE pt" >/dev/null

# Прогрев: загнать индекс и таблицу в буферный кэш.
$PSQL -c "SELECT count(*) FROM pt WHERE p <@ box(point(0,0),point(1,1))" >/dev/null

cat > /tmp/scanload.sql <<'EOF'
-- Координаты подставляются константами, а не вызовом random() внутри запроса:
-- volatile-функция в предикате мешает планировщику оценить селективность, и
-- он уходит в последовательное сканирование — тогда профиль измеряет не то.
\set xi random(0, 999999)
\set yi random(0, 999999)
SELECT count(*) FROM pt
 WHERE p <@ box(point(:xi / 1000000.0 - 0.0005, :yi / 1000000.0 - 0.0005),
                point(:xi / 1000000.0 + 0.0005, :yi / 1000000.0 + 0.0005));
EOF

# Фоновая нагрузка точечными запросами.
PGOPTIONS="-c enable_seqscan=off -c enable_bitmapscan=off" \
    "$HOME/bench-$NAME/bin/pgbench" -h /tmp -p $PORT -d postgres -n -f /tmp/scanload.sql \
    -T $((SECS + 5)) -c 1 > /tmp/pgbench-$NAME.log 2>&1 &
PGB=$!
sleep 2

BE=$($PSQL -c "SELECT pid FROM pg_stat_activity WHERE application_name='pgbench' LIMIT 1")
[ -n "$BE" ] || { echo "не нашёл серверный процесс pgbench" >&2; exit 1; }

sudo perf record -q -g -p "$BE" -o /tmp/perf-$NAME.data -- sleep "$SECS" 2>/dev/null || true
wait $PGB 2>/dev/null || true

echo "=== tps:"
grep -E "^tps" /tmp/pgbench-$NAME.log || true
echo "=== профиль (самостоятельное время, доля):"
sudo perf report -i /tmp/perf-$NAME.data --no-children --percent-limit 1 --stdio 2>/dev/null \
  | grep -E "^ +[0-9]" | head -25
