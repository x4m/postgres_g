#!/bin/sh
# Профиль процессорного времени спуска по GiST.
#
# Нагрузка — пространственное соединение: для каждой строки внешнего набора
# ищутся точки в окрестности. Один SQL-запрос выполняет сотни тысяч спусков по
# дереву, поэтому разбор, планирование и работа с каталогом амортизируются и в
# профиле остаётся собственно индексный код. Предыдущая попытка мерила
# одиночные запросы через pgbench, и до 8 % времени уходило в парсер.
#
# Индекс и таблица целиком в буферном кэше: измеряется процессор, не диск.
#
# usage: profile_join.sh <name> <port> <N> <probes>
set -e
export LANG=C LC_ALL=C

NAME="$1"; PORT="$2"; N="${3:-10000000}"; NP="${4:-200000}"
PSQL="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A"

$PSQL -c "DROP TABLE IF EXISTS pt, probes" >/dev/null 2>&1 || true
$PSQL -c "CREATE TABLE pt AS SELECT gen_uniform($N) AS p" >/dev/null
$PSQL -c "CREATE INDEX pti ON pt USING gist(p)" >/dev/null
$PSQL -c "CREATE TABLE probes AS SELECT random() AS x, random() AS y FROM generate_series(1,$NP)" >/dev/null
$PSQL -c "VACUUM ANALYZE pt, probes" >/dev/null
$PSQL -c "SELECT count(*) FROM pt WHERE p <@ box(point(0,0),point(1,1))" >/dev/null

cat > /tmp/joinload.sql <<'EOF'
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;
SELECT count(*)
  FROM probes q
  JOIN pt ON pt.p <@ box(point(q.x - 0.0005, q.y - 0.0005),
                         point(q.x + 0.0005, q.y + 0.0005));
EOF

"$HOME/bench-$NAME/bin/psql" -h /tmp -p "$PORT" -d postgres -X -q -t -A \
    -c "SET application_name='profiled'" -f /tmp/joinload.sql > /tmp/join-$NAME.out 2>&1 &
JOB=$!
sleep 1
BE=$($PSQL -c "SELECT pid FROM pg_stat_activity WHERE query LIKE '%JOIN pt%' AND pid <> pg_backend_pid() LIMIT 1")
[ -n "$BE" ] || { echo "не нашёл серверный процесс" >&2; kill $JOB 2>/dev/null; exit 1; }

sudo perf record -q -F 999 -p "$BE" -o /tmp/perfj-$NAME.data -- sleep 20 2>/dev/null || true
wait $JOB 2>/dev/null || true

echo "=== результат запроса:"; cat /tmp/join-$NAME.out
echo "=== профиль, самостоятельное время:"
sudo perf report -i /tmp/perfj-$NAME.data --no-children --percent-limit 0.8 --stdio 2>/dev/null \
  | grep -E "^ +[0-9]+\.[0-9]+%" | head -25
