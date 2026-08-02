#!/bin/sh
# Базовая точка «построение вставкой» на OSM.
#
# Опорная функция сортировки снимается из семейства операторов, после чего ядро
# выбирает обычный путь вставки. ALTER OPERATOR FAMILY ... DROP FUNCTION для
# системного объекта запрещён, поэтому строка удаляется из каталога напрямую;
# кластер одноразовый.
#
# Проверка того, что путь действительно сменился: плотность упаковки. При
# сортированном построении получается около 165 точек на страницу, при
# построении вставкой — около 110.
set -e
export LANG=C LC_ALL=C

NAME=${1:-naive}
PORT=${2:-5442}
PSQL="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A"
DATA=/mnt/nvme/data/$NAME

$PSQL -c "DELETE FROM pg_amproc WHERE amprocnum = 11 AND amproc::regproc::text ~ 'sortsupport'"
"$HOME/bench-$NAME/bin/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w restart > /dev/null
sleep 2

LEFT=$($PSQL -c "SELECT count(*) FROM pg_amproc ap JOIN pg_opfamily f ON f.oid = ap.amprocfamily
                  WHERE f.opfname LIKE 'point%' AND ap.amprocnum = 11")
[ "$LEFT" = "0" ] || { echo "опорная функция не снята: осталось $LEFT" >&2; exit 1; }

# контроль пути построения на маленькой таблице
$PSQL -c "DROP TABLE IF EXISTS chk" > /dev/null 2>&1 || true
$PSQL -c "CREATE TABLE chk AS SELECT point(random(),random()) AS p FROM generate_series(1,200000)" > /dev/null
$PSQL -c "CREATE INDEX chki ON chk USING gist(p)" > /dev/null
DENS=$($PSQL -c "SELECT round(200000.0 / (pg_relation_size('chki')/8192), 1)")
echo "плотность упаковки на контрольной таблице: $DENS точек на страницу" >&2
$PSQL -c "DROP TABLE chk" > /dev/null

$PSQL -c "DROP INDEX IF EXISTS ti" > /dev/null 2>&1 || true
T0=$($PSQL -c "select extract(epoch from clock_timestamp())")
$PSQL -c "CREATE INDEX ti ON t USING gist(p)" > /dev/null
T1=$($PSQL -c "select extract(epoch from clock_timestamp())")
SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')
PAGES=$($PSQL -c "select pg_relation_size('ti')/8192")

$PSQL -f "$HOME/probe_osm.sql" > /dev/null 2>&1
SEL=$($PSQL -c "select probe_at_data('t','ti',5000,0.00005)")
UNSEL=$($PSQL -c "select probe_at_data('t','ti',500,0.0005)")

printf 'insert|%s|%s|%s|%s|%s\n' "$SECS" "$PAGES" "$DENS" "$SEL" "$UNSEL"
