#!/bin/sh
# Чистый прогон по OSM: последовательно, с калиброванным окном, 5000 зондов.
#
# Калибровка окна (среднее число строк в ответе, зонды у точек данных):
#   d = 0,00002 -> 1,2      d = 0,0001  -> 7,0
#   d = 0,00005 -> 2,7      d = 0,0005  -> 112,1   <- так мерить нельзя
# Берём 0,00005 как аналог точечного запроса и 0,0005 как заведомо
# неселективную нагрузку — чтобы показать, где различие между способами
# построения пропадает.
#
# «Покрытие всех точек» реализовано не сеткой по ограничивающему
# прямоугольнику, а зондами из самих данных: сетка по стране неизбежно
# неселективна (ячейка содержит десятки тысяч узлов), а выборка из данных
# воспроизводит их распределение.
#
# usage: run_osm_clean.sh <name> <port>
set -e
export LANG=C LC_ALL=C

NAME="$1"; PORT="$2"
PSQL="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A"
NPROBE=5000

$PSQL -c "CREATE EXTENSION IF NOT EXISTS gist_hilbert" >/dev/null 2>&1 || true
$PSQL -f "$HOME/probe_osm.sql" >/dev/null 2>&1

for OC in point_ops point_hilbert_ops; do
  $PSQL -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
  $PSQL -c "CHECKPOINT" >/dev/null
  T0=$($PSQL -c "select extract(epoch from clock_timestamp())")
  $PSQL -c "CREATE INDEX ti ON t USING gist(p $OC)" >/dev/null
  T1=$($PSQL -c "select extract(epoch from clock_timestamp())")
  SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')
  PAGES=$($PSQL -c "select pg_relation_size('ti')/8192")
  SEL=$($PSQL -c "select probe_at_data('t','ti',$NPROBE,0.00005)")
  UNSEL=$($PSQL -c "select probe_at_data('t','ti',500,0.0005)")
  printf '%s|%s|%s|%s|%s|%s\n' "$NAME" "$OC" "$SECS" "$PAGES" "$SEL" "$UNSEL"
done
