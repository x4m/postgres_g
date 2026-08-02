#!/bin/sh
# Полный прогон по реальным данным OSM.
# usage: run_osm.sh <name> <port>
# Для каждого класса операторов: сборка, объём, зонды у существующих точек,
# покрытие сеткой.
set -e
export LANG=C LC_ALL=C

NAME="$1"; PORT="$2"
PSQL="$HOME/bench-$NAME/bin/psql -h /tmp -p $PORT -d postgres -X -q -t -A"

$PSQL -c "CREATE EXTENSION IF NOT EXISTS gist_hilbert" >/dev/null 2>&1 || true
$PSQL -c "CREATE EXTENSION IF NOT EXISTS tsm_system_rows" >/dev/null 2>&1 || true
$PSQL -f "$HOME/probe_osm.sql" >/dev/null 2>&1

# окно ~0.0005 градуса ≈ 50 м: точечный запрос по смыслу задачи
D=0.0005
GRID=32

for OC in point_ops point_hilbert_ops; do
  $PSQL -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
  T0=$($PSQL -c "select extract(epoch from clock_timestamp())")
  $PSQL -c "CREATE INDEX ti ON t USING gist(p $OC)" >/dev/null
  T1=$($PSQL -c "select extract(epoch from clock_timestamp())")
  SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')
  PAGES=$($PSQL -c "select pg_relation_size('ti')/8192")
  SIZE=$($PSQL -c "select pg_size_pretty(pg_relation_size('ti'))")
  AT=$($PSQL -c "select probe_at_data('t','ti',300,$D)")
  GR=$($PSQL -c "select blocks_avg from probe_grid('t','ti',$GRID)")
  printf '%s|%s|%s|%s|%s|%s|%s\n' "$NAME" "$OC" "$SECS" "$PAGES" "$SIZE" "$AT" "$GR"
done
