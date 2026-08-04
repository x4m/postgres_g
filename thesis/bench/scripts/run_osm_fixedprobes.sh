#!/bin/bash
# Три способа построения на OSM с ОДНИМ И ТЕМ ЖЕ набором зондов.
#
# Повод: probe_at_data выбирает зонды случайно при каждом вызове, и на данных
# со скошенной плотностью это даёт большой разброс. Одна и та же конфигурация
# (построение вставкой) дала 23,66 и 16,83 обращения в двух прогонах по 5000
# зондов. Вывод «вставка даёт худшее дерево», стоящий в аннотации P1, на таком
# измерении не держится.
#
# Здесь набор зондов формируется один раз, выгружается и загружается в оба
# кластера, и все конфигурации меряются на нём.
set -e
export LANG=C LC_ALL=C

NPROBE=${NPROBE:-20000}
D=0.00005

q() { local n="$1" p="$2"; shift 2; "$HOME/bench-$n/bin/psql" -h /tmp -p "$p" -d postgres -X -q -t -A "$@"; }

# --- общий набор зондов: берём из данных на одном кластере и переносим
q picksplit 5443 -c "DROP TABLE IF EXISTS fp" >/dev/null 2>&1 || true
q picksplit 5443 -c "SELECT setseed(0.19);
    CREATE TABLE fp AS SELECT (p)[0] AS x, (p)[1] AS y FROM t
     TABLESAMPLE SYSTEM (0.05) LIMIT $NPROBE" >/dev/null
q picksplit 5443 -c "COPY fp TO '/mnt/nvme/osm/probes.csv' WITH (FORMAT csv)" >/dev/null
q naive 5442 -c "DROP TABLE IF EXISTS fp" >/dev/null 2>&1 || true
q naive 5442 -c "CREATE TABLE fp(x float8, y float8)" >/dev/null
q naive 5442 -c "COPY fp FROM '/mnt/nvme/osm/probes.csv' WITH (FORMAT csv)" >/dev/null
for v in naive:5442 picksplit:5443; do
  n=${v%%:*}; p=${v##*:}
  q "$n" "$p" -c "VACUUM ANALYZE fp" >/dev/null
  q "$n" "$p" >/dev/null <<'EOF'
CREATE OR REPLACE FUNCTION probe_fixed_osm(d float8) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE r record; js json; total bigint := 0; n int := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  FOR r IN SELECT x, y FROM fp LOOP
    EXECUTE format(
      'EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) '
      'SELECT count(*) FROM t WHERE p <@ box(point(%s,%s), point(%s,%s))',
      r.x-d, r.y-d, r.x+d, r.y+d) INTO js;
    total := total
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Hit Blocks')::bigint, 0)
           + coalesce((js -> 0 -> 'Plan' ->> 'Shared Read Blocks')::bigint, 0);
    n := n + 1;
  END LOOP;
  RETURN round(total::numeric / greatest(n,1), 3);
END $$;
EOF
done

printf 'способ|страниц|обращений на запрос\n'

# picksplit уже построен на своём кластере
printf 'picksplit|%s|%s\n' \
  "$(q picksplit 5443 -c "select pg_relation_size('ti')/8192")" \
  "$(q picksplit 5443 -c "select probe_fixed_osm($D)")"

# naive: сейчас там индекс от вставки (опорная функция снята предыдущим прогоном)
HAS=$(q naive 5442 -c "select count(*) from pg_amproc ap join pg_opfamily f on f.oid=ap.amprocfamily where f.opfname='point%' and ap.amprocnum=11" 2>/dev/null || echo 0)
printf 'вставка|%s|%s\n' \
  "$(q naive 5442 -c "select pg_relation_size('ti')/8192")" \
  "$(q naive 5442 -c "select probe_fixed_osm($D)")"

# вернуть опорную функцию и перестроить по заполнению
q naive 5442 -c "insert into pg_amproc (oid, amprocfamily, amproclefttype, amprocrighttype, amprocnum, amproc)
   select 90006, f.oid, 'point'::regtype, 'point'::regtype, 11, 'gist_point_sortsupport'::regproc
   from pg_opfamily f join pg_am a on a.oid=f.opfmethod
   where f.opfname='point_ops' and a.amname='gist'
     and not exists (select 1 from pg_amproc ap where ap.amprocfamily=f.oid and ap.amprocnum=11)" >/dev/null
"$HOME/bench-naive/bin/pg_ctl" -D /mnt/nvme/data/naive -w restart -l /mnt/nvme/data/naive/pg.log >/dev/null 2>&1
sleep 3
q naive 5442 -c "DROP INDEX IF EXISTS ti" >/dev/null
q naive 5442 -c "CREATE INDEX ti ON t USING gist(p)" >/dev/null
printf 'по заполнению|%s|%s\n' \
  "$(q naive 5442 -c "select pg_relation_size('ti')/8192")" \
  "$(q naive 5442 -c "select probe_fixed_osm($D)")"
