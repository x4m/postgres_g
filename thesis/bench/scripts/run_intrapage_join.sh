#!/bin/bash
# Соединение по индексу: базовая сборка против скип-групп, на индексе,
# построенном сортировкой и построенном вставкой.
#
# Второе — проверка догадки: пропуск эффективен только когда элементы на
# странице упорядочены, то есть после сортированной сборки. Если выигрыш
# одинаков на обоих индексах, догадка неверна.
set -e
export LANG=C LC_ALL=C

q() { local n="$1" p="$2"; shift 2; "$HOME/bench-$n/bin/psql" -h /tmp -p "$p" -d postgres -X -q -t -A "$@"; }
now() { q "$1" "$2" -c "select extract(epoch from clock_timestamp())"; }
el() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }

JOIN_SQL="SET enable_seqscan=off; SET enable_bitmapscan=off; SET max_parallel_workers_per_gather=0;
SELECT count(*) FROM bigprobes q JOIN t ON t.p <@ box(point(q.x-0.0007,q.y-0.0007), point(q.x+0.0007,q.y+0.0007))"

for v in ip-base:5447 ip-skip:5448; do
  n=${v%%:*}; p=${v##*:}
  q "$n" "$p" -c "DROP TABLE IF EXISTS bigprobes" >/dev/null 2>&1 || true
  q "$n" "$p" -c "SELECT setseed(0.31);
      CREATE TABLE bigprobes AS SELECT random() AS x, random() AS y FROM generate_series(1,200000)" >/dev/null
  q "$n" "$p" -c "VACUUM ANALYZE bigprobes" >/dev/null
done

printf 'сборка|индекс|страниц|соединение 200k, с|повтор, с\n'
for v in ip-base:5447 ip-skip:5448; do
  n=${v%%:*}; p=${v##*:}
  for kind in sorted insert; do
    q "$n" "$p" -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true

    if [ "$kind" = insert ]; then
      # снять опорную функцию сортировки: индекс построится вставкой
      q "$n" "$p" -c "DELETE FROM pg_amproc WHERE amprocnum=11 AND amproc::regproc::text ~ 'gist_point'" >/dev/null
      "$HOME/bench-$n/bin/pg_ctl" -D "/mnt/nvme/data/$n" -w restart -l "/mnt/nvme/data/$n/pg.log" >/dev/null 2>&1
      sleep 2
    fi

    q "$n" "$p" -c "CREATE INDEX ti ON t USING gist(p)" >/dev/null
    PAGES=$(q "$n" "$p" -c "select pg_relation_size('ti')/8192")
    q "$n" "$p" -c "$JOIN_SQL" >/dev/null      # прогрев

    R=""
    for i in 1 2; do
      T0=$(now "$n" "$p")
      q "$n" "$p" -c "$JOIN_SQL" >/dev/null
      T1=$(now "$n" "$p")
      R="$R|$(el "$T0" "$T1")"
    done
    printf '%s|%s|%s%s\n' "$n" "$kind" "$PAGES" "$R"

    if [ "$kind" = insert ]; then
      q "$n" "$p" -c "insert into pg_amproc (oid, amprocfamily, amproclefttype, amprocrighttype, amprocnum, amproc)
         select 90003, f.oid, 'point'::regtype, 'point'::regtype, 11, 'gist_point_sortsupport'::regproc
         from pg_opfamily f join pg_am a on a.oid=f.opfmethod
         where f.opfname='point_ops' and a.amname='gist'
           and not exists (select 1 from pg_amproc ap where ap.amprocfamily=f.oid and ap.amprocnum=11)" >/dev/null
      "$HOME/bench-$n/bin/pg_ctl" -D "/mnt/nvme/data/$n" -w restart -l "/mnt/nvme/data/$n/pg.log" >/dev/null 2>&1
      sleep 2
    fi
  done
done
