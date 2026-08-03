#!/bin/bash
# Перепроверка времени построения: базовая сборка против скип-групп.
#
# Первый прогон дал 6,57 против 8,41 с, то есть 28 % платы. Это подозрительно:
# скип-кортежи создаются только на внутренних страницах, а их 497 из 50 049.
# Формирование групп там — около 3 100 вызовов union против сортировки пяти
# миллионов точек, столько стоить не может.
#
# Здесь прогоны чередуются и повторяются, чтобы отделить эффект от разогрева и
# от состояния машины.
set -e
export LANG=C LC_ALL=C

q() { local n="$1" p="$2"; shift 2; "$HOME/bench-$n/bin/psql" -h /tmp -p "$p" -d postgres -X -q -t -A "$@"; }
now() { q "$1" "$2" -c "select extract(epoch from clock_timestamp())"; }
el() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }

# одинаковые данные в обоих кластерах
for v in ip-base:5447 ip-skip:5448; do
  n=${v%%:*}; p=${v##*:}
  q "$n" "$p" -c "DROP INDEX IF EXISTS ti" >/dev/null 2>&1 || true
  q "$n" "$p" -c "DROP TABLE IF EXISTS bt" >/dev/null 2>&1 || true
  q "$n" "$p" -c "SELECT setseed(0.42);
      CREATE TABLE bt AS SELECT point(random(), random()) AS p FROM generate_series(1,5000000)" >/dev/null
  q "$n" "$p" -c "VACUUM ANALYZE bt" >/dev/null
  q "$n" "$p" -c "CHECKPOINT" >/dev/null
done

printf 'прогон|сборка|построение, с|страниц\n'
for i in 1 2 3; do
  for v in ip-base:5447 ip-skip:5448; do
    n=${v%%:*}; p=${v##*:}
    q "$n" "$p" -c "DROP INDEX IF EXISTS bti" >/dev/null 2>&1 || true
    q "$n" "$p" -c "CHECKPOINT" >/dev/null
    T0=$(now "$n" "$p")
    q "$n" "$p" -c "CREATE INDEX bti ON bt USING gist(p)" >/dev/null
    T1=$(now "$n" "$p")
    PAGES=$(q "$n" "$p" -c "select pg_relation_size('bti')/8192")
    printf '%s|%s|%s|%s\n' "$i" "$n" "$(el "$T0" "$T1")" "$PAGES"
  done
done

# отдельно: сколько стоит сама сортировка, без записи индекса
printf '\nсортировка тех же данных без построения индекса, с\n'
for v in ip-base:5447 ip-skip:5448; do
  n=${v%%:*}; p=${v##*:}
  T0=$(now "$n" "$p")
  q "$n" "$p" -c "SET work_mem='1GB'; SELECT count(*) FROM (SELECT p FROM bt ORDER BY p <-> point(0,0)) s" >/dev/null
  T1=$(now "$n" "$p")
  printf '%s|%s\n' "$n" "$(el "$T0" "$T1")"
done
