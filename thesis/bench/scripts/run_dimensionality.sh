#!/bin/bash
# Проверка предсказания модели о влиянии размерности (P7, формула eq:curse).
#
# Модель утверждает: при равномерных данных в единичном D-мерном кубе сторона
# ключа узла a_l ≈ (f^{l+1}/N)^{1/D}, откуда перекрытие ω_l ≈ |U_l| a_l^D.
# Показатель 1/D означает, что при больших D сторона ключа близка к единице уже
# на средних уровнях, и число посещаемых узлов растёт.
#
# ЧТО МЕРЯЕТСЯ. Точечный запрос по данным-точкам: `c @> <точка>`. Ответ почти
# всегда пуст, но обход посещает ровно те узлы, чей ключ содержит точку, — то
# есть Σ ω_l. Это прямое измерение величины из модели, а не её следствия.
# Первичная величина — число обращений к страницам индекса (попадания плюс
# промахи), она детерминирована и от машины не зависит.
#
# КЛАСС ОПЕРАТОРОВ. cube — единственный в поставке, работающий с данными
# переменной размерности. Сортированное построение для него недоступно (нет
# функции поддержки сортировки), поэтому дерево строится вставкой; это
# ограничение, а не выбор.
#
# ЛОВУШКА, НА КОТОРУЮ Я УЖЕ ПОПАЛСЯ. Запись
#     SELECT cube(ARRAY(SELECT random() FROM generate_series(1,D))) FROM generate_series(1,N)
# порождает N одинаковых точек: вложенный запрос не связан с внешней строкой,
# и планировщик вычисляет его один раз. Индекс при этом строится, растёт по D
# как положено, ветвистость выглядит правдоподобно — и всё измерение
# бессмысленно. Ниже значения порождаются группировкой по номеру строки, а
# результат проверяется явно: число различных точек обязано быть близко к N.
#
# СОПУТСТВУЮЩИЙ ЭФФЕКТ, КОТОРЫЙ НАДО РАЗДЕЛИТЬ. Ключ cube размерности D занимает
# 16D байт, поэтому ветвистость f падает как 1/D. Число обращений вырастет и по
# этой причине тоже. Поэтому f измеряется отдельно, по распределению страниц
# индекса на уровни, и в разбор идут обе величины.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight

N=${N:-500000}
PROBES=${PROBES:-2000}
DIMS=${DIMS:-"2 3 4 6 8 12 16 24 32"}
PORT=5465
D=/mnt/nvme/data/dim
PGBIN=/usr/lib/postgresql/18/bin

q() { "$PGBIN/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }


if [ ! -d "$D" ]; then
  "$PGBIN/initdb" 9>&- -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = $PORT"; echo "shared_buffers = 2GB"; echo "maintenance_work_mem = 1GB"
    echo "max_wal_size = 8GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
  } >> "$D/postgresql.conf"
fi
"$PGBIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
"$PGBIN/pg_ctl" 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null
q -c "CREATE EXTENSION IF NOT EXISTS cube" >/dev/null
q -c "CREATE EXTENSION IF NOT EXISTS pageinspect" >/dev/null

printf 'D|строк|различных|страниц индекса|высота|ветвистость|обращений на запрос\n'
for d in $DIMS; do
  q -c "DROP TABLE IF EXISTS ct; DROP TABLE IF EXISTS cp" >/dev/null
  # данные: точки в единичном D-мерном кубе, зерно фиксировано
  q -c "SELECT setseed(0.5);
        CREATE TABLE ct AS
          SELECT cube(array_agg(v ORDER BY j)) AS c
          FROM (SELECT i, j, random() AS v
                FROM generate_series(1,$N) i, generate_series(1,$d) j) s
          GROUP BY i" >/dev/null
  NDIST=$(q -c "SELECT count(DISTINCT c) FROM ct")
  if [ "$NDIST" -lt $((N * 9 / 10)) ]; then
    echo "D=$d: различных точек $NDIST из $N — данные вырождены, замер недействителен" >&2
    exit 1
  fi
  q -c "CREATE INDEX cti ON ct USING gist (c)" >/dev/null
  q -c "VACUUM ANALYZE ct" >/dev/null
  # зонды: тот же приём, что в P1 — фиксированное множество в таблице, а не
  # случайные точки на лету, иначе счётчик зависит от выбора зондов
  q -c "SELECT setseed(0.25);
        CREATE TABLE cp AS
          SELECT cube(array_agg(v ORDER BY j)) AS p
          FROM (SELECT i, j, random() AS v
                FROM generate_series(1,$PROBES) i, generate_series(1,$d) j) s
          GROUP BY i" >/dev/null
  PDIST=$(q -c "SELECT count(DISTINCT p) FROM cp")
  if [ "$PDIST" -lt $((PROBES * 9 / 10)) ]; then
    echo "D=$d: различных зондов $PDIST из $PROBES — замер недействителен" >&2
    exit 1
  fi

  # прогрев, чтобы считались попадания, а не чтения
  q -c "SELECT count(*) FROM ct WHERE c @> (SELECT p FROM cp LIMIT 1)" >/dev/null
  B0=$(q -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0)
             FROM pg_statio_user_indexes WHERE indexrelname='cti'")
  q -c "SET enable_seqscan=off;
        SELECT sum(n) FROM (SELECT (SELECT count(*) FROM ct WHERE c @> cp.p) AS n FROM cp) s" >/dev/null
  sleep 1
  B1=$(q -c "SELECT coalesce(idx_blks_read,0)+coalesce(idx_blks_hit,0)
             FROM pg_statio_user_indexes WHERE indexrelname='cti'")

  PAGES=$(q -c "SELECT pg_relation_size('cti')/8192")
  # высота и ветвистость: по распределению страниц на уровни
  LEV=$(q -c "SELECT count(*) FILTER (WHERE NOT leaf) AS internal,
                     count(*) FILTER (WHERE leaf) AS leaves
              FROM (SELECT (gist_page_opaque_info(get_raw_page('cti', i))).flags @> ARRAY['leaf'] AS leaf
                    FROM generate_series(1, pg_relation_size('cti')/8192 - 1) i) t" | tr '|' ' ')
  INTERNAL=$(echo $LEV | cut -d' ' -f1); LEAVES=$(echo $LEV | cut -d' ' -f2)
  HEIGHT=$(q -c "SELECT ceil(ln(greatest($LEAVES,2))/ln(greatest(($LEAVES::numeric/greatest($INTERNAL,1)),1.0001)))+1")
  FANOUT=$(q -c "SELECT round($LEAVES::numeric/greatest($INTERNAL,1),1)")
  printf '%s|%s|%s|%s|%s|%s|%s\n' "$d" "$N" "$NDIST" "$PAGES" "$HEIGHT" "$FANOUT" \
    "$(q -c "SELECT round((($B1-$B0)::numeric)/$PROBES,2)")"
done

"$PGBIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true
bench_postflight
