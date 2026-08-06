#!/bin/bash
# Полнота проверки инвариантов: матрица «нарушенный инвариант → сработавшая
# проверка».
#
# ЗАЧЕМ. Средство контроля целостности строится по условию «ложных срабатываний
# не бывает» и потому расходует весь запас ошибок на молчание: где проверка не
# уверена, она обязана воздержаться. Воздержавшаяся проверка не даёт никакого
# сигнала, и пробел в покрытии обнаружить нечем — функциональный тест,
# запускающий проверку на исправном индексе и ожидающий молчания, проходит
# независимо от того, выполнилась ли хоть одна проверка.
#
# Единственный способ измерить покрытие — нарушить каждый инвариант по
# отдельности и потребовать, чтобы срабатывала именно соответствующая проверка.
# Такая матрица поймала бы cdd1a431f21 (охранное условие сравнивало отсутствие
# правой ссылки с нулём, тогда как записывается она наибольшим номером блока) в
# день написания.
#
# Контрольные суммы отключены намеренно: иначе порча ловится слоем контрольных
# сумм, а не проверкой инвариантов. Дефект в самой СУБД оставил бы контрольную
# сумму согласованной, так что это верная постановка, а не поблажка.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight

N=${N:-200000}
PORT=5466
D=/mnt/nvme/data/amchk
PGBIN=/usr/lib/postgresql/18/bin
KINDS=${KINDS:-"order rightlink deleted maxoff"}

q() { "$PGBIN/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
stop() { "$PGBIN/pg_ctl" 9>&- -D "$D" -w stop >/dev/null 2>&1 || true; }
start() { "$PGBIN/pg_ctl" 9>&- -D "$D" -l "$D/pg.log" -w start >/dev/null; }


stop; rm -f /tmp/.s.PGSQL.$PORT*; rm -rf "$D"
"$PGBIN/initdb" 9>&- -D "$D" --locale=C --encoding=UTF8 --no-data-checksums >/dev/null 2>&1
{ echo "port = $PORT"; echo "shared_buffers = 256MB"; echo "autovacuum = off"
  echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"; } >> "$D/postgresql.conf"
start
q -c "CREATE EXTENSION IF NOT EXISTS amcheck" >/dev/null
q -c "CREATE EXTENSION IF NOT EXISTS pageinspect" >/dev/null
q -c "SELECT setseed(0.5);
      CREATE TABLE gt AS
        SELECT ARRAY[42, (random()*20000)::int, (random()*20000)::int] AS a, i
        FROM generate_series(1,$N) i" >/dev/null
q -c "CREATE INDEX gti ON gt USING gin (a) WITH (fastupdate = off)" >/dev/null
q -c "VACUUM ANALYZE gt" >/dev/null
FILE=$("$PGBIN/psql" 9>&- -h /tmp -p $PORT -d postgres -X -q -t -A \
        -c "SELECT current_setting('data_directory')||'/'||pg_relation_filepath('gti')")
stop
cp "$FILE" "$FILE.orig"

echo "### опорный прогон на исправном индексе"
start
BASE=$(q -c "SELECT gin_index_check('gti')" 2>&1 || true)
echo "исправный индекс: ${BASE:-(без сообщений — ошибок не обнаружено)}"
stop

printf 'вид порчи|страница|что изменено|проверка сработала|сообщение\n'
for k in $KINDS; do
  cp "$FILE.orig" "$FILE"
  R=$(python3 ~/corrupt_index.py "$k" "$FILE")
  BLK=${R%%|*}; WHAT=${R##*|}
  if [ "$BLK" = "None" ]; then
    printf '%s|—|%s|не применимо|\n' "$k" "$WHAT"; continue
  fi
  start
  OUT=$(q -c "SELECT gin_index_check('gti')" 2>&1 || true)
  stop
  if echo "$OUT" | grep -qi "error"; then FIRED=да; else FIRED="НЕТ"; fi
  MSG=$(echo "$OUT" | tr '\n' ' ' | sed 's/  */ /g' | cut -c1-110)
  printf '%s|%s|%s|%s|%s\n' "$k" "$BLK" "$WHAT" "$FIRED" "$MSG"
done

cp "$FILE.orig" "$FILE"
bench_postflight
