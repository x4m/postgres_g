#!/bin/bash
# Порядок обхода при сборке мусора в зависимости от упреждающего чтения
# операционной системы.
#
# Зачем. Первое измерение (run_vacuum_order.sh) показало ровно двукратное
# сокращение числа читаемых страниц и никакого выигрыша во времени. Объяснение
# «доминирует просмотр таблицы» оказалось неверным: при 31 ГБ памяти страничный
# кэш операционной системы держал весь индекс (710 МБ), и «чтения» были
# копированием из памяти. Здесь кэш сбрасывается перед каждым замером, а
# таблица прогревается, чтобы холодными оставались только страницы индекса.
#
# Что меряется. Время сборки мусора при логическом и физическом обходе для
# нескольких значений read_ahead_kb. Величина упреждающего чтения задаёт
# эффективное отношение стоимостей произвольного и последовательного чтения
# c_r/c_s, а от него по модели и зависит выигрыш физического порядка. Это
# позволяет на одном носителе воспроизвести разброс, о котором сообщал
# Дж. Джейнс в 2019 году: 50-кратный выигрыш на жёстком диске, 30-кратный на
# дешёвом SSD и 3-кратный на сетевом томе.
set -e
export LANG=C LC_ALL=C

N=${N:-5000000}
DELFRAC=${DELFRAC:-5}
SHB=${SHB:-128MB}
REP=${REP:-3}
RAS=${RAS:-"0 128 4096"}
DEV=${DEV:-vdb}
PORT=5463
D=/mnt/nvme/data/rasweep

q() { local n="$1"; shift; "$HOME/bench-$n/bin/psql" -h /tmp -p $PORT -d postgres -X -q -t -A "$@"; }
stop()  { "$HOME/bench-$1/bin/pg_ctl" -D "$D" -w stop >/dev/null 2>&1 || true; }
start() { "$HOME/bench-$1/bin/pg_ctl" -D "$D" -l "$D/pg.log" -w start >/dev/null; }
setra() { sudo -n sh -c "echo $1 > /sys/block/$DEV/queue/read_ahead_kb"; }

trap 'setra 128' EXIT   # вернуть исходное значение при любом выходе

if [ ! -d "$D" ]; then
  "$HOME/bench-vac-before/bin/initdb" -D "$D" --locale=C --encoding=UTF8 >/dev/null 2>&1
  {
    echo "port = $PORT"; echo "shared_buffers = $SHB"; echo "maintenance_work_mem = 512MB"
    echo "max_wal_size = 16GB"; echo "autovacuum = off"
    echo "listen_addresses = ''"; echo "unix_socket_directories = '/tmp'"
  } >> "$D/postgresql.conf"
fi

stop vac-before; start vac-before
if [ "$(q vac-before -c "select count(*) from pg_class where relname='rt0'")" = "0" ]; then
  q vac-before -c "SELECT setseed(0.5);
      CREATE TABLE rt0 AS SELECT point(random(), random()) AS p, i FROM generate_series(1,$N) i" >/dev/null
  q vac-before -c "VACUUM ANALYZE rt0" >/dev/null
fi

printf 'read_ahead_kb|прогон|обход|сборка мусора, с|прочитано страниц индекса\n'
for ra in $RAS; do
  for i in $(seq 1 $REP); do
    for v in vac-before vac-after; do
      stop "$v"; setra 128; start "$v"       # подготовка — при обычном упреждении
      q "$v" -c "DROP TABLE IF EXISTS rt" >/dev/null 2>&1 || true
      q "$v" -c "CREATE TABLE rt AS SELECT * FROM rt0" >/dev/null
      q "$v" -c "VACUUM ANALYZE rt" >/dev/null
      q "$v" -c "CREATE INDEX rti ON rt USING gist(p)" >/dev/null
      q "$v" -c "DELETE FROM rt WHERE i % $DELFRAC = 0" >/dev/null
      q "$v" -c "CHECKPOINT" >/dev/null
      stop "$v"
      sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      setra "$ra"                            # замер — при заданном упреждении
      start "$v"
      q "$v" -c "SELECT count(*) FROM rt" >/dev/null   # прогрев таблицы
      B0=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='rti'")
      T0=$(q "$v" -c "select extract(epoch from clock_timestamp())")
      q "$v" -c "VACUUM rt" >/dev/null
      T1=$(q "$v" -c "select extract(epoch from clock_timestamp())")
      sleep 1
      B1=$(q "$v" -c "select coalesce(sum(idx_blks_read),0) from pg_statio_user_indexes where indexrelname='rti'")
      printf '%s|%s|%s|%s|%s\n' "$ra" "$i" \
        "$([ "$v" = vac-before ] && echo логический || echo физический)" \
        "$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.1f", b-a}')" "$((B1 - B0))"
    done
  done
done
