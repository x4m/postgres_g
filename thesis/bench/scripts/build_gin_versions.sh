#!/bin/bash
# Четыре сборки для измерения блокировок при сборке мусора в обратном индексе.
#
# Две пары, каждая изолирует одно изменение:
#   gin-a0 / gin-a1  —  218f51584d5^ / 218f51584d5  (2017-03-23, выпуск 10):
#       обе оптимизации сразу — отложенный захват блокировки и блокирование
#       поддерева вместо всего дерева вхождений;
#   gin-b0 / gin-b1  —  fd83c83d094^ / fd83c83d094  (2018-12-13):
#       отмена второй оптимизации; уцелевает только отложенный захват.
#
# Пары нельзя сравнивать между собой: между 2017 и 2018 годами в дереве много
# постороннего. Сравнивать можно только внутри пары.
#
# Код 2017 года целиком современной glibc не собирается: в pg_rewind есть своя
# функция copy_file_range, а с glibc 2.27 (2018) такая функция появилась в
# libc. Переименовать её через -D нельзя — переименовывается и объявление в
# libc, конфликт остаётся. Поэтому pg_rewind просто не собирается: нужны только
# сервер, initdb, pg_ctl и psql. plpgsql тоже нужен: без него initdb падает на
# post-bootstrap, где выполняется CREATE EXTENSION plpgsql.
#
# Скрипт держит замок: параллельные запуски портят друг другу ~/pgsrc.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight

LOCK=/tmp/build-gin.lock
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "сборка уже идёт ($LOCK); выход"; exit 1
fi
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT


cd "$HOME/pgsrc"
for spec in "gin-a0:218f51584d5^" "gin-a1:218f51584d5" \
            "gin-b0:fd83c83d094^" "gin-b1:fd83c83d094"; do
  name=${spec%%:*}; commit=${spec##*:}
  if [ -x "$HOME/bench-$name/bin/postgres" ]; then echo "$name: уже собран"; continue; fi
  git checkout -qf "$commit"
  git clean -qfdx
  if ! ./configure 9>&- --prefix="$HOME/bench-$name" --without-readline --without-zlib \
        CFLAGS="-O2 -Wno-error" > "/tmp/conf-$name.log" 2>&1; then
    echo "$name: configure не прошёл"; tail -5 "/tmp/conf-$name.log"; continue
  fi
  ok=1
  for d in src/port src/common src/timezone src/backend \
           src/backend/utils/mb/conversion_procs src/backend/snowball \
           src/pl/plpgsql src/interfaces/libpq \
           src/bin/initdb src/bin/pg_ctl src/bin/psql; do
    [ -d "$d" ] || continue
    if ! make -s -j"$(nproc)" 9>&- -C "$d" > "/tmp/make-$name-$(basename $d).log" 2>&1; then
      echo "$name: не собралось в $d"; grep -iE "error" "/tmp/make-$name-$(basename $d).log" | head -5
      ok=0; break
    fi
    make -s -C "$d" install > /dev/null 2>&1 || true
  done
  [ "$ok" = 1 ] && echo "$name: собран" || echo "$name: пропущен"
done
echo DONE

bench_postflight
