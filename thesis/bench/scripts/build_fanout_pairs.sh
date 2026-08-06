#!/bin/bash
# Пары версий для P4:
#   opt-before / opt-after   — d3a4f89d8a3^ / d3a4f89d8a3 (необязательные
#                              функции преобразования ключа, выпуск 11)
#   ovw-before / ovw-after   — b1328d78f88^ / b1328d78f88 (перезапись элемента
#                              на месте, выпуск 10)
#
# Код 2017-2018 годов целиком современной glibc не собирается (pg_rewind
# определяет свою copy_file_range, а с glibc 2.27 такая есть в libc), поэтому
# собираются только нужные подкаталоги: сервер, initdb, pg_ctl, psql и plpgsql,
# без которого initdb падает на post-bootstrap.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight

cd "$HOME/pgsrc"
for spec in "opt-before:d3a4f89d8a3^" "opt-after:d3a4f89d8a3" \
            "ovw-before:b1328d78f88^" "ovw-after:b1328d78f88"; do
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
    if ! make -s -j"$(nproc)" 9>&- -C "$d" > "/tmp/make-$name.log" 2>&1; then
      echo "$name: не собралось в $d"; grep -iE "error" "/tmp/make-$name.log" | head -4; ok=0; break
    fi
    make -s -C "$d" install > /dev/null 2>&1 || true
  done
  [ "$ok" = 1 ] && echo "$name: собран" || echo "$name: пропущен"
done
echo DONE
