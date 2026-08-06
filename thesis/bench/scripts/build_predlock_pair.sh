#!/bin/bash
# Пара версий вокруг предикатных блокировок для обобщённого дерева поиска:
# 3ad55863e93 «Add predicate locking for GiST» (выпуск 11) и её родитель.
# Нужна для P5: до изменения предикатная блокировка ставится на отношение
# целиком, после — на страницу.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight
cd "$HOME/pgsrc"
for spec in "pl-before:3ad55863e93^" "pl-after:3ad55863e93"; do
  name=${spec%%:*}; commit=${spec##*:}
  if [ -x "$HOME/bench-$name/bin/postgres" ]; then echo "$name: уже собран"; continue; fi
  git checkout -qf "$commit"; git clean -qfdx
  if ! ./configure 9>&- --prefix="$HOME/bench-$name" --without-readline --without-zlib \
        CFLAGS="-O2 -Wno-error" > "/tmp/conf-$name.log" 2>&1; then
    echo "$name: configure не прошёл"; tail -4 "/tmp/conf-$name.log"; continue; fi
  ok=1
  for d in src/port src/common src/timezone src/backend \
           src/backend/utils/mb/conversion_procs src/backend/snowball \
           src/pl/plpgsql src/interfaces/libpq \
           src/bin/initdb src/bin/pg_ctl src/bin/psql; do
    [ -d "$d" ] || continue
    if ! make -s -j"$(nproc)" 9>&- -C "$d" > "/tmp/make-$name.log" 2>&1; then
      echo "$name: не собралось в $d"; grep -iE "error" "/tmp/make-$name.log" | head -3; ok=0; break; fi
    make -s -C "$d" install >/dev/null 2>&1 || true
  done
  [ "$ok" = 1 ] && echo "$name: собран" || echo "$name: пропущен"
done
echo DONE
