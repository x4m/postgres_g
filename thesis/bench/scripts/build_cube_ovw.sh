#!/bin/bash
# Доустановка contrib/cube в сборки ovw-before/ovw-after: нужен крупный ключ,
# чтобы проверить перезапись элемента на месте. Каждая версия требует своего
# configure — Makefile.global хранит prefix от последнего запуска, и без этого
# install уходит в чужой каталог (уже обжигались на plpgsql).
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

# Замок берётся ДО любой работы: initdb, создание данных и сборка — это уже
# нагрузка на машину, и делать их вне замка значит мешать другому агенту.
bench_lock
bench_preflight
cd "$HOME/pgsrc"
for spec in "ovw-before:b1328d78f88^" "ovw-after:b1328d78f88"; do
  name=${spec%%:*}; commit=${spec##*:}
  [ -f "$HOME/bench-$name/share/postgresql/extension/cube.control" ] && { echo "$name: cube уже есть"; continue; }
  git checkout -qf "$commit"; git clean -qfdx
  ./configure 9>&- --prefix="$HOME/bench-$name" --without-readline --without-zlib \
      CFLAGS="-O2 -Wno-error" > "/tmp/conf-cube-$name.log" 2>&1
  for d in src/port src/common src/timezone src/backend src/interfaces/libpq contrib/cube; do
    make -s -j"$(nproc)" 9>&- -C "$d" > "/tmp/make-cube-$name.log" 2>&1 || { echo "$name: не собралось в $d"; break; }
  done
  make -s -C contrib/cube install >/dev/null 2>&1 && echo "$name: cube установлен" || echo "$name: cube не установился"
done
echo DONE
