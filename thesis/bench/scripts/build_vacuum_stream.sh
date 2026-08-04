#!/bin/bash
# Пара версий вокруг упреждающего чтения при сборке мусора: 69273b818b1
# («Use streaming read I/O in GiST vacuuming», выпуск 18) и её родитель.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

bench_lock
bench_preflight

cd "$HOME/pgsrc"
for spec in "stream-before:69273b818b1^" "stream-after:69273b818b1"; do
  name=${spec%%:*}; commit=${spec##*:}
  if [ -x "$HOME/bench-$name/bin/postgres" ]; then echo "$name: уже собран"; continue; fi
  git checkout -qf "$commit"
  git clean -qfdx
  if ! ./configure 9>&- --prefix="$HOME/bench-$name" --without-readline --without-zlib \
        --without-icu CFLAGS="-O2 -Wno-error" > "/tmp/conf-$name.log" 2>&1; then
    echo "$name: configure не прошёл"; tail -5 "/tmp/conf-$name.log"; exit 1
  fi
  if ! make -s -j"$(nproc)" 9>&- > "/tmp/make-$name.log" 2>&1; then
    echo "$name: сборка не прошла"; grep -iE "error" "/tmp/make-$name.log" | head -8; exit 1
  fi
  make -s install > /dev/null 2>&1
  echo "$name: собран"
done
echo DONE

bench_postflight
