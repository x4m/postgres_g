#!/bin/bash
# Третья сборка для измерения возврата страниц: 7df159a620b («Delete pages
# during GiST VACUUM», выпуск 12) и её родитель. Родитель уже содержит
# физический обход, так что пара изолирует именно возврат страниц.
set -e
export LANG=C LC_ALL=C

cd "$HOME/pgsrc"
for spec in "reuse-before:7df159a620b^" "reuse-after:7df159a620b"; do
  name=${spec%%:*}
  commit=${spec##*:}
  if [ -x "$HOME/bench-$name/bin/postgres" ]; then
    echo "$name: уже собран"
    continue
  fi
  git checkout -qf "$commit"
  git clean -qfdx
  if ! ./configure --prefix="$HOME/bench-$name" \
        --without-readline --without-zlib \
        CFLAGS="-O2 -Wno-error" > "/tmp/conf-$name.log" 2>&1; then
    echo "$name: configure не прошёл"; tail -5 "/tmp/conf-$name.log"; exit 1
  fi
  if ! make -s -j"$(nproc)" > "/tmp/make-$name.log" 2>&1; then
    echo "$name: сборка не прошла"; grep -iE "error" "/tmp/make-$name.log" | head -8; exit 1
  fi
  make -s install > /dev/null 2>&1
  echo "$name: собран"
done
echo DONE
