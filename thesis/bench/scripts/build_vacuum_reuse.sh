#!/bin/bash
# Третья сборка для измерения возврата страниц: 7df159a620b («Delete empty
# pages during GiST VACUUM», выпуск 12) и её родитель. Родитель уже содержит
# физический обход, так что пара изолирует именно возврат страниц.
#
# Взят не сам 7df159a620b, а d1b9ee4e440 — исправление к нему, вышедшее на
# следующий день: во втором проходе использовался указатель IndexVacuumInfo,
# уже недействительный к фазе очистки. На нашей нагрузке 7df159a620b падает по
# SIGSEGV на первой же сборке мусора, которой есть что удалять.
set -e
export LANG=C LC_ALL=C

cd "$HOME/pgsrc"
for spec in "reuse-before:7df159a620b^" "reuse-after:d1b9ee4e440"; do
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
