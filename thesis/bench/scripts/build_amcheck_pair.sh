#!/bin/bash
# Пара версий вокруг исправления утечки памяти в проверке обратного индекса:
# 1f8ab91c11e «amcheck: Fix memory leak with gin_index_check()» (М. Пакье,
# 2026-07-06) и её родитель.
#
# Зачем. Измерение на системной сборке 18.4 показало линейный рост пиковой
# памяти проверки с размером индекса (25 -> 75 МБ при индексе 9 -> 84 МБ), и
# это похоже на описанную в коммите утечку: prev_tuple перезаписывался
# результатом CopyIndexTuple() на каждом витке цикла. Но приписывать рост
# утечке без сравнения нельзя — он мог бы быть и собственным состоянием
# проверки. Пара версий решает вопрос.
set -e
export LANG=C LC_ALL=C
. "$HOME/benchlock.sh"

bench_lock
cd "$HOME/pgsrc"
git fetch -q origin 2>/dev/null || true
for spec in "leak-before:1f8ab91c11e^" "leak-after:1f8ab91c11e"; do
  name=${spec%%:*}; commit=${spec##*:}
  if [ -x "$HOME/bench-$name/bin/postgres" ]; then echo "$name: уже собран"; continue; fi
  if ! git rev-parse -q --verify "${commit%^}" >/dev/null; then
    echo "$name: коммита ${commit%^} нет в ~/pgsrc"; exit 1
  fi
  git checkout -qf "$commit"
  git clean -qfdx
  if ! ./configure 9>&- --prefix="$HOME/bench-$name" --without-readline --without-zlib \
        --without-icu CFLAGS="-O2 -Wno-error" > "/tmp/conf-$name.log" 2>&1; then
    echo "$name: configure не прошёл"; tail -5 "/tmp/conf-$name.log"; exit 1
  fi
  if ! make -s -j"$(nproc)" 9>&- > "/tmp/make-$name.log" 2>&1; then
    echo "$name: сборка не прошла"; grep -iE "error" "/tmp/make-$name.log" | head -5; exit 1
  fi
  make -s install > /dev/null 2>&1
  make -s -C contrib/amcheck install > /dev/null 2>&1
  echo "$name: собран"
done
echo DONE
