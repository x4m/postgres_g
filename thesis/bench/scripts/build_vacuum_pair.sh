#!/bin/bash
# Сборка пары версий вокруг перехода к физическому порядку обхода при сборке
# мусора: fe280694d0d («Scan GiST indexes in physical order during VACUUM»,
# выпуск 12) и её родителя.
#
# Код 2019 года современным компилятором собирается не всегда; при неудаче
# смотреть /tmp/make-*.log.
set -e
export LANG=C LC_ALL=C

cd "$HOME/pgsrc"
for spec in "vac-before:fe280694d0d^" "vac-after:fe280694d0d"; do
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
    echo "$name: configure не прошёл"
    tail -5 "/tmp/conf-$name.log"
    exit 1
  fi
  if ! make -s -j"$(nproc)" > "/tmp/make-$name.log" 2>&1; then
    echo "$name: сборка не прошла"
    grep -iE "error" "/tmp/make-$name.log" | head -8
    exit 1
  fi
  make -s install > /dev/null 2>&1
  echo "$name: собран"
done
echo DONE
