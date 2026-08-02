#!/bin/sh
# build_pg.sh <commit> <prefix-name>
# Собирает PostgreSQL из ~/pgsrc в ~/<prefix-name>.
# Полная очистка обязательна: инкрементальная пересборка после смены базового
# коммита оставляет устаревшие объектные файлы, симптом — падение initdb с
# "table access method handler 3 did not return a TableAmRoutine struct".
set -e

COMMIT="$1"
NAME="$2"
[ -n "$COMMIT" ] && [ -n "$NAME" ] || { echo "usage: build_pg.sh <commit> <name>" >&2; exit 1; }

export LANG=C LC_ALL=C
PREFIX="$HOME/$NAME"
SRC="$HOME/pgsrc"

cd "$SRC"
git checkout -q --detach "$COMMIT"
git clean -qfdx

./configure --prefix="$PREFIX" \
            --enable-debug \
            --without-icu --without-readline --without-zlib \
            CFLAGS="-O2 -fno-omit-frame-pointer" > "$HOME/configure-$NAME.log" 2>&1

make -s -j"$(nproc)" > "$HOME/make-$NAME.log" 2>&1
make -s install >> "$HOME/make-$NAME.log" 2>&1
make -s -C contrib/pageinspect install >> "$HOME/make-$NAME.log" 2>&1
make -s -C contrib/btree_gist install >> "$HOME/make-$NAME.log" 2>&1

"$PREFIX/bin/postgres" --version
echo "built $COMMIT -> $PREFIX"
