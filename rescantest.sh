#!/usr/bin/env bash

set -e
pkill -9 postgres || true
make -j 16 && make install

DB=~/DemoDb
BINDIR=~/project/bin

rm -rf $DB
cp *.sql $BINDIR
cd $BINDIR
./initdb $DB
./pg_ctl -D $DB start
./psql postgres -c "create extension cube;"


./psql postgres -c "create table x as select cube(random()) c from generate_series(1,10000) y; create index on x using gist(c);"
./psql postgres -c "delete from x where (c~>1)>0.1;"
./pgbench -f insert.sql postgres -T 30 &
./pgbench -f vacuum.sql postgres -T 30

./pg_ctl -D $DB stop
