#!/usr/bin/env bash

set -e
pkill -9 postgres || true
#make -j 16 && make install

rm -rf ~/DemoDb
cd ~/project/bin/
./initdb ~/DemoDb
./pg_ctl -D ~/DemoDb start
./psql postgres -c "create extension cube;"


for i in $(seq 1 16); do 
size=$((100 * 2**$i))
time ./psql postgres -c "create table x(c cube); create index on x using gist(c); insert into x select cube(random()) c from generate_series(1,$size) y; "
./psql postgres -c "drop table x;"
done

#./pg_ctl -D ~/DemoDb stop