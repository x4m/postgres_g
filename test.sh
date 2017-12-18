#!/usr/bin/env bash

set -e
pkill -9 postgres || true
#make -j 16 && make install

rm -rf ~/DemoDb
cd ~/project/bin/
./initdb ~/DemoDb
./pg_ctl -D ~/DemoDb start
./psql postgres -c "create extension cube;"


for i in $(seq 1 12); do 
size=$((100 * 2**$i))
./psql postgres -c "create table x as select cube(random()) c from generate_series(1,$size) y; create index on x using gist(c);"
./psql postgres -c "delete from x;"
./psql postgres -c "VACUUM x;"
./psql postgres -c "VACUUM x;"
./psql postgres -c "drop table x;"
./psql postgres -c "create table x as select cube(random()) c from generate_series(1,$size) y; create index on x using gist(c);"
./psql postgres -c "delete from x where (c~>1)>0.1;"
./psql postgres -c "VACUUM x;"
./psql postgres -c "insert into x select cube(random()) c from generate_series(1,$size) y;"
./psql postgres -c "VACUUM x;"
./psql postgres -c "delete from x where (c~>1)>0.1;"
./psql postgres -c "select pg_size_pretty(pg_relation_size('x_c_idx'));"
./psql postgres -c "VACUUM FULL x;"
./psql postgres -c "select pg_size_pretty(pg_relation_size('x_c_idx'));"
./psql postgres -c "drop table x;"
done

./pg_ctl -D ~/DemoDb stop