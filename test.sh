#!/bin/sh
../project/bin/psql postgres<gin_init.sql

../project/bin/pgbench -n -j 32  -c 32 -T 3600 -f gin_shot.sql
