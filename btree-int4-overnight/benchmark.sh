#!/usr/bin/env bash
set -eu

. "$HOME/benchlock.sh"
bench_lock
bench_preflight

root="$HOME/bench-btree-int4-overnight"
driver_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo="$root/repo.git"
master_commit=9825488c13de4d7b8cae3e7f3b1094441c78ab64
special_commit=ccf85bc65d88adf4c63fd3541a2673ebe34e6703
interp_commit=0e07ad2850d2c0b1ea65649a80e38ca937b72876
port=55447
active_datadir=

mkdir -p "$root"

cleanup()
{
	if [ -n "$active_datadir" ] && [ -s "$active_datadir/PG_VERSION" ]; then
		"$root/master-install/bin/pg_ctl" -D "$active_datadir" status >/dev/null 2>&1 &&
			"$root/master-install/bin/pg_ctl" -D "$active_datadir" stop -m fast -w >/dev/null 2>&1 || true
	fi
	bench_postflight
}
trap cleanup EXIT INT TERM

if [ ! -d "$repo" ]; then
	git clone --bare https://github.com/x4m/postgres_g.git "$repo" 9>&-
else
	git --git-dir="$repo" fetch origin 9>&-
fi

build_variant()
{
	variant=$1
	commit=$2
	src="$root/$variant-src"
	prefix="$root/$variant-install"

	if [ ! -d "$src" ]; then
		git --git-dir="$repo" worktree add --detach "$src" "$commit" 9>&-
	fi
	if [ ! -x "$prefix/bin/postgres" ]; then
		(cd "$src" &&
			./configure --prefix="$prefix" --with-lz4 --with-zstd \
				CFLAGS="-O2 -g -fno-omit-frame-pointer" 9>&- >/dev/null &&
			make -j16 -s 9>&- && make -j16 -s install 9>&-)
	fi
}

build_variant master "$master_commit"
build_variant special "$special_commit"
build_variant interp "$interp_commit"

start_variant()
{
	variant=$1
	prefix="$root/$variant-install"
	active_datadir="$root/$variant-data"
	"$prefix/bin/pg_ctl" -D "$active_datadir" -l "$root/$variant-server.log" \
		-o "-p $port -c shared_buffers=8GB -c fsync=off -c synchronous_commit=off -c autovacuum=off -c max_connections=100 -c huge_pages=off -c jit=off -c checkpoint_timeout=1h -c max_wal_size=20GB" \
		start -w 9>&- >/dev/null
}

stop_variant()
{
	variant=$1
	prefix="$root/$variant-install"
	"$prefix/bin/pg_ctl" -D "$active_datadir" stop -m fast -w 9>&- >/dev/null
	active_datadir=
}

setup_variant()
{
	variant=$1
	prefix="$root/$variant-install"
	datadir="$root/$variant-data"

	if [ -e "$datadir/READY" ]; then
		return
	fi
	rm -rf "$datadir"
	"$prefix/bin/initdb" -D "$datadir" --no-sync -A trust 9>&- >/dev/null
	start_variant "$variant"
	"$prefix/bin/createdb" -p "$port" bench 9>&-
	"$prefix/bin/psql" -X -p "$port" -d bench -v ON_ERROR_STOP=1 9>&- <<'SQL'
SET max_parallel_maintenance_workers = 0;

CREATE UNLOGGED TABLE dense (k int4 NOT NULL);
INSERT INTO dense SELECT g FROM generate_series(1, 5000000) AS g;
CREATE INDEX dense_idx ON dense (k);

CREATE UNLOGGED TABLE dense_desc (LIKE dense);
INSERT INTO dense_desc SELECT * FROM dense;
CREATE INDEX dense_desc_idx ON dense_desc (k DESC);

CREATE UNLOGGED TABLE even_keys (k int4 NOT NULL);
INSERT INTO even_keys SELECT g * 2 FROM generate_series(1, 2500000) AS g;
CREATE INDEX even_keys_idx ON even_keys (k);

CREATE UNLOGGED TABLE random_keys (k int4 NOT NULL);
INSERT INTO random_keys
SELECT ((g::bigint * 15485863) % 2147483647)::int4
FROM generate_series(1, 5000000) AS g;
CREATE INDEX random_keys_idx ON random_keys (k);

CREATE UNLOGGED TABLE clustered (k int4 NOT NULL);
INSERT INTO clustered
SELECT (((g - 1) / 500) * 500000 + ((g - 1) % 500))::int4
FROM generate_series(1, 2000000) AS g;
CREATE INDEX clustered_idx ON clustered (k);

CREATE UNLOGGED TABLE duplicates (k int4 NOT NULL);
INSERT INTO duplicates SELECT g % 10000 FROM generate_series(1, 5000000) AS g;
CREATE INDEX duplicates_dedup_idx ON duplicates (k) WITH (deduplicate_items=on);

CREATE UNLOGGED TABLE duplicates_plain (LIKE duplicates);
INSERT INTO duplicates_plain SELECT * FROM duplicates;
CREATE INDEX duplicates_plain_idx ON duplicates_plain (k) WITH (deduplicate_items=off);

CREATE UNLOGGED TABLE wide (k int4 NOT NULL);
INSERT INTO wide
SELECT (-2147483648::bigint + (g - 1)::bigint * 858)::int4
FROM generate_series(1, 5000000) AS g;
CREATE INDEX wide_idx ON wide (k);

CREATE UNLOGGED TABLE probes (n int4 PRIMARY KEY);
INSERT INTO probes SELECT g FROM generate_series(1, 1000000) AS g;
ANALYZE;
CHECKPOINT;
SQL
	stop_variant "$variant"
	touch "$datadir/READY"
}

setup_variant master
setup_variant special
setup_variant interp

query_for()
{
	case "$1" in
		dense_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM dense t WHERE t.k = ((p.n::bigint * 15485863) % 5000000 + 1)::int4 LIMIT 1) IS NOT NULL"
			;;
		dense_desc_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM dense_desc t WHERE t.k = ((p.n::bigint * 15485863) % 5000000 + 1)::int4 LIMIT 1) IS NOT NULL"
			;;
		dense_miss)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM even_keys t WHERE t.k = 2 * (((p.n::bigint * 15485863) % 2500000 + 1)::int4) - 1 LIMIT 1) IS NOT NULL"
			;;
		random_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM random_keys t WHERE t.k = (((((p.n::bigint * 32452843) % 5000000) + 1) * 15485863) % 2147483647)::int4 LIMIT 1) IS NOT NULL"
			;;
		cluster_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM clustered t WHERE t.k = (((((p.n::bigint * 15485863) % 2000000)::bigint / 500) * 500000) + (((p.n::bigint * 15485863) % 2000000) % 500))::int4 LIMIT 1) IS NOT NULL"
			;;
		cluster_gap_miss)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM clustered t WHERE t.k = (((p.n::bigint % 4000) * 500000) + 250000)::int4 LIMIT 1) IS NOT NULL"
			;;
		duplicates_dedup_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM duplicates t WHERE t.k = ((p.n::bigint * 15485863) % 10000)::int4 LIMIT 1) IS NOT NULL"
			;;
		duplicates_plain_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM duplicates_plain t WHERE t.k = ((p.n::bigint * 15485863) % 10000)::int4 LIMIT 1) IS NOT NULL"
			;;
		wide_hit)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM wide t WHERE t.k = (-2147483648::bigint + (((p.n::bigint * 15485863) % 5000000)::bigint) * 858)::int4 LIMIT 1) IS NOT NULL"
			;;
		below_min_miss)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM dense t WHERE t.k = -(p.n % 1024) LIMIT 1) IS NOT NULL"
			;;
		above_max_miss)
			echo "SELECT count(*) FROM probes p WHERE (SELECT k FROM dense t WHERE t.k = 5000001 + (p.n % 1024) LIMIT 1) IS NOT NULL"
			;;
		*) return 1 ;;
	esac
}

lookup_workloads="dense_hit dense_desc_hit dense_miss random_hit cluster_hit cluster_gap_miss duplicates_dedup_hit duplicates_plain_hit wide_hit below_min_miss above_max_miss"

run_lookup_variant()
{
	variant=$1
	round=$2
	prefix="$root/$variant-install"
	start_variant "$variant"
	for workload in $lookup_workloads; do
		query=$(query_for "$workload")
		# Populate buffers and initialize support-function caches outside timing.
		"$prefix/bin/psql" -X -p "$port" -d bench -Atqc \
			"SET max_parallel_workers_per_gather=0; SET enable_hashjoin=off; SET enable_mergejoin=off; $query" 9>&- >/dev/null
		echo "LOOKUP workload=$workload round=$round variant=$variant"
		/usr/bin/time -f 'elapsed=%e client_user=%U client_system=%S' \
			"$prefix/bin/psql" -X -p "$port" -d bench -Atqc \
			"SET max_parallel_workers_per_gather=0; SET enable_hashjoin=off; SET enable_mergejoin=off; $query" 9>&- >/dev/null
	done
	stop_variant "$variant"
}

variant_order()
{
	case $((($1 - 1) % 3)) in
		0) echo "master special interp" ;;
		1) echo "special interp master" ;;
		2) echo "interp master special" ;;
	esac
}

echo "LOOKUPS_START $(date -u +%FT%TZ)"
for round in 1 2 3 4 5 6 7; do
	for variant in $(variant_order "$round"); do
		run_lookup_variant "$variant" "$round"
	done
done
echo "LOOKUPS_DONE $(date -u +%FT%TZ)"

run_pgbench_variant()
{
	variant=$1
	workload=$2
	clients=$3
	round=$4
	prefix="$root/$variant-install"
	start_variant "$variant"
	"$prefix/bin/pgbench" -n -M prepared -p "$port" -d bench \
		-c "$clients" -j "$clients" -T 10 -f "$driver_dir/scripts/$workload.sql" 9>&- >/dev/null
	echo "PGBENCH workload=$workload clients=$clients round=$round variant=$variant"
	"$prefix/bin/pgbench" -n -M prepared -p "$port" -d bench \
		-c "$clients" -j "$clients" -T 30 -P 30 -f "$driver_dir/scripts/$workload.sql" 9>&-
	stop_variant "$variant"
}

echo "PGBENCH_START $(date -u +%FT%TZ)"
for clients in 1 16; do
	for workload in dense random duplicates; do
		for round in 1 2 3 4 5; do
			for variant in $(variant_order "$round"); do
				run_pgbench_variant "$variant" "$workload" "$clients" "$round"
			done
		done
	done
done
echo "PGBENCH_DONE $(date -u +%FT%TZ)"

run_insert_variant()
{
	variant=$1
	round=$2
	prefix="$root/$variant-install"
	start_variant "$variant"
	"$prefix/bin/psql" -X -p "$port" -d bench -v ON_ERROR_STOP=1 9>&- >/dev/null <<'SQL'
DROP TABLE IF EXISTS insert_test;
CREATE UNLOGGED TABLE insert_test (k int4 NOT NULL);
INSERT INTO insert_test SELECT g * 2 FROM generate_series(1, 2000000) AS g;
CREATE INDEX insert_test_idx ON insert_test (k);
CHECKPOINT;
SQL
	echo "INSERT round=$round variant=$variant"
	/usr/bin/time -f 'elapsed=%e client_user=%U client_system=%S' \
		"$prefix/bin/psql" -X -p "$port" -d bench -v ON_ERROR_STOP=1 -Atqc \
		"BEGIN; INSERT INTO insert_test SELECT 2 * (((g::bigint * 15485863) % 2000000 + 1)::int4) - 1 FROM generate_series(1, 1000000) AS g; ROLLBACK" 9>&- >/dev/null
	stop_variant "$variant"
}

echo "INSERT_START $(date -u +%FT%TZ)"
for round in 1 2 3 4 5; do
	for variant in $(variant_order "$round"); do
		run_insert_variant "$variant" "$round"
	done
done
echo "INSERT_DONE $(date -u +%FT%TZ)"
echo "DONE $(date -u +%FT%TZ)"
