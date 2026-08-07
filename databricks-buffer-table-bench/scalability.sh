#!/usr/bin/env bash
set -eu

. "$HOME/benchlock.sh"
bench_lock

root="$HOME/dbt-scalability"
port=55444
active_datadir=

mkdir -p "$root"

cleanup() {
	if [ -n "$active_datadir" ] && [ -s "$active_datadir/PG_VERSION" ]; then
		"$HOME/dbt-patch/bin/pg_ctl" -D "$active_datadir" status >/dev/null 2>&1 &&
			"$HOME/dbt-patch/bin/pg_ctl" -D "$active_datadir" stop -m immediate -w >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT INT TERM

run_one() {
	variant=$1
	profile=$2
	clients=$3
	round=$4
	prefix="$HOME/dbt-$variant"
	active_datadir=$(mktemp -d "$root/run.XXXXXX")

	case "$profile" in
		churn)
			scale=100
			shared_buffers=128MB
			warmup=8
			;;
		resident)
			scale=5
			shared_buffers=256MB
			warmup=15
			;;
	esac

	"$prefix/bin/initdb" -D "$active_datadir" --no-sync -A trust 9>&- >/dev/null
	"$prefix/bin/pg_ctl" -D "$active_datadir" -l "$root/server.log" \
		-o "-p $port -c shared_buffers=$shared_buffers -c fsync=off -c synchronous_commit=off -c autovacuum=off -c max_connections=600 -c huge_pages=off -c checkpoint_timeout=1h -c max_wal_size=20GB" \
		start -w 9>&- >/dev/null
	"$prefix/bin/createdb" -p "$port" bench 9>&-
	"$prefix/bin/pgbench" -p "$port" -i -s "$scale" bench 9>&- >/dev/null
	"$prefix/bin/psql" -p "$port" -d bench -c "CHECKPOINT" 9>&- >/dev/null

	"$prefix/bin/pgbench" -p "$port" -n -S -M prepared \
		--random-seed=104729 -c "$clients" -j 64 -T "$warmup" bench 9>&- >/dev/null
	echo "RESULT profile=$profile clients=$clients round=$round variant=$variant"
	"$prefix/bin/pgbench" -p "$port" -n -S -M prepared \
		--random-seed=15485863 -c "$clients" -j 64 -T 30 bench 9>&- | grep "tps ="

	"$prefix/bin/pg_ctl" -D "$active_datadir" stop -m immediate -w 9>&- >/dev/null
	rm -rf "$active_datadir"
	active_datadir=
}

run_pair() {
	profile=$1
	clients=$2
	round=$3

	if [ $((round % 2)) -eq 1 ]; then
		run_one base "$profile" "$clients" "$round"
		run_one patch "$profile" "$clients" "$round"
	else
		run_one patch "$profile" "$clients" "$round"
		run_one base "$profile" "$clients" "$round"
	fi
}

echo "START $(date -u +%FT%TZ)"

for clients in 128 512; do
	for round in 1 2 3 4 5; do
		run_pair churn "$clients" "$round"
	done
done

for clients in 128 256 512; do
	for round in 1 2 3 4 5; do
		run_pair resident "$clients" "$round"
	done
done

echo "DONE $(date -u +%FT%TZ)"
