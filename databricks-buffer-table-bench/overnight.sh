#!/usr/bin/env bash
set -eu

. "$HOME/benchlock.sh"
bench_lock

cleanup() {
	for datadir in "$HOME/dbt-data" "$HOME/swizzle-data"; do
		"$HOME/dbt-patch/bin/pg_ctl" -D "$datadir" status >/dev/null 2>&1 &&
			"$HOME/dbt-patch/bin/pg_ctl" -D "$datadir" stop -m fast -w >/dev/null 2>&1 || true
	done
}
trap cleanup EXIT INT TERM

port=55443

run_one() {
	dataset=$1
	db=$2
	variant=$3
	sb=$4
	clients=$5
	mode=$6
	round=$7
	prefix="$HOME/dbt-$variant"

	if [ "$mode" = select ]; then
		flags="-S"
	else
		flags=""
	fi

	"$prefix/bin/pg_ctl" -D "$dataset" -l "$HOME/dbt-overnight-server.log" \
		-o "-p $port -c shared_buffers=$sb -c fsync=off -c synchronous_commit=off -c autovacuum=off -c max_connections=100 -c huge_pages=off -c checkpoint_timeout=1h -c max_wal_size=20GB" \
		start -w 9>&-
	"$prefix/bin/pgbench" -p "$port" -n $flags -M prepared \
		-c "$clients" -j "$clients" -T 8 "$db" 9>&- >/dev/null
	echo "RESULT mode=$mode dataset=$(basename "$dataset") sb=$sb clients=$clients round=$round variant=$variant"
	"$prefix/bin/pgbench" -p "$port" -n $flags -M prepared \
		-c "$clients" -j "$clients" -T 30 "$db" 9>&- | grep "tps ="
	"$prefix/bin/pg_ctl" -D "$dataset" stop -m fast -w 9>&-
}

run_pair() {
	dataset=$1
	db=$2
	sb=$3
	clients=$4
	mode=$5
	round=$6

	if [ $((round % 2)) -eq 1 ]; then
		run_one "$dataset" "$db" base "$sb" "$clients" "$mode" "$round"
		run_one "$dataset" "$db" patch "$sb" "$clients" "$mode" "$round"
	else
		run_one "$dataset" "$db" patch "$sb" "$clients" "$mode" "$round"
		run_one "$dataset" "$db" base "$sb" "$clients" "$mode" "$round"
	fi
}

echo "START $(date -u +%FT%TZ)"

for clients in 1 16 64; do
	for round in 1 2 3 4 5; do
		run_pair "$HOME/swizzle-data" pgbench_swizzle 4GB "$clients" select "$round"
	done
done

for clients in 1 16 64; do
	for round in 1 2 3 4 5; do
		run_pair "$HOME/dbt-data" bench 512MB "$clients" select "$round"
	done
done

for clients in 1 16 64; do
	for round in 1 2 3 4 5; do
		run_pair "$HOME/dbt-data" bench 512MB "$clients" rw "$round"
	done
done

for clients in 16 64; do
	for round in 1 2 3; do
		run_pair "$HOME/dbt-data" bench 8GB "$clients" select "$round"
	done
done

echo "DONE $(date -u +%FT%TZ)"
