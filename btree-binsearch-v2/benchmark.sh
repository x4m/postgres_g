#!/usr/bin/env bash
set -eu

. "$HOME/benchlock.sh"
bench_lock
bench_preflight

root="$HOME/bench-btree-binsearch-types"
repo="$root/repo.git"
base_commit=0e07ad2850d2c0b1ea65649a80e38ca937b72876
patch_commit=13194616fe4d884a5e94e789a805a4ded34c9be9
port=55446
active_datadir=
vmstat_pid=

mkdir -p "$root"

cleanup() {
	if [ -n "$vmstat_pid" ]; then
		kill "$vmstat_pid" 2>/dev/null || true
	fi
	if [ -n "$active_datadir" ] && [ -s "$active_datadir/PG_VERSION" ]; then
		"$root/patch-install/bin/pg_ctl" -D "$active_datadir" status >/dev/null 2>&1 &&
			"$root/patch-install/bin/pg_ctl" -D "$active_datadir" stop -m immediate -w >/dev/null 2>&1 || true
	fi
	bench_postflight
}
trap cleanup EXIT INT TERM

if [ ! -d "$repo" ]; then
	git clone --bare https://github.com/x4m/postgres_g.git "$repo" 9>&-
else
	git --git-dir="$repo" fetch origin 9>&-
fi

build_variant() {
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

build_variant base "$base_commit"
build_variant patch "$patch_commit"

setup_variant() {
	variant=$1
	prefix="$root/$variant-install"
	datadir="$root/$variant-data"

	if [ -e "$datadir/READY" ]; then
		return
	fi
	rm -rf "$datadir"
	"$prefix/bin/initdb" -D "$datadir" --no-sync -A trust 9>&- >/dev/null
	active_datadir="$datadir"
	"$prefix/bin/pg_ctl" -D "$datadir" -l "$root/$variant-server.log" \
		-o "-p $port -c shared_buffers=4GB -c fsync=off -c synchronous_commit=off -c autovacuum=off -c max_connections=100 -c huge_pages=off -c jit=off -c checkpoint_timeout=1h -c max_wal_size=20GB" \
		start -w 9>&- >/dev/null
	"$prefix/bin/createdb" -p "$port" bench 9>&-
	"$prefix/bin/psql" -X -p "$port" -d bench -v ON_ERROR_STOP=1 9>&- <<'SQL'
CREATE TABLE text_c_dense
  (k text COLLATE "C" PRIMARY KEY);
INSERT INTO text_c_dense
SELECT repeat('common-prefix-', 4) || lpad(g::text, 12, '0')
FROM generate_series(1, 1000000) g;

CREATE TABLE text_locale_dense
  (k text PRIMARY KEY);
INSERT INTO text_locale_dense SELECT k FROM text_c_dense;

CREATE TABLE text_c_md5
  (k text COLLATE "C" PRIMARY KEY);
INSERT INTO text_c_md5
SELECT md5(g::text) FROM generate_series(1, 1000000) g;

CREATE TABLE uuid_dense (k uuid PRIMARY KEY);
INSERT INTO uuid_dense
SELECT lpad(to_hex(g), 32, '0')::uuid
FROM generate_series(1, 1000000) g;

CREATE TABLE uuid_md5 (k uuid PRIMARY KEY);
INSERT INTO uuid_md5
SELECT md5(g::text)::uuid FROM generate_series(1, 1000000) g;

CREATE TABLE uuid_v7 (k uuid PRIMARY KEY);
INSERT INTO uuid_v7
SELECT (lpad(to_hex(1700000000000 + g / 1000), 12, '0') ||
        '7' || lpad(to_hex(g % 4096), 3, '0') ||
        '8' || substr(md5(g::text), 18, 15))::uuid
FROM generate_series(1, 1000000) g;

VACUUM (FREEZE, ANALYZE) text_c_dense;
VACUUM (FREEZE, ANALYZE) text_locale_dense;
VACUUM (FREEZE, ANALYZE) text_c_md5;
VACUUM (FREEZE, ANALYZE) uuid_dense;
VACUUM (FREEZE, ANALYZE) uuid_md5;
VACUUM (FREEZE, ANALYZE) uuid_v7;
CHECKPOINT;
SQL
	"$prefix/bin/pg_ctl" -D "$datadir" stop -m immediate -w 9>&- >/dev/null
	touch "$datadir/READY"
	active_datadir=
}

setup_variant base
setup_variant patch

mkdir -p "$root/scripts"
for workload in text_c_dense text_locale_dense text_c_md5 uuid_dense uuid_md5 uuid_v7; do
	: >"$root/scripts/$workload.sql"
	for n in $(seq 1 256); do
		id=$((1 + (n - 1) * 3906))
		case "$workload" in
			text_c_dense|text_locale_dense)
				value=$(printf 'common-prefix-common-prefix-common-prefix-common-prefix-%012d' "$id")
				;;
			text_c_md5|uuid_md5)
				value=$(printf '%s' "$id" | md5sum | cut -d' ' -f1)
				;;
			uuid_dense)
				value=$(printf '%032x' "$id")
				;;
			uuid_v7)
				timestamp=$(printf '%012x' $((1700000000000 + id / 1000)))
				fraction=$(printf '%03x' $((id % 4096)))
				random=$(printf '%s' "$id" | md5sum | cut -c18-32)
				value="${timestamp}7${fraction}8${random}"
				;;
		esac
		echo "SELECT k FROM $workload WHERE k = '$value';" >>"$root/scripts/$workload.sql"
	done
done

run_one() {
	variant=$1
	workload=$2
	clients=$3
	round=$4
	prefix="$root/$variant-install"
	active_datadir="$root/$variant-data"
	# Each transaction executes 256 prepared point lookups.
	tx_per_client=$((1000000 / clients / 256))

	"$prefix/bin/pg_ctl" -D "$active_datadir" -l "$root/$variant-server.log" \
		-o "-p $port -c shared_buffers=4GB -c fsync=off -c synchronous_commit=off -c autovacuum=off -c max_connections=100 -c huge_pages=off -c jit=off -c checkpoint_timeout=1h -c max_wal_size=20GB" \
		start -w 9>&- >/dev/null
	"$prefix/bin/pgbench" -n -M prepared -p "$port" -d bench \
		-c "$clients" -j "$clients" -T 5 -f "$root/scripts/$workload.sql" 9>&- >/dev/null
	echo "RESULT workload=$workload clients=$clients round=$round variant=$variant"
	vmstat_log="$root/vmstat-$workload-$clients-$round-$variant.log"
	vmstat -n -y 1 >"$vmstat_log" 2>&1 9>&- &
	vmstat_pid=$!
	/usr/bin/time -f 'client_user=%U client_system=%S elapsed=%e' \
		"$prefix/bin/pgbench" -n -M prepared -p "$port" -d bench \
		-c "$clients" -j "$clients" -t "$tx_per_client" \
		-f "$root/scripts/$workload.sql" 9>&-
	kill "$vmstat_pid"
	wait "$vmstat_pid" 2>/dev/null || true
	vmstat_pid=
	awk 'NR > 2 { user += $13; syscpu += $14; idle += $15; n++ }
	     END { if (n) printf "cpu_user=%.2f cpu_system=%.2f cpu_idle=%.2f samples=%d\n", user/n, syscpu/n, idle/n, n }' \
		"$vmstat_log"
	"$prefix/bin/pg_ctl" -D "$active_datadir" stop -m immediate -w 9>&- >/dev/null
	active_datadir=
}

run_pair() {
	workload=$1
	clients=$2
	round=$3

	if [ $((round % 2)) -eq 1 ]; then
		run_one base "$workload" "$clients" "$round"
		run_one patch "$workload" "$clients" "$round"
	else
		run_one patch "$workload" "$clients" "$round"
		run_one base "$workload" "$clients" "$round"
	fi
}

echo "START $(date -u +%FT%TZ)"
for clients in 1 16; do
	for workload in text_c_dense text_locale_dense text_c_md5 uuid_dense uuid_md5 uuid_v7; do
		for round in 1 2 3 4 5; do
			run_pair "$workload" "$clients" "$round"
		done
	done
done
echo "DONE $(date -u +%FT%TZ)"
