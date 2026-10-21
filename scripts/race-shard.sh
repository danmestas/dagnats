#!/usr/bin/env bash
# race-shard.sh PKG SHARD TOTAL
#
# Runs one shard of a package's top-level tests under -race. Tests are
# listed with `go test -list`, sorted, and dealt round-robin into TOTAL
# shards; shard SHARD (0-based) runs its share with an anchored -run
# pattern. Tests inside a shard still run serially, exactly as in a
# whole-package run, so sharding changes wall time and nothing else
# (#815). Used by .github/workflows/race.yml for internal/engine.
set -euo pipefail

if [ "$#" -ne 3 ]; then
	echo "usage: $0 PKG SHARD TOTAL" >&2
	exit 2
fi
pkg="$1"
shard="$2"
total="$3"
if [ "$total" -lt 1 ] || [ "$shard" -lt 0 ] || [ "$shard" -ge "$total" ]; then
	echo "race-shard: shard $shard out of range for total $total" >&2
	exit 2
fi

tests=$(go test -list '^Test' "$pkg" | grep -E '^Test' | sort)
if [ -z "$tests" ]; then
	echo "race-shard: no tests found in $pkg" >&2
	exit 1
fi

names=$(printf '%s\n' "$tests" | awk -v s="$shard" -v n="$total" '(NR - 1) % n == s')
count=$(printf '%s\n' "$names" | grep -c . || true)
if [ "$count" -eq 0 ]; then
	echo "race-shard: shard $shard of $total is empty" >&2
	exit 1
fi
echo "race-shard: $pkg shard $shard/$total runs $count of $(printf '%s\n' "$tests" | wc -l | tr -d ' ') tests"

pattern="^($(printf '%s\n' "$names" | paste -sd '|' -))\$"
go test -race -count=1 -timeout 300s -run "$pattern" "$pkg"
