#!/usr/bin/env bash
set -euo pipefail

DUCKDB="./build/release/duckdb"

if [ $# -lt 1 ]; then
    echo "Usage: $0 example.osm.pbf" >&2
    exit 1
fi

PBF="$1"

benchmark() {
    hyperfine --warmup 1 --command-name "$1" "$DUCKDB -c \"$1\""
}

benchmark "SELECT count(*) FROM '$PBF'"
benchmark "SELECT count(*) FROM '$PBF' WHERE kind = 'node'"
benchmark "SELECT count(*) FROM '$PBF' WHERE kind = 'line'"
benchmark "SELECT count(*) FROM '$PBF' WHERE kind = 'relation'"
benchmark "SELECT count(*) FROM '$PBF' WHERE tags['building'] IS NOT NULL"
benchmark "SELECT count(*) FROM '$PBF' WHERE tags['building'] = 'house'"
benchmark "SELECT count(*) FROM '$PBF' WHERE tags['building'] = 'commercial' AND tags['name'] IS NOT NULL"
benchmark "SELECT id FROM '$PBF' WHERE kind = 'node' LIMIT 1"
benchmark "SELECT id, tags FROM '$PBF' WHERE kind = 'node' LIMIT 1"
benchmark "SELECT id, tags, geometry FROM '$PBF' WHERE kind = 'line' AND tags['highway'] IS NOT NULL LIMIT 1"
benchmark "SELECT id, tags, geometry FROM '$PBF' WHERE kind = 'line' AND tags['highway'] IS NOT NULL"
benchmark "SELECT id, tags, geometry FROM '$PBF' WHERE kind = 'area' AND tags['building'] IS NOT NULL LIMIT 1"
benchmark "SELECT id, tags, geometry FROM '$PBF' WHERE kind = 'area' AND tags['building'] IS NOT NULL"
