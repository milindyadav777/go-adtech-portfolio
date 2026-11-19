#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
go build -o "$tmp/report-collector" ./cmd/report-collector

collect() {
  "$tmp/report-collector" -date 2026-01-15 -out "$tmp/snapshots" "$@"
}

collect
cp -R "$tmp/snapshots" "$tmp/baseline"
collect
diff -r "$tmp/baseline" "$tmp/snapshots"
echo 'Verified byte-identical snapshots on rerun'

status=0
collect -fail-second-page || status=$?
test "$status" -eq 1
diff -r "$tmp/baseline" "$tmp/snapshots"
echo 'Verified exit status 1 and previous snapshot preserved after page two failed'
