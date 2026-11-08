#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

go build -o bully bully.go
pids=""
cleanup() {
    trap - EXIT INT TERM
    for pid in $pids; do kill "$pid" 2>/dev/null || true; done
    for pid in $pids; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for id in 0 1 2 3 4 5 6 7; do
    mode=live
    case "$id" in
        4) mode=live_init ;;
        7) mode=dead ;;
    esac
    ./bully "$id" "$mode" &
    pids="$pids $!"
done

echo 'Menunggu election. Tekan Ctrl+C untuk menghentikan semua node.'
wait
