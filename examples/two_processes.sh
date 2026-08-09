#!/usr/bin/env bash
#
# Two processes, one data directory.
#
#   1. `embedded_server` opens the engine, writes five timestamps to it from an
#      in-process thread, and serves a ZeroMQ socket at the same time.
#   2. `remote_writer` connects from outside and writes five more.
#   3. `orc-cli` reads the counters back and asks the server to stop.
#
# Both writers' records end up in the same Parquet files, which is the point:
# embedding the engine and talking to it over a socket are the same write path.
#
#   ./examples/two_processes.sh
#
# Leaves nothing behind unless KEEP=1 is set.

set -euo pipefail

INGEST="tcp://127.0.0.1:5655"
CONTROL="tcp://127.0.0.1:5656"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug"
DATA="$(mktemp -d "${TMPDIR:-/tmp}/orc-example.XXXXXX")"

cleanup() {
    [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null || true
    if [ "${KEEP:-0}" = "1" ]; then
        echo "data directory kept at $DATA"
    else
        rm -rf "$DATA"
    fi
}
trap cleanup EXIT

echo "==> building (the first --features cli build compiles libzmq, so it is slow)"
cargo build --quiet --features cli --examples --bins --manifest-path "$ROOT/Cargo.toml"

echo
echo "==> starting the embedded server on $INGEST"
"$BIN/examples/embedded_server" "$DATA" "$INGEST" "$CONTROL" &
SERVER_PID=$!

# The socket is not up the instant the process is, so poll rather than sleep a
# guessed amount. `ping` is there for exactly this.
echo "==> waiting for the control socket"
for _ in $(seq 100); do
    if "$BIN/orc-cli" --ingest "$INGEST" ping >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
"$BIN/orc-cli" --ingest "$INGEST" ping

echo
echo "==> writing from a second process"
"$BIN/examples/remote_writer" "$INGEST" "$CONTROL"

echo
echo "==> counters (appended counts both writers; batches_received counts only the remote one)"
"$BIN/orc-cli" --ingest "$INGEST" stats

echo
echo "==> stopping the server"
"$BIN/orc-cli" --ingest "$INGEST" shutdown
wait "$SERVER_PID" || true
unset SERVER_PID

echo
echo "==> what landed on disk"
find "$DATA/series" -name '*.parquet' | sed "s|^$DATA/||"
echo
echo "==> and the view a reader would use"
sed -n '5,20p' "$DATA/series/pulse/view.sql"
