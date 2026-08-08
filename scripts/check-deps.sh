#!/bin/sh
# Assert orc-core's direct dependencies match the documented budget.
#
# The point is not tidiness. `orc-core` is the embeddable artifact: a library
# someone links into their own process. If ZeroMQ or a CLI parser drifts into it
# transitively, "embeddable" quietly stops being true and nobody notices until a
# consumer complains about a C++ toolchain requirement they never asked for.
set -eu

cd "$(dirname "$0")/.."

expected='arrow-array
arrow-schema
crc32fast
parquet
serde
serde_json
thiserror
tracing'

# --depth 1 lists direct dependencies only; -e no-dev excludes dev-dependencies
# (tempfile), which never ship to a consumer.
actual=$(cargo tree --package orc-core --depth 1 --edges no-dev --prefix none \
    | tail -n +2 \
    | awk 'NF {print $1}' \
    | sort -u)

if [ "$expected" != "$actual" ]; then
    echo "orc-core direct dependencies do not match the documented budget." >&2
    printf '%s\n' "$expected" > /tmp/orc-deps-expected.$$
    printf '%s\n' "$actual" > /tmp/orc-deps-actual.$$
    diff -u /tmp/orc-deps-expected.$$ /tmp/orc-deps-actual.$$ >&2 || true
    rm -f /tmp/orc-deps-expected.$$ /tmp/orc-deps-actual.$$
    echo "Update this script, README.md and the plan if the change is intentional." >&2
    exit 1
fi

# The two that matter most, checked by name so the failure message is obvious
# even if the list above is edited carelessly.
for forbidden in zmq clap; do
    if cargo tree --package orc-core --edges no-dev --prefix none \
        | awk 'NF {print $1}' | grep -qx "$forbidden"; then
        echo "FAIL: '$forbidden' reachable from orc-core -- it must stay embeddable." >&2
        exit 1
    fi
done

echo "orc-core dependency budget OK:"
echo "$actual" | sed 's/^/  /'
