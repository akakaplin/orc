#!/bin/sh
# Assert the default (embeddable) build's dependencies match the documented budget.
set -eu

cd "$(dirname "$0")/.."

expected='bytes
crc32fast
parquet
serde
serde_json
tracing'

# Direct, non-dev dependencies of the default build.
actual=$(cargo tree --depth 1 --edges no-dev --prefix none \
    | tail -n +2 \
    | awk 'NF {print $1}' \
    | grep -v '^\[' \
    | sort -u)

if [ "$expected" != "$actual" ]; then
    echo "Default-build dependencies do not match the documented budget." >&2
    printf '%s\n' "$expected" > /tmp/orc-deps-expected.$$
    printf '%s\n' "$actual" > /tmp/orc-deps-actual.$$
    diff -u /tmp/orc-deps-expected.$$ /tmp/orc-deps-actual.$$ >&2 || true
    rm -f /tmp/orc-deps-expected.$$ /tmp/orc-deps-actual.$$
    echo "Update this script and README.md if the change is intentional." >&2
    exit 1
fi

# Named explicitly so the failure is obvious even if the list above drifts.
for forbidden in zmq clap tracing-subscriber; do
    if cargo tree --edges no-dev --prefix none \
        | awk 'NF {print $1}' | grep -qx "$forbidden"; then
        echo "FAIL: '$forbidden' is in the default build -- it belongs behind a feature." >&2
        exit 1
    fi
done

# Any arrow-* crate means parquet's `arrow` feature was re-enabled.
if cargo tree --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -qE '^(arrow-|flatbuffers$)'; then
    echo "FAIL: the arrow stack is back in the default build." >&2
    echo "      parquet's 'arrow' feature pulls all six arrow-* crates plus" >&2
    echo "      flatbuffers, bitflags, base64, num-complex, rustc_version, semver." >&2
    exit 1
fi

# The default build must stay pure Rust; `cc` means something compiles C.
if cargo tree --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -qx cc; then
    echo "FAIL: 'cc' is in the default build -- something now compiles C." >&2
    echo "      The embeddable engine is meant to build with rustc alone." >&2
    exit 1
fi

# A zmq dev-dependency (can't be feature-gated) would make `cargo test` build libzmq.
if cargo tree --edges all --prefix none \
    | awk 'NF {print $1}' | grep -qx zmq; then
    echo "FAIL: 'zmq' reaches the default build through a dev-dependency." >&2
    echo "      Plain 'cargo test' would compile libzmq from C source." >&2
    echo "      Tests should use the 'orc::zmq' re-export under --features net." >&2
    exit 1
fi

# `net` is the library transport: no arg parser or log formatter.
net_only=$(cargo tree --features net --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -v '^\[' | sort -u)
if ! printf '%s\n' "$net_only" | grep -qx zmq; then
    echo "FAIL: 'zmq' is missing from the 'net' build." >&2
    exit 1
fi
for forbidden in clap tracing-subscriber; do
    if printf '%s\n' "$net_only" | grep -qx "$forbidden"; then
        echo "FAIL: '$forbidden' is in 'net' -- it belongs behind 'cli'." >&2
        exit 1
    fi
done

# `cli` must supply what the binaries need.
cli=$(cargo tree --features cli --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -v '^\[' | sort -u)
for required in zmq clap tracing-subscriber; do
    if ! printf '%s\n' "$cli" | grep -qx "$required"; then
        echo "FAIL: '$required' is missing from the 'cli' build." >&2
        exit 1
    fi
done

count() { printf '%s\n' "$1" | grep -vxc orc; }

echo "Default build (embeddable engine), $(count "$(cargo tree --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -v '^\[' | sort -u)") crates:"
echo "$actual" | sed 's/^/  /'
echo "  + 'net' (library transport): $(count "$net_only") crates total"
echo "  + 'cli' (shipped binaries):  $(count "$cli") crates total"
