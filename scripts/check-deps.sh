#!/bin/sh
# Assert the default build's dependencies match the documented budget.
#
# The point is not tidiness. The default feature set is the embeddable engine: a
# library someone links into their own process. If ZeroMQ drifts into it, that
# consumer suddenly needs cmake and a C++ toolchain they never asked for, and
# nobody notices until they complain. `net` is where the transport belongs.
set -eu

cd "$(dirname "$0")/.."

expected='crc32fast
parquet
serde
serde_json
tracing'

# --depth 1 lists direct dependencies only; -e no-dev excludes dev-dependencies,
# which never reach a consumer. No --features, so this is the default build.
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

# The ones that matter, checked by name so the failure message is obvious even
# if the list above is edited carelessly.
for forbidden in zmq clap tracing-subscriber; do
    if cargo tree --edges no-dev --prefix none \
        | awk 'NF {print $1}' | grep -qx "$forbidden"; then
        echo "FAIL: '$forbidden' is in the default build -- it belongs behind a feature." >&2
        exit 1
    fi
done

# Arrow is a whole subtree rather than one crate, and parquet's `arrow` feature
# is all-or-nothing: turning it on for ArrowWriter also pulls arrow-ipc and
# flatbuffers, which this engine never touches. src/flush/parquet.rs writes
# against the low-level API instead, so any arrow-* crate reappearing means
# someone re-enabled the feature -- probably by accident, since nothing here
# names those crates directly.
if cargo tree --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -qE '^(arrow-|flatbuffers$)'; then
    echo "FAIL: the arrow stack is back in the default build." >&2
    echo "      parquet's 'arrow' feature pulls all six arrow-* crates plus" >&2
    echo "      flatbuffers, bitflags, base64, num-complex, rustc_version, semver." >&2
    exit 1
fi

# The default build must stay pure Rust. `cc` is the tell: nothing here compiles
# C, and the compression codec (lz4_flex) was chosen over zstd precisely so an
# embedder needs no C compiler. If `cc` appears, some dependency started building
# native code -- which is a portability change, not a size one.
if cargo tree --edges no-dev --prefix none \
    | awk 'NF {print $1}' | grep -qx cc; then
    echo "FAIL: 'cc' is in the default build -- something now compiles C." >&2
    echo "      The embeddable engine is meant to build with rustc alone." >&2
    exit 1
fi

# `--edges all` includes dev-dependencies, and this is the check the earlier
# no-dev-only version could not make. Cargo cannot feature-gate a dev-dependency,
# so a `zmq` entry under [dev-dependencies] silently makes plain `cargo test`
# build libzmq from source -- the toolchain requirement `net` exists to avoid,
# reintroduced through the one edge the guard was not looking at.
if cargo tree --edges all --prefix none \
    | awk 'NF {print $1}' | grep -qx zmq; then
    echo "FAIL: 'zmq' reaches the default build through a dev-dependency." >&2
    echo "      Plain 'cargo test' would compile libzmq from C source." >&2
    echo "      Tests should use the 'orc::zmq' re-export under --features net." >&2
    exit 1
fi

# `net` is the library transport and must stay that way: a program embedding the
# client has no use for an argument parser or a log formatter.
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

# And the binaries' feature must actually supply what they need.
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
