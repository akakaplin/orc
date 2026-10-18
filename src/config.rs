//! The user-facing configuration surface: `<data_dir>/config.json`.
//!
//! Every field has a default (in the `Default` impls), so a minimal config is
//! just a `series` list. Unknown fields are rejected so typos fail at load.
//! Durations are integer milliseconds.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::codec::{BATCH_HEADER_BYTES, SEGMENT_HEADER_BYTES};
use crate::error::{Error, Result};
use crate::record::KeyType;
use crate::time::parse_rfc3339_utc;

/// The parsed contents of `config.json`. [`Config::load`] validates; a
/// hand-built `Config` needs [`Config::validate`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Root of everything the engine writes. Default `./data`.
    pub data_dir: PathBuf,
    pub server: ServerConfig,
    pub limits: LimitsConfig,
    pub wal: WalConfig,
    pub flush: FlushConfig,
    /// The declared series. Records naming anything else are rejected.
    pub series: Vec<SeriesConfig>,
}

/// ZeroMQ endpoints and socket tuning. Unused by an embedded engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// PULL socket for ingest.
    pub ingest_endpoint: String,
    /// REP socket for `ping`, `stats`, `flush`, `schema`, `reload-config`.
    pub control_endpoint: String,
    /// Receive high-water mark, in messages.
    pub rcv_hwm: i32,
    /// Largest ingest message libzmq accepts, in bytes (libzmq's own default is
    /// unlimited). Must hold one max-size record plus the batch header.
    pub max_batch_bytes: i64,
}

/// The ways bad input can hurt the engine, each with a bound.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Max encoded record size; checked before any decoder allocation.
    pub max_record_bytes: usize,
    /// Total cap on `rejects/`, so unparseable traffic cannot fill the volume.
    pub reject_max_bytes: u64,
    /// Lower bound of the timestamp accept window, RFC 3339 UTC. Parsed once by
    /// [`LimitsConfig::ts_min_us`].
    pub ts_min: String,
    /// How far past `now` a timestamp may be before it is rejected.
    pub ts_max_skew_ms: u64,
    /// Accepted but currently unused; kept so existing configs still load.
    pub validate_json: bool,

    /// Cache for [`LimitsConfig::ts_min_us`].
    #[serde(skip)]
    ts_min_us: OnceLock<u64>,
}

/// Write-ahead log durability and rolling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    /// Group-commit window. Appends sit in a 256 KiB user-space buffer until the
    /// committer flushes and fsyncs, so a process crash or power cut can lose up
    /// to one window.
    pub fsync_interval_ms: u64,
    /// Commit sooner once this much dirty data has accumulated.
    pub fsync_bytes: u64,
    /// Roll to a new segment past this size; also bounds the startup tail scan.
    pub segment_max_bytes: u64,
}

/// The periodic Parquet dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FlushConfig {
    /// Flush period, i.e. the delay before data is visible. `0` disables the
    /// timer; then only [`Engine::flush`](crate::engine::Engine::flush) and the
    /// control socket flush, and the WAL grows unbounded.
    pub interval_ms: u64,
    /// Flush at startup if `interval_ms` has elapsed since `last_flush_at`.
    pub on_startup: bool,
    /// Parquet compression codec; must be compiled in.
    pub compression: String,
    /// Rows per Parquet row group (readers' `ts` pruning granularity).
    pub row_group_rows: usize,
    /// Max sorted runs merged at once (default 64, min 2). Past it, the
    /// smallest runs are merged first.
    pub merge_fan_in: usize,
    /// Bytes the flush sorts in memory before spilling sorted runs to `runs/`
    /// (default 256 MiB); roughly the flush's peak memory.
    pub memory_budget_bytes: u64,
}

/// One declared series: a name, and the typed columns it promises to carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesConfig {
    /// Carried in every frame; also the directory name under `series/`.
    pub name: String,
    /// Declared key columns, in frame order. Undeclared keys go to `extra`.
    #[serde(default)]
    pub keys: Vec<KeyDef>,
}

/// One declared key column. `name` and `type` are required; `nullable`
/// defaults to `false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyDef {
    pub name: String,
    /// Spelled `"string"`, `"i64"`, `"f64"` or `"bool"` in JSON.
    #[serde(rename = "type", with = "key_type_serde")]
    pub ty: KeyType,
    /// Enforced at ingest only; Parquet key columns are always nullable.
    #[serde(default)]
    pub nullable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            server: ServerConfig::default(),
            limits: LimitsConfig::default(),
            wal: WalConfig::default(),
            flush: FlushConfig::default(),
            series: Vec::new(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        // Loopback on purpose: the ingest socket is unauthenticated.
        Self {
            ingest_endpoint: "tcp://127.0.0.1:5555".into(),
            control_endpoint: "tcp://127.0.0.1:5556".into(),
            rcv_hwm: 100_000,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
        }
    }
}

/// Default for [`ServerConfig::max_batch_bytes`], and the client's fallback.
/// Client and server must agree: libzmq silently drops oversized messages.
pub const DEFAULT_MAX_BATCH_BYTES: i64 = 8_388_608; // 8 MiB

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_record_bytes: 16_384,     // 16 KiB
            reject_max_bytes: 67_108_864, // 64 MiB
            ts_min: "2000-01-01T00:00:00Z".into(),
            ts_max_skew_ms: 86_400_000, // 24 h
            validate_json: false,
            ts_min_us: OnceLock::new(),
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            fsync_interval_ms: 10,
            fsync_bytes: 4_194_304,        // 4 MiB
            segment_max_bytes: 67_108_864, // 64 MiB
        }
    }
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            interval_ms: 3_600_000, // 1 h
            on_startup: true,
            compression: "lz4_raw".into(),
            row_group_rows: 131_072,
            merge_fan_in: 64,
            memory_budget_bytes: 268_435_456, // 256 MiB
        }
    }
}

/// Single source of truth: the Parquet writer's codec list.
use crate::flush::parquet::SUPPORTED_COMPRESSION;

impl LimitsConfig {
    /// `ts_min` as epoch microseconds, parsed once and cached for the hot path.
    pub fn ts_min_us(&self) -> Result<u64> {
        if let Some(us) = self.ts_min_us.get() {
            return Ok(*us);
        }
        let us = parse_rfc3339_utc(&self.ts_min)?;
        let _ = self.ts_min_us.set(us);
        Ok(us)
    }
}

impl Config {
    /// Read and validate a config file; errors name the path.
    pub fn load(path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display())))?;
        let cfg: Config = serde_json::from_str(&text)
            .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check every invariant the engine assumes, and pre-parse `limits.ts_min`.
    pub fn validate(&self) -> Result<()> {
        self.limits.ts_min_us()?;
        self.check_bounds()?;
        self.check_series()
    }

    /// Numeric tunables, each against the invariant that makes it usable.
    fn check_bounds(&self) -> Result<()> {
        let bad = |msg: String| Err(Error::Config(msg));

        if self.limits.max_record_bytes == 0 {
            return bad("limits.max_record_bytes must be greater than 0".into());
        }
        if self.flush.merge_fan_in < 2 {
            return bad(
                "flush.merge_fan_in must be at least 2, or a merge cannot make progress".into(),
            );
        }
        if self.flush.memory_budget_bytes == 0 {
            return bad("flush.memory_budget_bytes must be greater than 0".into());
        }
        if self.flush.row_group_rows == 0 {
            return bad("flush.row_group_rows must be greater than 0".into());
        }
        // Also keeps the microsecond conversion from overflowing.
        if self.flush.interval_ms > MAX_FLUSH_INTERVAL_MS {
            return bad(format!(
                "flush.interval_ms {} is out of range; the maximum is {} (~100 years), \
                 and 0 disables the timer",
                self.flush.interval_ms, MAX_FLUSH_INTERVAL_MS
            ));
        }
        if self.wal.fsync_interval_ms == 0 {
            return bad(
                "wal.fsync_interval_ms must be greater than 0; it is the group-commit window, \
                 and 0 would mean the committer never parks"
                    .into(),
            );
        }
        if self.wal.fsync_bytes == 0 {
            return bad(
                "wal.fsync_bytes must be greater than 0; 0 leaves the committer's wait \
                 predicate permanently false, which spins a core"
                    .into(),
            );
        }
        // A segment must hold one max-size record, or every record rolls a file.
        let smallest_segment = self.limits.max_record_bytes as u64 + SEGMENT_HEADER_BYTES as u64;
        if self.wal.segment_max_bytes < smallest_segment {
            return bad(format!(
                "wal.segment_max_bytes {} cannot hold one limits.max_record_bytes record \
                 ({} plus a {}-byte segment header): every record would roll its own segment",
                self.wal.segment_max_bytes, self.limits.max_record_bytes, SEGMENT_HEADER_BYTES
            ));
        }
        if self.server.max_batch_bytes <= 0 {
            return bad(format!(
                "server.max_batch_bytes must be greater than 0 (got {})",
                self.server.max_batch_bytes
            ));
        }
        // Else a max-size record is silently dropped by libzmq.
        let smallest_useful = self.limits.max_record_bytes as u64 + BATCH_HEADER_BYTES as u64;
        if (self.server.max_batch_bytes as u64) < smallest_useful {
            return bad(format!(
                "server.max_batch_bytes {} cannot hold one limits.max_record_bytes record \
                 ({} plus a {}-byte batch header): a full-size record would be dropped by \
                 zeromq before the server saw it, with no error on either side",
                self.server.max_batch_bytes, self.limits.max_record_bytes, BATCH_HEADER_BYTES
            ));
        }
        if !SUPPORTED_COMPRESSION.contains(&self.flush.compression.as_str()) {
            return bad(format!(
                "flush.compression {:?} is not compiled in; supported: {}",
                self.flush.compression,
                SUPPORTED_COMPRESSION.join(", ")
            ));
        }

        Ok(())
    }

    /// Series names must be safe directory components; key lists must yield a
    /// readable Parquet schema.
    fn check_series(&self) -> Result<()> {
        let bad = |msg: String| Err(Error::Config(msg));

        let mut seen_series = BTreeSet::new();
        // Case-folded: case-insensitive filesystems would merge directories.
        let mut seen_folded = BTreeSet::new();
        for s in &self.series {
            check_series_name(&s.name)?;
            if !seen_series.insert(s.name.as_str()) {
                return bad(format!(
                    "duplicate series {:?}: two definitions would share one directory under series/",
                    s.name
                ));
            }
            if !seen_folded.insert(s.name.to_lowercase()) {
                return bad(format!(
                    "series {:?} collides with another series differing only in case; \
                     on a case-insensitive filesystem they would share one directory under series/",
                    s.name
                ));
            }

            let mut seen_keys = BTreeSet::new();
            for k in &s.keys {
                if k.name.is_empty() {
                    return bad(format!("series {:?} has a key with an empty name", s.name));
                }
                if RESERVED_KEY_NAMES.contains(&k.name.as_str()) {
                    return bad(format!(
                        "series {:?} declares a key named {:?}, which collides with a column \
                         every Parquet file already has; the reserved names are {}. \
                         Parquet does not reject a duplicate field name, so this would write \
                         files with two columns of one name -- unreadable by the generated \
                         view.sql and mis-parsed by anything that goes by column name.",
                        s.name,
                        k.name,
                        RESERVED_KEY_NAMES.join(", ")
                    ));
                }
                if !seen_keys.insert(k.name.as_str()) {
                    return bad(format!(
                        "series {:?} declares key {:?} twice; keys are positional, so a \
                         duplicate name makes the frame layout ambiguous",
                        s.name, k.name
                    ));
                }
            }
        }
        Ok(())
    }

    /// The declared series with this name, if any.
    pub fn find_series(&self, name: &str) -> Option<&SeriesConfig> {
        self.series.iter().find(|s| s.name == name)
    }
}

/// Fixed Parquet columns a key may not reuse; the Parquet writer would not
/// catch the duplicate.
pub const RESERVED_KEY_NAMES: [&str; 4] = ["ts", "id", "extra", "data"];

/// Upper bound on `flush.interval_ms` (~100 years).
const MAX_FLUSH_INTERVAL_MS: u64 = 100 * 365 * 24 * 60 * 60 * 1_000;

/// Series names are path components under `series/` and carry a `u16` length.
fn check_series_name(name: &str) -> Result<()> {
    let bad = |why: &str| Err(Error::Config(format!("series name {name:?}: {why}")));

    if name.is_empty() {
        return bad("must not be empty");
    }
    if u16::try_from(name.len()).is_err() {
        return bad("must be under 64 KiB; the frame encodes it with a u16 length");
    }
    if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return bad("must not be a path component like '.', '..' or contain a separator");
    }
    if name.chars().any(|c| c.is_control()) {
        return bad("must not contain control characters");
    }
    Ok(())
}

/// A series resolved once for the ingest hot path. Pins its `epoch`, so a
/// handle held across `reload-config` keeps its layout.
#[derive(Debug, Clone)]
pub struct SeriesHandle {
    name: String,
    keys: Vec<KeyDef>,
    epoch: u32,
}

impl SeriesHandle {
    pub fn new(series: &SeriesConfig, epoch: u32) -> Result<Self> {
        check_series_name(&series.name)?;
        Ok(Self {
            name: series.name.clone(),
            keys: series.keys.clone(),
            epoch,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared key columns, in the positional order frames encode them.
    pub fn keys(&self) -> &[KeyDef] {
        &self.keys
    }

    /// The schema epoch stamped into every frame encoded through this handle.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// Number of positional key values per frame.
    pub fn arity(&self) -> usize {
        self.keys.len()
    }
}

/// The config spelling of a key type (JSON and error messages).
pub fn key_type_name(ty: KeyType) -> &'static str {
    match ty {
        KeyType::Str => "string",
        KeyType::I64 => "i64",
        KeyType::F64 => "f64",
        KeyType::Bool => "bool",
    }
}

/// Serde bridge for [`KeyType`], keeping JSON spellings out of the codec.
mod key_type_serde {
    use super::{KeyType, key_type_name};
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    const NAMES: [&str; 4] = ["string", "i64", "f64", "bool"];

    pub fn serialize<S: Serializer>(ty: &KeyType, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(key_type_name(*ty))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<KeyType, D::Error> {
        let raw = String::deserialize(d)?;
        match raw.as_str() {
            "string" => Ok(KeyType::Str),
            "i64" => Ok(KeyType::I64),
            "f64" => Ok(KeyType::F64),
            "bool" => Ok(KeyType::Bool),
            other => Err(D::Error::unknown_variant(other, &NAMES)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str =
        r#"{"series":[{"name":"trades","keys":[{"name":"symbol","type":"string"}]}]}"#;

    fn parse(json: &str) -> Result<Config> {
        let cfg: Config = serde_json::from_str(json).map_err(Error::Json)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn minimal_config_materialises_every_default() {
        let cfg = parse(MINIMAL).unwrap();

        assert_eq!(cfg.data_dir, PathBuf::from("./data"));
        assert_eq!(cfg.server.ingest_endpoint, "tcp://127.0.0.1:5555");
        assert_eq!(cfg.server.control_endpoint, "tcp://127.0.0.1:5556");
        assert_eq!(cfg.server.rcv_hwm, 100_000);

        assert_eq!(cfg.limits.max_record_bytes, 16_384);
        assert_eq!(cfg.limits.reject_max_bytes, 67_108_864);
        assert_eq!(cfg.limits.ts_min, "2000-01-01T00:00:00Z");
        assert_eq!(cfg.limits.ts_max_skew_ms, 86_400_000);
        assert!(!cfg.limits.validate_json);

        assert_eq!(cfg.wal.fsync_interval_ms, 10);
        assert_eq!(cfg.wal.fsync_bytes, 4_194_304);
        assert_eq!(cfg.wal.segment_max_bytes, 67_108_864);

        assert_eq!(cfg.flush.interval_ms, 3_600_000);
        assert!(cfg.flush.on_startup);
        assert_eq!(cfg.flush.compression, "lz4_raw");
        assert_eq!(cfg.flush.row_group_rows, 131_072);
        assert_eq!(cfg.flush.merge_fan_in, 64);

        assert_eq!(
            cfg.series[0].keys[0],
            KeyDef {
                name: "symbol".into(),
                ty: KeyType::Str,
                nullable: false
            }
        );
    }

    #[test]
    fn ts_min_is_pre_parsed_to_micros() {
        let cfg = parse(MINIMAL).unwrap();
        assert_eq!(cfg.limits.ts_min_us().unwrap(), 946_684_800_000_000);
        assert_eq!(cfg.limits.ts_min_us().unwrap(), 946_684_800_000_000);
    }

    #[test]
    fn nested_blocks_override_only_named_fields() {
        let cfg = parse(r#"{"wal":{"fsync_interval_ms":50},"series":[]}"#).unwrap();
        assert_eq!(cfg.wal.fsync_interval_ms, 50);
        assert_eq!(cfg.wal.segment_max_bytes, 67_108_864);
    }

    #[test]
    fn plan_example_round_trips() {
        let json = r#"{
          "data_dir": "./data",
          "server":  { "ingest_endpoint": "tcp://127.0.0.1:5555",
                       "control_endpoint": "tcp://127.0.0.1:5556", "rcv_hwm": 100000 },
          "limits":  { "max_record_bytes": 16384, "reject_max_bytes": 67108864,
                       "ts_min": "2000-01-01T00:00:00Z", "ts_max_skew_ms": 86400000,
                       "validate_json": false },
          "wal":     { "fsync_interval_ms": 10, "fsync_bytes": 4194304,
                       "segment_max_bytes": 67108864 },
          "flush":   { "interval_ms": 3600000, "on_startup": true, "compression": "lz4_raw",
                       "row_group_rows": 131072, "merge_fan_in": 64 },
          "series": [
            { "name": "trades",
              "keys": [ {"name": "symbol", "type": "string"},
                        {"name": "venue",  "type": "string", "nullable": true} ] }
          ]
        }"#;
        let cfg = parse(json).unwrap();
        assert!(cfg.series[0].keys[1].nullable);

        let round_tripped = parse(&serde_json::to_string(&cfg).unwrap()).unwrap();
        assert_eq!(round_tripped.series, cfg.series);
        assert_eq!(round_tripped.data_dir, cfg.data_dir);
    }

    #[test]
    fn every_key_type_spelling_round_trips() {
        for (spelling, ty) in [
            ("string", KeyType::Str),
            ("i64", KeyType::I64),
            ("f64", KeyType::F64),
            ("bool", KeyType::Bool),
        ] {
            let json = format!(
                r#"{{"series":[{{"name":"s","keys":[{{"name":"k","type":"{spelling}"}}]}}]}}"#
            );
            let cfg = parse(&json).unwrap();
            assert_eq!(cfg.series[0].keys[0].ty, ty);
            assert_eq!(key_type_name(ty), spelling);
            assert!(serde_json::to_string(&cfg).unwrap().contains(spelling));
        }
    }

    #[test]
    fn rejects_bad_configs() {
        let cases: &[(&str, &str)] = &[
            (
                "duplicate series",
                r#"{"series":[{"name":"a","keys":[]},{"name":"a","keys":[]}]}"#,
            ),
            (
                "duplicate key",
                r#"{"series":[{"name":"a","keys":[{"name":"k","type":"i64"},{"name":"k","type":"i64"}]}]}"#,
            ),
            (
                // Distinct names, one directory on macOS and Windows.
                "series names differing only in case",
                r#"{"series":[{"name":"trades","keys":[]},{"name":"Trades","keys":[]}]}"#,
            ),
            ("empty series name", r#"{"series":[{"name":"","keys":[]}]}"#),
            (
                "empty key name",
                r#"{"series":[{"name":"a","keys":[{"name":"","type":"i64"}]}]}"#,
            ),
            (
                "unparseable ts_min",
                r#"{"limits":{"ts_min":"yesterday"},"series":[]}"#,
            ),
            (
                "ts_min with an offset instead of Z",
                r#"{"limits":{"ts_min":"2000-01-01T00:00:00+02:00"},"series":[]}"#,
            ),
            (
                "series name escaping data_dir",
                r#"{"series":[{"name":"../etc","keys":[]}]}"#,
            ),
            (
                "zero max_record_bytes",
                r#"{"limits":{"max_record_bytes":0},"series":[]}"#,
            ),
            (
                "unbuildable merge",
                r#"{"flush":{"merge_fan_in":1},"series":[]}"#,
            ),
            (
                "uncompiled compression codec",
                r#"{"flush":{"compression":"brotli"},"series":[]}"#,
            ),
            (
                "misspelled field",
                r#"{"wal":{"fsync_interval":10},"series":[]}"#,
            ),
            (
                "unknown key type",
                r#"{"series":[{"name":"a","keys":[{"name":"k","type":"u32"}]}]}"#,
            ),
            (
                "key with no type",
                r#"{"series":[{"name":"a","keys":[{"name":"k"}]}]}"#,
            ),
            (
                // Refused by the parser (`u64`).
                "negative ts_max_skew_ms",
                r#"{"limits":{"ts_max_skew_ms":-1},"series":[]}"#,
            ),
            (
                "absurd flush interval",
                r#"{"flush":{"interval_ms":18446744073709551615},"series":[]}"#,
            ),
            (
                "zero max_batch_bytes",
                r#"{"server":{"max_batch_bytes":0},"series":[]}"#,
            ),
            (
                "max_batch_bytes below max_record_bytes",
                r#"{"server":{"max_batch_bytes":100},
                    "limits":{"max_record_bytes":16384},"series":[]}"#,
            ),
            (
                "zero fsync_interval_ms",
                r#"{"wal":{"fsync_interval_ms":0},"series":[]}"#,
            ),
            (
                "zero fsync_bytes",
                r#"{"wal":{"fsync_bytes":0},"series":[]}"#,
            ),
            (
                "segment_max_bytes below max_record_bytes",
                r#"{"wal":{"segment_max_bytes":100},
                    "limits":{"max_record_bytes":16384},"series":[]}"#,
            ),
            (
                "zero segment_max_bytes",
                r#"{"wal":{"segment_max_bytes":0},"series":[]}"#,
            ),
        ];
        for (what, json) in cases {
            assert!(parse(json).is_err(), "should have rejected {what}: {json}");
        }
    }

    #[test]
    fn a_key_may_not_be_named_after_a_fixed_column() {
        for reserved in RESERVED_KEY_NAMES {
            let json = format!(
                r#"{{"series":[{{"name":"trades","keys":[{{"name":"{reserved}","type":"i64"}}]}}]}}"#
            );
            let err = parse(&json).unwrap_err().to_string();
            assert!(
                err.contains(reserved),
                "rejecting {reserved:?} should name it: {err}"
            );
        }
        // Only exact collisions are rejected.
        assert!(
            parse(r#"{"series":[{"name":"a","keys":[{"name":"ts_local","type":"i64"}]}]}"#).is_ok()
        );
    }

    #[test]
    fn small_segments_are_fine_when_the_record_cap_matches() {
        let cfg = parse(
            r#"{"wal":{"segment_max_bytes":2048},
                "limits":{"max_record_bytes":512},"series":[]}"#,
        )
        .unwrap();
        assert_eq!(cfg.wal.segment_max_bytes, 2048);

        // The exact boundary is allowed.
        let exact = 512 + SEGMENT_HEADER_BYTES;
        assert!(
            parse(&format!(
                r#"{{"wal":{{"segment_max_bytes":{exact}}},
                    "limits":{{"max_record_bytes":512}},"series":[]}}"#
            ))
            .is_ok()
        );
        assert!(
            parse(&format!(
                r#"{{"wal":{{"segment_max_bytes":{}}},
                    "limits":{{"max_record_bytes":512}},"series":[]}}"#,
                exact - 1
            ))
            .is_err()
        );
    }

    #[test]
    fn a_zero_flush_interval_is_allowed_and_means_no_timer() {
        let cfg = parse(r#"{"flush":{"interval_ms":0},"series":[]}"#).unwrap();
        assert_eq!(cfg.flush.interval_ms, 0);
    }

    #[test]
    fn load_reads_a_file_and_names_it_in_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, MINIMAL).unwrap();
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.series[0].name, "trades");

        let missing = dir.path().join("nope.json");
        let err = Config::load(&missing).unwrap_err().to_string();
        assert!(
            err.contains("nope.json"),
            "error should name the file: {err}"
        );

        std::fs::write(&path, "{ not json").unwrap();
        let err = Config::load(&path).unwrap_err().to_string();
        assert!(
            err.contains("config.json"),
            "error should name the file: {err}"
        );
    }

    #[test]
    fn a_handle_carries_the_schema_and_the_epoch_it_resolved_at() {
        let cfg = parse(MINIMAL).unwrap();
        let h = SeriesHandle::new(cfg.find_series("trades").unwrap(), 7).unwrap();

        assert_eq!(h.name(), "trades");
        assert_eq!(h.epoch(), 7, "pinned, so a later reload cannot move it");
        assert_eq!(h.arity(), 1);
        assert_eq!(h.keys()[0].name, "symbol");
    }

    #[test]
    fn a_handle_refuses_a_name_that_would_escape_the_data_directory() {
        let s = SeriesConfig {
            name: "../etc".into(),
            keys: vec![],
        };
        assert!(SeriesHandle::new(&s, 0).is_err());
    }
}
