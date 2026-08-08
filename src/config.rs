//! The user-facing configuration surface: `<data_dir>/config.json`.
//!
//! Two rules shape this module.
//!
//! **Every field has a default**, so a working config is just a `series` list.
//! Defaults live in hand-written `Default` impls rather than in `#[serde(default
//! = "...")]` helpers, because a reader comparing the file against the README
//! should find every tunable's value in one place per block.
//!
//! **Unknown fields are rejected.** This config is hand-edited, and the whole
//! point of the `limits` block is to bound how badly the engine can be hurt by
//! bad input — a `fsync_interval` that silently means `fsync_interval_ms`
//! stayed at its default would defeat that quietly. Failing at load names the
//! typo instead.
//!
//! Durations are integer milliseconds throughout: no duration-parsing crate, and
//! no ambiguity about whether `10` meant seconds.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::record::KeyType;
use crate::time::parse_rfc3339_utc;

/// The parsed contents of `config.json`.
///
/// [`Config::load`] validates before returning, so a `Config` obtained from it
/// is always internally consistent. A `Config` assembled by hand is not —
/// call [`Config::validate`] on it before opening an engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Root of the on-disk layout. Everything the engine writes lives under it.
    ///
    /// Relative by default, so a data directory stays movable — the same
    /// property the generated `view.sql` relies on.
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
}

/// The ways bad input can hurt the engine, each with a bound.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Checked *before any allocation* in the decoder: without it one corrupt
    /// length prefix asks the process for 4 GiB.
    pub max_record_bytes: usize,
    /// Total cap on `rejects/`, so unparseable traffic cannot fill the volume.
    pub reject_max_bytes: u64,
    /// Lower bound of the timestamp accept window, RFC 3339 UTC.
    ///
    /// Written as a string because a reader can check `2000-01-01T00:00:00Z` at
    /// a glance and cannot check `946684800000000`. Parsed once — see
    /// [`LimitsConfig::ts_min_us`].
    pub ts_min: String,
    /// How far past `now` a timestamp may be before it is rejected.
    pub ts_max_skew_ms: i64,
    /// The one exception to "`serde_json` is never on the ingest path". Off by
    /// default because it costs latency on every record.
    pub validate_json: bool,

    /// Cache for [`LimitsConfig::ts_min_us`]. Not serialized: it is derived
    /// from `ts_min`, and a second copy in the file could disagree with it.
    #[serde(skip)]
    ts_min_us: OnceLock<i64>,
}

/// Write-ahead log durability and rolling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    /// Group-commit window: the committer thread fsyncs at least this often, so
    /// a power cut loses at most this much. A process crash loses nothing.
    pub fsync_interval_ms: u64,
    /// ...or sooner, once this much dirty data has accumulated.
    pub fsync_bytes: u64,
    /// Roll to a new segment past this size. Also the bound on startup scan
    /// cost, since recovery reads only the tail segment.
    pub segment_max_bytes: u64,
}

/// The periodic Parquet dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FlushConfig {
    /// How often to dump. Also the delay before ingested data becomes visible
    /// to readers, since the engine is write-only.
    pub interval_ms: u64,
    /// Flush at startup *if more than `interval_ms` has already elapsed* since
    /// the manifest's `last_flush_at` — not unconditionally, which on an engine
    /// that restarts every few minutes would produce a tiny file per restart.
    pub on_startup: bool,
    /// Parquet compression codec. Only what the pinned `parquet` features
    /// actually compile in is accepted; see [`Config::validate`].
    pub compression: String,
    /// Rows per Parquet row group, which is also the granularity readers prune
    /// `ts` at.
    pub row_group_rows: usize,
    /// Maximum sorted runs merged in one pass. An hour at 100k rec/s is ~560
    /// runs, which would blow past macOS's 256-descriptor default in one pass.
    pub merge_fan_in: usize,
}

/// One declared series: a name, and the typed columns it promises to carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesConfig {
    /// Carried verbatim in every frame and used as the directory name under
    /// `series/`.
    pub name: String,
    /// Declared key columns, in the order frames encode them. Undeclared keys
    /// are not an error — they land in `extra`.
    #[serde(default)]
    pub keys: Vec<KeyDef>,
}

/// One declared key column.
///
/// `name` and `type` are required: a key with no type is meaningless, and
/// defaulting it to `string` would silently mistype a column for the life of the
/// dataset. `nullable` defaults to `false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyDef {
    pub name: String,
    /// Spelled `"string"`, `"i64"`, `"f64"` or `"bool"` in JSON.
    #[serde(rename = "type", with = "key_type_serde")]
    pub ty: KeyType,
    /// Enforced at ingest only. Every key column is written *nullable* in
    /// Parquet regardless, so a column added in a later epoch reads as NULL from
    /// older files instead of failing the union.
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
        // Loopback is deliberate: there is no authentication on the ingest
        // socket, so anything that can reach it can write records and consume
        // disk. Binding 0.0.0.0 by default would mean a config copied from the
        // README silently exposes a writable database to the network.
        Self {
            ingest_endpoint: "tcp://127.0.0.1:5555".into(),
            control_endpoint: "tcp://127.0.0.1:5556".into(),
            rcv_hwm: 100_000,
        }
    }
}

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
        }
    }
}

/// Re-exported from the writer that implements it.
///
/// This list used to be duplicated here, and had already drifted: it accepted
/// `"none"` but not `"uncompressed"`, which the writer has always handled — so a
/// perfectly writable config was rejected at load. Taking it from the one place
/// that turns a name into a codec is what stops that recurring.
use crate::flush::parquet::SUPPORTED_COMPRESSION;

impl LimitsConfig {
    /// `ts_min` as epoch microseconds.
    ///
    /// Parsed at most once and cached, because the ingest path compares this
    /// against every record's `ts`: re-running an RFC 3339 parser per record
    /// would put a string parse on the hot path for a value that never changes.
    /// [`Config::validate`] calls this, so a validated config has already paid
    /// the parse.
    pub fn ts_min_us(&self) -> Result<i64> {
        if let Some(us) = self.ts_min_us.get() {
            return Ok(*us);
        }
        let us = parse_rfc3339_utc(&self.ts_min)?;
        let _ = self.ts_min_us.set(us);
        Ok(us)
    }
}

impl Config {
    /// Read and validate a config file.
    ///
    /// I/O and parse failures are reported as [`Error::Config`] with the path
    /// interpolated: a bare `No such file or directory` from a hand-edited file
    /// tells the operator nothing about *which* file the engine wanted.
    pub fn load(path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display())))?;
        let cfg: Config = serde_json::from_str(&text)
            .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check every invariant the rest of the engine assumes, and pre-parse
    /// `limits.ts_min`.
    ///
    /// Everything checked here is something that would otherwise fail later and
    /// worse: a duplicate series name means two schemas sharing one directory, a
    /// name with a path separator escapes `data_dir`, and a `ts_min` that only
    /// fails to parse on the first record turns a typo into a total ingest
    /// outage an hour after startup.
    pub fn validate(&self) -> Result<()> {
        let bad = |msg: String| Err(Error::Config(msg));

        // Parses and caches, so the ingest path never sees the string form.
        self.limits.ts_min_us()?;

        if self.limits.max_record_bytes == 0 {
            return bad("limits.max_record_bytes must be greater than 0".into());
        }
        if self.flush.merge_fan_in < 2 {
            return bad(
                "flush.merge_fan_in must be at least 2, or a merge cannot make progress".into(),
            );
        }
        if self.flush.row_group_rows == 0 {
            return bad("flush.row_group_rows must be greater than 0".into());
        }
        if !SUPPORTED_COMPRESSION.contains(&self.flush.compression.as_str()) {
            return bad(format!(
                "flush.compression {:?} is not compiled in; supported: {}",
                self.flush.compression,
                SUPPORTED_COMPRESSION.join(", ")
            ));
        }

        let mut seen_series = BTreeSet::new();
        // Folded to lowercase, because the collision that matters is the one the
        // *filesystem* sees. macOS and Windows are case-insensitive by default,
        // so `trades` and `Trades` are two schemas silently sharing one
        // `series/` directory -- two epochs' Parquet interleaved under one name,
        // discovered much later by a reader. Rejecting costs nothing; the same
        // config is portable to a case-sensitive filesystem either way.
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

/// Series names become path components under `series/` and are carried in every
/// frame with a `u16` length, so they are constrained beyond "non-empty".
fn check_series_name(name: &str) -> Result<()> {
    let bad = |why: &str| Err(Error::Config(format!("series name {name:?}: {why}")));

    if name.is_empty() {
        return bad("must not be empty");
    }
    if u16::try_from(name.len()).is_err() {
        return bad("must be under 64 KiB; the frame encodes it with a u16 length");
    }
    // The name is a directory component: `..` or a separator would let a config
    // edit write outside data_dir, and a control character makes a path that is
    // nearly impossible to inspect or delete by hand.
    if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return bad("must not be a path component like '.', '..' or contain a separator");
    }
    if name.chars().any(|c| c.is_control()) {
        return bad("must not contain control characters");
    }
    Ok(())
}

/// A series resolved for the ingest hot path.
///
/// This exists for one reason: the frame carries the series *name*, not an id,
/// so without a handle every `append` would re-encode the same length prefix and
/// UTF-8 bytes per record. The handle pre-encodes them once at `engine.series()`
/// and the hot path `memcpy`s [`SeriesHandle::encoded_name`] — no hashing, no
/// string comparison, no allocation.
///
/// It also pins the `epoch` the schema was resolved under, because every frame
/// stamps its own epoch and a handle held across a `reload-config` must keep
/// encoding under the layout it was built for.
#[derive(Debug, Clone)]
pub struct SeriesHandle {
    name: String,
    keys: Vec<KeyDef>,
    epoch: u32,
    encoded_name: Vec<u8>,
}

impl SeriesHandle {
    /// Resolve a series definition at a given schema epoch.
    pub fn new(series: &SeriesConfig, epoch: u32) -> Result<Self> {
        check_series_name(&series.name)?;
        // u16 little-endian length prefix + UTF-8 bytes: byte-for-byte the
        // frame's `series` field, so the encoder copies this verbatim. It must
        // stay in step with the codec's field layout.
        let len = u16::try_from(series.name.len()).expect("checked above");
        let mut encoded_name = Vec::with_capacity(2 + series.name.len());
        encoded_name.extend_from_slice(&len.to_le_bytes());
        encoded_name.extend_from_slice(series.name.as_bytes());

        Ok(Self {
            name: series.name.clone(),
            keys: series.keys.clone(),
            epoch,
            encoded_name,
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

    /// Number of positional key values a frame for this series must carry.
    pub fn arity(&self) -> usize {
        self.keys.len()
    }

    /// The pre-encoded `series` field: `u16` little-endian length, then UTF-8.
    pub fn encoded_name(&self) -> &[u8] {
        &self.encoded_name
    }

    /// Position of a declared key, for `append_named` and tests. Linear because
    /// key lists are short and this is deliberately *not* on the hot path — that
    /// is what positional `keys` exists to avoid.
    pub fn key_index(&self, name: &str) -> Option<usize> {
        self.keys.iter().position(|k| k.name == name)
    }
}

/// The config spelling of a key type. Used in JSON and in error messages, so an
/// error names the type the way the user wrote it rather than the way Rust does.
pub fn key_type_name(ty: KeyType) -> &'static str {
    match ty {
        KeyType::Str => "string",
        KeyType::I64 => "i64",
        KeyType::F64 => "f64",
        KeyType::Bool => "bool",
    }
}

/// Serde bridge for [`KeyType`], which lives in `record.rs` and stays free of
/// serde derives: the record model is the codec's contract, and the on-disk type
/// *tags* are numeric bytes that have nothing to do with these JSON spellings.
/// Keeping the bridge here means the JSON vocabulary can change without touching
/// the wire format.
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

        // `nullable` defaults to false; `type` and `name` are required.
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
        // 2000-01-01T00:00:00Z, independently: 946684800 seconds.
        assert_eq!(cfg.limits.ts_min_us().unwrap(), 946_684_800_000_000);
        // Cached: a second call must agree without reparsing.
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
        ];
        for (what, json) in cases {
            assert!(parse(json).is_err(), "should have rejected {what}: {json}");
        }
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
    fn handle_pre_encodes_the_series_name() {
        let cfg = parse(MINIMAL).unwrap();
        let h = SeriesHandle::new(cfg.find_series("trades").unwrap(), 7).unwrap();

        // u16 little-endian length, then the UTF-8 bytes -- exactly the frame's
        // `series` field, which the hot path copies verbatim.
        assert_eq!(h.encoded_name(), b"\x06\x00trades");
        assert_eq!(h.name(), "trades");
        assert_eq!(h.epoch(), 7);
        assert_eq!(h.arity(), 1);
        assert_eq!(h.key_index("symbol"), Some(0));
        assert_eq!(h.key_index("nope"), None);
    }

    #[test]
    fn handle_encodes_multibyte_names_by_byte_length() {
        // The length prefix counts bytes, not characters: a two-byte 'é' must
        // not make the decoder read one byte short.
        let s = SeriesConfig {
            name: "café".into(),
            keys: vec![],
        };
        let h = SeriesHandle::new(&s, 0).unwrap();
        assert_eq!(h.encoded_name(), b"\x05\x00caf\xc3\xa9");
    }
}
