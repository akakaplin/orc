//! `manifest.json` — schema history and the flush commit point.
//!
//! Holds what the data directory cannot tell us: each series' keys per schema
//! epoch, and how far the flush has got. Size is O(epochs), never O(files): the
//! epoch tag in each filename (`….e7.parquet`) links files to schemas.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::{Config, KeyDef, key_type_name};
use crate::error::{Error, Result};

/// Committed manifest filename, relative to `data_dir`.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Staging filename, next to the manifest so the rename stays atomic.
pub const MANIFEST_TMP_FILE: &str = "manifest.json.tmp";

/// One entry of a series' schema history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochSchema {
    pub epoch: u32,
    /// Keys in positional order; frames stamped with this epoch decode against it.
    pub keys: Vec<KeyDef>,
}

/// The contents of `manifest.json`. Unknown fields are tolerated so an older
/// binary can read a newer manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Manifest {
    /// Per-series schema history, oldest first (`BTreeMap` for stable output).
    pub series: BTreeMap<String, Vec<EpochSchema>>,
    /// WAL segments with `id <= last_flushed_segment` are durable in Parquet.
    pub last_flushed_segment: u64,
    /// Last flush commit time (epoch µs). The next flush is scheduled from this,
    /// not process start, so frequent restarts still flush. `None`: never flushed.
    pub last_flush_at: Option<u64>,
}

impl Manifest {
    /// Read `<data_dir>/manifest.json`; a missing file yields the default. A
    /// leftover `manifest.json.tmp` is from an uncommitted flush and is deleted.
    pub fn load(data_dir: impl AsRef<Path>) -> Result<Manifest> {
        let data_dir = data_dir.as_ref();
        let tmp = data_dir.join(MANIFEST_TMP_FILE);
        match std::fs::remove_file(&tmp) {
            Ok(()) => {
                tracing::warn!(path = %tmp.display(), "discarded a stale manifest staging file from an uncommitted flush")
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::Io(e)),
        }

        let path = data_dir.join(MANIFEST_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Manifest::default()),
            Err(e) => return Err(Error::Io(e)),
        };
        serde_json::from_str(&text).map_err(|e| Error::Config(format!("{}: {e}", path.display())))
    }

    /// Is there a committed manifest? [`Manifest::load`] returns the default for
    /// both a fresh and a lost manifest; a lost one must not reach
    /// [`sweep_uncommitted`](crate::flush::planner::sweep_uncommitted), which
    /// would treat everything as uncommitted.
    pub fn exists(data_dir: impl AsRef<Path>) -> bool {
        data_dir.as_ref().join(MANIFEST_FILE).is_file()
    }

    /// Atomically replace `manifest.json`: the flush commit point. Order matters:
    /// write tmp, fsync it, rename over `manifest.json`, fsync the directory.
    /// The rename is the commit; see [`crate::flush::planner`] for crash cases.
    pub fn commit(&self, data_dir: impl AsRef<Path>) -> Result<()> {
        use std::io::Write;

        let data_dir = data_dir.as_ref();
        let tmp: PathBuf = data_dir.join(MANIFEST_TMP_FILE);
        let final_path = data_dir.join(MANIFEST_FILE);

        // Pretty-printed for humans; it is small.
        let bytes = serde_json::to_vec_pretty(self)?;

        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
        drop(f);

        std::fs::rename(&tmp, &final_path)?;

        // Makes the rename durable.
        std::fs::File::open(data_dir)?.sync_all()?;
        Ok(())
    }

    /// Fold a config into the schema history, bumping per-series epochs on
    /// layout changes. Add/remove/reorder is fine; retyping is rejected (readers
    /// union by name). All-or-nothing. Returns whether anything changed: the
    /// caller must then [`Manifest::commit`] before stamping frames with it.
    pub fn reconcile(&mut self, config: &Config) -> Result<bool> {
        for series in &config.series {
            let history = self
                .series
                .get(&series.name)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            check_no_retype(&series.name, history, &series.keys)?;
        }

        let mut changed = false;
        for series in &config.series {
            let history = self.series.entry(series.name.clone()).or_default();

            // An epoch is a layout (order + types); `nullable` does not bump it.
            let same_layout = history.last().is_some_and(|last| {
                last.keys.len() == series.keys.len()
                    && std::iter::zip(&last.keys, &series.keys)
                        .all(|(a, b)| a.name == b.name && a.ty == b.ty)
            });
            if same_layout {
                if let Some(last) = history.last_mut()
                    && last.keys != series.keys
                {
                    last.keys = series.keys.clone();
                    changed = true;
                }
                continue;
            }

            let epoch = match history.last() {
                None => 0,
                Some(last) => last.epoch.checked_add(1).ok_or_else(|| {
                    Error::Config(format!(
                        "series {:?} has exhausted its schema epochs at {}",
                        series.name,
                        u32::MAX
                    ))
                })?,
            };
            tracing::info!(
                series = %series.name,
                epoch,
                keys = series.keys.len(),
                "schema epoch bumped"
            );
            history.push(EpochSchema {
                epoch,
                keys: series.keys.clone(),
            });
            changed = true;
        }
        Ok(changed)
    }

    /// The epoch new frames for this series must be stamped with.
    pub fn current_epoch(&self, series: &str) -> Option<u32> {
        self.series.get(series)?.last().map(|e| e.epoch)
    }

    /// Keys for `epoch`. `None` becomes [`FrameFault::UnknownSchema`], which
    /// the flush counts and skips.
    ///
    /// [`FrameFault::UnknownSchema`]: crate::wal::reader::FrameFault::UnknownSchema
    pub fn schema(&self, series: &str, epoch: u32) -> Option<&[KeyDef]> {
        self.series
            .get(series)?
            .iter()
            .find(|e| e.epoch == epoch)
            .map(|e| e.keys.as_slice())
    }
}

/// Reject a key whose type differs from any it has had in any epoch.
fn check_no_retype(series: &str, history: &[EpochSchema], keys: &[KeyDef]) -> Result<()> {
    for past in history {
        for old in &past.keys {
            if let Some(new) = keys.iter().find(|n| n.name == old.name && n.ty != old.ty) {
                return Err(Error::Config(format!(
                    "series {:?} key {:?} was {} at epoch {}, now {}: retyping a key makes \
                     historical Parquet unreadable in the same query as new data. \
                     Rename the key, or start a new series.",
                    series,
                    new.name,
                    key_type_name(old.ty),
                    past.epoch,
                    key_type_name(new.ty),
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SeriesConfig;
    use crate::record::KeyType;

    fn key(name: &str, ty: KeyType) -> KeyDef {
        KeyDef {
            name: name.into(),
            ty,
            nullable: false,
        }
    }

    fn config(series: &[(&str, Vec<KeyDef>)]) -> Config {
        Config {
            series: series
                .iter()
                .map(|(name, keys)| SeriesConfig {
                    name: (*name).into(),
                    keys: keys.clone(),
                })
                .collect(),
            ..Config::default()
        }
    }

    #[test]
    fn fresh_series_start_at_epoch_zero() {
        let mut m = Manifest::default();
        m.reconcile(&config(&[("trades", vec![key("symbol", KeyType::Str)])]))
            .unwrap();
        assert_eq!(m.current_epoch("trades"), Some(0));
        assert_eq!(m.series["trades"].len(), 1);
        assert_eq!(m.current_epoch("quotes"), None);
    }

    #[test]
    fn unchanged_keys_do_not_bump_the_epoch() {
        let cfg = config(&[("trades", vec![key("symbol", KeyType::Str)])]);
        let mut m = Manifest::default();
        m.reconcile(&cfg).unwrap();
        m.reconcile(&cfg).unwrap();
        m.reconcile(&cfg).unwrap();
        assert_eq!(m.current_epoch("trades"), Some(0));
        assert_eq!(m.series["trades"].len(), 1, "history must not grow");
    }

    #[test]
    fn reconcile_reports_whether_it_changed_anything() {
        let cfg = config(&[("trades", vec![key("symbol", KeyType::Str)])]);
        let mut m = Manifest::default();
        assert!(m.reconcile(&cfg).unwrap(), "minting epoch 0 is a change");
        assert!(!m.reconcile(&cfg).unwrap(), "an identical config is not");

        // A new epoch.
        let grown = config(&[(
            "trades",
            vec![key("symbol", KeyType::Str), key("size", KeyType::I64)],
        )]);
        assert!(m.reconcile(&grown).unwrap());
        assert!(!m.reconcile(&grown).unwrap());

        // Nullable-only: no epoch, but still a change.
        let mut relaxed = key("symbol", KeyType::Str);
        relaxed.nullable = true;
        let same_layout = config(&[("trades", vec![relaxed, key("size", KeyType::I64)])]);
        assert!(m.reconcile(&same_layout).unwrap(), "nullable edit persists");
        assert!(!m.reconcile(&same_layout).unwrap());
        assert_eq!(m.current_epoch("trades"), Some(1), "and mints no epoch");
    }

    #[test]
    fn epochs_are_isolated_per_series() {
        let mut m = Manifest::default();
        m.reconcile(&config(&[
            ("trades", vec![key("symbol", KeyType::Str)]),
            ("quotes", vec![key("venue", KeyType::Str)]),
        ]))
        .unwrap();
        let quotes_before = m.series["quotes"].clone();

        // Edit trades only.
        m.reconcile(&config(&[
            (
                "trades",
                vec![key("symbol", KeyType::Str), key("size", KeyType::I64)],
            ),
            ("quotes", vec![key("venue", KeyType::Str)]),
        ]))
        .unwrap();

        assert_eq!(m.current_epoch("trades"), Some(1));
        assert_eq!(m.current_epoch("quotes"), Some(0));
        assert_eq!(m.series["quotes"], quotes_before);
    }

    #[test]
    fn add_remove_and_reorder_are_allowed() {
        let mut m = Manifest::default();
        let a = key("symbol", KeyType::Str);
        let b = key("venue", KeyType::Str);

        m.reconcile(&config(&[("t", vec![a.clone()])])).unwrap();
        m.reconcile(&config(&[("t", vec![a.clone(), b.clone()])]))
            .unwrap(); // add
        m.reconcile(&config(&[("t", vec![b.clone(), a.clone()])]))
            .unwrap(); // reorder
        m.reconcile(&config(&[("t", vec![b.clone()])])).unwrap(); // remove

        assert_eq!(m.current_epoch("t"), Some(3));
        assert_eq!(m.schema("t", 0), Some([a.clone()].as_slice()));
        assert_eq!(m.schema("t", 2), Some([b.clone(), a].as_slice()));
        assert_eq!(m.schema("t", 9), None);
    }

    #[test]
    fn retyping_a_key_is_rejected_naming_both_types() {
        let mut m = Manifest::default();
        m.reconcile(&config(&[("t", vec![key("size", KeyType::Str)])]))
            .unwrap();
        let err = m
            .reconcile(&config(&[("t", vec![key("size", KeyType::I64)])]))
            .unwrap_err();

        let msg = err.to_string();
        assert!(msg.contains("\"size\""), "{msg}");
        assert!(msg.contains("string"), "{msg}");
        assert!(msg.contains("i64"), "{msg}");
        assert_eq!(m.current_epoch("t"), Some(0));
    }

    #[test]
    fn retyping_a_key_removed_two_epochs_ago_is_still_rejected() {
        let mut m = Manifest::default();
        m.reconcile(&config(&[("t", vec![key("size", KeyType::Str)])]))
            .unwrap();
        m.reconcile(&config(&[("t", vec![])])).unwrap();
        assert!(
            m.reconcile(&config(&[("t", vec![key("size", KeyType::I64)])]))
                .is_err()
        );
    }

    #[test]
    fn nullable_only_change_does_not_mint_an_epoch() {
        let mut m = Manifest::default();
        m.reconcile(&config(&[("t", vec![key("k", KeyType::Str)])]))
            .unwrap();
        let mut relaxed = key("k", KeyType::Str);
        relaxed.nullable = true;
        m.reconcile(&config(&[("t", vec![relaxed])])).unwrap();

        assert_eq!(m.current_epoch("t"), Some(0), "no new epoch for nullable");
        assert!(
            m.schema("t", 0).unwrap()[0].nullable,
            "history still reflects what ingest enforces"
        );
    }

    #[test]
    fn dropped_series_keep_their_history_and_resume_on_re_add() {
        let mut m = Manifest::default();
        m.reconcile(&config(&[
            ("t", vec![key("k", KeyType::Str)]),
            ("u", vec![]),
        ]))
        .unwrap();
        m.reconcile(&config(&[("t", vec![key("k", KeyType::Str)])]))
            .unwrap();
        assert_eq!(m.current_epoch("u"), Some(0), "history is never pruned");

        m.reconcile(&config(&[("u", vec![])])).unwrap();
        assert_eq!(m.current_epoch("u"), Some(0), "same keys, same epoch");
    }

    #[test]
    fn commit_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::default();
        m.reconcile(&config(&[
            ("trades", vec![key("symbol", KeyType::Str)]),
            ("quotes", vec![key("px", KeyType::F64)]),
        ]))
        .unwrap();
        m.last_flushed_segment = 42;
        m.last_flush_at = Some(1_786_310_400_000_000);
        m.commit(dir.path()).unwrap();

        assert_eq!(Manifest::load(dir.path()).unwrap(), m);
        assert!(!dir.path().join(MANIFEST_TMP_FILE).exists());
    }

    #[test]
    fn load_tolerates_a_fresh_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), Manifest::default());
        assert_eq!(Manifest::default().last_flush_at, None);
    }

    #[test]
    fn exists_separates_a_fresh_directory_from_a_lost_manifest() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!Manifest::exists(dir.path()));

        Manifest::default().commit(dir.path()).unwrap();
        assert!(Manifest::exists(dir.path()));

        std::fs::remove_file(dir.path().join(MANIFEST_FILE)).unwrap();
        assert!(!Manifest::exists(dir.path()));
        assert_eq!(Manifest::load(dir.path()).unwrap(), Manifest::default());
    }

    #[test]
    fn load_discards_a_stale_staging_file() {
        let dir = tempfile::tempdir().unwrap();
        let committed = Manifest {
            last_flushed_segment: 7,
            ..Manifest::default()
        };
        committed.commit(dir.path()).unwrap();

        // Left by a crash before the rename.
        std::fs::write(
            dir.path().join(MANIFEST_TMP_FILE),
            br#"{"last_flushed_segment": 999}"#,
        )
        .unwrap();

        let loaded = Manifest::load(dir.path()).unwrap();
        assert_eq!(loaded.last_flushed_segment, 7);
        assert!(!dir.path().join(MANIFEST_TMP_FILE).exists());
    }

    #[test]
    fn commit_replaces_an_existing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        Manifest::default().commit(dir.path()).unwrap();
        let second = Manifest {
            last_flushed_segment: 3,
            last_flush_at: Some(1),
            ..Manifest::default()
        };
        second.commit(dir.path()).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), second);
    }

    #[test]
    fn a_corrupt_manifest_names_its_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILE), "{ not json").unwrap();
        let err = Manifest::load(dir.path()).unwrap_err().to_string();
        assert!(err.contains(MANIFEST_FILE), "{err}");
    }
}
