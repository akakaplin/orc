//! `manifest.json` — schema history and the flush commit point.
//!
//! The manifest holds the two things that cannot be recovered by looking at the
//! data directory: what each series' key list was at each schema epoch, and how
//! far the flush has got.
//!
//! It is deliberately **O(epochs), never O(files)**. A file inventory would grow
//! ~8,760 entries per series per year and be rewritten on the crash-critical path
//! every hour; the epoch tag in each filename (`…-….e7.parquet`) carries the
//! file→schema association instead, so a series whose keys never change keeps one
//! history entry forever.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::{Config, KeyDef, key_type_name};
use crate::error::{Error, Result};

/// Committed manifest filename, relative to `data_dir`.
pub const MANIFEST_FILE: &str = "manifest.json";
/// Staging filename. Lives next to the manifest, not in `tmp/`, so the rename
/// is guaranteed to stay within one filesystem and therefore atomic.
pub const MANIFEST_TMP_FILE: &str = "manifest.json.tmp";

/// One entry of a series' schema history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochSchema {
    pub epoch: u32,
    /// The declared keys in force at this epoch, in positional order. Frames
    /// stamped with this epoch are decoded against exactly this list.
    pub keys: Vec<KeyDef>,
}

/// The contents of `manifest.json`.
///
/// Unknown fields are tolerated, unlike in `Config`: the engine writes this, so
/// the useful property is that an older binary can read a newer one's manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Manifest {
    /// Per-series schema history, oldest epoch first. `BTreeMap` so the
    /// serialized file has a stable key order and a diff between two commits
    /// shows only what actually changed.
    pub series: BTreeMap<String, Vec<EpochSchema>>,
    /// Every WAL segment with `id <= last_flushed_segment` is durable in
    /// Parquet and is deleted at startup.
    pub last_flushed_segment: u64,
    /// When the last flush committed, in epoch microseconds.
    ///
    /// The next flush is scheduled relative to *this*, not to process start: an
    /// engine restarting every few minutes would otherwise never reach an hourly
    /// timer. `None` means never flushed, making the first startup flush
    /// unconditional.
    pub last_flush_at: Option<u64>,
}

impl Manifest {
    /// Read `<data_dir>/manifest.json`, tolerating a fresh directory.
    ///
    /// A missing file is not an error — it is what an unopened data directory
    /// looks like. Any `manifest.json.tmp` is discarded first: it can only come
    /// from a crash between the staging write and the rename, so the flush it
    /// belonged to never committed and its segments will be replayed.
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

    /// Is there a committed manifest in this directory?
    ///
    /// [`Manifest::load`] cannot answer this — a fresh directory and one whose
    /// manifest went missing both come back as [`Manifest::default`] — and the
    /// two call for opposite responses. The second has lost the only record of
    /// how far the flush got, and
    /// [`sweep_uncommitted`](crate::flush::planner::sweep_uncommitted) would read
    /// the resulting `last_flushed_segment: 0` as "every Parquet file here is
    /// uncommitted".
    pub fn exists(data_dir: impl AsRef<Path>) -> bool {
        data_dir.as_ref().join(MANIFEST_FILE).is_file()
    }

    /// Atomically replace `<data_dir>/manifest.json`.
    ///
    /// **This is the flush commit point, and the order below is the durability
    /// guarantee — it must be exactly:**
    ///
    /// 1. write `manifest.json.tmp`
    /// 2. `fsync` that file — otherwise the rename can be durable while the
    ///    bytes it points at are not, and a power cut leaves a committed
    ///    manifest with truncated or zero-filled contents
    /// 3. `rename` over `manifest.json` — atomic, so a reader sees either the
    ///    whole old manifest or the whole new one, never a half-written file
    /// 4. `fsync` the directory — otherwise the rename itself is not durable,
    ///    and a power cut can resurrect the old manifest even though the new
    ///    file's contents were synced
    ///
    /// Step 3 is the point either side of which a crash is survivable — see
    /// [`crate::flush::planner`] for what each side leaves behind.
    pub fn commit(&self, data_dir: impl AsRef<Path>) -> Result<()> {
        use std::io::Write;

        let data_dir = data_dir.as_ref();
        let tmp: PathBuf = data_dir.join(MANIFEST_TMP_FILE);
        let final_path = data_dir.join(MANIFEST_FILE);

        // Pretty-printed: this file is read by humans debugging a stalled flush
        // far more often than it is written, and it is at most a few KiB.
        let bytes = serde_json::to_vec_pretty(self)?;

        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
        drop(f);

        std::fs::rename(&tmp, &final_path)?;

        // Syncing the *directory* is what makes the rename itself durable.
        std::fs::File::open(data_dir)?.sync_all()?;
        Ok(())
    }

    /// Fold a config into the schema history, bumping epochs where needed.
    ///
    /// The epoch is **per series**. A global counter would fragment bystanders:
    /// adding a key to `trades` would roll `quotes` from `.e7.` to `.e8.` too,
    /// splitting its files across two epochs describing identical schemas.
    ///
    /// Adding, removing and reordering keys are safe — readers union by name, and
    /// frames are positional *per epoch*. **Retyping is rejected**, because
    /// `union_by_name` cannot reconcile a `utf8` column with an `int64` one of
    /// the same name. Rename the key, or start a new series.
    ///
    /// Series in the history but absent from the config are left alone; re-adding
    /// the name resumes the same epoch if the keys match. All-or-nothing: every
    /// series is checked before any is mutated, so a rejected retype cannot leave
    /// the manifest half-updated.
    ///
    /// Returns whether anything changed, so the caller knows whether it owes a
    /// [`Manifest::commit`]. An epoch that exists only in memory is worse than
    /// none — frames stamped with it are already durable, and the next start
    /// would mint the same number for a different key list.
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

            // An epoch identifies a *layout*: positional order and types.
            // `nullable` is neither -- flipping it leaves the frame encoding and
            // the Parquet schema byte-identical -- so bumping for it would split
            // a series across two epochs describing the same thing.
            let same_layout = history.last().is_some_and(|last| {
                last.keys.len() == series.keys.len()
                    && std::iter::zip(&last.keys, &series.keys)
                        .all(|(a, b)| a.name == b.name && a.ty == b.ty)
            });
            if same_layout {
                // Refresh in place so the history still reports what ingest
                // currently enforces, without minting an epoch for it.
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

    /// The key list a frame stamped with `epoch` must be decoded against.
    ///
    /// `None` becomes a [`FrameFault::UnknownSchema`] at the reader, which the
    /// flush counts and skips: old epochs are never removed, so a frame naming
    /// one that is absent can only come from a truncated or hand-edited
    /// manifest.
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

/// Reject a key whose type differs from any it has ever had.
///
/// The whole history, not just the latest epoch: `union_by_name` spans every
/// epoch's files, so dropping a `string` key and re-adding it as an `i64` two
/// epochs later breaks the same query as changing it in place.
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

    /// The caller commits on `true` and skips the write on `false`, so a wrong
    /// answer either loses a minted epoch or rewrites the file on every restart.
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

        // A nullable-only edit mints no epoch but still rewrites the history,
        // so it has to count as a change or the file goes stale.
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
        // The bystander keeps its epoch *and* its history verbatim -- this is
        // the whole reason the epoch is per series rather than global.
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
        // A rejected reconcile must leave the history untouched.
        assert_eq!(m.current_epoch("t"), Some(0));
    }

    #[test]
    fn retyping_a_key_removed_two_epochs_ago_is_still_rejected() {
        // Old Parquet still carries the column, and readers union by name across
        // every epoch -- so the gap does not make this safe.
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
        // An epoch names a layout. `nullable` changes neither the frame encoding
        // nor the Parquet schema (key columns are always written nullable), so
        // minting one here would fragment the series' files across two epochs
        // describing the same thing -- while the history must still report what
        // ingest now enforces.
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
        // The staging file must not survive a successful commit.
        assert!(!dir.path().join(MANIFEST_TMP_FILE).exists());
    }

    #[test]
    fn load_tolerates_a_fresh_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), Manifest::default());
        assert_eq!(Manifest::default().last_flush_at, None);
    }

    /// `load` cannot distinguish "fresh" from "lost". This is how the caller
    /// tells them apart.
    #[test]
    fn exists_separates_a_fresh_directory_from_a_lost_manifest() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!Manifest::exists(dir.path()));

        Manifest::default().commit(dir.path()).unwrap();
        assert!(Manifest::exists(dir.path()));

        std::fs::remove_file(dir.path().join(MANIFEST_FILE)).unwrap();
        assert!(!Manifest::exists(dir.path()));
        // ...while `load` reports the same thing in both cases.
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

        // A crash between the staging write and the rename leaves this behind,
        // describing a flush that never committed.
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
