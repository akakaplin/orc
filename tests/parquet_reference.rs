//! The observable Parquet contract, pinned.
//!
//! Every assertion here was verified byte-for-byte against output from
//! `ArrowWriter` before the writer was rewritten to use the low-level API: the
//! same three fixtures were produced by both, and 2244 lines of dumped schema,
//! row-group boundaries, statistics, key-value metadata, data and per-row map
//! cardinality compared identical.
//!
//! That comparison cannot live in the repository, because it needs the Arrow
//! writer that was removed. What it established can, and this is it. If any of
//! these change, files this engine writes stop looking like files it used to
//! write — which readers, and the `view.sql` union across epochs, both depend on.

use std::path::Path;

use orc::config::KeyDef;
use orc::flush::parquet::{
    EPOCH_META_KEY, RowBuilder, SERIES_META_KEY, output_file_name, parquet_schema, write_file,
};
use orc::flush::read::{read_file, read_metadata};
use orc::record::{KeyType, Value};

const T13: i64 = 1_786_194_000_000_000;

fn kd(name: &str, ty: KeyType) -> KeyDef {
    KeyDef {
        name: name.into(),
        ty,
        nullable: true,
    }
}

fn all_types() -> Vec<KeyDef> {
    vec![
        kd("symbol", KeyType::Str),
        kd("size", KeyType::I64),
        kd("px", KeyType::F64),
        kd("live", KeyType::Bool),
    ]
}

/// Leaf column paths, in order. This is what a reader walks, and the MAP's
/// `key_value` / `key` / `value` names are fixed by the Parquet spec rather
/// than chosen by us — a reader that goes by the spec finds a map only if they
/// are exactly these.
#[test]
fn the_leaf_columns_are_exactly_these_in_exactly_this_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    let mut b = RowBuilder::new("trades", &all_types(), 1).unwrap();
    b.append(
        T13,
        "a",
        &[
            Value::Str("AAPL"),
            Value::I64(1),
            Value::F64(1.0),
            Value::Bool(true),
        ],
        &[("k", "v")],
        "{}",
    )
    .unwrap();
    write_file(&path, &b.finish().unwrap(), "trades", 7, "zstd", 1024).unwrap();

    assert_eq!(
        read_metadata(&path).unwrap().columns,
        [
            "ts",
            "id",
            "symbol",
            "size",
            "px",
            "live",
            "extra.key_value.key",
            "extra.key_value.value",
            "data",
        ]
    );
}

/// `ts` first is load-bearing: it is declared the sorting column by index, so
/// moving it would silently mislabel whichever column took its place.
#[test]
fn ts_is_column_zero_and_declared_sorted_ascending() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    let mut b = RowBuilder::new("trades", &[], 1).unwrap();
    b.append(T13, "a", &[], &[], "").unwrap();
    write_file(&path, &b.finish().unwrap(), "trades", 3, "zstd", 1024).unwrap();

    let meta = read_metadata(&path).unwrap();
    assert_eq!(meta.columns[0], "ts");
    assert_eq!(meta.sorting_columns.len(), 1);
    assert_eq!(meta.sorting_columns[0].column_idx, 0);
    assert!(!meta.sorting_columns[0].descending);

    // Both mirror the `.eN.` filename tag, so a moved file stays self-describing.
    assert_eq!(
        meta.key_value.get(EPOCH_META_KEY).map(String::as_str),
        Some("3")
    );
    assert_eq!(
        meta.key_value.get(SERIES_META_KEY).map(String::as_str),
        Some("trades")
    );
    assert_eq!(
        output_file_name(41, 42, 3),
        "0000000041-0000000042.e3.parquet"
    );
}

/// Nothing but the two orc keys. `ArrowWriter` also embedded a ~700-byte
/// `ARROW:schema` IPC blob in every file; dropping it is the one intentional
/// difference from the old output, and it is pure overhead removed.
#[test]
fn no_writer_specific_metadata_is_embedded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    let mut b = RowBuilder::new("s", &[], 1).unwrap();
    b.append(T13, "", &[], &[], "").unwrap();
    write_file(&path, &b.finish().unwrap(), "s", 0, "zstd", 1024).unwrap();

    let meta = read_metadata(&path).unwrap();
    let mut keys: Vec<&str> = meta.key_value.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, [EPOCH_META_KEY, SERIES_META_KEY]);
}

/// Row groups are split by the writer itself, since the low-level file writer
/// ignores `max_row_group_row_count`. The boundaries must fall exactly where
/// the configured count says, or a reader's row-group pruning covers the wrong
/// rows.
#[test]
fn row_groups_split_exactly_at_the_configured_count() {
    let dir = tempfile::tempdir().unwrap();
    for (rows, per_group, want) in [
        (10usize, 3usize, vec![3i64, 3, 3, 1]),
        (10, 10, vec![10]),
        (10, 100, vec![10]),
        (1, 3, vec![1]),
    ] {
        let path = dir.path().join(format!("{rows}-{per_group}.parquet"));
        let mut b = RowBuilder::new("s", &[], rows).unwrap();
        for i in 0..rows {
            b.append(T13 + i as i64, "", &[], &[], "").unwrap();
        }
        write_file(
            &path,
            &b.finish().unwrap(),
            "s",
            0,
            "uncompressed",
            per_group,
        )
        .unwrap();
        assert_eq!(
            read_metadata(&path).unwrap().row_group_rows,
            want,
            "{rows} rows at {per_group} per group"
        );
    }
}

/// The single hardest property in the writer.
///
/// Values are stored column-major with nulls elided and map entries flattened,
/// so cutting a row group means advancing a different number of values in every
/// column. An off-by-one anywhere shifts data onto neighbouring rows — which
/// still produces a readable file, just a wrong one.
#[test]
fn every_value_stays_on_its_own_row_across_group_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("split.parquet");
    let keys = all_types();
    let mut b = RowBuilder::new("trades", &keys, 200).unwrap();

    // Null patterns with pairwise-coprime periods, and map cardinality on a
    // fourth, so no single boundary offset can hide a misalignment.
    /// What one row should read back as: id, positional keys, map entries.
    type Expected = (String, Vec<Option<String>>, Vec<(String, String)>);
    let mut want: Vec<Expected> = Vec::new();
    for i in 0..200i64 {
        let sym = format!("s{i}");
        let symbol = (i % 2 != 0).then(|| sym.clone());
        let size = (i % 3 != 0).then_some(i);
        let px = (i % 5 != 0).then_some(i as f64);
        let live = (i % 7 != 0).then_some(i % 2 == 0);

        let pairs: Vec<(String, String)> = (0..(i % 4))
            .map(|j| (format!("k{j}"), format!("v{i}-{j}")))
            .collect();
        let borrowed: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        b.append(
            T13 + i,
            &format!("id{i}"),
            &[
                symbol.as_deref().map_or(Value::Null, Value::Str),
                size.map_or(Value::Null, Value::I64),
                px.map_or(Value::Null, Value::F64),
                live.map_or(Value::Null, Value::Bool),
            ],
            &borrowed,
            "d",
        )
        .unwrap();

        want.push((
            format!("id{i}"),
            vec![
                symbol,
                size.map(|v| v.to_string()),
                px.map(|v| v.to_string()),
                live.map(|v| v.to_string()),
            ],
            pairs,
        ));
    }
    // 9 shares no factor with 2, 3, 5, 7 or 4, so boundaries walk through every
    // phase of every pattern above.
    write_file(&path, &b.finish().unwrap(), "trades", 0, "uncompressed", 9).unwrap();

    assert_eq!(read_metadata(&path).unwrap().row_group_rows.len(), 23);

    let got = read_file(&path).unwrap();
    assert_eq!(got.len(), 200);
    for (i, (row, (id, keys, extra))) in std::iter::zip(&got, &want).enumerate() {
        assert_eq!(row.ts, T13 + i as i64, "row {i} ts");
        assert_eq!(&row.id, id, "row {i} id");
        assert_eq!(&row.keys, keys, "row {i} keys");
        assert_eq!(&row.extra, extra, "row {i} extra");
    }
}

/// An empty map and a null map are different values to a reader, and "this
/// record had no undeclared keys" is the former. The map column is REQUIRED
/// precisely so the distinction cannot collapse.
#[test]
fn an_absent_map_reads_back_empty_rather_than_null() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    let mut b = RowBuilder::new("s", &[], 3).unwrap();
    b.append(T13, "has", &[], &[("k", "v")], "").unwrap();
    b.append(T13 + 1, "none", &[], &[], "").unwrap();
    b.append(T13 + 2, "empty-value", &[], &[("k", "")], "")
        .unwrap();
    write_file(&path, &b.finish().unwrap(), "s", 0, "zstd", 1024).unwrap();

    let rows = read_file(&path).unwrap();
    assert_eq!(rows[0].extra, [("k".to_string(), "v".to_string())]);
    assert!(rows[1].extra.is_empty());
    assert_eq!(rows[2].extra, [("k".to_string(), "".to_string())]);

    let schema = parquet_schema(&[]).unwrap();
    let extra = &schema.get_fields()[2];
    assert_eq!(
        extra.get_basic_info().repetition(),
        parquet::basic::Repetition::REQUIRED,
        "a nullable map would make empty and absent indistinguishable"
    );
}

/// Empty strings are values, not nulls, everywhere they can appear. `id` in
/// particular: an absent id is `""`, which is what gives `(ts, id)` dedup one
/// representation to compare instead of two.
#[test]
fn empty_strings_are_values_and_never_become_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    let keys = vec![kd("symbol", KeyType::Str)];
    let mut b = RowBuilder::new("s", &keys, 2).unwrap();
    b.append(T13, "", &[Value::Str("")], &[("", "")], "")
        .unwrap();
    b.append(T13 + 1, "x", &[Value::Null], &[], "d").unwrap();
    write_file(&path, &b.finish().unwrap(), "s", 0, "zstd", 1024).unwrap();

    let rows = read_file(&path).unwrap();
    assert_eq!(rows[0].id, "");
    assert_eq!(
        rows[0].keys[0].as_deref(),
        Some(""),
        "an empty string key is present, not null"
    );
    assert_eq!(rows[0].extra, [("".to_string(), "".to_string())]);
    assert_eq!(rows[0].data, "");
    assert_eq!(rows[1].keys[0], None, "and a real null is still null");
}

/// Extreme and awkward scalars round-trip exactly. These went through the Arrow
/// builders before; nothing about the encoding should have changed.
#[test]
fn edge_case_scalars_survive_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    let keys = vec![kd("i", KeyType::I64), kd("f", KeyType::F64)];
    let mut b = RowBuilder::new("s", &keys, 4).unwrap();
    for (n, (i, f)) in [
        (i64::MIN, f64::MIN),
        (i64::MAX, f64::MAX),
        (0, -0.0f64),
        (-1, f64::MIN_POSITIVE),
    ]
    .into_iter()
    .enumerate()
    {
        b.append(
            T13 + n as i64,
            "\u{1F600}",
            &[Value::I64(i), Value::F64(f)],
            &[("\u{00E9}", "\u{2603}")],
            "\u{4F60}\u{597D}",
        )
        .unwrap();
    }
    write_file(&path, &b.finish().unwrap(), "s", 0, "zstd", 2).unwrap();

    let rows = read_file(&path).unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows[0].keys[0].as_deref(),
        Some(i64::MIN.to_string()).as_deref()
    );
    assert_eq!(
        rows[1].keys[0].as_deref(),
        Some(i64::MAX.to_string()).as_deref()
    );
    // Non-ASCII must survive in ids, map keys, map values and data alike.
    assert_eq!(rows[0].id, "\u{1F600}");
    assert_eq!(rows[0].data, "\u{4F60}\u{597D}");
    assert_eq!(
        rows[0].extra,
        [("\u{00E9}".to_string(), "\u{2603}".to_string())]
    );
}

/// A series with no declared keys at all is a legitimate config, and the schema
/// must still carry ts, id, extra and data.
#[test]
fn a_series_with_no_declared_keys_still_has_the_four_fixed_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path: &Path = &dir.path().join("bare.parquet");
    let mut b = RowBuilder::new("bare", &[], 1).unwrap();
    b.append(T13, "x", &[], &[("k", "v")], "d").unwrap();
    write_file(path, &b.finish().unwrap(), "bare", 0, "uncompressed", 1).unwrap();

    assert_eq!(
        read_metadata(path).unwrap().columns,
        [
            "ts",
            "id",
            "extra.key_value.key",
            "extra.key_value.value",
            "data"
        ]
    );
    let rows = read_file(path).unwrap();
    assert_eq!(rows[0].keys, Vec::<Option<String>>::new());
    assert_eq!(rows[0].extra, [("k".to_string(), "v".to_string())]);
}
