//! Reading back what the flush wrote.
//!
//! The engine is write-only by design — readers point DuckDB or polars at the
//! data directory. This module is not a query path and is not built for one: it
//! materialises whole rows as owned `String`s, which is exactly wrong for
//! scanning and exactly right for asserting that a file says what it should.
//!
//! It exists so the write path can be verified without pulling Arrow in, and so
//! integration tests can check output without a Parquet dev-dependency of their
//! own. Anything performance-sensitive should use a real reader.

use std::collections::HashMap;
use std::path::Path;

use parquet::file::metadata::SortingColumn;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row as ParquetRow};

use crate::error::{Error, Result};

fn read_err(path: &Path, e: parquet::errors::ParquetError) -> Error {
    Error::Config(format!("reading parquet {path:?}: {e}"))
}

/// One row, with every value rendered as text.
///
/// Types are deliberately flattened to `String`: callers are asserting on
/// content, and a single representation keeps those assertions readable. `keys`
/// is positional, matching the series' declared key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRow {
    pub ts: i64,
    pub id: String,
    pub keys: Vec<Option<String>>,
    /// Map entries in the order the file stores them.
    pub extra: Vec<(String, String)>,
    pub data: String,
}

/// What a reader can see about a file without decoding its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub key_value: HashMap<String, String>,
    pub sorting_columns: Vec<SortingColumn>,
    /// Rows in each row group, in order.
    pub row_group_rows: Vec<i64>,
    pub num_rows: i64,
    /// Leaf column paths, dotted, in schema order.
    pub columns: Vec<String>,
}

/// Render one field as text. `None` is a null.
fn field_text(f: &Field) -> Option<String> {
    match f {
        Field::Null => None,
        Field::Bool(v) => Some(v.to_string()),
        Field::Long(v) => Some(v.to_string()),
        Field::Int(v) => Some(v.to_string()),
        Field::Double(v) => Some(v.to_string()),
        Field::Float(v) => Some(v.to_string()),
        Field::Str(v) => Some(v.clone()),
        Field::TimestampMicros(v) => Some(v.to_string()),
        Field::TimestampMillis(v) => Some(v.to_string()),
        other => Some(format!("{other}")),
    }
}

/// The `ts` column, whose logical type makes the record reader hand back a
/// timestamp rather than a plain i64.
fn field_ts(f: &Field) -> Result<i64> {
    match f {
        Field::TimestampMicros(v) | Field::Long(v) => Ok(*v),
        other => Err(Error::Config(format!("ts is not an integer: {other}"))),
    }
}

fn row_from(row: &ParquetRow) -> Result<ReadRow> {
    let mut ts = None;
    let mut id = String::new();
    let mut keys = Vec::new();
    let mut extra = Vec::new();
    let mut data = String::new();

    for (name, field) in row.get_column_iter() {
        match name.as_str() {
            "ts" => ts = Some(field_ts(field)?),
            "id" => id = field_text(field).unwrap_or_default(),
            "data" => data = field_text(field).unwrap_or_default(),
            "extra" => match field {
                Field::MapInternal(m) => {
                    for (k, v) in m.entries() {
                        extra.push((
                            field_text(k).unwrap_or_default(),
                            field_text(v).unwrap_or_default(),
                        ));
                    }
                }
                Field::Null => {}
                other => {
                    return Err(Error::Config(format!("extra is not a map: {other}")));
                }
            },
            // Anything else is a declared key, and they arrive in schema order.
            _ => keys.push(field_text(field)),
        }
    }

    Ok(ReadRow {
        ts: ts.ok_or_else(|| Error::Config("row has no ts column".into()))?,
        id,
        keys,
        extra,
        data,
    })
}

/// Every row in a Parquet file, in file order.
pub fn read_file(path: &Path) -> Result<Vec<ReadRow>> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file).map_err(|e| read_err(path, e))?;
    let mut out = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| read_err(path, e))? {
        out.push(row_from(&row.map_err(|e| read_err(path, e))?)?);
    }
    Ok(out)
}

/// A file's metadata, without decoding any rows.
pub fn read_metadata(path: &Path) -> Result<FileMeta> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file).map_err(|e| read_err(path, e))?;
    let meta = reader.metadata();

    let key_value = meta
        .file_metadata()
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .map(|k| (k.key.clone(), k.value.clone().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();

    // Every row group carries the same declaration; the first is representative,
    // and an empty file has none.
    let sorting_columns = meta
        .row_groups()
        .first()
        .and_then(|rg| rg.sorting_columns().cloned())
        .unwrap_or_default();

    let columns = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.path().string())
        .collect();

    Ok(FileMeta {
        key_value,
        sorting_columns,
        row_group_rows: meta.row_groups().iter().map(|rg| rg.num_rows()).collect(),
        num_rows: meta.file_metadata().num_rows(),
        columns,
    })
}

/// `(ts, id)` of every row — the pair the flush sorts and deduplicates on.
pub fn read_ts_id(path: &Path) -> Result<Vec<(i64, String)>> {
    Ok(read_file(path)?.into_iter().map(|r| (r.ts, r.id)).collect())
}
