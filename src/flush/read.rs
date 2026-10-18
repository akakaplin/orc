//! Reading back flushed Parquet, for verification and tests only.
//!
//! Not a query path: rows are materialised as owned `String`s. Real readers
//! use DuckDB or polars.

use std::collections::HashMap;
use std::path::Path;

use parquet::file::metadata::SortingColumn;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row as ParquetRow};

use crate::error::{Error, Result};

fn read_err(path: &Path, e: parquet::errors::ParquetError) -> Error {
    Error::Config(format!("reading parquet {path:?}: {e}"))
}

/// One row, every value rendered as text. `keys` follow declared key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRow {
    /// Stored signed in Parquet; negative means not written by this engine.
    pub ts: u64,
    pub id: String,
    pub keys: Vec<Option<String>>,
    /// Map entries in the order the file stores them.
    pub extra: Vec<(String, String)>,
    pub data: String,
}

/// File metadata, without decoding rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub key_value: HashMap<String, String>,
    pub sorting_columns: Vec<SortingColumn>,
    pub row_group_rows: Vec<i64>,
    pub num_rows: i64,
    /// Dotted leaf column paths, in schema order.
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

/// `ts` arrives as a timestamp field (its logical type), not a plain i64.
fn field_ts(f: &Field) -> Result<u64> {
    let raw = match f {
        Field::TimestampMicros(v) | Field::Long(v) => *v,
        other => return Err(Error::Config(format!("ts is not an integer: {other}"))),
    };
    u64::try_from(raw).map_err(|_| Error::Config(format!("ts {raw} is before the epoch")))
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
            // Declared keys, in schema order.
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

    // All row groups declare the same order; an empty file has none.
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

/// `(ts, id)` of every row.
pub fn read_ts_id(path: &Path) -> Result<Vec<(u64, String)>> {
    Ok(read_file(path)?.into_iter().map(|r| (r.ts, r.id)).collect())
}
