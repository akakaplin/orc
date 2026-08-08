//! Building column chunks and writing them as Parquet.
//!
//! This is the only place the engine's record model meets the Parquet format.
//! One file holds exactly one `(series, epoch)` pair, so the schema here is fixed
//! for the lifetime of a file and never has to change mid-write.
//!
//! Column order is `ts, id, <declared keys...>, extra, data`. `ts` first is
//! deliberate: it is declared as the file's sorting column, and readers prune
//! row groups on its statistics.
//!
//! # Why this is written against the low-level API
//!
//! `ArrowWriter` would do the same job, but reaching it means enabling
//! `parquet/arrow`, which is all-or-nothing. Measured, it costs twelve crates —
//! the six `arrow-*`, plus `arrow-ipc`'s `flatbuffers` and `bitflags`, plus
//! `base64`, `num-complex`, `rustc_version` and `semver` — and five seconds of
//! clean build, for a schema of four scalar types and one map. `arrow-ipc` in
//! particular is pure toll: this engine never touches IPC, but the feature has
//! no finer grain than "all of it". It also embeds a ~700-byte `ARROW:schema`
//! blob in every file we write.
//!
//! The only thing it actually does for us is compute definition and repetition
//! levels, which for this schema is the twenty lines in [`Columns::write_group`].
//!
//! Everything a reader can observe is unchanged, and
//! `tests/parquet_reference.rs` pins that: identical physical schema, identical
//! data, identical row-group boundaries, identical statistics.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::{
    Compression, ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType,
};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{Type, TypePtr};

use crate::config::{KeyDef, key_type_name};
use crate::error::{Error, Result};
use crate::record::{KeyType, Value};

/// Parquet key-value metadata key recording which schema epoch wrote a file.
///
/// Mirrors the `.eN.` tag in the filename. The filename is what `view.sql`
/// generation reads (so the manifest never needs a file inventory); this copy
/// survives a rename, so a file that gets moved is still self-describing.
pub const EPOCH_META_KEY: &str = "orc.schema_epoch";

/// Metadata key recording the series name, for the same reason.
pub const SERIES_META_KEY: &str = "orc.series";

/// Index of the `ts` column. Fixed, and relied on by the sorting-column
/// declaration below.
const TS_COLUMN_IDX: i32 = 0;

/// Compression codecs the pinned `parquet` feature set can actually write.
///
/// Config validation checks against this at load, so an unwritable codec fails
/// immediately rather than at the first flush an hour later, with a WAL that has
/// nowhere to go. Keeping the list here — next to the match that implements it —
/// is what stops the two drifting apart.
pub const SUPPORTED_COMPRESSION: [&str; 4] = ["lz4_raw", "lz4", "uncompressed", "none"];

fn schema_err(what: &str, e: parquet::errors::ParquetError) -> Error {
    Error::Config(format!("building parquet schema ({what}): {e}"))
}

/// The Parquet schema for one series at one epoch.
///
/// Built programmatically rather than through `parse_message_type`: key names
/// come from user config and a string-built schema would need escaping rules
/// for whatever they contain.
///
/// **Every key column is OPTIONAL regardless of its config `nullable`.** That is
/// not an oversight: nullability is enforced at ingest, and writing a column as
/// REQUIRED would mean a column added in a later epoch could not read back as
/// NULL from older files, breaking the union that `view.sql` depends on.
pub fn parquet_schema(keys: &[KeyDef]) -> Result<TypePtr> {
    let mut fields: Vec<TypePtr> = Vec::with_capacity(keys.len() + 4);

    fields.push(Arc::new(
        Type::primitive_type_builder("ts", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::timestamp(true, TimeUnit::MICROS)))
            // Set alongside the logical type so readers that predate logical
            // types still see a UTC microsecond timestamp rather than an i64.
            .with_converted_type(ConvertedType::TIMESTAMP_MICROS)
            .build()
            .map_err(|e| schema_err("ts", e))?,
    ));

    // Never null: an absent id is the empty string, so `(ts, id)` dedup has one
    // representation to compare rather than two that mean the same thing.
    fields.push(Arc::new(utf8("id", Repetition::REQUIRED)?));

    for k in keys {
        let physical = match k.ty {
            KeyType::Bool => PhysicalType::BOOLEAN,
            KeyType::I64 => PhysicalType::INT64,
            KeyType::F64 => PhysicalType::DOUBLE,
            KeyType::Str => PhysicalType::BYTE_ARRAY,
        };
        let field = if k.ty == KeyType::Str {
            utf8(&k.name, Repetition::OPTIONAL)?
        } else {
            Type::primitive_type_builder(&k.name, physical)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .map_err(|e| schema_err(&k.name, e))?
        };
        fields.push(Arc::new(field));
    }

    // The MAP shape the Parquet spec fixes: a REQUIRED group annotated MAP,
    // holding one REPEATED group named `key_value` with a REQUIRED `key` and an
    // OPTIONAL `value`. The names are not ours to choose -- readers that go by
    // the spec rather than by a writer's conventions rely on exactly these.
    let key_value = Type::group_type_builder("key_value")
        .with_repetition(Repetition::REPEATED)
        .with_fields(vec![
            Arc::new(utf8("key", Repetition::REQUIRED)?),
            Arc::new(utf8("value", Repetition::OPTIONAL)?),
        ])
        .build()
        .map_err(|e| schema_err("key_value", e))?;
    fields.push(Arc::new(
        Type::group_type_builder("extra")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value)])
            .build()
            .map_err(|e| schema_err("extra", e))?,
    ));

    fields.push(Arc::new(utf8("data", Repetition::REQUIRED)?));

    Ok(Arc::new(
        Type::group_type_builder("orc")
            .with_fields(fields)
            .build()
            .map_err(|e| schema_err("message", e))?,
    ))
}

/// A UTF-8 string column. `ConvertedType::UTF8` is set alongside the logical
/// type so readers that predate logical types still see a string.
fn utf8(name: &str, rep: Repetition) -> Result<Type> {
    Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
        .with_repetition(rep)
        .with_logical_type(Some(LogicalType::String))
        .with_converted_type(ConvertedType::UTF8)
        .build()
        .map_err(|e| schema_err(name, e))
}

/// One declared key column: the present values, plus a definition level per row.
///
/// `defs` has one entry per row (1 present, 0 null) while `vals` has one entry
/// per *present* row. That asymmetry is the whole encoding: Parquet stores only
/// the values that exist and reconstructs the nulls from the levels.
#[derive(Debug)]
enum KeyColumn {
    Bool {
        vals: Vec<bool>,
        defs: Vec<i16>,
    },
    I64 {
        vals: Vec<i64>,
        defs: Vec<i16>,
    },
    F64 {
        vals: Vec<f64>,
        defs: Vec<i16>,
    },
    Str {
        vals: Vec<ByteArray>,
        defs: Vec<i16>,
    },
}

impl KeyColumn {
    fn new(ty: KeyType, cap: usize) -> Self {
        match ty {
            KeyType::Bool => KeyColumn::Bool {
                vals: Vec::with_capacity(cap),
                defs: Vec::with_capacity(cap),
            },
            KeyType::I64 => KeyColumn::I64 {
                vals: Vec::with_capacity(cap),
                defs: Vec::with_capacity(cap),
            },
            KeyType::F64 => KeyColumn::F64 {
                vals: Vec::with_capacity(cap),
                defs: Vec::with_capacity(cap),
            },
            KeyType::Str => KeyColumn::Str {
                vals: Vec::with_capacity(cap),
                defs: Vec::with_capacity(cap),
            },
        }
    }

    fn key_type(&self) -> KeyType {
        match self {
            KeyColumn::Bool { .. } => KeyType::Bool,
            KeyColumn::I64 { .. } => KeyType::I64,
            KeyColumn::F64 { .. } => KeyType::F64,
            KeyColumn::Str { .. } => KeyType::Str,
        }
    }

    fn defs(&self) -> &[i16] {
        match self {
            KeyColumn::Bool { defs, .. }
            | KeyColumn::I64 { defs, .. }
            | KeyColumn::F64 { defs, .. }
            | KeyColumn::Str { defs, .. } => defs,
        }
    }

    fn append(&mut self, v: &Value<'_>, series: &str, key: &str) -> Result<()> {
        // Read the expected type before the match borrows `self` mutably.
        let expected = self.key_type();
        let got = v.key_type();
        match (self, v) {
            (KeyColumn::Bool { vals, defs }, Value::Bool(x)) => {
                vals.push(*x);
                defs.push(1);
            }
            (KeyColumn::I64 { vals, defs }, Value::I64(x)) => {
                vals.push(*x);
                defs.push(1);
            }
            (KeyColumn::F64 { vals, defs }, Value::F64(x)) => {
                vals.push(*x);
                defs.push(1);
            }
            (KeyColumn::Str { vals, defs }, Value::Str(x)) => {
                vals.push(ByteArray::from(x.as_bytes().to_vec()));
                defs.push(1);
            }
            (KeyColumn::Bool { defs, .. }, Value::Null)
            | (KeyColumn::I64 { defs, .. }, Value::Null)
            | (KeyColumn::F64 { defs, .. }, Value::Null)
            | (KeyColumn::Str { defs, .. }, Value::Null) => defs.push(0),
            _ => {
                return Err(Error::TypeMismatch {
                    series: series.to_string(),
                    key: key.to_string(),
                    expected,
                    got,
                });
            }
        }
        Ok(())
    }
}

/// Where each column has been written up to, so successive row groups pick up
/// where the last left off.
///
/// Values are not indexed by row: a nullable column skips nulls and the map
/// columns hold a variable number of entries per row. Carrying a cursor forward
/// is what keeps splitting O(rows) overall instead of rescanning the prefix for
/// every group.
#[derive(Debug, Default)]
struct Cursors {
    keys: Vec<usize>,
    extra: usize,
}

/// Accumulated rows for one `(series, epoch)`, ready to write.
///
/// Rows arrive already sorted and deduplicated; this type only materialises
/// them. It is the first point in the flush where a record becomes owned data,
/// which is why the sort upstream works on an index instead.
#[derive(Debug)]
pub struct Columns {
    series: String,
    schema: TypePtr,
    key_names: Vec<String>,
    rows: usize,
    ts: Vec<i64>,
    id: Vec<ByteArray>,
    keys: Vec<KeyColumn>,
    /// Flattened map entries, and how many belong to each row. The counts are
    /// what let a row group be cut without ever splitting a row's entries.
    extra_keys: Vec<ByteArray>,
    extra_values: Vec<ByteArray>,
    extra_counts: Vec<u32>,
    data: Vec<ByteArray>,
}

/// Accumulates rows into [`Columns`].
///
/// A separate type from `Columns` only so `finish()` marks the point after which
/// nothing more can be appended.
#[derive(Debug)]
pub struct RowBuilder {
    cols: Columns,
}

impl RowBuilder {
    pub fn new(series: &str, key_defs: &[KeyDef], cap: usize) -> Result<Self> {
        Ok(Self {
            cols: Columns {
                series: series.to_string(),
                schema: parquet_schema(key_defs)?,
                key_names: key_defs.iter().map(|k| k.name.clone()).collect(),
                rows: 0,
                ts: Vec::with_capacity(cap),
                id: Vec::with_capacity(cap),
                keys: key_defs.iter().map(|k| KeyColumn::new(k.ty, cap)).collect(),
                extra_keys: Vec::new(),
                extra_values: Vec::new(),
                extra_counts: Vec::with_capacity(cap),
                data: Vec::with_capacity(cap),
            },
        })
    }

    pub fn schema(&self) -> TypePtr {
        Arc::clone(&self.cols.schema)
    }

    pub fn len(&self) -> usize {
        self.cols.rows
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append one row. `keys` must have exactly the declared arity — ingest
    /// already guaranteed that, so a mismatch here means a frame was decoded
    /// under the wrong epoch, which is worth an error rather than a silent
    /// short row.
    pub fn append(
        &mut self,
        ts: i64,
        id: &str,
        keys: &[Value<'_>],
        extra: &[(&str, &str)],
        data: &str,
    ) -> Result<()> {
        let c = &mut self.cols;
        if keys.len() != c.keys.len() {
            return Err(Error::KeyArity {
                series: c.series.clone(),
                expected: c.keys.len(),
                got: keys.len(),
            });
        }
        c.ts.push(ts);
        c.id.push(ByteArray::from(id.as_bytes().to_vec()));
        for (i, (column, value)) in std::iter::zip(&mut c.keys, keys).enumerate() {
            column.append(value, &c.series, &c.key_names[i])?;
        }
        for (k, v) in extra {
            c.extra_keys.push(ByteArray::from(k.as_bytes().to_vec()));
            c.extra_values.push(ByteArray::from(v.as_bytes().to_vec()));
        }
        // A row with no undeclared keys gets a count of 0, which encodes as an
        // *empty* map rather than a null one. The two are different values to a
        // reader, and "this record had no undeclared keys" is the former.
        c.extra_counts.push(extra.len() as u32);
        c.data.push(ByteArray::from(data.as_bytes().to_vec()));
        c.rows += 1;
        Ok(())
    }

    /// Freeze the accumulated rows.
    pub fn finish(self) -> Result<Columns> {
        Ok(self.cols)
    }
}

impl Columns {
    pub fn num_rows(&self) -> usize {
        self.rows
    }

    pub fn schema(&self) -> TypePtr {
        Arc::clone(&self.schema)
    }

    /// Write rows `[start, end)` as one row group.
    ///
    /// Columns must be handed to `next_column()` in schema order and each closed
    /// before the next is opened; the writer enforces that, and the order here
    /// is the order [`parquet_schema`] declares.
    fn write_group<W: std::io::Write + Send>(
        &self,
        rg: &mut parquet::file::writer::SerializedRowGroupWriter<'_, W>,
        start: usize,
        end: usize,
        cursors: &mut Cursors,
    ) -> Result<()> {
        let io = |e: parquet::errors::ParquetError| Error::Config(format!("writing parquet: {e}"));

        // -- ts: REQUIRED, so no levels are needed at all ---------------------
        let mut c = rg.next_column().map_err(io)?.expect("ts column");
        match c.untyped() {
            ColumnWriter::Int64ColumnWriter(w) => w
                .write_batch(&self.ts[start..end], None, None)
                .map_err(io)?,
            _ => unreachable!("ts is INT64"),
        };
        c.close().map_err(io)?;

        // -- id: REQUIRED -----------------------------------------------------
        let mut c = rg.next_column().map_err(io)?.expect("id column");
        match c.untyped() {
            ColumnWriter::ByteArrayColumnWriter(w) => w
                .write_batch(&self.id[start..end], None, None)
                .map_err(io)?,
            _ => unreachable!("id is BYTE_ARRAY"),
        };
        c.close().map_err(io)?;

        // -- declared keys: OPTIONAL, def level 1 present / 0 null ------------
        for (i, column) in self.keys.iter().enumerate() {
            let defs = &column.defs()[start..end];
            // Only present rows contributed a value, so the slice of values this
            // group owns is as long as the number of 1s in its levels.
            let present = defs.iter().filter(|&&d| d == 1).count();
            let from = cursors.keys[i];
            let to = from + present;
            cursors.keys[i] = to;

            let mut c = rg.next_column().map_err(io)?.expect("key column");
            match (c.untyped(), column) {
                (ColumnWriter::BoolColumnWriter(w), KeyColumn::Bool { vals, .. }) => w
                    .write_batch(&vals[from..to], Some(defs), None)
                    .map_err(io)?,
                (ColumnWriter::Int64ColumnWriter(w), KeyColumn::I64 { vals, .. }) => w
                    .write_batch(&vals[from..to], Some(defs), None)
                    .map_err(io)?,
                (ColumnWriter::DoubleColumnWriter(w), KeyColumn::F64 { vals, .. }) => w
                    .write_batch(&vals[from..to], Some(defs), None)
                    .map_err(io)?,
                (ColumnWriter::ByteArrayColumnWriter(w), KeyColumn::Str { vals, .. }) => w
                    .write_batch(&vals[from..to], Some(defs), None)
                    .map_err(io)?,
                _ => unreachable!("column writer disagrees with the schema we built"),
            };
            c.close().map_err(io)?;
        }

        // -- extra: the only place levels are non-trivial ---------------------
        //
        // `key` sits inside a REPEATED group under a REQUIRED one, so its max
        // definition level is 1 and its max repetition level is 1. `value` is
        // OPTIONAL inside that group, so its max definition level is 2.
        //
        //   empty map   -> def 0, rep 0   (one slot standing for "no entries")
        //   entry 0     -> def 1/2, rep 0 (rep 0 opens a new row)
        //   entry i > 0 -> def 1/2, rep 1 (rep 1 continues this row's list)
        //
        // A row's entries are always written together, so a row group boundary
        // can never land inside a list and the first record of every group is
        // guaranteed rep 0 — which is what the format requires.
        let entries: usize = self.extra_counts[start..end]
            .iter()
            .map(|&n| n as usize)
            .sum();
        let slots = entries
            + self.extra_counts[start..end]
                .iter()
                .filter(|&&n| n == 0)
                .count();

        let mut key_defs: Vec<i16> = Vec::with_capacity(slots);
        let mut value_defs: Vec<i16> = Vec::with_capacity(slots);
        let mut reps: Vec<i16> = Vec::with_capacity(slots);
        for &count in &self.extra_counts[start..end] {
            if count == 0 {
                key_defs.push(0);
                value_defs.push(0);
                reps.push(0);
                continue;
            }
            for i in 0..count {
                key_defs.push(1);
                // We never append a null value, so a present entry is always at
                // the maximum definition level.
                value_defs.push(2);
                reps.push(if i == 0 { 0 } else { 1 });
            }
        }

        let from = cursors.extra;
        let to = from + entries;
        cursors.extra = to;

        let mut c = rg.next_column().map_err(io)?.expect("extra.key column");
        match c.untyped() {
            ColumnWriter::ByteArrayColumnWriter(w) => w
                .write_batch(&self.extra_keys[from..to], Some(&key_defs), Some(&reps))
                .map_err(io)?,
            _ => unreachable!("extra.key is BYTE_ARRAY"),
        };
        c.close().map_err(io)?;

        let mut c = rg.next_column().map_err(io)?.expect("extra.value column");
        match c.untyped() {
            ColumnWriter::ByteArrayColumnWriter(w) => w
                .write_batch(&self.extra_values[from..to], Some(&value_defs), Some(&reps))
                .map_err(io)?,
            _ => unreachable!("extra.value is BYTE_ARRAY"),
        };
        c.close().map_err(io)?;

        // -- data: REQUIRED ---------------------------------------------------
        let mut c = rg.next_column().map_err(io)?.expect("data column");
        match c.untyped() {
            ColumnWriter::ByteArrayColumnWriter(w) => w
                .write_batch(&self.data[start..end], None, None)
                .map_err(io)?,
            _ => unreachable!("data is BYTE_ARRAY"),
        };
        c.close().map_err(io)?;

        Ok(())
    }
}

/// Writer properties for a flush output file.
///
/// Declaring `ts` as a sorting column is not decoration: the rows really are
/// sorted by it, and a reader that trusts the declaration can skip row groups
/// on statistics alone. It would be actively harmful to set this if the sort
/// upstream were ever dropped.
pub fn writer_properties(
    series: &str,
    epoch: u32,
    compression: &str,
    _row_group_rows: usize,
) -> Result<WriterProperties> {
    let codec = match compression {
        // LZ4_RAW (codec 7, Parquet 2.9), never the deprecated LZ4 (codec 5),
        // whose non-standard Hadoop framing is why it was deprecated. Bare
        // "lz4" is accepted as a spelling of the same thing rather than
        // rejected, because the one it might have meant is not one we write.
        "lz4_raw" | "lz4" => Compression::LZ4_RAW,
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            return Err(Error::Config(format!(
                "unsupported compression {other:?}; supported: {}",
                SUPPORTED_COMPRESSION.join(", ")
            )));
        }
    };
    // Note: `set_max_row_group_row_count` is deliberately NOT set. Only
    // `ArrowWriter` consults it; `SerializedFileWriter` splits row groups
    // wherever the caller calls `next_row_group()`, so `write_file` owns the
    // split and setting the property here would be a claim nothing enforces.
    Ok(WriterProperties::builder()
        .set_compression(codec)
        .set_sorting_columns(Some(vec![SortingColumn {
            column_idx: TS_COLUMN_IDX,
            descending: false,
            // ts is non-null, so this is moot; stated for the reader's benefit.
            nulls_first: false,
        }]))
        .set_key_value_metadata(Some(vec![
            KeyValue::new(EPOCH_META_KEY.to_string(), epoch.to_string()),
            KeyValue::new(SERIES_META_KEY.to_string(), series.to_string()),
        ]))
        .build())
}

/// Write `cols` to `path` as a complete Parquet file, then fsync it.
///
/// The fsync matters: the flush renames this file into `series/` and only then
/// commits the manifest. If the contents were not durable before the rename, a
/// power cut could leave a committed manifest pointing at a file with a valid
/// name and no data.
pub fn write_file(
    path: &Path,
    cols: &Columns,
    series: &str,
    epoch: u32,
    compression: &str,
    row_group_rows: usize,
) -> Result<()> {
    let props = writer_properties(series, epoch, compression, row_group_rows)?;
    let file = File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, cols.schema(), Arc::new(props))
        .map_err(|e| Error::Config(format!("opening parquet writer for {path:?}: {e}")))?;

    let group_rows = row_group_rows.max(1);
    let mut cursors = Cursors {
        keys: vec![0; cols.keys.len()],
        extra: 0,
    };
    let mut start = 0;
    while start < cols.rows {
        let end = (start + group_rows).min(cols.rows);
        let mut rg = writer
            .next_row_group()
            .map_err(|e| Error::Config(format!("starting a row group in {path:?}: {e}")))?;
        cols.write_group(&mut rg, start, end, &mut cursors)?;
        rg.close()
            .map_err(|e| Error::Config(format!("closing a row group in {path:?}: {e}")))?;
        start = end;
    }

    let file = writer
        .into_inner()
        .map_err(|e| Error::Config(format!("closing parquet {path:?}: {e}")))?;
    file.sync_all()?;
    Ok(())
}

/// The name of a flush output file: segment range, epoch tag, extension.
///
/// The segment range makes a re-run after a crash produce the *same* filename
/// rather than a duplicate, which is what makes flush idempotent. The epoch tag
/// is what lets `view.sql` be regenerated without the manifest tracking files.
pub fn output_file_name(first_segment: u64, last_segment: u64, epoch: u32) -> String {
    format!("{first_segment:010}-{last_segment:010}.e{epoch}.parquet")
}

/// Human-readable summary of a written file, for logs and `stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    pub series: String,
    pub epoch: u32,
    pub hour: String,
    pub rows: usize,
}

/// Column names in schema order — useful for generating `view.sql`.
pub fn column_names(keys: &[KeyDef]) -> Vec<String> {
    let mut v = vec!["ts".to_string(), "id".to_string()];
    v.extend(keys.iter().map(|k| k.name.clone()));
    v.push("extra".to_string());
    v.push("data".to_string());
    v
}

/// Declared key types as config spells them, for diagnostics.
pub fn key_type_names(keys: &[KeyDef]) -> HashMap<String, &'static str> {
    keys.iter()
        .map(|k| (k.name.clone(), key_type_name(k.ty)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flush::read::{ReadRow, read_file, read_metadata};

    fn keydef(name: &str, ty: KeyType) -> KeyDef {
        KeyDef {
            name: name.to_string(),
            ty,
            nullable: true,
        }
    }

    fn sample_keys() -> Vec<KeyDef> {
        vec![
            keydef("symbol", KeyType::Str),
            keydef("size", KeyType::I64),
            keydef("px", KeyType::F64),
            keydef("live", KeyType::Bool),
        ]
    }

    #[test]
    fn schema_shape_and_nullability() {
        let s = parquet_schema(&sample_keys()).unwrap();
        let names: Vec<&str> = s.get_fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            ["ts", "id", "symbol", "size", "px", "live", "extra", "data"]
        );

        let rep = |i: usize| s.get_fields()[i].get_basic_info().repetition();
        assert_eq!(rep(0), Repetition::REQUIRED, "ts is always present");
        assert_eq!(rep(1), Repetition::REQUIRED, "absent id is \"\", not null");
        // Every key column is optional regardless of config, so a column added
        // in a later epoch reads as NULL from older files.
        for i in 2..6 {
            assert_eq!(rep(i), Repetition::OPTIONAL, "key column {i}");
        }
        assert_eq!(
            rep(6),
            Repetition::REQUIRED,
            "extra is an empty map, not null"
        );

        assert_eq!(
            s.get_fields()[0].get_basic_info().logical_type_ref(),
            Some(&LogicalType::timestamp(true, TimeUnit::MICROS))
        );
        assert_eq!(
            s.get_fields()[6].get_basic_info().logical_type_ref(),
            Some(&LogicalType::Map)
        );
    }

    /// The MAP nesting the spec fixes: readers key off these exact names.
    #[test]
    fn the_map_uses_the_names_the_parquet_spec_fixes() {
        let s = parquet_schema(&[]).unwrap();
        let extra = &s.get_fields()[2];
        assert_eq!(extra.name(), "extra");
        let kv = &extra.get_fields()[0];
        assert_eq!(kv.name(), "key_value");
        assert_eq!(kv.get_basic_info().repetition(), Repetition::REPEATED);
        let names: Vec<&str> = kv.get_fields().iter().map(|f| f.name()).collect();
        assert_eq!(names, ["key", "value"]);
        assert_eq!(
            kv.get_fields()[0].get_basic_info().repetition(),
            Repetition::REQUIRED
        );
        assert_eq!(
            kv.get_fields()[1].get_basic_info().repetition(),
            Repetition::OPTIONAL
        );
    }

    fn write_sample(path: &Path, row_group_rows: usize) -> Vec<ReadRow> {
        let keys = sample_keys();
        let mut b = RowBuilder::new("trades", &keys, 4).unwrap();
        b.append(
            1_786_194_000_000_000,
            "a1",
            &[
                Value::Str("AAPL"),
                Value::I64(100),
                Value::F64(193.4),
                Value::Bool(true),
            ],
            &[("feed", "itch")],
            r#"{"x":1}"#,
        )
        .unwrap();
        // Nulls, an empty id and no extra keys all have to survive.
        b.append(
            1_786_194_000_000_001,
            "",
            &[Value::Null, Value::Null, Value::Null, Value::Null],
            &[],
            "",
        )
        .unwrap();
        let cols = b.finish().unwrap();
        write_file(path, &cols, "trades", 7, "lz4_raw", row_group_rows).unwrap();
        read_file(path).unwrap()
    }

    #[test]
    fn round_trips_through_a_real_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(output_file_name(41, 42, 7));
        let rows = write_sample(&path, 1024);
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "0000000041-0000000042.e7.parquet"
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, 1_786_194_000_000_000);
        assert_eq!(rows[0].id, "a1");
        assert_eq!(rows[0].keys[0].as_deref(), Some("AAPL"));
        assert_eq!(rows[0].keys[1].as_deref(), Some("100"));
        assert_eq!(
            rows[0].extra,
            vec![("feed".to_string(), "itch".to_string())]
        );
        assert_eq!(rows[0].data, r#"{"x":1}"#);

        assert_eq!(rows[1].id, "");
        assert!(rows[1].keys.iter().all(Option::is_none), "nulls survive");
        assert!(rows[1].extra.is_empty(), "empty map, not null");
        assert_eq!(rows[1].data, "");

        let meta = read_metadata(&path).unwrap();
        // The epoch must be readable from the file itself, not just its name.
        assert_eq!(
            meta.key_value.get(EPOCH_META_KEY).map(String::as_str),
            Some("7")
        );
        assert_eq!(
            meta.key_value.get(SERIES_META_KEY).map(String::as_str),
            Some("trades")
        );
        // The sorting-column declaration is what lets readers prune on ts.
        assert_eq!(
            meta.sorting_columns,
            vec![SortingColumn {
                column_idx: TS_COLUMN_IDX,
                descending: false,
                nulls_first: false,
            }]
        );
        assert_eq!(meta.row_group_rows, vec![2], "one group at this size");
    }

    /// The property the hand-written repetition levels exist to guarantee.
    #[test]
    fn a_row_group_boundary_never_splits_a_map() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("split.parquet");
        let keys = vec![keydef("symbol", KeyType::Str)];
        let mut b = RowBuilder::new("s", &keys, 40).unwrap();

        // Map cardinality cycles 0..3 so every possible offset within a row's
        // list of entries coincides with a group boundary somewhere.
        let mut expected: Vec<Vec<(String, String)>> = Vec::new();
        for i in 0..40i64 {
            let pairs: Vec<(String, String)> = (0..(i % 4))
                .map(|j| (format!("k{j}"), format!("v{i}-{j}")))
                .collect();
            let borrowed: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            b.append(i, &format!("id{i}"), &[Value::Str("AAPL")], &borrowed, "d")
                .unwrap();
            expected.push(pairs);
        }
        let cols = b.finish().unwrap();
        // 3 does not divide 4, so boundaries walk through every phase of the
        // cardinality cycle rather than always landing in the same place.
        write_file(&path, &cols, "s", 0, "uncompressed", 3).unwrap();

        let meta = read_metadata(&path).unwrap();
        assert_eq!(meta.row_group_rows.len(), 14, "40 rows in groups of 3");

        let rows = read_file(&path).unwrap();
        assert_eq!(rows.len(), 40);
        for (i, (row, want)) in std::iter::zip(&rows, &expected).enumerate() {
            assert_eq!(row.ts, i as i64);
            assert_eq!(row.id, format!("id{i}"));
            assert_eq!(&row.extra, want, "row {i} map entries");
        }
    }

    /// Nulls are stored as absent values plus a level, so a group boundary must
    /// advance the value cursor by the number of *present* rows, not all rows.
    #[test]
    fn a_row_group_boundary_keeps_nulls_aligned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nulls.parquet");
        let keys = vec![keydef("a", KeyType::I64), keydef("b", KeyType::Str)];
        let mut b = RowBuilder::new("s", &keys, 30).unwrap();
        for i in 0..30i64 {
            // Two columns with different null patterns, so a shared cursor or an
            // off-by-one in either would shift values onto the wrong rows.
            let a = if i % 3 == 0 {
                Value::Null
            } else {
                Value::I64(i)
            };
            let owned = format!("s{i}");
            let s = if i % 2 == 0 {
                Value::Null
            } else {
                Value::Str(&owned)
            };
            b.append(i, "", &[a, s], &[], "").unwrap();
        }
        let cols = b.finish().unwrap();
        write_file(&path, &cols, "s", 0, "uncompressed", 4).unwrap();

        let rows = read_file(&path).unwrap();
        assert_eq!(rows.len(), 30);
        for (i, row) in rows.iter().enumerate() {
            let i = i as i64;
            let want_a = (i % 3 != 0).then(|| i.to_string());
            let want_b = (i % 2 != 0).then(|| format!("s{i}"));
            assert_eq!(row.keys[0], want_a, "row {i} column a");
            assert_eq!(row.keys[1], want_b, "row {i} column b");
        }
    }

    #[test]
    fn rejects_wrong_arity_and_wrong_type() {
        let keys = vec![keydef("symbol", KeyType::Str)];
        let mut b = RowBuilder::new("trades", &keys, 1).unwrap();

        let err = b.append(1, "", &[], &[], "").unwrap_err();
        assert!(matches!(err, Error::KeyArity { .. }), "got {err:?}");

        let err = b.append(1, "", &[Value::I64(3)], &[], "").unwrap_err();
        assert!(matches!(err, Error::TypeMismatch { .. }), "got {err:?}");
    }

    #[test]
    fn unsupported_compression_is_rejected_before_any_io() {
        // Compiled out, so it must fail at config load rather than at the first
        // flush an hour later.
        assert!(writer_properties("s", 0, "brotli", 128).is_err());
        assert!(writer_properties("s", 0, "zstd", 128).is_err());
    }

    /// The list config validates against and the match that implements it must
    /// agree in both directions.
    ///
    /// They used to be two separate lists and had already drifted: config
    /// accepted `"none"` but not `"uncompressed"`, so a codec the writer handles
    /// fine was refused at load. One list, checked both ways, is the fix.
    #[test]
    fn every_advertised_codec_can_actually_be_written() {
        for name in SUPPORTED_COMPRESSION {
            assert!(
                writer_properties("s", 0, name, 128).is_ok(),
                "config advertises {name:?} but the writer rejects it"
            );
        }
        // And the error names the real list, so a typo is self-correcting.
        let err = writer_properties("s", 0, "gzip", 128)
            .unwrap_err()
            .to_string();
        for name in SUPPORTED_COMPRESSION {
            assert!(err.contains(name), "{err:?} should list {name:?}");
        }
    }

    /// LZ4_RAW (codec 7), never the deprecated LZ4 (codec 5) whose non-standard
    /// Hadoop framing is why it was deprecated. Readers treat them differently,
    /// so writing the wrong one is a compatibility bug, not a performance one.
    #[test]
    fn lz4_means_lz4_raw() {
        for name in ["lz4", "lz4_raw"] {
            let props = writer_properties("s", 0, name, 128).unwrap();
            assert_eq!(
                props.compression(&"ts".into()),
                Compression::LZ4_RAW,
                "{name:?}"
            );
        }
    }

    /// A zero-row file is never written by the flush (an empty flush is a
    /// no-op), but the writer must not produce a corrupt file if it ever is.
    #[test]
    fn zero_rows_writes_a_readable_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        let b = RowBuilder::new("s", &[], 0).unwrap();
        let cols = b.finish().unwrap();
        write_file(&path, &cols, "s", 0, "lz4_raw", 128).unwrap();
        assert!(read_file(&path).unwrap().is_empty());
    }
}
