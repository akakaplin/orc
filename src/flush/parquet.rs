//! Building Arrow record batches and writing them as Parquet.
//!
//! This is the only place the engine's record model meets Arrow. One Parquet
//! file holds exactly one `(series, epoch)` pair, so the schema here is fixed
//! for the lifetime of a file and never has to change mid-write.
//!
//! Column order is `ts, id, <declared keys...>, extra, data`. `ts` first is
//! deliberate: it is declared as the file's sorting column, and readers prune
//! row groups on its statistics.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, MapBuilder, MapFieldNames,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::WriterProperties;

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

/// Arrow field names for the `extra` map.
///
/// Deliberately `key_value` / `key` / `value` rather than Arrow's default
/// `entries` / `keys` / `values`: those are the names the Parquet `MAP` logical
/// type specifies, and readers that go by the spec rather than by Arrow's
/// conventions get a map instead of an opaque nested struct.
fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

fn arrow_type(ty: KeyType) -> DataType {
    match ty {
        KeyType::Bool => DataType::Boolean,
        KeyType::I64 => DataType::Int64,
        KeyType::F64 => DataType::Float64,
        KeyType::Str => DataType::Utf8,
    }
}

/// The Arrow schema for one series at one epoch.
///
/// **Every key column is nullable regardless of its config `nullable`.** That is
/// not an oversight: nullability is enforced at ingest, and writing a column as
/// required would mean a column added in a later epoch could not read back as
/// NULL from older files, breaking the union that `view.sql` depends on.
pub fn arrow_schema(keys: &[KeyDef]) -> SchemaRef {
    let mut fields = Vec::with_capacity(keys.len() + 4);
    fields.push(Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    // Never null: an absent id is the empty string, so `(ts, id)` dedup has one
    // representation to compare rather than two that mean the same thing.
    fields.push(Field::new("id", DataType::Utf8, false));

    for k in keys {
        fields.push(Field::new(&k.name, arrow_type(k.ty), true));
    }

    let names = map_field_names();
    let entry = Field::new(
        &names.entry,
        DataType::Struct(
            vec![
                Field::new(&names.key, DataType::Utf8, false),
                Field::new(&names.value, DataType::Utf8, true),
            ]
            .into(),
        ),
        false,
    );
    fields.push(Field::new(
        "extra",
        DataType::Map(entry.into(), false),
        false,
    ));
    fields.push(Field::new("data", DataType::Utf8, false));

    Arc::new(Schema::new(fields))
}

/// One column's builder, dispatched on the declared type.
#[derive(Debug)]
enum KeyBuilder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Str(StringBuilder),
}

impl KeyBuilder {
    fn new(ty: KeyType, cap: usize) -> Self {
        match ty {
            KeyType::Bool => KeyBuilder::Bool(BooleanBuilder::with_capacity(cap)),
            KeyType::I64 => KeyBuilder::I64(Int64Builder::with_capacity(cap)),
            KeyType::F64 => KeyBuilder::F64(Float64Builder::with_capacity(cap)),
            KeyType::Str => KeyBuilder::Str(StringBuilder::with_capacity(cap, cap * 8)),
        }
    }

    fn key_type(&self) -> KeyType {
        match self {
            KeyBuilder::Bool(_) => KeyType::Bool,
            KeyBuilder::I64(_) => KeyType::I64,
            KeyBuilder::F64(_) => KeyType::F64,
            KeyBuilder::Str(_) => KeyType::Str,
        }
    }

    fn append(&mut self, v: &Value<'_>, series: &str, key: &str) -> Result<()> {
        // Read the expected type before the match takes `self` by value.
        let expected = self.key_type();
        let got = v.key_type();
        match (self, v) {
            (KeyBuilder::Bool(b), Value::Bool(x)) => b.append_value(*x),
            (KeyBuilder::I64(b), Value::I64(x)) => b.append_value(*x),
            (KeyBuilder::F64(b), Value::F64(x)) => b.append_value(*x),
            (KeyBuilder::Str(b), Value::Str(x)) => b.append_value(x),
            (KeyBuilder::Bool(b), Value::Null) => b.append_null(),
            (KeyBuilder::I64(b), Value::Null) => b.append_null(),
            (KeyBuilder::F64(b), Value::Null) => b.append_null(),
            (KeyBuilder::Str(b), Value::Null) => b.append_null(),
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

    fn finish(&mut self) -> ArrayRef {
        match self {
            KeyBuilder::Bool(b) => Arc::new(b.finish()),
            KeyBuilder::I64(b) => Arc::new(b.finish()),
            KeyBuilder::F64(b) => Arc::new(b.finish()),
            KeyBuilder::Str(b) => Arc::new(b.finish()),
        }
    }
}

/// Accumulates rows for one `(series, epoch)` into an Arrow [`RecordBatch`].
///
/// Rows arrive already sorted and deduplicated; this type only materialises
/// them. It is the first point in the flush where a record becomes owned data,
/// which is why the sort upstream works on an index instead.
#[derive(Debug)]
pub struct RowBuilder {
    series: String,
    schema: SchemaRef,
    key_names: Vec<String>,
    ts: TimestampMicrosecondBuilder,
    id: StringBuilder,
    keys: Vec<KeyBuilder>,
    extra: MapBuilder<StringBuilder, StringBuilder>,
    data: StringBuilder,
}

impl RowBuilder {
    pub fn new(series: &str, key_defs: &[KeyDef], cap: usize) -> Self {
        Self {
            series: series.to_string(),
            schema: arrow_schema(key_defs),
            key_names: key_defs.iter().map(|k| k.name.clone()).collect(),
            ts: TimestampMicrosecondBuilder::with_capacity(cap),
            id: StringBuilder::with_capacity(cap, cap * 8),
            keys: key_defs
                .iter()
                .map(|k| KeyBuilder::new(k.ty, cap))
                .collect(),
            extra: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            data: StringBuilder::with_capacity(cap, cap * 32),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn len(&self) -> usize {
        self.ts.len()
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
        if keys.len() != self.keys.len() {
            return Err(Error::KeyArity {
                series: self.series.clone(),
                expected: self.keys.len(),
                got: keys.len(),
            });
        }
        self.ts.append_value(ts);
        self.id.append_value(id);
        for (i, (builder, value)) in std::iter::zip(&mut self.keys, keys).enumerate() {
            builder.append(value, &self.series, &self.key_names[i])?;
        }
        for (k, v) in extra {
            self.extra.keys().append_value(k);
            self.extra.values().append_value(v);
        }
        // Never `append_null`: an empty map and a null map are different values
        // to a reader, and "this record had no undeclared keys" is the former.
        self.extra
            .append(true)
            .map_err(|e| Error::Config(format!("building extra map: {e}")))?;
        self.data.append_value(data);
        Ok(())
    }

    /// Consume the accumulated rows into a batch.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.keys.len() + 4);
        columns.push(Arc::new(self.ts.finish().with_timezone("UTC")));
        columns.push(Arc::new(self.id.finish()));
        for b in &mut self.keys {
            columns.push(b.finish());
        }
        columns.push(Arc::new(self.extra.finish()));
        columns.push(Arc::new(self.data.finish()));
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|e| Error::Config(format!("building record batch: {e}")))
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
    row_group_rows: usize,
) -> Result<WriterProperties> {
    let codec = match compression {
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            return Err(Error::Config(format!(
                "unsupported compression {other:?}; supported: zstd, uncompressed"
            )));
        }
    };
    Ok(WriterProperties::builder()
        .set_compression(codec)
        .set_max_row_group_row_count(Some(row_group_rows))
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

/// Write one batch to `path` as a complete Parquet file, then fsync it.
///
/// The fsync matters: the flush renames this file into `series/` and only then
/// commits the manifest. If the contents were not durable before the rename, a
/// power cut could leave a committed manifest pointing at a file with a valid
/// name and no data.
pub fn write_file(
    path: &Path,
    batch: &RecordBatch,
    series: &str,
    epoch: u32,
    compression: &str,
    row_group_rows: usize,
) -> Result<()> {
    let props = writer_properties(series, epoch, compression, row_group_rows)?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| Error::Config(format!("opening parquet writer for {path:?}: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| Error::Config(format!("writing parquet {path:?}: {e}")))?;
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
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
        let s = arrow_schema(&sample_keys());
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            ["ts", "id", "symbol", "size", "px", "live", "extra", "data"]
        );
        assert!(!s.field(0).is_nullable(), "ts is always present");
        assert!(!s.field(1).is_nullable(), "absent id is \"\", not null");
        // Every key column is nullable regardless of config, so a column added
        // in a later epoch reads as NULL from older files.
        for i in 2..6 {
            assert!(s.field(i).is_nullable(), "key column {i} must be nullable");
        }
        assert!(matches!(
            s.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        ));
    }

    #[test]
    fn round_trips_through_a_real_parquet_file() {
        let keys = sample_keys();
        let mut b = RowBuilder::new("trades", &keys, 4);
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
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(output_file_name(41, 42, 7));
        write_file(&path, &batch, "trades", 7, "zstd", 1024).unwrap();
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "0000000041-0000000042.e7.parquet"
        );

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        // The epoch must be readable from the file itself, not just its name.
        let kv = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("key-value metadata")
            .clone();
        let epoch = kv
            .iter()
            .find(|k| k.key == EPOCH_META_KEY)
            .and_then(|k| k.value.clone());
        assert_eq!(epoch.as_deref(), Some("7"));

        // The sorting-column declaration is what lets readers prune on ts.
        let sorting = builder.metadata().row_group(0).sorting_columns().cloned();
        assert_eq!(
            sorting,
            Some(vec![SortingColumn {
                column_idx: TS_COLUMN_IDX,
                descending: false,
                nulls_first: false,
            }])
        );

        let read: Vec<RecordBatch> = builder
            .build()
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        let total: usize = read.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
        assert_eq!(read[0].schema().fields().len(), 8);
    }

    #[test]
    fn rejects_wrong_arity_and_wrong_type() {
        let keys = vec![keydef("symbol", KeyType::Str)];
        let mut b = RowBuilder::new("trades", &keys, 1);

        let err = b.append(1, "", &[], &[], "").unwrap_err();
        assert!(matches!(err, Error::KeyArity { .. }), "got {err:?}");

        let err = b.append(1, "", &[Value::I64(3)], &[], "").unwrap_err();
        assert!(matches!(err, Error::TypeMismatch { .. }), "got {err:?}");
    }

    #[test]
    fn empty_extra_is_an_empty_map_not_null() {
        let keys: Vec<KeyDef> = vec![];
        let mut b = RowBuilder::new("s", &keys, 1);
        b.append(1, "x", &[], &[], "d").unwrap();
        let batch = b.finish().unwrap();
        let extra = batch.column_by_name("extra").unwrap();
        assert_eq!(extra.null_count(), 0, "no undeclared keys != unknown");
    }

    #[test]
    fn unsupported_compression_is_rejected_before_any_io() {
        assert!(writer_properties("s", 0, "brotli", 128).is_err());
        assert!(writer_properties("s", 0, "zstd", 128).is_ok());
        assert!(writer_properties("s", 0, "uncompressed", 128).is_ok());
    }
}
