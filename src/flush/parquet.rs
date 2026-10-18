//! Building column chunks and writing them as Parquet.
//!
//! One file holds one `(series, epoch)`, so its schema is fixed. Columns are
//! `ts, id, <declared keys...>, extra, data`; `ts` is the declared sorting column.
//!
//! Written against the low-level API, not `ArrowWriter`: `parquet/arrow` costs a
//! dozen crates and an `ARROW:schema` blob per file, only to compute the map
//! levels in `MapLevels::for_rows`. `tests/parquet_reference.rs` pins the output.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{
    Compression, ConvertedType, Encoding, LogicalType, Repetition, TimeUnit, Type as PhysicalType,
};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{ColumnPath, Type, TypePtr};

use crate::config::KeyDef;
use crate::error::{Error, Result};
use crate::record::{KeyType, Value};
use crate::wal::SEGMENT_ID_DIGITS;

/// Key-value metadata key for the schema epoch; mirrors the filename's `.eN.` tag
/// so a renamed file stays self-describing.
pub const EPOCH_META_KEY: &str = "orc.schema_epoch";

/// Key-value metadata key for the series name.
pub const SERIES_META_KEY: &str = "orc.series";

/// Index of the `ts` column, the declared sorting column.
const TS_COLUMN_IDX: i32 = 0;

/// Codecs the writer supports; config validates against this at load.
pub const SUPPORTED_COMPRESSION: [&str; 4] = ["lz4_raw", "lz4", "uncompressed", "none"];

fn schema_err(what: &str, e: parquet::errors::ParquetError) -> Error {
    Error::Config(format!("building parquet schema ({what}): {e}"))
}

/// The Parquet schema for one series at one epoch, built programmatically since
/// key names come from user config.
///
/// Every key column is OPTIONAL regardless of `nullable`, so a key added in a
/// later epoch reads as NULL from older files (`view.sql`'s union needs that).
pub fn parquet_schema(keys: &[KeyDef]) -> Result<TypePtr> {
    let mut fields: Vec<TypePtr> = Vec::with_capacity(keys.len() + 4);

    fields.push(Arc::new(
        Type::primitive_type_builder("ts", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::timestamp(true, TimeUnit::MICROS)))
            // Also set for readers that predate logical types.
            .with_converted_type(ConvertedType::TIMESTAMP_MICROS)
            .build()
            .map_err(|e| schema_err("ts", e))?,
    ));

    // Never null: an absent id is "", so `(ts, id)` dedup has one representation.
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

    // The spec's MAP shape; readers rely on these exact names.
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

/// A UTF-8 string column (also `ConvertedType::UTF8` for older readers).
fn utf8(name: &str, rep: Repetition) -> Result<Type> {
    Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
        .with_repetition(rep)
        .with_logical_type(Some(LogicalType::String))
        .with_converted_type(ConvertedType::UTF8)
        .build()
        .map_err(|e| schema_err(name, e))
}

/// One declared key column: a definition level per row (1 present, 0 null) and
/// values for present rows only.
#[derive(Debug)]
struct KeyColumn {
    vals: Values,
    defs: Vec<i16>,
}

/// The present values of one key column, in its declared type.
#[derive(Debug)]
enum Values {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(StrColumn),
}

/// A string column: all bytes in one buffer plus end offsets. [`StrColumn::arrays`]
/// makes `ByteArray`s as slices of one shared `Bytes` per row group.
#[derive(Debug, Default)]
struct StrColumn {
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

impl StrColumn {
    fn with_capacity(values: usize) -> Self {
        Self {
            bytes: Vec::new(),
            ends: Vec::with_capacity(values),
        }
    }

    fn push(&mut self, s: &str) {
        self.bytes.extend_from_slice(s.as_bytes());
        self.ends.push(self.bytes.len());
    }

    fn start(&self, i: usize) -> usize {
        if i == 0 { 0 } else { self.ends[i - 1] }
    }

    /// Values `[range)` as `ByteArray`s sharing one buffer.
    fn arrays(&self, range: std::ops::Range<usize>) -> Vec<ByteArray> {
        if range.is_empty() {
            return Vec::new();
        }
        let base = self.start(range.start);
        let shared = Bytes::copy_from_slice(&self.bytes[base..self.ends[range.end - 1]]);
        range
            .map(|i| ByteArray::from(shared.slice(self.start(i) - base..self.ends[i] - base)))
            .collect()
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
    }
}

impl KeyColumn {
    fn new(ty: KeyType, cap: usize) -> Self {
        KeyColumn {
            vals: match ty {
                KeyType::Bool => Values::Bool(Vec::with_capacity(cap)),
                KeyType::I64 => Values::I64(Vec::with_capacity(cap)),
                KeyType::F64 => Values::F64(Vec::with_capacity(cap)),
                KeyType::Str => Values::Str(StrColumn::with_capacity(cap)),
            },
            defs: Vec::with_capacity(cap),
        }
    }

    fn key_type(&self) -> KeyType {
        match self.vals {
            Values::Bool(_) => KeyType::Bool,
            Values::I64(_) => KeyType::I64,
            Values::F64(_) => KeyType::F64,
            Values::Str(_) => KeyType::Str,
        }
    }

    fn append(&mut self, v: &Value<'_>, series: &str, key: &str) -> Result<()> {
        // Null satisfies any column: a level, and no value to go with it.
        if matches!(v, Value::Null) {
            self.defs.push(0);
            return Ok(());
        }
        // Read the expected type before the match borrows the values mutably.
        let expected = self.key_type();
        match (&mut self.vals, v) {
            (Values::Bool(xs), Value::Bool(x)) => xs.push(*x),
            (Values::I64(xs), Value::I64(x)) => xs.push(*x),
            (Values::F64(xs), Value::F64(x)) => xs.push(*x),
            (Values::Str(xs), Value::Str(x)) => xs.push(x),
            _ => {
                return Err(Error::TypeMismatch {
                    series: series.to_string(),
                    key: key.to_string(),
                    expected,
                    got: v.key_type(),
                });
            }
        }
        self.defs.push(1);
        Ok(())
    }
}

/// Per-column value offsets carried across row groups (values are not indexed
/// by row: nulls are skipped and maps vary in length).
#[derive(Debug, Default)]
struct Cursors {
    keys: Vec<usize>,
    extra: usize,
}

/// Accumulated rows for one `(series, epoch)`, already sorted and deduplicated.
#[derive(Debug)]
pub struct Columns {
    series: String,
    schema: TypePtr,
    key_names: Vec<String>,
    rows: usize,
    ts: Vec<i64>,
    id: StrColumn,
    keys: Vec<KeyColumn>,
    /// Flattened map entries plus a count per row, so groups never split a row.
    extra_keys: StrColumn,
    extra_values: StrColumn,
    extra_counts: Vec<u32>,
    data: StrColumn,
}

/// Accumulates rows into [`Columns`].
#[derive(Debug)]
pub struct RowBuilder {
    cols: Columns,
    /// Index scratch for collapsing duplicate `extra` names.
    order: Vec<u32>,
}

impl RowBuilder {
    pub fn new(series: &str, key_defs: &[KeyDef], cap: usize) -> Result<Self> {
        Ok(Self {
            order: Vec::new(),
            cols: Columns {
                series: series.to_string(),
                schema: parquet_schema(key_defs)?,
                key_names: key_defs.iter().map(|k| k.name.clone()).collect(),
                rows: 0,
                ts: Vec::with_capacity(cap),
                id: StrColumn::with_capacity(cap),
                keys: key_defs.iter().map(|k| KeyColumn::new(k.ty, cap)).collect(),
                extra_keys: StrColumn::default(),
                extra_values: StrColumn::default(),
                extra_counts: Vec::with_capacity(cap),
                data: StrColumn::with_capacity(cap),
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

    /// Append one row. A `keys` arity mismatch means the frame was decoded under
    /// the wrong epoch, and is an error.
    pub fn append(
        &mut self,
        ts: u64,
        id: &str,
        keys: &[Value<'_>],
        extra: &[(&str, &str)],
        data: &str,
    ) -> Result<()> {
        // Destructured so `order` and the columns are disjoint borrows.
        let Self { cols: c, order } = self;
        if keys.len() != c.keys.len() {
            return Err(Error::KeyArity {
                series: c.series.clone(),
                expected: c.keys.len(),
                got: keys.len(),
            });
        }
        // TIMESTAMP(MICROS) is signed. Unreachable within the accept window, but
        // an error rather than a wrap for direct callers.
        let ts = i64::try_from(ts).map_err(|_| {
            Error::Config(format!(
                "ts {ts} is past the largest instant Parquet can record \
                 (TIMESTAMP(MICROS) is a signed INT64)"
            ))
        })?;
        c.ts.push(ts);
        c.id.push(id);
        for (i, (column, value)) in std::iter::zip(&mut c.keys, keys).enumerate() {
            column.append(value, &c.series, &c.key_names[i])?;
        }
        // Backstop for frames the codec never canonicalised (`append_raw` checks
        // only the prefix): a repeated MAP key makes the whole file unreadable.
        // Duplicates need not be adjacent, so sort and collapse like the codec.
        let mut kept = 0u32;
        let mut push = |k: &str, v: &str| {
            c.extra_keys.push(k);
            c.extra_values.push(v);
            kept += 1;
        };
        if crate::codec::is_canonical(extra) {
            for &(k, v) in extra {
                push(k, v);
            }
        } else {
            order.clear();
            order.extend(0..extra.len() as u32);
            order.sort_unstable_by_key(|&i| extra[i as usize]);
            // Collapse runs to their first member, matching the codec.
            order.dedup_by(|a, b| extra[*a as usize].0 == extra[*b as usize].0);
            for &i in order.iter() {
                let (k, v) = extra[i as usize];
                push(k, v);
            }
        }
        // Count 0 is an empty map, not null.
        c.extra_counts.push(kept);
        c.data.push(data);
        c.rows += 1;
        Ok(())
    }

    /// Freeze the accumulated rows.
    pub fn finish(self) -> Result<Columns> {
        Ok(self.cols)
    }

    /// Drop every accumulated row, keeping the allocations for the next batch.
    fn clear(&mut self) {
        let c = &mut self.cols;
        c.rows = 0;
        c.ts.clear();
        c.id.clear();
        for k in &mut c.keys {
            k.defs.clear();
            match &mut k.vals {
                Values::Bool(v) => v.clear(),
                Values::I64(v) => v.clear(),
                Values::F64(v) => v.clear(),
                Values::Str(v) => v.clear(),
            }
        }
        c.extra_keys.clear();
        c.extra_values.clear();
        c.extra_counts.clear();
        c.data.clear();
    }
}

/// Definition/repetition levels for one row group's `extra` map. Max def is 1
/// for `key`, 2 for the OPTIONAL `value`; max rep is 1.
///
/// ```text
/// empty map    -> def 0,   rep 0
/// entry 0      -> def 1/2, rep 0   (opens a row)
/// entry i > 0  -> def 1/2, rep 1
/// ```
///
/// A row's entries are emitted together, so every group starts at rep 0.
#[derive(Debug)]
struct MapLevels {
    key_defs: Vec<i16>,
    value_defs: Vec<i16>,
    reps: Vec<i16>,
    /// Present entries in these rows: how far to advance the value cursor.
    entries: usize,
}

impl MapLevels {
    fn for_rows(counts: &[u32]) -> Self {
        let entries: usize = counts.iter().map(|&n| n as usize).sum();
        // An empty map still occupies one level slot.
        let slots = entries + counts.iter().filter(|&&n| n == 0).count();

        let mut out = MapLevels {
            key_defs: Vec::with_capacity(slots),
            value_defs: Vec::with_capacity(slots),
            reps: Vec::with_capacity(slots),
            entries,
        };
        for &count in counts {
            if count == 0 {
                out.key_defs.push(0);
                out.value_defs.push(0);
                out.reps.push(0);
                continue;
            }
            for i in 0..count {
                out.key_defs.push(1);
                // Values are never null, so always max def.
                out.value_defs.push(2);
                out.reps.push(if i == 0 { 0 } else { 1 });
            }
        }
        out
    }
}

fn io_err(e: parquet::errors::ParquetError) -> Error {
    Error::Config(format!("writing parquet: {e}"))
}

/// Write one BYTE_ARRAY column with the given levels and close it.
fn write_bytes<W: std::io::Write + Send>(
    rg: &mut parquet::file::writer::SerializedRowGroupWriter<'_, W>,
    what: &'static str,
    vals: &[ByteArray],
    defs: Option<&[i16]>,
    reps: Option<&[i16]>,
) -> Result<()> {
    let mut c = rg.next_column().map_err(io_err)?.expect(what);
    match c.untyped() {
        ColumnWriter::ByteArrayColumnWriter(w) => {
            w.write_batch(vals, defs, reps).map_err(io_err)?
        }
        _ => unreachable!("{what} is BYTE_ARRAY"),
    };
    c.close().map_err(io_err)?;
    Ok(())
}

impl Columns {
    pub fn num_rows(&self) -> usize {
        self.rows
    }

    pub fn schema(&self) -> TypePtr {
        Arc::clone(&self.schema)
    }

    /// Write rows `[start, end)` as one row group, columns in [`parquet_schema`] order.
    fn write_group<W: std::io::Write + Send>(
        &self,
        rg: &mut parquet::file::writer::SerializedRowGroupWriter<'_, W>,
        start: usize,
        end: usize,
        cursors: &mut Cursors,
    ) -> Result<()> {
        // -- ts: REQUIRED, so no levels are needed at all ---------------------
        let mut c = rg.next_column().map_err(io_err)?.expect("ts column");
        match c.untyped() {
            ColumnWriter::Int64ColumnWriter(w) => w
                .write_batch(&self.ts[start..end], None, None)
                .map_err(io_err)?,
            _ => unreachable!("ts is INT64"),
        };
        c.close().map_err(io_err)?;

        // -- id: REQUIRED -----------------------------------------------------
        write_bytes(rg, "id", &self.id.arrays(start..end), None, None)?;

        // -- declared keys: OPTIONAL, def level 1 present / 0 null ------------
        for (i, column) in self.keys.iter().enumerate() {
            let defs = &column.defs[start..end];
            // This group's values: one per present (def 1) row.
            let present = defs.iter().filter(|&&d| d == 1).count();
            let from = cursors.keys[i];
            let to = from + present;
            cursors.keys[i] = to;

            let mut c = rg.next_column().map_err(io_err)?.expect("key column");
            match (c.untyped(), &column.vals) {
                (ColumnWriter::BoolColumnWriter(w), Values::Bool(v)) => w
                    .write_batch(&v[from..to], Some(defs), None)
                    .map_err(io_err)?,
                (ColumnWriter::Int64ColumnWriter(w), Values::I64(v)) => w
                    .write_batch(&v[from..to], Some(defs), None)
                    .map_err(io_err)?,
                (ColumnWriter::DoubleColumnWriter(w), Values::F64(v)) => w
                    .write_batch(&v[from..to], Some(defs), None)
                    .map_err(io_err)?,
                (ColumnWriter::ByteArrayColumnWriter(w), Values::Str(v)) => w
                    .write_batch(&v.arrays(from..to), Some(defs), None)
                    .map_err(io_err)?,
                _ => unreachable!("column writer disagrees with the schema we built"),
            };
            c.close().map_err(io_err)?;
        }

        // -- extra: the only place levels are non-trivial ---------------------
        let levels = MapLevels::for_rows(&self.extra_counts[start..end]);

        let from = cursors.extra;
        let to = from + levels.entries;
        cursors.extra = to;

        let keys = &self.extra_keys.arrays(from..to);
        let values = &self.extra_values.arrays(from..to);
        let reps = Some(levels.reps.as_slice());
        write_bytes(rg, "extra.key", keys, Some(&levels.key_defs), reps)?;
        write_bytes(rg, "extra.value", values, Some(&levels.value_defs), reps)?;

        // -- data: REQUIRED ---------------------------------------------------
        write_bytes(rg, "data", &self.data.arrays(start..end), None, None)?;

        Ok(())
    }
}

/// Writer properties for a flush output file. Declaring `ts` the sorting column
/// is true only because rows are sorted upstream.
pub fn writer_properties(
    series: &str,
    epoch: u32,
    compression: &str,
    _row_group_rows: usize,
) -> Result<WriterProperties> {
    let codec = match compression {
        // LZ4_RAW, never the deprecated Hadoop-framed LZ4; "lz4" is an alias.
        "lz4_raw" | "lz4" => Compression::LZ4_RAW,
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            return Err(Error::Config(format!(
                "unsupported compression {other:?}; supported: {}",
                SUPPORTED_COMPRESSION.join(", ")
            )));
        }
    };
    // No max row group size: `SerializedFileWriter` ignores it; callers cut groups.
    let col = |name: &str| ColumnPath::from(name);
    Ok(WriterProperties::builder()
        .set_compression(codec)
        // No dictionary on near-unique ts/id/data; declared keys and `extra` keep it.
        .set_column_dictionary_enabled(col("ts"), false)
        .set_column_dictionary_enabled(col("id"), false)
        .set_column_dictionary_enabled(col("data"), false)
        // Sorted timestamps delta-encode into a few bits each.
        .set_column_encoding(col("ts"), Encoding::DELTA_BINARY_PACKED)
        // Opaque payload: min/max would prune nothing.
        .set_column_statistics_enabled(col("data"), EnabledStatistics::None)
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

/// Write `cols` to `path` as one Parquet file and fsync it. The flush streams via
/// [`PartitionWriter`]; this is the reference it must match.
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

/// A Parquet file streamed one row group at a time, so memory is one group.
/// Byte-identical to [`write_file`] over the same rows (same group cuts).
pub struct PartitionWriter {
    writer: SerializedFileWriter<File>,
    builder: RowBuilder,
    group_rows: usize,
    rows: usize,
}

impl std::fmt::Debug for PartitionWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionWriter")
            .field("group_rows", &self.group_rows)
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

impl PartitionWriter {
    pub fn create(
        path: &Path,
        series: &str,
        epoch: u32,
        key_defs: &[KeyDef],
        compression: &str,
        row_group_rows: usize,
    ) -> Result<Self> {
        let props = writer_properties(series, epoch, compression, row_group_rows)?;
        let group_rows = row_group_rows.max(1);
        // One group's capacity, capped: config bounds `row_group_rows`, not file size.
        let builder = RowBuilder::new(series, key_defs, group_rows.min(1 << 16))?;
        let file = File::create(path)?;
        let writer = SerializedFileWriter::new(file, builder.schema(), Arc::new(props))
            .map_err(|e| Error::Config(format!("opening parquet writer for {path:?}: {e}")))?;
        Ok(Self {
            writer,
            builder,
            group_rows,
            rows: 0,
        })
    }

    /// Append one row; a full row group is written out immediately.
    pub fn append(
        &mut self,
        ts: u64,
        id: &str,
        keys: &[Value<'_>],
        extra: &[(&str, &str)],
        data: &str,
    ) -> Result<()> {
        self.builder.append(ts, id, keys, extra, data)?;
        self.rows += 1;
        if self.builder.len() >= self.group_rows {
            self.write_group()?;
        }
        Ok(())
    }

    fn write_group(&mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let cols = &self.builder.cols;
        let mut cursors = Cursors {
            keys: vec![0; cols.keys.len()],
            extra: 0,
        };
        let mut rg = self.writer.next_row_group().map_err(io_err)?;
        cols.write_group(&mut rg, 0, cols.rows, &mut cursors)?;
        rg.close().map_err(io_err)?;
        self.builder.clear();
        Ok(())
    }

    /// Write the last row group, close and fsync; returns the row count. The fsync
    /// matters: the flush then commits the manifest and deletes source segments.
    pub fn finish(mut self) -> Result<usize> {
        self.write_group()?;
        let file = self.writer.into_inner().map_err(io_err)?;
        file.sync_all()?;
        Ok(self.rows)
    }
}

/// Flush output name: segment range and epoch tag. The range makes a re-run
/// after a crash reuse the same name, keeping flush idempotent.
pub fn output_file_name(first_segment: u64, last_segment: u64, epoch: u32) -> String {
    format!("{first_segment:010}-{last_segment:010}.e{epoch}.parquet")
}

/// Inverse of [`output_file_name`], strict: the startup sweep leaves anything it
/// cannot parse alone.
pub fn parse_output_file_name(name: &str) -> Option<(u64, u64, u32)> {
    let rest = name.strip_suffix(".parquet")?;
    let (range, epoch) = rest.rsplit_once(".e")?;
    let (first, last) = range.split_once('-')?;
    if !epoch.bytes().all(|b| b.is_ascii_digit()) || epoch.is_empty() {
        return None;
    }
    Some((
        parse_segment_field(first)?,
        parse_segment_field(last)?,
        epoch.parse().ok()?,
    ))
}

/// One segment id from a file name, with `parse_segment_name`'s width rule (at
/// least ten digits, no leading zero beyond ten), so a range has one name.
fn parse_segment_field(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if s.len() < SEGMENT_ID_DIGITS || (s.len() != SEGMENT_ID_DIGITS && s.starts_with('0')) {
        return None;
    }
    s.parse().ok()
}

/// Human-readable summary of a written file, for logs and `stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    pub series: String,
    pub epoch: u32,
    pub hour: String,
    pub rows: usize,
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

    /// `PartitionWriter` must match `write_file` byte for byte, across group cuts.
    #[test]
    fn streaming_output_is_byte_identical_to_write_file() {
        let keys = sample_keys();
        type TestRow = (
            u64,
            String,
            Vec<Value<'static>>,
            Vec<(&'static str, &'static str)>,
        );
        let rows: Vec<TestRow> = (0..11u64)
            .map(|i| {
                let vals = vec![
                    if i % 4 == 0 {
                        Value::Null
                    } else {
                        Value::Str("AAPL")
                    },
                    Value::I64(i as i64),
                    if i % 3 == 0 {
                        Value::Null
                    } else {
                        Value::F64(i as f64 / 2.0)
                    },
                    Value::Bool(i % 2 == 0),
                ];
                let extra = match i % 3 {
                    0 => vec![],
                    1 => vec![("feed", "itch")],
                    _ => vec![("a", "1"), ("b", "2")],
                };
                (1_000 + i, format!("id{i}"), vals, extra)
            })
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let whole = dir.path().join("whole.parquet");
        let streamed = dir.path().join("streamed.parquet");

        let mut b = RowBuilder::new("s", &keys, rows.len()).unwrap();
        for (ts, id, vals, extra) in &rows {
            b.append(*ts, id, vals, extra, "{}").unwrap();
        }
        write_file(&whole, &b.finish().unwrap(), "s", 4, "lz4_raw", 4).unwrap();

        let mut w = PartitionWriter::create(&streamed, "s", 4, &keys, "lz4_raw", 4).unwrap();
        for (ts, id, vals, extra) in &rows {
            w.append(*ts, id, vals, extra, "{}").unwrap();
        }
        assert_eq!(w.finish().unwrap(), rows.len());

        assert_eq!(
            std::fs::read(&streamed).unwrap(),
            std::fs::read(&whole).unwrap()
        );
        assert_eq!(read_metadata(&streamed).unwrap().row_group_rows, [4, 4, 3]);
    }

    /// The per-column choices in `writer_properties`, read back from a real file.
    #[test]
    fn columns_are_encoded_for_what_they_hold() {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let keys = [keydef("symbol", KeyType::Str)];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc.parquet");
        let mut w = PartitionWriter::create(&path, "s", 0, &keys, "lz4_raw", 1024).unwrap();
        for i in 0..100u64 {
            let id = format!("id{i}");
            w.append(1_000 + i, &id, &[Value::Str("AAPL")], &[], "{}")
                .unwrap();
        }
        w.finish().unwrap();

        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let rg = reader.metadata().row_group(0);
        let col = |name: &str| {
            (0..rg.num_columns())
                .map(|i| rg.column(i))
                .find(|c| c.column_path().string() == name)
                .unwrap()
        };
        let has = |name: &str, enc: Encoding| col(name).encodings().any(|e| e == enc);

        assert!(
            has("ts", Encoding::DELTA_BINARY_PACKED),
            "ts is delta-encoded"
        );
        for unique in ["ts", "id", "data"] {
            assert!(
                col(unique).dictionary_page_offset().is_none(),
                "{unique} has no dictionary"
            );
        }
        assert!(
            col("symbol").dictionary_page_offset().is_some(),
            "a declared key keeps its dictionary"
        );
        assert!(col("data").statistics().is_none(), "no statistics on data");
        assert!(
            col("ts").statistics().is_some(),
            "ts keeps min/max for pruning"
        );
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
        // Keys are OPTIONAL regardless of config.
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

        // Cardinality cycles 0..3 so cuts hit every offset in a row's list.
        let mut expected: Vec<Vec<(String, String)>> = Vec::new();
        for i in 0..40u64 {
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
        // Groups of 3 walk through every phase of the 4-cycle.
        write_file(&path, &cols, "s", 0, "uncompressed", 3).unwrap();

        let meta = read_metadata(&path).unwrap();
        assert_eq!(meta.row_group_rows.len(), 14, "40 rows in groups of 3");

        let rows = read_file(&path).unwrap();
        assert_eq!(rows.len(), 40);
        for (i, (row, want)) in std::iter::zip(&rows, &expected).enumerate() {
            assert_eq!(row.ts, i as u64);
            assert_eq!(row.id, format!("id{i}"));
            assert_eq!(&row.extra, want, "row {i} map entries");
        }
    }

    /// Group cuts must advance value cursors by present rows only.
    #[test]
    fn a_row_group_boundary_keeps_nulls_aligned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nulls.parquet");
        let keys = vec![keydef("a", KeyType::I64), keydef("b", KeyType::Str)];
        let mut b = RowBuilder::new("s", &keys, 30).unwrap();
        for i in 0..30u64 {
            // Different null patterns per column catch a shared or off-by-one cursor.
            let a = if i % 3 == 0 {
                Value::Null
            } else {
                Value::I64(i as i64)
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

    /// The startup sweep deletes files based on this parse.
    #[test]
    fn output_file_names_round_trip_and_reject_everything_else() {
        for (first, last, epoch) in [(1u64, 2u64, 0u32), (41, 42, 7), (0, u64::MAX, u32::MAX)] {
            let name = output_file_name(first, last, epoch);
            assert_eq!(
                parse_output_file_name(&name),
                Some((first, last, epoch)),
                "{name}"
            );
        }

        for bad in [
            "0000000041-0000000042.e3.txt",     // not parquet
            "0000000041-0000000042.parquet",    // no epoch tag
            "41-42.e3.parquet",                 // unpadded
            "000000041-0000000042.e3.parquet",  // nine digits
            "0000000041.e3.parquet",            // no range
            "0000000041-0000000042.ex.parquet", // non-numeric epoch
            "0000000041-0000000042.e.parquet",  // empty epoch
            "view.sql",
            "",
        ] {
            assert!(
                parse_output_file_name(bad).is_none(),
                "should not have parsed {bad:?}"
            );
        }
    }

    /// Backstop for frames that reach the builder via `append_raw`, uncanonicalised.
    #[test]
    fn a_repeated_extra_key_cannot_reach_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dupes.parquet");
        let mut b = RowBuilder::new("s", &[], 4).unwrap();
        b.append(1, "a", &[], &[("k", "1"), ("k", "2"), ("z", "9")], "")
            .unwrap();
        // The next row must stay aligned with the flattened map columns.
        b.append(2, "b", &[], &[("k", "3")], "").unwrap();
        // Non-adjacent duplicates.
        b.append(3, "c", &[], &[("k", "1"), ("z", "9"), ("k", "2")], "")
            .unwrap();
        // ...including when the run wraps the whole row.
        b.append(4, "d", &[], &[("z", "9"), ("k", "1"), ("z", "8")], "")
            .unwrap();
        write_file(&path, &b.finish().unwrap(), "s", 0, "uncompressed", 1024).unwrap();

        let rows = read_file(&path).unwrap();
        let kz = [
            ("k".to_string(), "1".to_string()),
            ("z".to_string(), "9".to_string()),
        ];
        assert_eq!(rows[0].extra, kz);
        assert_eq!(rows[1].extra, [("k".to_string(), "3".to_string())]);
        // Canonical order, first of run wins: same as the codec.
        assert_eq!(rows[2].extra, kz);
        assert_eq!(
            rows[3].extra,
            [
                ("k".to_string(), "1".to_string()),
                ("z".to_string(), "8".to_string())
            ],
            "sorted by (name, value), so the lower value is the survivor"
        );
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
        // Compiled out, so rejected at config load.
        assert!(writer_properties("s", 0, "brotli", 128).is_err());
        assert!(writer_properties("s", 0, "zstd", 128).is_err());
    }

    /// The advertised list and the writer must agree both ways.
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

    /// LZ4_RAW (codec 7), never deprecated LZ4 (codec 5): readers treat them differently.
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

    /// The flush never writes zero rows, but the result must still be a valid file.
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
