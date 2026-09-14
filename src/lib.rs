#![forbid(unsafe_code)]

//! Parquet archive: an [`ArchiveStore`] that writes each retained item as an
//! Apache Parquet file under a directory, and restores it by reading the file
//! back.
//!
//! A xmip-core-archive **technology** (repository-model.md): it depends on the
//! archive capability for the [`ArchiveStore`] trait and its item, receipt and
//! error types, never the reverse. One item is one Parquet file at
//! `<root>/<data_type>/<identifier>.parquet`, four columns — `data_type`,
//! `identifier`, `bytes`, `metadata` — so the file is self-describing and an
//! operator can open it with any Parquet reader. The metadata text and the safe
//! file name come from the capability, `archive::metadata` and `archive::layout`
//! (ADR-0044).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use archive::layout::sanitise;
use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore, metadata};
use arrow::array::{Array, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// An archive that persists items as Apache Parquet files rooted at a directory.
pub struct ParquetArchive {
    root: PathBuf,
}

impl ParquetArchive {
    /// An archive writing under `root`; the directory tree is created on demand
    /// as items are archived.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The Parquet file path for an item: `<root>/<data_type>/<identifier>.parquet`,
    /// each segment made filesystem-safe.
    fn path_for(&self, data_type: &str, identifier: &str) -> PathBuf {
        self.root
            .join(sanitise(data_type))
            .join(format!("{}.parquet", sanitise(identifier)))
    }
}

impl ArchiveStore for ParquetArchive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        let path = self.path_for(&item.data_type, &item.identifier);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(ArchiveError::caused_by)?;
        }

        let batch = to_batch(&item)?;
        let file = File::create(&path).map_err(ArchiveError::caused_by)?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), None).map_err(ArchiveError::caused_by)?;
        writer.write(&batch).map_err(ArchiveError::caused_by)?;
        writer.close().map_err(ArchiveError::caused_by)?;

        Ok(ArchiveReceipt {
            location: path.display().to_string(),
            checksum: None,
        })
    }

    fn restore(&self, receipt: &ArchiveReceipt) -> Result<ArchiveItem, ArchiveError> {
        let file = File::open(&receipt.location).map_err(ArchiveError::caused_by)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(ArchiveError::caused_by)?
            .build()
            .map_err(ArchiveError::caused_by)?;
        let batch = reader
            .next()
            .ok_or_else(|| ArchiveError {
                message: format!("no rows in {}", receipt.location),
            })?
            .map_err(ArchiveError::caused_by)?;
        from_batch(&batch, &receipt.location)
    }
}

/// The schema every item is written under.
fn schema() -> Schema {
    Schema::new(vec![
        Field::new("data_type", DataType::Utf8, false),
        Field::new("identifier", DataType::Utf8, false),
        Field::new("bytes", DataType::Binary, false),
        Field::new("metadata", DataType::Utf8, false),
    ])
}

/// One item as a single-row record batch.
fn to_batch(item: &ArchiveItem) -> Result<RecordBatch, ArchiveError> {
    RecordBatch::try_new(
        Arc::new(schema()),
        vec![
            Arc::new(StringArray::from(vec![item.data_type.as_str()])),
            Arc::new(StringArray::from(vec![item.identifier.as_str()])),
            Arc::new(BinaryArray::from_iter_values([item.bytes.as_slice()])),
            Arc::new(StringArray::from(vec![metadata::encode(&item.metadata)])),
        ],
    )
    .map_err(ArchiveError::caused_by)
}

/// The first row of a batch back into an item.
fn from_batch(batch: &RecordBatch, location: &str) -> Result<ArchiveItem, ArchiveError> {
    let data_type = string_at(batch, 0, location)?;
    let identifier = string_at(batch, 1, location)?;
    let bytes = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| malformed(location, "bytes"))?
        .value(0)
        .to_vec();
    let metadata = metadata::decode(&string_at(batch, 3, location)?);
    Ok(ArchiveItem {
        data_type,
        identifier,
        bytes,
        metadata,
    })
}

fn string_at(batch: &RecordBatch, column: usize, location: &str) -> Result<String, ArchiveError> {
    let name = batch.schema().field(column).name().clone();
    let value = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| malformed(location, &name))?
        .value(0)
        .to_string();
    Ok(value)
}

fn malformed(location: &str, column: &str) -> ArchiveError {
    ArchiveError {
        message: format!("column {column} is not the expected type in {location}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use archive::fixture::item;
    use std::path::Path;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-parquet-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn an_archived_item_becomes_a_parquet_file_on_disk() {
        let root = scratch("write");
        let store = ParquetArchive::new(&root);
        let receipt = store.archive(item("json#1")).expect("archive");
        assert!(receipt.location.ends_with(".parquet"));
        assert!(Path::new(&receipt.location).exists(), "the file is on disk");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_file_is_real_parquet_and_restores_the_item() {
        let root = scratch("roundtrip");
        let store = ParquetArchive::new(&root);
        let original = item("json#2");
        let receipt = store.archive(original.clone()).expect("archive");
        let restored = store.restore(&receipt).expect("restore");
        assert_eq!(
            restored, original,
            "a real Parquet read gives the item back"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_risky_identifier_is_made_a_safe_file_name() {
        let root = scratch("sanitise");
        let store = ParquetArchive::new(&root);
        let receipt = store.archive(item("poison-json#3/../x")).expect("archive");
        assert!(Path::new(&receipt.location).exists());
        assert!(!receipt.location.contains(".."), "no traversal survives");
        std::fs::remove_dir_all(&root).ok();
    }
}
