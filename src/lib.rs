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
//!
//! An archived item is never written over (ADR-0040, amendment 2026-11-19):
//! the file is created whole and linked to its name only where the name is
//! free (`archive::whole`). Where a file is there already, the item it holds
//! decides (`archive::named::Claim`): the same item answers the same receipt,
//! another — or the same identifier with other bytes — is refused in words.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use archive::checksum::sha256_hex;
use archive::layout::sanitize;
use archive::named::{Claim, Created};
use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore, metadata, whole};
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
            .join(sanitize(data_type))
            .join(format!("{}.parquet", sanitize(identifier)))
    }
}

impl ArchiveStore for ParquetArchive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        let path = self.path_for(&item.data_type, &item.identifier);
        let batch = to_batch(&item)?;
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None)
            .map_err(ArchiveError::caused_by)?;
        writer.write(&batch).map_err(ArchiveError::caused_by)?;
        writer.close().map_err(ArchiveError::caused_by)?;

        let receipt = ArchiveReceipt {
            location: path.display().to_string(),
            checksum: None,
        };
        let created =
            whole::create(&path, |out| out.write_all(&file)).map_err(|cause| ArchiveError {
                message: format!("{}: {cause}", path.display()),
            })?;
        if created == Created::Exists {
            // The file there is judged by the item it holds, read back as a
            // restore reads it: the same item carries on, any other is refused.
            let held = self.restore(&receipt)?;
            let checksum = sha256_hex(&item.bytes);
            let held_checksum = sha256_hex(&held.bytes);
            Claim {
                data_type: &item.data_type,
                identifier: &item.identifier,
                checksum: &checksum,
            }
            .against(
                &Claim {
                    data_type: &held.data_type,
                    identifier: &held.identifier,
                    checksum: &held_checksum,
                },
                &receipt.location,
            )?;
        }
        Ok(receipt)
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

    /// A root of its own under the temporary directory, removed when the
    /// test ends, also when it fails.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("xmip-parquet-{name}-{}", std::process::id()));
            std::fs::remove_dir_all(&dir).ok();
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn other_bytes(id: &str) -> ArchiveItem {
        ArchiveItem {
            bytes: b"{\"kept\":false}".to_vec(),
            ..item(id)
        }
    }

    #[test]
    fn an_archived_item_becomes_a_parquet_file_on_disk() {
        let root = Scratch::new("write");
        let store = ParquetArchive::new(&root.0);
        let receipt = store.archive(item("json#1")).expect("archive");
        assert!(receipt.location.ends_with(".parquet"));
        assert!(Path::new(&receipt.location).exists(), "the file is on disk");
    }

    #[test]
    fn the_file_is_real_parquet_and_restores_the_item() {
        let root = Scratch::new("roundtrip");
        let store = ParquetArchive::new(&root.0);
        let original = item("json#2");
        let receipt = store.archive(original.clone()).expect("archive");
        let restored = store.restore(&receipt).expect("restore");
        assert_eq!(
            restored, original,
            "a real Parquet read gives the item back"
        );
    }

    #[test]
    fn a_risky_identifier_is_made_a_safe_file_name() {
        let root = Scratch::new("sanitize");
        let store = ParquetArchive::new(&root.0);
        let receipt = store.archive(item("poison-json#3/../x")).expect("archive");
        assert!(Path::new(&receipt.location).exists());
        assert!(!receipt.location.contains(".."), "no traversal survives");
    }

    #[test]
    fn an_identifier_archived_again_with_the_same_bytes_answers_the_same_receipt() {
        let root = Scratch::new("same");
        let store = ParquetArchive::new(&root.0);
        let first = store.archive(item("json#1")).expect("archive");
        let again = store.archive(item("json#1")).expect("archived again");
        assert_eq!(again, first);
        assert_eq!(store.restore(&again).expect("restore"), item("json#1"));
    }

    #[test]
    fn an_identifier_archived_again_with_other_bytes_is_refused_and_the_earlier_receipt_restores() {
        let root = Scratch::new("other");
        let store = ParquetArchive::new(&root.0);
        let receipt = store.archive(item("json#1")).expect("archive");
        let refused = store
            .archive(other_bytes("json#1"))
            .expect_err("never written over");
        assert!(
            refused
                .message
                .contains("is archived already with other bytes"),
            "{refused}"
        );
        let refused = store
            .archive(item("json_1"))
            .expect_err("the same file name");
        assert!(
            refused
                .message
                .contains("is laid out at the same name and is refused"),
            "{refused}"
        );
        assert_eq!(store.restore(&receipt).expect("restore"), item("json#1"));
    }
}
