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
//! A file a process that died left under its mark is removed by the sweep
//! the next write into its directory starts beside it, never one a live
//! process holds.
//!
//! The receipt is the file archive's (ADR-0040, amendment 2026-11-20):
//! `file:///<root>/<data_type>/<identifier>.parquet`, read back under the
//! root alone (`archive::directory`), and the SHA-256 of the bytes, which a
//! restore checks (`archive::checksum::verify`).

use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use archive::checksum::{self, sha256_hex};
use archive::directory::Directory;
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
    directory: Directory,
}

impl ParquetArchive {
    /// An archive writing under `root`; the directory tree is created on demand
    /// as items are archived.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            directory: Directory::new(root),
        }
    }
}

/// The key of an item's file under the root:
/// `<data_type>/<identifier>.parquet`, each segment made filesystem-safe.
fn key_of(item: &ArchiveItem) -> String {
    format!(
        "{}/{}.parquet",
        sanitize(&item.data_type),
        sanitize(&item.identifier)
    )
}

impl ArchiveStore for ParquetArchive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        let key = key_of(&item);
        let path = self.directory.path_of(&key)?;
        let batch = to_batch(&item)?;
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None)
            .map_err(ArchiveError::caused_by)?;
        writer.write(&batch).map_err(ArchiveError::caused_by)?;
        writer.close().map_err(ArchiveError::caused_by)?;

        let checksum = sha256_hex(&item.bytes);
        let location = self.directory.location(&key);
        let created =
            whole::create(&path, |out| out.write_all(&file)).map_err(|cause| at(&path, cause))?;
        if created == Created::Exists {
            // The file there is judged by the item it holds, read back as a
            // restore reads it: the same item carries on, any other is refused.
            let held = read(&path, &location)?;
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
                &location,
            )?;
        }
        Ok(ArchiveReceipt {
            location,
            checksum: Some(checksum),
        })
    }

    fn restore(&self, receipt: &ArchiveReceipt) -> Result<ArchiveItem, ArchiveError> {
        let key = self.directory.key(&receipt.location)?;
        let item = read(&self.directory.path_of(key)?, &receipt.location)?;
        checksum::verify(receipt, None, &item.bytes)?;
        Ok(item)
    }
}

/// The item the Parquet file at `path` holds, which `location` names.
fn read(path: &Path, location: &str) -> Result<ArchiveItem, ArchiveError> {
    let file = File::open(path).map_err(|cause| at(path, cause))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(ArchiveError::caused_by)?
        .build()
        .map_err(ArchiveError::caused_by)?;
    let batch = reader
        .next()
        .ok_or_else(|| ArchiveError {
            message: format!("no rows in {location}"),
        })?
        .map_err(ArchiveError::caused_by)?;
    from_batch(&batch, location)
}

fn at(path: &Path, cause: impl Display) -> ArchiveError {
    ArchiveError {
        message: format!("{}: {cause}", path.display()),
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
    use file::temporary::Folder;

    fn other_bytes(id: &str) -> ArchiveItem {
        ArchiveItem {
            bytes: b"{\"kept\":false}".to_vec(),
            ..item(id)
        }
    }

    #[test]
    fn an_archived_item_becomes_a_parquet_file_on_disk() {
        let root = Folder::new("archive-parquet-write");
        let store = ParquetArchive::new(&*root);
        let receipt = store.archive(item("json#1")).expect("archive");
        assert!(receipt.location.ends_with(".parquet"));
        let path = root.join("json").join("json_1.parquet");
        assert!(path.exists(), "the file is on disk");
    }

    #[test]
    fn the_file_is_real_parquet_and_restores_the_item() {
        let root = Folder::new("archive-parquet-roundtrip");
        let store = ParquetArchive::new(&*root);
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
        let root = Folder::new("archive-parquet-sanitize");
        let store = ParquetArchive::new(&*root);
        let receipt = store.archive(item("poison-json#3/../x")).expect("archive");
        let path = root.join("json").join("poison-json_3____x.parquet");
        assert!(path.exists(), "the file is on disk");
        assert!(!receipt.location.contains(".."), "no traversal survives");
    }

    #[test]
    fn an_identifier_archived_again_with_the_same_bytes_answers_the_same_receipt() {
        let root = Folder::new("archive-parquet-same");
        let store = ParquetArchive::new(&*root);
        let first = store.archive(item("json#1")).expect("archive");
        let again = store.archive(item("json#1")).expect("archived again");
        assert_eq!(again, first);
        assert_eq!(store.restore(&again).expect("restore"), item("json#1"));
    }

    #[test]
    fn an_identifier_archived_again_with_other_bytes_is_refused_and_the_earlier_receipt_restores() {
        let root = Folder::new("archive-parquet-other");
        let store = ParquetArchive::new(&*root);
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

    /// The names in `dir`, sorted.
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .expect("listing")
            .map(|entry| {
                entry
                    .expect("an entry")
                    .file_name()
                    .to_string_lossy()
                    .into()
            })
            .collect();
        names.sort();
        names
    }

    #[test]
    fn the_receipt_is_a_file_uri_under_the_root_with_the_sha256_of_the_bytes() {
        let root = Folder::new("archive-parquet-receipt");
        let store = ParquetArchive::new(&*root);
        let original = item("json#1");
        let receipt = store.archive(original.clone()).expect("archive");
        assert!(
            receipt.location.starts_with("file:///"),
            "{}",
            receipt.location
        );
        assert!(
            receipt.location.ends_with("/json/json_1.parquet"),
            "{}",
            receipt.location
        );
        assert_eq!(receipt.checksum, Some(sha256_hex(&original.bytes)));
        assert_eq!(store.restore(&receipt).expect("restore"), original);
    }

    #[test]
    fn bytes_changed_since_they_were_archived_are_refused_on_restore() {
        let root = Folder::new("archive-parquet-changed");
        let store = ParquetArchive::new(&*root);
        let receipt = store.archive(item("json#1")).expect("archive");
        // The same item with other bytes, archived elsewhere, put over the
        // file the receipt names.
        let elsewhere = Folder::new("archive-parquet-changed-elsewhere");
        ParquetArchive::new(&*elsewhere)
            .archive(other_bytes("json#1"))
            .expect("archived elsewhere");
        let file = Path::new("json").join("json_1.parquet");
        std::fs::remove_file(root.join(&file)).expect("the archived file");
        std::fs::copy(elsewhere.join(&file), root.join(&file)).expect("other bytes");
        let refused = store.restore(&receipt).expect_err("changed underneath");
        assert!(refused.message.contains("checksum mismatch"), "{refused}");
        let mut lying = receipt;
        lying.checksum = Some("0".repeat(64));
        let refused = store.restore(&lying).expect_err("the receipt lies");
        assert!(refused.message.contains("checksum mismatch"), "{refused}");
    }

    #[test]
    fn a_receipt_from_under_another_root_is_refused() {
        let root = Folder::new("archive-parquet-elsewhere");
        let store = ParquetArchive::new(&*root);
        let receipt = ArchiveReceipt {
            location: "file:///elsewhere/json/x.parquet".to_string(),
            checksum: None,
        };
        let refused = store.restore(&receipt).expect_err("another root");
        assert!(refused.message.contains("not a receipt"), "{refused}");
    }

    #[test]
    fn a_file_left_under_its_mark_is_removed_by_the_next_archive_into_its_directory() {
        let root = Folder::new("archive-parquet-left");
        let store = ParquetArchive::new(&*root);
        // What a process that died between a write and its link left: a
        // file under the mark, held by no process.
        let directory = root.join("json");
        std::fs::create_dir_all(&directory).expect("the directory");
        let left = "json_0.parquet.xmip-archiving.4294967295.0";
        std::fs::write(directory.join(left), b"PAR1").expect("a file left behind");
        store.archive(item("json#1")).expect("archive");
        whole::swept(&directory);
        assert_eq!(names(&directory), ["json_1.parquet"]);
    }
}
