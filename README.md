# xmip-core-archive-parquet

Parquet archive target for Xmip: retained items written as Apache Parquet
files. A **technology** of the `xmip-core-archive` capability — it implements
`ArchiveStore`, depending on the capability, never the reverse
(`doc/architecture/repository-model.md`).

One archived item is one Parquet file at
`<root>/<data_type>/<identifier>.parquet`, with four columns — `data_type`,
`identifier`, `bytes`, `metadata` — so the file is self-describing and opens in
any Parquet reader. `restore` reads the file back into the original item.

An archived item is never written over (ADR-0040, amendment 2026-11-19):
the file is written beside its name, synced, hard-linked to the name only
where the name is free, and its directory synced (`archive::whole`). Where
a file is there already, the item it holds decides: the same data type,
identifier and bytes answer the same receipt; the same identifier with
other bytes, or another item whose name becomes the same file name, is
refused in words (`archive::named::Claim`). A file a process that died
left under its mark beside a name is removed by the sweep the next write
into its directory starts beside it, never one a live process holds
(`archive::whole`).

The receipt is the file archive's (ADR-0040, amendment 2026-11-20):
`file:///<root>/<data_type>/<identifier>.parquet` and the SHA-256 of the
bytes (`archive::directory`). A restore reads the file the receipt names
under the root, and refuses a receipt from under another root and bytes
whose SHA-256 is not the receipt's (`archive::checksum::verify`).

```rust
use archive::{ArchiveItem, ArchiveStore};
use xmip_core_archive_parquet::ParquetArchive;

let store = ParquetArchive::new("/var/xmip/archive");
let receipt = store.archive(item)?;   // writes a .parquet file
let restored = store.restore(&receipt)?;
```
