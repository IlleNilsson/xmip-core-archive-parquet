# xmip-core-archive-parquet

Parquet archive target for Xmip: retained items written as Apache Parquet
files. A **technology** of the `xmip-core-archive` capability — it implements
`ArchiveStore`, depending on the capability, never the reverse
(`doc/architecture/repository-model.md`).

One archived item is one Parquet file at
`<root>/<data_type>/<identifier>.parquet`, with four columns — `data_type`,
`identifier`, `bytes`, `metadata` — so the file is self-describing and opens in
any Parquet reader. `restore` reads the file back into the original item.

```rust
use archive::{ArchiveItem, ArchiveStore};
use xmip_core_archive_parquet::ParquetArchive;

let store = ParquetArchive::new("/var/xmip/archive");
let receipt = store.archive(item)?;   // writes a .parquet file
let restored = store.restore(&receipt)?;
```
