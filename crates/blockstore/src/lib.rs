//! # BlockStore Module for Mycelium P2P Storage
//!
//! Lưu trữ cục bộ các mảnh nhị phân (shards) với cơ sở dữ liệu nhúng siêu tốc `sled`.
//! Khóa là mã băm SHA-256 (`shard_hash`), giá trị là mảng byte nhị phân (`shard_bytes`).

pub mod error;
pub mod store;

// Re-exports
pub use error::BlockStoreError;
pub use store::{BlockStore, DEFAULT_BLOCKSTORE_DIR, DEFAULT_CONFIG_DIR};
