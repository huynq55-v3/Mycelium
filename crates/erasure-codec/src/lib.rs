//! # Erasure Codec Module for Mycelium P2P Storage
//!
//! Module phân đoạn và dự phòng dữ liệu theo thuật toán Reed-Solomon Erasure Coding:
//! - Cấu hình mặc định $K=10$ data shards và $M=30$ parity shards ($N=40$ total shards).
//! - Phục hồi 100% dữ liệu gốc chỉ với tối thiểu 10 shards hợp lệ bất kỳ.
//! - Tự động đệm padding và cắt bỏ padding khi giải mã.
//! - Quản lý `FileManifest` và `Shard` với kiểm tra toàn vẹn mã băm SHA-256.

pub mod codec;
pub mod error;
pub mod types;

// Re-exports
pub use codec::{decode, encode, encode_with_name, sha256_hex, DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS};
pub use error::CodecError;
pub use types::{FileManifest, Shard};
