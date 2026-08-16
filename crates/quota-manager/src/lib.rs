//! # Quota Manager Module for Mycelium P2P Storage
//!
//! Quản lý hạn mức lưu trữ mạng theo nguyên lý đóng góp ổ đĩa $1:4$ (`REDUNDANCY_FACTOR = 4.0`).
//! Một node cam kết đóng góp 60 GB ổ cứng sẽ được phép tải lên tối đa 15 GB dữ liệu gốc.

pub mod error;
pub mod manager;

// Re-exports
pub use error::QuotaError;
pub use manager::{QuotaManager, DEFAULT_ALLOCATED_DISK_BYTES, REDUNDANCY_FACTOR};
