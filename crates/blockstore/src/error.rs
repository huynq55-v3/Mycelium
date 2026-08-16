use thiserror::Error;

/// Các lỗi có thể xảy ra trong `BlockStore`.
#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error("Lỗi cơ sở dữ liệu Sled: {0}")]
    SledError(#[from] sled::Error),

    #[error("Lỗi I/O khi thao tác với ổ đĩa: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Không thể xác định thư mục home người dùng")]
    HomeDirectoryNotFound,

    #[error("Không tìm thấy shard với hash: {0}")]
    ShardNotFound(String),
}
