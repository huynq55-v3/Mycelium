use thiserror::Error;

/// Các lỗi liên quan đến quản lý hạn mức và dung lượng đóng góp `QuotaManager`.
#[derive(Debug, Error)]
pub enum QuotaError {
    #[error("Vượt quá hạn mức tải lên cho phép: Yêu cầu {requested_bytes} bytes, khả dụng còn lại {available_bytes} bytes (Đã dùng: {current_uploaded_bytes}/{allowed_capacity_bytes} bytes)")]
    UploadQuotaExceeded {
        requested_bytes: u64,
        available_bytes: u64,
        current_uploaded_bytes: u64,
        allowed_capacity_bytes: u64,
    },

    #[error("Chưa đủ điều kiện đóng góp (R < 4.0): Cần nhận thêm ít nhất {required_shard_bytes} bytes Shard từ mạng trước khi tải lên file {file_size} bytes")]
    InsufficientContribution {
        file_size: u64,
        required_shard_bytes: u64,
        current_r_ratio: Option<f64>,
    },

    #[error("Lần commit đầu tiên yêu cầu dung lượng từ {min_mb} MB đến {max_mb} MB (Hiện tại: {actual_mb:.2} MB) để kích hoạt hệ thống P2P")]
    FirstCommitSizeOutOfRange {
        actual_mb: f64,
        min_mb: u64,
        max_mb: u64,
    },

    #[error("Lỗi I/O khi lưu/nạp cấu hình quota: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Lỗi serialize/deserialize JSON: {0}")]
    SerializationError(#[from] serde_json::Error),
}
