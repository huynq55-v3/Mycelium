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

    #[error("Lỗi I/O khi lưu/nạp cấu hình quota: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Lỗi serialize/deserialize JSON: {0}")]
    SerializationError(#[from] serde_json::Error),
}
