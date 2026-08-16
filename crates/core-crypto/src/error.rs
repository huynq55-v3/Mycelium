use thiserror::Error;

/// Các lỗi có thể xảy ra trong module mật mã `core-crypto`.
#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("Lỗi mã hóa dữ liệu: {0}")]
    EncryptionError(String),

    #[error("Lỗi giải mã dữ liệu: {0}")]
    DecryptionError(String),

    #[error("Độ dài dữ liệu mã hóa không hợp lệ. Yêu cầu tối thiểu {expected_min} bytes (Nonce + Tag), nhưng nhận được {actual} bytes")]
    InvalidCiphertextLength {
        expected_min: usize,
        actual: usize,
    },

    #[error("Độ dài khóa không hợp lệ: Yêu cầu {expected} bytes, nhận {actual} bytes")]
    InvalidKeyLength {
        expected: usize,
        actual: usize,
    },

    #[error("Lỗi định dạng Hex: {0}")]
    InvalidHex(#[from] hex::FromHexError),

    #[error("Lỗi I/O khi đọc/ghi file khóa: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Lỗi serialize/deserialize JSON: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Lỗi chữ ký số Ed25519: {0}")]
    SignatureError(#[from] ed25519_dalek::SignatureError),

    #[error("Định dạng DID không hợp lệ: {0}")]
    InvalidDidFormat(String),

    #[error("Không thể xác định thư mục home người dùng")]
    HomeDirectoryNotFound,
}
