use thiserror::Error;

/// Các lỗi có thể xảy ra trong module mã hóa phân đoạn `erasure-codec`.
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("Lỗi thuật toán Reed-Solomon: {0}")]
    ReedSolomonError(#[from] reed_solomon_erasure::Error),

    #[error("Không đủ số lượng shards để khôi phục: yêu cầu tối thiểu {required}, nhưng chỉ có {available}")]
    InsufficientShards {
        required: usize,
        available: usize,
    },

    #[error("Độ dài mảng shards không hợp lệ: yêu cầu {expected} shards, nhưng nhận được {actual}")]
    InvalidShardArrayLength {
        expected: usize,
        actual: usize,
    },

    #[error("Kích thước các shard không đồng nhất hoặc không hợp lệ")]
    InvalidShardSize,

    #[error("Mã băm của shard tại index {index} không khớp: kỳ vọng {expected}, nhận {actual}")]
    InvalidShardHash {
        index: usize,
        expected: String,
        actual: String,
    },

    #[error("Tính toàn vẹn dữ liệu gốc bị vi phạm: SHA-256 sau khi khôi phục không khớp với manifest")]
    CorruptedDataIntegrity,

    #[error("Dữ liệu đầu vào rỗng (0 bytes)")]
    EmptyData,

    #[error("Lỗi serialize/deserialize JSON: {0}")]
    SerializationError(#[from] serde_json::Error),
}
