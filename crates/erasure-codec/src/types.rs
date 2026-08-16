use serde::{Deserialize, Serialize};

use crate::error::CodecError;

/// Đại diện cho một mảnh phân đoạn (Shard) dữ liệu được tạo ra từ thuật toán Reed-Solomon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shard {
    /// Chỉ số thứ tự của shard trong tập tổng số N shards (0 .. N-1).
    pub index: usize,
    /// Dữ liệu byte của shard (đã bao gồm padding nếu là data shard, hoặc dữ liệu bù nếu là parity shard).
    pub data: Vec<u8>,
    /// Mã băm SHA-256 dạng hex để kiểm tra tính toàn vẹn độc lập của shard này.
    pub hash: String,
}

/// Bản kê khai thông tin tệp (Manifest) chứa siêu dữ liệu cần thiết để định danh,
/// kiểm tra và tái tạo lại toàn bộ tệp gốc từ các mảnh phân đoạn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileManifest {
    /// Tên của tệp dữ liệu.
    pub file_name: String,
    /// Kích thước chính xác của dữ liệu gốc tính bằng bytes (trước khi padding).
    pub original_size: usize,
    /// Mã băm SHA-256 hex của toàn bộ dữ liệu gốc.
    pub original_hash: String,
    /// Số lượng data shards cần thiết ($K$).
    pub k_data_shards: usize,
    /// Tổng số shards được sinh ra ($N = K + M$).
    pub n_total_shards: usize,
    /// Danh sách mã băm SHA-256 của từng shard theo thứ tự index (0 .. N-1).
    pub shard_hashes: Vec<String>,
}

impl FileManifest {
    /// Xuất manifest ra định dạng chuỗi JSON định dạng đẹp (pretty printed).
    pub fn to_json(&self) -> Result<String, CodecError> {
        serde_json::to_string_pretty(self).map_err(CodecError::SerializationError)
    }

    /// Nạp manifest từ một chuỗi JSON.
    pub fn from_json(json_str: &str) -> Result<Self, CodecError> {
        serde_json::from_str(json_str).map_err(CodecError::SerializationError)
    }
}
