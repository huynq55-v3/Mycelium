use serde::{Deserialize, Serialize};

/// Thông điệp gửi lên một mảnh shard (`PushShard`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushShard {
    /// Mã băm SHA-256 của shard.
    pub hash: String,
    /// Dữ liệu nhị phân của shard.
    pub data: Vec<u8>,
}

/// Phản hồi sau khi tiếp nhận `PushShard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushResponse {
    /// Node nhận có chấp thuận lưu trữ hay không (phụ thuộc QuotaManager).
    pub accepted: bool,
    /// Lý do nếu bị từ chối.
    pub reason: Option<String>,
}

/// Thông điệp yêu cầu tải về một mảnh shard (`PullShard`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullShard {
    /// Mã băm SHA-256 của shard cần tải.
    pub hash: String,
}

/// Phản hồi dữ liệu cho `PullShard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullResponse {
    /// Dữ liệu byte của shard (hoặc `None` nếu node không lưu giữ).
    pub data: Option<Vec<u8>>,
}

/// Định nghĩa thông điệp Request tổng hợp của giao thức lưu trữ P2P Mycelium.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardRequest {
    Push(PushShard),
    Pull(PullShard),
}

/// Định nghĩa thông điệp Response tổng hợp của giao thức lưu trữ P2P Mycelium.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardResponse {
    Push(PushResponse),
    Pull(PullResponse),
}
