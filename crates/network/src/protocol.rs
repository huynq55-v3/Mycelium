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

/// Thông điệp yêu cầu thu hồi / xóa bỏ shard thừa (`PruneShard`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneShard {
    /// Mã băm SHA-256 của shard cần thu hồi.
    pub hash: String,
}

/// Phản hồi sau khi tiếp nhận `PruneShard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneResponse {
    /// Node đã xóa thành công shard hay không.
    pub pruned: bool,
    /// Lý do nếu không xóa (ví dụ: làm R tụt dưới 4.0).
    pub reason: Option<String>,
}

/// Định nghĩa thông điệp Request tổng hợp của giao thức lưu trữ P2P Mycelium.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardRequest {
    Push(PushShard),
    Pull(PullShard),
    Prune(PruneShard),
}

/// Định nghĩa thông điệp Response tổng hợp của giao thức lưu trữ P2P Mycelium.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardResponse {
    Push(PushResponse),
    Pull(PullResponse),
    Prune(PruneResponse),
}
