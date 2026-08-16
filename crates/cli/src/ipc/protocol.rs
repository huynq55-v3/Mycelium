use serde::{Deserialize, Serialize};

pub const DEFAULT_IPC_ADDR: &str = "127.0.0.1:5001";

/// Các yêu cầu từ Thin CLI Client gửi tới P2P Daemon qua Local IPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Yêu cầu tải lên và phân tán tệp tin
    Upload {
        file_path: String,
    },
    /// Yêu cầu tải về và khôi phục tệp tin từ manifest
    Download {
        manifest_path: String,
        output_path: String,
    },
    /// Yêu cầu lấy thông tin trạng thái hoạt động của daemon
    GetStatus,
}

/// Thông tin trạng thái của P2P Daemon trả về cho client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatusInfo {
    pub did: String,
    pub is_private_swarm: bool,
    pub swarm_key_preview: Option<String>,
    pub rendezvous_url: String,
    pub rendezvous_online: bool,
    pub shard_count: usize,
    pub disk_used_mb: f64,
    pub allocated_gb: f64,
    pub upload_used_gb: f64,
    pub upload_limit_gb: f64,
    pub connected_peers_count: usize,
    pub listen_addrs: Vec<String>,
}

/// Các phản hồi từ P2P Daemon gửi về cho Thin CLI Client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcResponse {
    /// Cập nhật tiến độ xử lý
    Progress {
        step: String,
        current: u64,
        total: u64,
        message: String,
    },
    /// Kết quả hoàn thành tải lên
    UploadSuccess {
        file_name: String,
        original_size: usize,
        encrypted_cid: String,
        manifest_path: String,
    },
    /// Kết quả hoàn thành tải về
    DownloadSuccess {
        file_name: String,
        bytes_written: usize,
        output_path: String,
    },
    /// Kết quả truy vấn trạng thái
    StatusSuccess(DaemonStatusInfo),
    /// Báo lỗi xử lý
    Error(String),
}
