use serde::{Deserialize, Serialize};

pub const DEFAULT_IPC_ADDR: &str = "127.0.0.1:5001";

/// Các yêu cầu từ Thin CLI Client gửi tới P2P Daemon qua Local IPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    /// Yêu cầu tải lên và phân tán tệp tin (Legacy Manifest-based)
    Upload {
        file_path: String,
    },
    /// Yêu cầu tải về và khôi phục tệp tin từ manifest (Legacy)
    Download {
        manifest_path: String,
        output_path: String,
    },
    /// Yêu cầu lấy thông tin trạng thái hoạt động của daemon
    GetStatus,
    /// Yêu cầu Commit danh sách tệp tin vào cây thư mục ảo Virtual Tree
    Commit {
        paths: Vec<String>,
        message: Option<String>,
    },
    /// Yêu cầu xem danh sách thư mục / tệp tin trong Virtual Tree
    VfsList {
        path: Option<String>,
    },
    /// Yêu cầu vẽ cây thư mục Virtual Tree
    VfsTree {
        path: Option<String>,
    },
    /// Yêu cầu xóa tệp tin / thư mục khỏi Virtual Tree
    VfsRemove {
        path: String,
    },
    /// Yêu cầu tải tệp tin từ Virtual Tree
    VfsDownload {
        vfs_path: String,
        output_path: Option<String>,
    },
    /// Yêu cầu khôi phục / dump toàn bộ dữ liệu người dùng từ Private Key
    Dump {
        private_key: String,
        output_dir: String,
        vfs_path: Option<String>,
    },
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
    pub merit_mb: f64,
    pub stored_shards_mb: f64,
    pub r_ratio: Option<f64>,
    pub r_status: String,
    pub connected_peers_count: usize,
    pub known_peers_count: usize,
    pub listen_addrs: Vec<String>,
    pub dirty_files: Vec<String>,
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
    /// Kết quả hoàn thành Commit Virtual Tree
    CommitSuccess {
        committed_files: Vec<String>,
        total_bytes: u64,
        r_ratio: Option<f64>,
    },
    /// Kết quả liệt kê VFS
    VfsListSuccess {
        entries: Vec<String>,
    },
    /// Kết quả vẽ cây VFS
    VfsTreeSuccess {
        tree_rendered: String,
    },
    /// Kết quả xóa VFS
    VfsRemoveSuccess {
        path: String,
        freed_bytes: u64,
        r_ratio: Option<f64>,
    },
    /// Kết quả hoàn thành Dump toàn bộ dữ liệu
    DumpSuccess {
        restored_files_count: usize,
        total_bytes: u64,
        output_dir: String,
    },
    /// Kết quả truy vấn trạng thái
    StatusSuccess(DaemonStatusInfo),
    /// Báo lỗi xử lý
    Error(String),
}
