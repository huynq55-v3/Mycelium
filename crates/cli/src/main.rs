mod commands;
mod config;
mod ipc;
mod manifest;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Ứng dụng dòng lệnh (CLI) Hệ thống Lưu trữ Phân tán Mã nguồn mở Mycelium P2P Drive.
#[derive(Parser, Debug)]
#[command(
    name = "p2pdrive",
    author = "Mycelium Team",
    version = "0.1.0",
    about = "Hệ thống lưu trữ đám mây phân tán P2P bảo mật bằng Reed-Solomon Erasure Coding & AES-256-GCM",
    long_about = "Mycelium P2P Drive cho phép người dùng lưu trữ dữ liệu an toàn trên mạng lưới phân tán ngang hàng.\n\
                  Kiến trúc Client-Daemon qua Local IPC: Daemon quản lý duy nhất BlockStore & Swarm,\n\
                  CLI đóng vai trò Thin Client thực thi các tác vụ upload, download, status,\n\
                  Relay đóng vai trò là Trạm trung chuyển vượt NAT độc lập."
)]
struct Cli {
    /// Bật chế độ verbose logging (debug)
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Khởi tạo cấu hình node, sinh DID Ed25519 và thiết lập hạn ngạch ổ đĩa
    Init {
        /// Đường dẫn tới file Swarm Key riêng (nếu muốn tham gia Private Swarm)
        #[arg(short, long)]
        swarm_key: Option<PathBuf>,

        /// Dung lượng ổ cứng cam kết đóng góp cho mạng (GB, mặc định 60 GB -> cấp 15 GB upload)
        #[arg(short, long)]
        contribute_gb: Option<u64>,

        /// URL của Rendezvous / Bootstrap Server
        #[arg(short, long)]
        rendezvous_url: Option<String>,
    },

    /// [STORAGE DAEMON MODE] Khởi chạy tiến trình P2P Storage Node chạy nền (Daemon & Local IPC Server)
    Daemon {
        /// Cổng TCP lắng nghe kết nối P2P (mặc định 4001)
        #[arg(short, long)]
        port: Option<u16>,

        /// URL của Rendezvous / Bootstrap Server
        #[arg(short, long)]
        rendezvous_url: Option<String>,

        /// Địa chỉ IP công cộng tĩnh (nếu cấu hình Port-Forwarding/VPS/ngrok)
        #[arg(long)]
        public_ip: Option<String>,
    },

    /// [DEDICATED RELAY MODE] Khởi chạy Circuit Relay v2 Server độc lập (Trạm trung chuyển vượt NAT - Zero Database)
    Relay {
        /// Cổng TCP lắng nghe kết nối Relay trên máy (mặc định 4002)
        #[arg(short, long)]
        port: Option<u16>,

        /// Cổng TCP công khai ra ngoài (nếu dùng ngrok / port-forwarding khác cổng nội bộ)
        #[arg(long)]
        public_port: Option<u16>,

        /// Địa chỉ Host/Domain công khai hoặc IP (ví dụ 0.tcp.ap.ngrok.io hoặc 13.228.x.x)
        #[arg(long)]
        public_host: Option<String>,

        /// URL của Rendezvous / Bootstrap Server
        #[arg(short, long)]
        rendezvous_url: Option<String>,

        /// Đường dẫn tới file Swarm Key riêng
        #[arg(short, long)]
        swarm_key: Option<PathBuf>,
    },

    /// [CLIENT MODE] Mã hóa và phân tán tệp tin lên mạng lưới P2P thông qua Daemon
    Upload {
        /// Đường dẫn tệp tin cần tải lên
        #[arg(value_name = "FILE_PATH")]
        file_path: PathBuf,
    },

    /// [CLIENT MODE] Tải về và tái tạo tệp tin từ Virtual Tree hoặc manifest (*.manifest.json)
    Download {
        /// Đường dẫn tệp ảo (ví dụ /Documents/report.pdf) hoặc file manifest (*.manifest.json)
        #[arg(value_name = "TARGET")]
        target: String,

        /// Đường dẫn tệp đích lưu kết quả khôi phục (tùy chọn)
        #[arg(short, long, value_name = "OUTPUT_PATH")]
        output: Option<PathBuf>,
    },

    /// [CLIENT MODE] Hiển thị thông tin trạng thái hoạt động của daemon, hạn ngạch R và tệp tin thay đổi
    Status,

    /// [CLIENT MODE] Commit các tệp tin / thư mục thay đổi vào Cây thư mục ảo (Virtual Tree) và đẩy lên P2P
    Commit {
        /// Danh sách đường dẫn tệp tin cần commit (để trống để commit toàn bộ ~/MyceliumDrive)
        #[arg(value_name = "PATHS")]
        paths: Vec<String>,

        /// Thông điệp ghi chú commit
        #[arg(short, long)]
        message: Option<String>,
    },

    /// [CLIENT MODE] Liệt kê danh sách tệp tin trong Cây thư mục ảo (Virtual Tree)
    Ls {
        /// Đường dẫn thư mục ảo cần xem (mặc định xem root /)
        #[arg(value_name = "PATH")]
        path: Option<String>,
    },

    /// [CLIENT MODE] Vẽ cây phân cấp thư mục ảo (Virtual Tree)
    Tree {
        /// Đường dẫn thư mục ảo cần vẽ (mặc định vẽ root /)
        #[arg(value_name = "PATH")]
        path: Option<String>,
    },

    /// [CLIENT MODE] Xóa tệp tin hoặc thư mục khỏi Cây thư mục ảo (Virtual Tree)
    Rm {
        /// Đường dẫn tệp tin / thư mục ảo cần xóa (ví dụ /Documents/report.pdf)
        #[arg(value_name = "PATH")]
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Cấu hình logging với tracing: Mặc định hiện info của ứng dụng, ẩn log rác từ lib bên ngoài
    let filter = if cli.verbose {
        EnvFilter::new("debug,network=debug,core_crypto=debug,cli=debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,libp2p=warn,libp2p_kad=warn,reqwest=warn,hyper=warn,tower=warn")
        })
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().compact())
        .init();

    match cli.command {
        Commands::Init {
            swarm_key,
            contribute_gb,
            rendezvous_url,
        } => {
            commands::init::handle_init(swarm_key, contribute_gb, rendezvous_url)?;
        }
        Commands::Daemon {
            port,
            rendezvous_url,
            public_ip,
        } => {
            commands::daemon::handle_daemon(port, rendezvous_url, public_ip).await?;
        }
        Commands::Relay {
            port,
            public_port,
            public_host,
            rendezvous_url,
            swarm_key,
        } => {
            commands::relay::handle_relay(port, public_port, public_host, rendezvous_url, swarm_key).await?;
        }
        Commands::Upload { file_path } => {
            commands::upload::handle_upload(file_path).await?;
        }
        Commands::Download { target, output } => {
            if target.ends_with(".json") && PathBuf::from(&target).exists() {
                let out = output.unwrap_or_else(|| PathBuf::from("downloaded.bin"));
                commands::download::handle_download(PathBuf::from(target), out).await?;
            } else {
                let out_str = output.map(|p| p.to_string_lossy().to_string());
                commands::vfs_cmds::handle_vfs_download(target, out_str).await?;
            }
        }
        Commands::Status => {
            commands::status::handle_status().await?;
        }
        Commands::Commit { paths, message } => {
            commands::commit::handle_commit(paths, message).await?;
        }
        Commands::Ls { path } => {
            commands::vfs_cmds::handle_ls(path).await?;
        }
        Commands::Tree { path } => {
            commands::vfs_cmds::handle_tree(path).await?;
        }
        Commands::Rm { path } => {
            commands::vfs_cmds::handle_rm(path).await?;
        }
    }

    Ok(())
}
