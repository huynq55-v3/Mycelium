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
                  CLI đóng vai trò Thin Client thực thi các tác vụ upload, download, status."
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

    /// [SERVER MODE] Khởi chạy tiến trình P2P Node chạy nền (Daemon & Local IPC Server)
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

        /// Bật tính năng Circuit Relay v2 Server (làm trạm trung chuyển vượt NAT cho mạng lưới)
        #[arg(long)]
        relay: bool,
    },

    /// [CLIENT MODE] Mã hóa và phân tán tệp tin lên mạng lưới P2P thông qua Daemon
    Upload {
        /// Đường dẫn tệp tin cần tải lên
        #[arg(value_name = "FILE_PATH")]
        file_path: PathBuf,
    },

    /// [CLIENT MODE] Tải về và tái tạo tệp tin từ file manifest thông qua Daemon
    Download {
        /// Đường dẫn tới file manifest (*.manifest.json)
        #[arg(value_name = "MANIFEST_PATH")]
        manifest_path: PathBuf,

        /// Đường dẫn tệp đích lưu kết quả khôi phục
        #[arg(short, long, value_name = "OUTPUT_PATH")]
        output: PathBuf,
    },

    /// [CLIENT MODE] Hiển thị thông tin trạng thái hoạt động của daemon, hạn ngạch và dung lượng
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Cấu hình logging với tracing
    let filter = if cli.verbose {
        EnvFilter::new("debug,network=debug,core_crypto=debug")
    } else {
        EnvFilter::new("warn")
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
            relay,
        } => {
            commands::daemon::handle_daemon(port, rendezvous_url, public_ip, relay).await?;
        }
        Commands::Upload { file_path } => {
            commands::upload::handle_upload(file_path).await?;
        }
        Commands::Download {
            manifest_path,
            output,
        } => {
            commands::download::handle_download(manifest_path, output).await?;
        }
        Commands::Status => {
            commands::status::handle_status().await?;
        }
    }

    Ok(())
}
