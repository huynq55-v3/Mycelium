use std::path::PathBuf;

use anyhow::{bail, Result};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

/// Xử lý lệnh `p2pdrive dump -k <private_key> -o <output_dir> [--vfs-path <path>]`.
pub async fn handle_dump(
    private_key: String,
    output_dir: PathBuf,
    vfs_path: Option<PathBuf>,
) -> Result<()> {
    println!("============================================================");
    println!("       📦 KHÔI PHỤC TOÀN BỘ DỮ LIỆU MYCELIUM P2P DRIVE       ");
    println!("============================================================");

    let output_dir_str = output_dir.to_string_lossy().to_string();
    let vfs_path_str = vfs_path.map(|p| p.to_string_lossy().to_string());
    let mut final_error = None;

    IpcClient::send_request(
        IpcRequest::Dump {
            private_key,
            output_dir: output_dir_str,
            vfs_path: vfs_path_str,
        },
        |response| match response {
            IpcResponse::Progress { message, .. } => {
                println!("⏳ {}", message);
                Ok(true)
            }
            IpcResponse::DumpSuccess {
                restored_files_count,
                total_bytes,
                output_dir,
            } => {
                println!("============================================================");
                println!("🎉 \x1b[1;32mKHÔI PHỤC THÀNH CÔNG TOÀN BỘ DỮ LIỆU!\x1b[0m");
                println!("📁 Thư mục lưu trữ: \x1b[1;36m{}\x1b[0m", output_dir);
                println!("📄 Tổng số tệp tin: \x1b[1;32m{}\x1b[0m", restored_files_count);
                println!(
                    "💾 Dung lượng khôi phục: \x1b[1;33m{:.2} MB\x1b[0m",
                    total_bytes as f64 / (1024.0 * 1024.0)
                );
                println!("============================================================");
                Ok(false)
            }
            IpcResponse::Error(msg) => {
                final_error = Some(msg);
                Ok(false)
            }
            _ => Ok(true),
        },
    )
    .await?;

    if let Some(err) = final_error {
        bail!("{}", err);
    }

    Ok(())
}
