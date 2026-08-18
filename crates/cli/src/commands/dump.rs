use std::path::PathBuf;

use anyhow::{bail, Result};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

/// Xử lý lệnh `p2pdrive dump -k <private_key> -o <output_dir> [--vfs-path <path>]`.
pub async fn handle_dump(
    private_key: Option<String>,
    output_dir: PathBuf,
    vfs_path: Option<PathBuf>,
) -> Result<()> {
    println!("============================================================");
    println!("       📦 KHÔI PHỤC TOÀN BỘ DỮ LIỆU MYCELIUM P2P DRIVE       ");
    println!("============================================================");

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    // Chuẩn hóa đường dẫn private_key nếu là file tương đối
    let resolved_key = private_key.map(|k| {
        let p = PathBuf::from(&k);
        if p.exists() {
            if let Ok(abs) = std::fs::canonicalize(&p) {
                return abs.to_string_lossy().to_string();
            }
        }
        let joined = cwd.join(&p);
        if joined.exists() {
            if let Ok(abs) = std::fs::canonicalize(&joined) {
                return abs.to_string_lossy().to_string();
            }
            return joined.to_string_lossy().to_string();
        }
        k
    });

    let abs_output_dir = if output_dir.is_relative() {
        cwd.join(&output_dir).to_string_lossy().to_string()
    } else {
        output_dir.to_string_lossy().to_string()
    };

    let abs_vfs_path = vfs_path.map(|p| {
        if p.is_relative() {
            cwd.join(&p).to_string_lossy().to_string()
        } else {
            p.to_string_lossy().to_string()
        }
    });

    let mut final_error = None;

    IpcClient::send_request(
        IpcRequest::Dump {
            private_key: resolved_key,
            output_dir: abs_output_dir,
            vfs_path: abs_vfs_path,
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
