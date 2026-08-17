use anyhow::{bail, Result};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

/// Xử lý lệnh `p2pdrive ls [path]`.
pub async fn handle_ls(path: Option<String>) -> Result<()> {
    let mut final_error = None;

    IpcClient::send_request(IpcRequest::VfsList { path }, |response| {
        match response {
            IpcResponse::VfsListSuccess { entries } => {
                println!("============================================================");
                println!("           📂 DANH SÁCH TỆP TIN TRÊN MYCELIUM DRIVE         ");
                println!("============================================================");
                if entries.is_empty() {
                    println!("(Drive trống - chưa có tệp tin nào)");
                } else {
                    for entry in entries {
                        println!(" - {}", entry);
                    }
                }
                println!("============================================================");
                Ok(false)
            }
            IpcResponse::Error(msg) => {
                final_error = Some(msg);
                Ok(false)
            }
            _ => Ok(true),
        }
    })
    .await?;

    if let Some(err) = final_error {
        bail!("{}", err);
    }
    Ok(())
}

/// Xử lý lệnh `p2pdrive tree [path]`.
pub async fn handle_tree(path: Option<String>) -> Result<()> {
    let mut final_error = None;

    IpcClient::send_request(IpcRequest::VfsTree { path }, |response| {
        match response {
            IpcResponse::VfsTreeSuccess { tree_rendered } => {
                println!("============================================================");
                println!("        🌳 CÂY THƯ MỤC ẢO PHÂN TÁN (VIRTUAL TREE)          ");
                println!("============================================================");
                print!("{}", tree_rendered);
                println!("============================================================");
                Ok(false)
            }
            IpcResponse::Error(msg) => {
                final_error = Some(msg);
                Ok(false)
            }
            _ => Ok(true),
        }
    })
    .await?;

    if let Some(err) = final_error {
        bail!("{}", err);
    }
    Ok(())
}

/// Xử lý lệnh `p2pdrive rm <path>`.
pub async fn handle_rm(path: String) -> Result<()> {
    let mut final_error = None;

    IpcClient::send_request(IpcRequest::VfsRemove { path }, |response| {
        match response {
            IpcResponse::VfsRemoveSuccess {
                path,
                freed_bytes,
                r_ratio,
            } => {
                println!("🗑️ \x1b[1;32mĐã xóa thành công '{}' khỏi Mycelium Drive!\x1b[0m", path);
                println!(
                    "💾 Dung lượng giải phóng: {:.2} MB",
                    freed_bytes as f64 / (1024.0 * 1024.0)
                );
                let r_str = match r_ratio {
                    Some(r) => format!("{:.2}", r),
                    None => "N/A".to_string(),
                };
                println!("📈 Chỉ số cống hiến R sau xóa: \x1b[1;33m{}\x1b[0m", r_str);
                Ok(false)
            }
            IpcResponse::Error(msg) => {
                final_error = Some(msg);
                Ok(false)
            }
            _ => Ok(true),
        }
    })
    .await?;

    if let Some(err) = final_error {
        bail!("{}", err);
    }
    Ok(())
}

/// Xử lý lệnh `p2pdrive download <vfs_path> [output_path]`.
pub async fn handle_vfs_download(vfs_path: String, output_path: Option<String>) -> Result<()> {
    let mut final_error = None;

    IpcClient::send_request(
        IpcRequest::VfsDownload {
            vfs_path,
            output_path,
        },
        |response| match response {
            IpcResponse::Progress {
                step: _,
                current,
                total,
                message,
            } => {
                let percent = if total > 0 { current * 100 / total } else { 0 };
                println!("⏳ [{:>3}%] {}", percent, message);
                Ok(true)
            }
            IpcResponse::DownloadSuccess {
                file_name,
                bytes_written,
                output_path,
            } => {
                println!("🎉 \x1b[1;32mTải xuống và khôi phục thành công 100% không cần Manifest!\x1b[0m");
                println!("📄 Tên tệp   : {}", file_name);
                println!(
                    "📦 Kích thước: {:.2} MB",
                    bytes_written as f64 / (1024.0 * 1024.0)
                );
                println!("💾 Lưu tại   : \x1b[1;36m{}\x1b[0m", output_path);
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
