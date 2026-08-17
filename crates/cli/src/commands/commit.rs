use anyhow::{bail, Result};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

/// Xử lý lệnh `p2pdrive commit [paths...] [-m message]`.
pub async fn handle_commit(paths: Vec<String>, message: Option<String>) -> Result<()> {
    println!("============================================================");
    println!("       🚀 ĐANG THỰC HIỆN COMMIT LÊN MYCELIUM DRIVE          ");
    println!("============================================================");

    let mut final_error = None;

    IpcClient::send_request(IpcRequest::Commit { paths, message }, |response| {
        match response {
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
            IpcResponse::CommitSuccess {
                committed_files,
                total_bytes,
                r_ratio,
            } => {
                println!("------------------------------------------------------------");
                println!("🎉 \x1b[1;32mCommit thành công vào Cây thư mục ảo (Virtual Tree)!\x1b[0m");
                println!(
                    "📦 Tổng dung lượng cập nhật : \x1b[1;36m{:.2} MB\x1b[0m",
                    total_bytes as f64 / (1024.0 * 1024.0)
                );
                let r_str = match r_ratio {
                    Some(r) => format!("{:.2}", r),
                    None => "N/A".to_string(),
                };
                println!(
                    "📈 Chỉ số cống hiến R hiện tại: \x1b[1;33m{}\x1b[0m (4.0 <= R <= 5.0)",
                    r_str
                );
                println!("📝 Danh sách tệp tin đã xử lý:");
                for f in committed_files {
                    println!("   \x1b[1;32m✓\x1b[0m {}", f);
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
