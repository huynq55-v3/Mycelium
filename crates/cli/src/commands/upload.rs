use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};
use indicatif::{ProgressBar, ProgressStyle};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

pub async fn handle_upload(file_path: PathBuf) -> Result<()> {
    let canonical_path = match std::fs::canonicalize(&file_path) {
        Ok(p) => p,
        Err(_) => file_path.clone(),
    };

    println!("============================================================");
    println!("           📤 TẢI LÊN TỆP TIN VÀO MẠNG P2P DRIVE            ");
    println!("============================================================");
    println!("📄 Đường dẫn tệp: {:?}", canonical_path);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .expect("style"),
    );
    pb.set_message("Đang kết nối tới p2pdrive daemon...");
    pb.enable_steady_tick(Duration::from_millis(80));

    let request = IpcRequest::Upload {
        file_path: canonical_path.to_string_lossy().to_string(),
    };

    let mut final_error = None;

    IpcClient::send_request(request, |response| {
        match response {
            IpcResponse::Progress { message, .. } => {
                pb.set_message(message);
                Ok(true)
            }
            IpcResponse::UploadSuccess {
                file_name,
                original_size,
                encrypted_cid,
                manifest_path,
            } => {
                pb.finish_with_message("✅ Quá trình phân tán hoàn tất!");
                println!("============================================================");
                println!("🎉 TẢI LÊN THÀNH CÔNG!");
                println!("📄 Tên tệp : \x1b[1;36m{}\x1b[0m", file_name);
                println!("📊 Dung lượng : \x1b[1;33m{} bytes\x1b[0m", original_size);
                println!("🔑 Content Identifier (CID): \x1b[1;32m{}\x1b[0m", encrypted_cid);
                println!("📜 File Manifest: \x1b[1;33m{}\x1b[0m", manifest_path);
                println!("💡 Giữ file manifest để khôi phục và giải mã dữ liệu sau này!");
                println!("============================================================");
                Ok(false) // Dừng stream
            }
            IpcResponse::Error(msg) => {
                pb.finish_with_message("❌ Thất bại");
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
