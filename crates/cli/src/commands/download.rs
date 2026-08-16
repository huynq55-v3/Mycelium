use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};
use indicatif::{ProgressBar, ProgressStyle};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

pub async fn handle_download(manifest_path: PathBuf, output_path: PathBuf) -> Result<()> {
    let canonical_manifest = match std::fs::canonicalize(&manifest_path) {
        Ok(p) => p,
        Err(_) => manifest_path.clone(),
    };

    println!("============================================================");
    println!("          📥 TẢI VỀ VÀ KHÔI PHỤC TỆP TIN TỪ P2P             ");
    println!("============================================================");
    println!("📜 File Manifest: {:?}", canonical_manifest);
    println!("📁 Tệp đích     : {:?}", output_path);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .expect("style"),
    );
    pb.set_message("Đang kết nối tới p2pdrive daemon...");
    pb.enable_steady_tick(Duration::from_millis(80));

    let request = IpcRequest::Download {
        manifest_path: canonical_manifest.to_string_lossy().to_string(),
        output_path: output_path.to_string_lossy().to_string(),
    };

    let mut final_error = None;

    IpcClient::send_request(request, |response| {
        match response {
            IpcResponse::Progress { message, .. } => {
                pb.set_message(message);
                Ok(true)
            }
            IpcResponse::DownloadSuccess {
                file_name,
                bytes_written,
                output_path,
            } => {
                pb.finish_with_message("✅ Khôi phục thành công!");
                println!("============================================================");
                println!("🎉 KHÔI PHỤC VÀ TẢI VỀ THÀNH CÔNG!");
                println!("📄 Tên tệp : \x1b[1;36m{}\x1b[0m", file_name);
                println!("📊 Dung lượng : \x1b[1;33m{} bytes\x1b[0m", bytes_written);
                println!("📁 Lưu tại   : \x1b[1;32m{}\x1b[0m", output_path);
                println!("============================================================");
                Ok(false)
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
