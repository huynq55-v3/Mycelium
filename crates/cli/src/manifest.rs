use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result};
use erasure_codec::FileManifest;
use serde::{Deserialize, Serialize};

/// Bản kê khai bảo mật hoàn chỉnh cho tệp tải lên P2P Drive (`<FILE_NAME>.manifest.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedFilePackage {
    /// Tên tệp gốc ban đầu.
    pub file_name: String,
    /// Kích thước dữ liệu gốc (trước mã hóa) tính bằng bytes.
    pub original_size: usize,
    /// Mã băm SHA-256 của dữ liệu gốc.
    pub original_hash: String,
    /// Content Identifier (CID) của dữ liệu đã mã hóa.
    pub encrypted_cid: String,
    /// Khóa đối xứng AES-256 bí mật dạng Hex (32 bytes) dùng để giải mã tệp.
    pub encryption_key_hex: String,
    /// Siêu dữ liệu phân đoạn Reed-Solomon của dữ liệu mã hóa.
    pub erasure_manifest: FileManifest,
}

impl EncryptedFilePackage {
    /// Lưu manifest ra file JSON.
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json_str = serde_json::to_string_pretty(self)
            .context("Không thể serialize manifest sang JSON")?;
        let mut file = File::create(path.as_ref())
            .with_context(|| format!("Không thể tạo file {:?}", path.as_ref()))?;
        file.write_all(json_str.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Nạp manifest từ file JSON.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path.as_ref())
            .with_context(|| format!("Không thể mở file manifest {:?}", path.as_ref()))?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let pkg: Self = serde_json::from_str(&content)
            .context("Định dạng file manifest không hợp lệ")?;
        Ok(pkg)
    }
}
