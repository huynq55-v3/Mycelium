use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use blockstore::BlockStore;
use serde::{Deserialize, Serialize};

use crate::error::QuotaError;

/// Tỷ lệ dự phòng và đóng góp mạng phân tán: Đóng góp 4 phần ổ đĩa để đổi lại quyền upload 1 phần dữ liệu ($1:4$).
pub const REDUNDANCY_FACTOR: f64 = 4.0;

/// Dung lượng ổ cứng mặc định mà một node cam kết chia sẻ cho mạng (60 GiB = 60 * 1024^3 bytes).
pub const DEFAULT_ALLOCATED_DISK_BYTES: u64 = 60 * 1024 * 1024 * 1024;

/// Quản lý hạn mức lưu trữ và quyền lợi tải lên dựa trên tỷ lệ đóng góp ổ cứng $1:4$.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaManager {
    /// Dung lượng ổ cứng máy này dành cho mạng lưu trữ (bytes).
    pub allocated_disk_bytes: u64,
    /// Tổng dung lượng file gốc người dùng này đã tải lên mạng (bytes).
    pub my_uploaded_bytes: u64,
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self {
            allocated_disk_bytes: DEFAULT_ALLOCATED_DISK_BYTES,
            my_uploaded_bytes: 0,
        }
    }
}

impl QuotaManager {
    /// Khởi tạo `QuotaManager` với dung lượng ổ cứng chỉ định (bytes).
    pub fn new(allocated_disk_bytes: u64) -> Self {
        Self {
            allocated_disk_bytes,
            my_uploaded_bytes: 0,
        }
    }

    /// Khởi tạo `QuotaManager` với cấu hình mặc định (60 GB ổ cứng -> 15 GB quyền tải lên).
    pub fn default_60gb() -> Self {
        Self::default()
    }

    /// Trả về hạn mức tối đa người dùng được phép tải lên (`allocated_disk_bytes / 4`).
    ///
    /// Ví dụ: Cấp 60 GB ổ cứng thì được phép upload tối đa 15 GB.
    pub fn allowed_upload_capacity(&self) -> u64 {
        (self.allocated_disk_bytes as f64 / REDUNDANCY_FACTOR) as u64
    }

    /// Trả về dung lượng upload còn lại có thể sử dụng (bytes).
    pub fn remaining_upload_capacity(&self) -> u64 {
        self.allowed_upload_capacity().saturating_sub(self.my_uploaded_bytes)
    }

    /// Xác thực xem việc tải lên một file có kích thước `file_size` bytes có hợp lệ hay không.
    ///
    /// Từ chối và trả về lỗi `UploadQuotaExceeded` nếu `my_uploaded_bytes + file_size > allowed_upload_capacity`.
    pub fn validate_upload(&self, file_size: u64) -> Result<(), QuotaError> {
        let allowed = self.allowed_upload_capacity();
        if self.my_uploaded_bytes.saturating_add(file_size) > allowed {
            return Err(QuotaError::UploadQuotaExceeded {
                requested_bytes: file_size,
                available_bytes: self.remaining_upload_capacity(),
                current_uploaded_bytes: self.my_uploaded_bytes,
                allowed_capacity_bytes: allowed,
            });
        }
        Ok(())
    }

    /// Ghi nhận dung lượng tải lên mới sau khi đã xác thực hợp lệ.
    pub fn record_upload(&mut self, file_size: u64) -> Result<(), QuotaError> {
        self.validate_upload(file_size)?;
        self.my_uploaded_bytes = self.my_uploaded_bytes.saturating_add(file_size);
        Ok(())
    }

    /// Kiểm tra xem node này có còn đủ dung lượng ổ cứng cam kết để lưu thêm một shard từ mạng gửi đến hay không.
    ///
    /// Trả về `true` nếu `current_disk_usage + shard_size <= allocated_disk_bytes`.
    pub fn can_store_incoming_shard(&self, blockstore: &BlockStore, shard_size: u64) -> bool {
        let current_usage = blockstore.current_disk_usage().unwrap_or(0);
        current_usage.saturating_add(shard_size) <= self.allocated_disk_bytes
    }

    /// Xuất trạng thái `QuotaManager` ra file JSON.
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), QuotaError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Nạp trạng thái `QuotaManager` từ file JSON.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, QuotaError> {
        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let manager: Self = serde_json::from_str(&content)?;
        Ok(manager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_calculation_60gb() {
        let manager = QuotaManager::default_60gb();
        let sixty_gb = 60 * 1024 * 1024 * 1024;
        let fifteen_gb = 15 * 1024 * 1024 * 1024;

        assert_eq!(manager.allocated_disk_bytes, sixty_gb);
        assert_eq!(manager.allowed_upload_capacity(), fifteen_gb);
        assert_eq!(manager.remaining_upload_capacity(), fifteen_gb);
    }

    #[test]
    fn test_validate_and_record_upload_within_limit() {
        let mut manager = QuotaManager::default_60gb();
        let ten_gb = 10 * 1024 * 1024 * 1024;
        let five_gb = 5 * 1024 * 1024 * 1024;

        assert!(manager.validate_upload(ten_gb).is_ok());
        manager.record_upload(ten_gb).unwrap();
        assert_eq!(manager.my_uploaded_bytes, ten_gb);
        assert_eq!(manager.remaining_upload_capacity(), five_gb);

        // Upload thêm 5GB vừa khít 15GB
        assert!(manager.validate_upload(five_gb).is_ok());
        manager.record_upload(five_gb).unwrap();
        assert_eq!(manager.remaining_upload_capacity(), 0);
    }

    #[test]
    fn test_block_upload_exceeding_15gb() {
        let manager = QuotaManager::default_60gb();
        let fifteen_gb_plus_one = 15 * 1024 * 1024 * 1024 + 1;

        let result = manager.validate_upload(fifteen_gb_plus_one);
        assert!(result.is_err());
        match result.unwrap_err() {
            QuotaError::UploadQuotaExceeded {
                requested_bytes,
                available_bytes,
                ..
            } => {
                assert_eq!(requested_bytes, fifteen_gb_plus_one);
                assert_eq!(available_bytes, 15 * 1024 * 1024 * 1024);
            }
            err => panic!("Unexpected error: {:?}", err),
        }
    }
}
