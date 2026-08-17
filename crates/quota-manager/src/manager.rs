use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use blockstore::BlockStore;
use serde::{Deserialize, Serialize};

use crate::error::QuotaError;

/// Tỷ lệ đóng góp tối thiểu bắt buộc ($R_{\min} = 4.0$).
pub const MIN_R_RATIO: f64 = 4.0;

/// Tỷ lệ đóng góp tối đa để nhận thêm Cache ($R_{\max} = 5.0$).
pub const MAX_R_RATIO: f64 = 5.0;

/// Dung lượng tối thiểu cho lần commit đầu tiên (10 MB).
pub const FIRST_COMMIT_MIN_BYTES: u64 = 10 * 1024 * 1024;

/// Dung lượng tối đa cho lần commit đầu tiên (40 MB).
pub const FIRST_COMMIT_MAX_BYTES: u64 = 40 * 1024 * 1024;

/// Dung lượng ổ cứng mặc định mà một node cam kết chia sẻ cho mạng (60 GiB).
pub const DEFAULT_ALLOCATED_DISK_BYTES: u64 = 60 * 1024 * 1024 * 1024;

/// Quản lý hạn mức lưu trữ và quyền lợi tải lên dựa trên công thức $4.0 \le R \le 5.0$.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaManager {
    /// Dung lượng ổ cứng máy này dành cho mạng lưu trữ (bytes).
    pub allocated_disk_bytes: u64,
    /// Dung lượng file gốc người dùng này đã tải lên mạng (Merit - bytes).
    pub my_uploaded_bytes: u64,
    /// Dung lượng Shard (thật + cache) node này đang thực lưu trữ cho mạng (Shard - bytes).
    pub stored_shard_bytes: u64,
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self {
            allocated_disk_bytes: DEFAULT_ALLOCATED_DISK_BYTES,
            my_uploaded_bytes: 0,
            stored_shard_bytes: 0,
        }
    }
}

impl QuotaManager {
    /// Khởi tạo `QuotaManager` với dung lượng ổ cứng chỉ định (bytes).
    pub fn new(allocated_disk_bytes: u64) -> Self {
        Self {
            allocated_disk_bytes,
            my_uploaded_bytes: 0,
            stored_shard_bytes: 0,
        }
    }

    /// Khởi tạo `QuotaManager` với cấu hình mặc định (60 GB ổ cứng).
    pub fn default_60gb() -> Self {
        Self::default()
    }

    /// Tính toán chỉ số $R = \frac{\text{stored\_shard\_bytes}}{\text{my\_uploaded\_bytes}}$ hiện tại.
    /// Trả về `None` (N/A) khi chưa có bất kỳ giao dịch nào (0 Merit, 0 Shard).
    pub fn current_r_ratio(&self) -> Option<f64> {
        if self.my_uploaded_bytes == 0 {
            if self.stored_shard_bytes == 0 {
                None // Chưa có giao dịch nào: N/A (Sẵn sàng cho First Atomic Commit)
            } else {
                Some(5.0) // Đã nhận shard mồi nhưng chưa upload -> Ở trần an toàn
            }
        } else {
            Some(self.stored_shard_bytes as f64 / self.my_uploaded_bytes as f64)
        }
    }

    /// Tính toán số bytes Shard cần phải nhận thêm từ mạng để đủ điều kiện upload file có kích thước `file_size` ($R_{\text{new}} \ge 4.0$).
    /// Trả về `0` nếu là lần đầu tiên (Atomic Commit) hoặc đã đủ điều kiện $R \ge 4.0$.
    pub fn calculate_required_ingest_for_upload(&self, file_size: u64) -> u64 {
        if self.my_uploaded_bytes == 0 && self.stored_shard_bytes == 0 {
            return 0; // Giao dịch đầu tiên là giao dịch nguyên tử (Atomic First Commit R = 4)
        }

        let target_uploaded = self.my_uploaded_bytes.saturating_add(file_size);
        let required_stored = target_uploaded.saturating_mul(4);

        required_stored.saturating_sub(self.stored_shard_bytes)
    }

    /// Hạn mức upload tối đa có thể sử dụng hiện tại (bytes).
    pub fn allowed_upload_capacity(&self) -> u64 {
        self.stored_shard_bytes / 4
    }

    /// Dung lượng upload còn lại có thể sử dụng ngay mà không cần nhận thêm shard.
    pub fn remaining_upload_capacity(&self) -> u64 {
        self.allowed_upload_capacity().saturating_sub(self.my_uploaded_bytes)
    }

    /// Xác thực xem việc tải lên file kích thước `file_size` có thỏa mãn quy tắc hay không.
    pub fn validate_upload(&self, file_size: u64) -> Result<(), QuotaError> {
        if self.my_uploaded_bytes == 0 && self.stored_shard_bytes == 0 {
            if file_size < FIRST_COMMIT_MIN_BYTES || file_size > FIRST_COMMIT_MAX_BYTES {
                return Err(QuotaError::FirstCommitSizeOutOfRange {
                    actual_mb: file_size as f64 / (1024.0 * 1024.0),
                    min_mb: FIRST_COMMIT_MIN_BYTES / (1024 * 1024),
                    max_mb: FIRST_COMMIT_MAX_BYTES / (1024 * 1024),
                });
            }
            return Ok(());
        }

        let needed_shards = self.calculate_required_ingest_for_upload(file_size);
        if needed_shards > 0 {
            return Err(QuotaError::InsufficientContribution {
                file_size,
                required_shard_bytes: needed_shards,
                current_r_ratio: self.current_r_ratio(),
            });
        }
        Ok(())
    }

    /// Ghi nhận dung lượng tải lên mới sau khi đã upload thành công.
    pub fn record_upload(&mut self, file_size: u64) -> Result<(), QuotaError> {
        self.validate_upload(file_size)?;
        if self.my_uploaded_bytes == 0 && self.stored_shard_bytes == 0 {
            // First Atomic Commit: Thiết lập ngay R = 4.0
            self.my_uploaded_bytes = file_size;
            self.stored_shard_bytes = file_size.saturating_mul(4);
        } else {
            self.my_uploaded_bytes = self.my_uploaded_bytes.saturating_add(file_size);
        }
        Ok(())
    }

    /// Ghi nhận xóa file, giảm `my_uploaded_bytes`.
    pub fn record_delete(&mut self, file_size: u64) {
        self.my_uploaded_bytes = self.my_uploaded_bytes.saturating_sub(file_size);
    }

    /// Ghi nhận lưu thêm một Shard mới (tăng `stored_shard_bytes`).
    pub fn record_stored_shard(&mut self, shard_size: u64) {
        self.stored_shard_bytes = self.stored_shard_bytes.saturating_add(shard_size);
    }

    /// Ghi nhận thu hồi / xóa bớt một Shard (giảm `stored_shard_bytes`).
    pub fn record_pruned_shard(&mut self, shard_size: u64) {
        self.stored_shard_bytes = self.stored_shard_bytes.saturating_sub(shard_size);
    }

    /// Kiểm tra xem node có thể nhận thêm Shard kích thước `shard_size` từ mạng hay không:
    /// 1. Chưa vượt quá dung lượng ổ cứng cam kết `allocated_disk_bytes`.
    /// 2. Nếu là Shard Cache: Chỉ nhận khi $R \le 5.0$ (chưa chạm trần cache).
    pub fn can_accept_shard(&self, blockstore: &BlockStore, shard_size: u64, is_cache: bool) -> bool {
        let current_usage = blockstore.current_disk_usage().unwrap_or(0);
        if current_usage.saturating_add(shard_size) > self.allocated_disk_bytes {
            return false;
        }

        if is_cache {
            if self.my_uploaded_bytes > 0 {
                let next_r = (self.stored_shard_bytes.saturating_add(shard_size)) as f64 / self.my_uploaded_bytes as f64;
                if next_r > MAX_R_RATIO {
                    return false; // Chạm trần Cache R > 5.0
                }
            } else if self.stored_shard_bytes.saturating_add(shard_size) > 4 * 1024 * 1024 * 1024 {
                // Node mới chưa up gì: cho phép đệm tối đa 4GB Shards
                return false;
            }
        }

        true
    }

    /// Đồng bộ lại `stored_shard_bytes` từ dung lượng thực tế trên `BlockStore`.
    pub fn sync_stored_from_blockstore(&mut self, blockstore: &BlockStore) {
        if let Ok(usage) = blockstore.current_disk_usage() {
            self.stored_shard_bytes = usage;
        }
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
        let path = path.as_ref();
        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let manager: Self = serde_json::from_str(&content)?;
        Ok(manager)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_atomic_first_commit() {
        let mut qm = QuotaManager::new(DEFAULT_ALLOCATED_DISK_BYTES);
        assert_eq!(qm.stored_shard_bytes, 0);
        assert_eq!(qm.my_uploaded_bytes, 0);
        assert_eq!(qm.current_r_ratio(), None); // N/A

        // Kiểm tra chặn file < 10MB
        let file_5mb = 5 * 1024 * 1024;
        assert!(qm.validate_upload(file_5mb).is_err());

        // Kiểm tra chặn file > 40MB
        let file_50mb = 50 * 1024 * 1024;
        assert!(qm.validate_upload(file_50mb).is_err());

        // Hợp lệ: 20MB (trong khoảng 10MB - 40MB)
        let file_20mb = 20 * 1024 * 1024;
        assert_eq!(qm.calculate_required_ingest_for_upload(file_20mb), 0);
        assert!(qm.validate_upload(file_20mb).is_ok());

        // Sau khi upload lần đầu: Tự động thiết lập Merit = 20MB, Shard = 80MB -> R = 4.0
        assert!(qm.record_upload(file_20mb).is_ok());
        assert_eq!(qm.my_uploaded_bytes, file_20mb);
        assert_eq!(qm.stored_shard_bytes, 80 * 1024 * 1024);
        assert_eq!(qm.current_r_ratio(), Some(4.0));

        // Lần 2 cố up tiếp mà chưa nhận thêm shard -> Bị yêu cầu nhận thêm shard
        let file_10mb = 10 * 1024 * 1024;
        assert_eq!(qm.calculate_required_ingest_for_upload(file_10mb), 40 * 1024 * 1024);
    }

    #[test]
    fn test_r_ratio_invariants() {
        let mut qm = QuotaManager::new(DEFAULT_ALLOCATED_DISK_BYTES);
        qm.record_stored_shard(800 * 1024 * 1024); // Có 800MB shards
        qm.my_uploaded_bytes = 200 * 1024 * 1024; // Up 200MB file -> R = 800 / 200 = 4.0

        assert_eq!(qm.current_r_ratio(), Some(4.0));

        // Cố up thêm 10MB mà chưa nhận thêm shard -> R sẽ tụt < 4.0 -> Bị chặn
        assert!(qm.validate_upload(10 * 1024 * 1024).is_err());
    }
}
