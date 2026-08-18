use std::path::{Path, PathBuf};

use sled::Db;

use crate::error::BlockStoreError;

pub const DEFAULT_CONFIG_DIR: &str = ".p2pdrive";
pub const DEFAULT_BLOCKSTORE_DIR: &str = "blockstore";

/// `BlockStore` chịu trách nhiệm lưu trữ phân tán các mảnh nhị phân (shards)
/// sử dụng cơ sở dữ liệu nhúng siêu tốc `sled` (Key: `shard_hash`, Value: `shard_bytes`).
#[derive(Clone)]
pub struct BlockStore {
    db: Db,
    path: Option<PathBuf>,
}

impl BlockStore {
    /// Mở hoặc khởi tạo một `BlockStore` tại đường dẫn thư mục chỉ định.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, BlockStoreError> {
        let p = path.as_ref();
        let db = sled::open(p)?;
        Ok(Self {
            db,
            path: Some(p.to_path_buf()),
        })
    }

    /// Mở `BlockStore` tại vị trí mặc định (`~/.p2pdrive/blockstore`).
    pub fn open_default() -> Result<Self, BlockStoreError> {
        let home = dirs::home_dir().ok_or(BlockStoreError::HomeDirectoryNotFound)?;
        let path = home.join(DEFAULT_CONFIG_DIR).join(DEFAULT_BLOCKSTORE_DIR);
        Self::open(path)
    }

    /// Mở một `BlockStore` tạm thời trong bộ nhớ (phục vụ unit test và testing).
    pub fn open_temporary() -> Result<Self, BlockStoreError> {
        let config = sled::Config::new().temporary(true);
        let db = config.open()?;
        Ok(Self { db, path: None })
    }

    /// Lưu một shard nhị phân vào kho lưu trữ với khóa là mã băm SHA-256 (`shard_hash`).
    ///
    /// # Arguments
    /// * `hash` - Chuỗi hex của mã băm shard làm khóa.
    /// * `data` - Mảng byte dữ liệu của shard.
    pub fn put_shard(&self, hash: &str, data: &[u8]) -> Result<(), BlockStoreError> {
        self.db.insert(hash.as_bytes(), data)?;
        self.db.flush()?;
        Ok(())
    }

    /// Lưu một shard nhị phân kèm mã định danh File (`file_cid`).
    pub fn put_shard_with_file(&self, hash: &str, file_cid: &str, data: &[u8]) -> Result<(), BlockStoreError> {
        self.db.insert(hash.as_bytes(), data)?;
        let tree = self.db.open_tree("shard_to_file")?;
        tree.insert(hash.as_bytes(), file_cid.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    /// Lấy mã định danh File (`file_cid`) tương ứng với `shard_hash`.
    pub fn get_file_cid_for_shard(&self, hash: &str) -> Result<Option<String>, BlockStoreError> {
        let tree = self.db.open_tree("shard_to_file")?;
        if let Some(val) = tree.get(hash.as_bytes())? {
            Ok(Some(String::from_utf8_lossy(&val).to_string()))
        } else {
            Ok(None)
        }
    }

    /// Đọc dữ liệu shard từ kho lưu trữ theo `shard_hash`.
    ///
    /// # Arguments
    /// * `hash` - Chuỗi hex của mã băm shard cần tìm.
    ///
    /// # Returns
    /// * `Ok(Some(Vec<u8>))` nếu tìm thấy.
    /// * `Ok(None)` nếu không tồn tại.
    pub fn get_shard(&self, hash: &str) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let result = self.db.get(hash.as_bytes())?;
        Ok(result.map(|ivec| ivec.to_vec()))
    }

    /// Kiểm tra xem một shard có tồn tại trong kho lưu trữ hay không.
    pub fn has_shard(&self, hash: &str) -> Result<bool, BlockStoreError> {
        let contains = self.db.contains_key(hash.as_bytes())?;
        Ok(contains)
    }

    /// Xóa một shard khỏi kho lưu trữ.
    ///
    /// Trả về `true` nếu shard đã tồn tại và bị xóa, `false` nếu không tìm thấy.
    pub fn delete_shard(&self, hash: &str) -> Result<bool, BlockStoreError> {
        let prev = self.db.remove(hash.as_bytes())?;
        if let Ok(tree) = self.db.open_tree("shard_to_file") {
            let _ = tree.remove(hash.as_bytes());
        }
        self.db.flush()?;
        Ok(prev.is_some())
    }

    /// Đếm số lượng shards thuộc về một file cụ thể (`file_cid`).
    pub fn count_shards_for_file(&self, file_cid: &str) -> Result<usize, BlockStoreError> {
        let tree = self.db.open_tree("shard_to_file")?;
        let mut count = 0;
        for item in tree.iter() {
            let (_, v) = item?;
            if let Ok(s) = std::str::from_utf8(&v) {
                if s == file_cid {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Trả về dung lượng ổ đĩa thực tế mà cơ sở dữ liệu `BlockStore` đang chiếm dụng (bytes).
    ///
    /// Nếu là cơ sở dữ liệu tạm thời (in-memory test), sẽ tính tổng kích thước dữ liệu thực tế đang lưu trữ.
    pub fn current_disk_usage(&self) -> Result<u64, BlockStoreError> {
        let disk_size = self.db.size_on_disk()?;
        if disk_size > 0 {
            Ok(disk_size)
        } else {
            // Khi db ở chế độ temporary in-memory, tính tổng bytes của (key + value)
            let mut total: u64 = 0;
            for item in self.db.iter() {
                let (k, v) = item?;
                total += (k.len() + v.len()) as u64;
            }
            Ok(total)
        }
    }

    /// Tính tổng dung lượng dữ liệu nhị phân (chỉ tính payload của các shards) đang lưu trong store.
    pub fn total_payload_bytes(&self) -> Result<u64, BlockStoreError> {
        let mut total: u64 = 0;
        for item in self.db.iter() {
            let (_, v) = item?;
            total += v.len() as u64;
        }
        Ok(total)
    }

    /// Đếm tổng số lượng shards đang được lưu trữ trong store.
    pub fn count_shards(&self) -> usize {
        self.db.len()
    }

    /// Liệt kê tất cả các `shard_hash` hiện có trong store.
    pub fn list_shard_hashes(&self) -> Result<Vec<String>, BlockStoreError> {
        let mut hashes = Vec::with_capacity(self.db.len());
        for item in self.db.iter() {
            let (k, _) = item?;
            if let Ok(s) = std::str::from_utf8(&k) {
                hashes.push(s.to_string());
            }
        }
        Ok(hashes)
    }

    /// Trả về đường dẫn lưu trữ nếu có.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_put_get_has_shard() {
        let store = BlockStore::open_temporary().expect("Mở temporary blockstore");

        let shard_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let shard_data = b"du lieu shard blockstore p2p sample chunk";

        // Ban đầu chưa có
        assert!(!store.has_shard(shard_hash).unwrap());
        assert_eq!(store.get_shard(shard_hash).unwrap(), None);

        // Put shard
        store.put_shard(shard_hash, shard_data).unwrap();

        // Kiểm tra has & get
        assert!(store.has_shard(shard_hash).unwrap());
        let retrieved = store.get_shard(shard_hash).unwrap().expect("Tìm thấy shard");
        assert_eq!(retrieved, shard_data);

        // Kiểm tra count
        assert_eq!(store.count_shards(), 1);

        // Delete shard
        let deleted = store.delete_shard(shard_hash).unwrap();
        assert!(deleted);
        assert!(!store.has_shard(shard_hash).unwrap());
        assert_eq!(store.get_shard(shard_hash).unwrap(), None);
    }

    #[test]
    fn test_disk_usage_persisted() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("db");

        let store = BlockStore::open(&store_path).expect("Mở persistent blockstore");
        let shard_data = vec![0xAB; 1024 * 100]; // 100KB

        store.put_shard("hash1", &shard_data).unwrap();
        store.put_shard("hash2", &shard_data).unwrap();

        let usage = store.current_disk_usage().unwrap();
        assert!(usage > 0);
        assert_eq!(store.count_shards(), 2);
    }
}
