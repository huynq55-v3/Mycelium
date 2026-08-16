use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::CryptoError;
use crate::identity::DEFAULT_CONFIG_DIR;

pub const DEFAULT_SWARM_KEY_FILE: &str = "swarm.key";
pub const PUBLIC_SWARM_IDENTIFIER: &[u8] = b"/mycelium/swarm/public/v1.0.0";

/// Đại diện cho khóa mạng bí mật chia sẻ trước (Pre-shared Key - PSK 32 bytes)
/// để tạo lập các cụm mạng riêng tư (Private Swarm) hoặc kết nối vào Public Swarm.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmKey(#[serde(with = "hex_serde")] pub [u8; 32]);

mod hex_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        if bytes.len() != 32 {
            return Err(serde::de::Error::custom(format!(
                "Độ dài hex không hợp lệ cho SwarmKey: nhận {} bytes thay vì 32",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(arr)
    }
}

impl SwarmKey {
    /// Sinh một `SwarmKey` 32 bytes ngẫu nhiên mới, dùng cho mạng nội bộ/gia đình/tổ chức riêng.
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Self(key)
    }

    /// Trả về `SwarmKey` mặc định dùng cho mạng công cộng (Public Swarm) của Mycelium.
    /// Khóa này được suy xuất đơn định từ chuỗi định danh phiên bản public protocol.
    pub fn public_default() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(PUBLIC_SWARM_IDENTIFIER);
        let result = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&result);
        Self(key)
    }

    /// Tạo `SwarmKey` từ mảng 32 bytes có sẵn.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Khởi tạo `SwarmKey` từ chuỗi Hex.
    pub fn from_hex(hex_str: &str) -> Result<Self, CryptoError> {
        let bytes = hex::decode(hex_str)?;
        if bytes.len() != 32 {
            return Err(CryptoError::InvalidKeyLength {
                expected: 32,
                actual: bytes.len(),
            });
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(Self(key))
    }

    /// Trả về chuỗi Hex của `SwarmKey`.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Trả về tham chiếu mảng bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lấy giá trị mảng bytes bên trong.
    pub fn into_inner(self) -> [u8; 32] {
        self.0
    }

    /// Trả về đường dẫn lưu trữ mặc định (`~/.p2pdrive/swarm.key`).
    pub fn default_path() -> Result<PathBuf, CryptoError> {
        let home = dirs::home_dir().ok_or(CryptoError::HomeDirectoryNotFound)?;
        Ok(home.join(DEFAULT_CONFIG_DIR).join(DEFAULT_SWARM_KEY_FILE))
    }

    /// Lưu `SwarmKey` ra file (dạng chuỗi Hex).
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), CryptoError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(path)?;
        file.write_all(self.to_hex().as_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Nạp `SwarmKey` từ file text chứa mã hex.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, CryptoError> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        Self::from_hex(contents.trim())
    }
}

impl std::fmt::Debug for SwarmKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SwarmKey")
            .field(&format!("{}...", &self.to_hex()[..8]))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_swarm_key_generate_unique() {
        let key1 = SwarmKey::generate();
        let key2 = SwarmKey::generate();
        assert_ne!(key1, key2);
        assert_eq!(key1.as_bytes().len(), 32);
        assert_eq!(key1.to_hex().len(), 64);
    }

    #[test]
    fn test_swarm_key_public_default_deterministic() {
        let key1 = SwarmKey::public_default();
        let key2 = SwarmKey::public_default();
        assert_eq!(key1, key2);
        assert_eq!(key1.to_hex().len(), 64);
    }

    #[test]
    fn test_swarm_key_save_and_load() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_swarm.key");

        let original_key = SwarmKey::generate();
        original_key
            .save_to_file(&file_path)
            .expect("Luu SwarmKey thanh cong");

        let loaded_key =
            SwarmKey::load_from_file(&file_path).expect("Nap SwarmKey thanh cong");

        assert_eq!(original_key, loaded_key);
    }

    #[test]
    fn test_swarm_key_serde_json() {
        let key = SwarmKey::generate();
        let json = serde_json::to_string(&key).unwrap();
        let deserialized: SwarmKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, deserialized);
    }
}
