use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use ed25519_dalek::{
    Signature, Signer, SigningKey, Verifier, VerifyingKey,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};

use crate::error::CryptoError;

pub const DID_PREFIX: &str = "did:key:";
pub const DEFAULT_CONFIG_DIR: &str = ".p2pdrive";
pub const DEFAULT_IDENTITY_FILE: &str = "identity.json";

/// Cấu trúc lưu trữ thông tin Identity dưới định dạng JSON trên đĩa.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityJson {
    pub did: String,
    pub public_key: String,
    pub secret_key: String,
}

/// Đại diện cho danh tính phân tán (Decentralized Identity) của một node trong mạng P2P Mycelium,
/// quản lý cặp khóa Ed25519.
#[derive(Clone)]
pub struct Identity {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl Identity {
    /// Tạo mới một `Identity` ngẫu nhiên sử dụng entropy an toàn từ hệ điều hành (`OsRng`).
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Khởi tạo `Identity` từ 32 bytes khóa bí mật (Secret/Signing Key).
    pub fn from_secret_bytes(bytes: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(bytes);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Khởi tạo `Identity` từ chuỗi hex của khóa bí mật.
    pub fn from_secret_hex(hex_str: &str) -> Result<Self, CryptoError> {
        let bytes = hex::decode(hex_str)?;
        if bytes.len() != 32 {
            return Err(CryptoError::InvalidKeyLength {
                expected: 32,
                actual: bytes.len(),
            });
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&bytes);
        Ok(Self::from_secret_bytes(&key_bytes))
    }

    /// Trả về tham chiếu tới khóa ký bí mật `SigningKey`.
    pub fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }

    /// Trả về tham chiếu tới khóa công khai `VerifyingKey`.
    pub fn verifying_key(&self) -> &VerifyingKey {
        &self.verifying_key
    }

    /// Trả về mảng 32 bytes khóa công khai.
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Trả về mảng 32 bytes khóa bí mật.
    pub fn secret_key_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    /// Trả về chuỗi khóa công khai dạng Hex.
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.public_key_bytes())
    }

    /// Trả về chuỗi khóa bí mật dạng Hex.
    pub fn secret_key_hex(&self) -> String {
        hex::encode(self.secret_key_bytes())
    }

    /// Sinh chuỗi Decentralized Identifier (DID) có dạng `did:key:<hex_public_key>`.
    pub fn to_did(&self) -> String {
        format!("{}{}", DID_PREFIX, self.public_key_hex())
    }

    /// Ký thông điệp byte bằng khóa bí mật Ed25519.
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    /// Xác minh chữ ký số của thông điệp với khóa công khai của `Identity`.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), CryptoError> {
        self.verifying_key
            .verify(message, signature)
            .map_err(CryptoError::SignatureError)
    }

    /// Trả về đường dẫn mặc định lưu trữ identity (`~/.p2pdrive/identity.json`).
    pub fn default_path() -> Result<PathBuf, CryptoError> {
        let home = dirs::home_dir().ok_or(CryptoError::HomeDirectoryNotFound)?;
        Ok(home.join(DEFAULT_CONFIG_DIR).join(DEFAULT_IDENTITY_FILE))
    }

    /// Chuyển đổi sang `IdentityJson` phục vụ serialization.
    pub fn to_json_dto(&self) -> IdentityJson {
        IdentityJson {
            did: self.to_did(),
            public_key: self.public_key_hex(),
            secret_key: self.secret_key_hex(),
        }
    }

    /// Xuất thông tin `Identity` ra file JSON theo đường dẫn chỉ định.
    /// Tự động tạo thư mục cha nếu chưa tồn tại.
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), CryptoError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let dto = self.to_json_dto();
        let json_string = serde_json::to_string_pretty(&dto)?;

        let mut file = File::create(path)?;
        file.write_all(json_string.as_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Xuất thông tin ra đường dẫn mặc định `~/.p2pdrive/identity.json`.
    pub fn save_default(&self) -> Result<PathBuf, CryptoError> {
        let path = Self::default_path()?;
        self.save_to_file(&path)?;
        Ok(path)
    }

    /// Nạp `Identity` từ file JSON theo đường dẫn chỉ định.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, CryptoError> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let dto: IdentityJson = serde_json::from_str(&contents)?;
        Self::from_secret_hex(&dto.secret_key)
    }

    /// Nạp `Identity` từ file mặc định `~/.p2pdrive/identity.json`.
    pub fn load_default() -> Result<Self, CryptoError> {
        let path = Self::default_path()?;
        Self::load_from_file(path)
    }

    /// Nạp hoặc tự động sinh mới nếu file mặc định chưa tồn tại.
    pub fn load_or_create_default() -> Result<Self, CryptoError> {
        let path = Self::default_path()?;
        if path.exists() {
            Self::load_from_file(path)
        } else {
            let identity = Self::generate();
            identity.save_to_file(&path)?;
            Ok(identity)
        }
    }
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("did", &self.to_did())
            .field("public_key", &self.public_key_hex())
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_identity_generate_and_did_format() {
        let identity = Identity::generate();
        let did = identity.to_did();

        assert!(did.starts_with(DID_PREFIX));
        assert_eq!(did.len(), DID_PREFIX.len() + 64); // 64 ký tự hex cho 32 bytes public key
        assert_eq!(identity.public_key_hex().len(), 64);
        assert_eq!(identity.secret_key_hex().len(), 64);
    }

    #[test]
    fn test_identity_sign_and_verify() {
        let identity = Identity::generate();
        let message = b"Xac thuc giao dich blockstore mycelium";

        let signature = identity.sign(message);
        let verify_result = identity.verify(message, &signature);
        assert!(verify_result.is_ok());

        // Kiểm tra với message khác phải thất bại
        let wrong_message = b"Thong diep bi gia mao";
        assert!(identity.verify(wrong_message, &signature).is_err());
    }

    #[test]
    fn test_identity_save_and_load_json() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("identity.json");

        let original_identity = Identity::generate();
        original_identity
            .save_to_file(&file_path)
            .expect("Luu identity file thanh cong");

        assert!(file_path.exists());

        let loaded_identity =
            Identity::load_from_file(&file_path).expect("Nap identity file thanh cong");

        assert_eq!(original_identity.to_did(), loaded_identity.to_did());
        assert_eq!(
            original_identity.public_key_bytes(),
            loaded_identity.public_key_bytes()
        );
        assert_eq!(
            original_identity.secret_key_bytes(),
            loaded_identity.secret_key_bytes()
        );
    }

    #[test]
    fn test_identity_from_secret_hex_invalid_len() {
        let invalid_hex = "abcd1234";
        let result = Identity::from_secret_hex(invalid_hex);
        assert!(result.is_err());
    }
}
