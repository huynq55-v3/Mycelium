//! # Core Crypto Module for Mycelium P2P Storage
//!
//! Cung cấp các chức năng mật mã nền tảng cho mạng lưu trữ P2P Mycelium:
//! - **Identity**: Quản lý cặp khóa Ed25519, sinh DID (`did:key:...`), ký và xác minh chữ ký số, lưu/nạp JSON.
//! - **SwarmKey**: Quản lý khóa Pre-shared Key (PSK 32 bytes) cho mạng riêng tư hoặc nạp mặc định cho mạng công cộng.
//! - **Cipher**: Mã hóa và giải mã dữ liệu an toàn bằng thuật toán AES-256-GCM với Nonce 96-bit ngẫu nhiên.
//! - **CID**: Tính toán Content Identifier bằng băm SHA-256 trả về chuỗi Hex.

pub mod cid;
pub mod cipher;
pub mod error;
pub mod identity;
pub mod swarm;

// Re-exports
pub use cid::compute_cid;
pub use cipher::{decrypt_data, encrypt_data, MIN_CIPHER_LEN, NONCE_SIZE, TAG_SIZE};
pub use error::CryptoError;
pub use identity::{Identity, IdentityJson, DID_PREFIX};
pub use swarm::SwarmKey;
