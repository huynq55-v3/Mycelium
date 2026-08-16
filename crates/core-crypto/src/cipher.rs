use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};

use crate::error::CryptoError;

pub const NONCE_SIZE: usize = 12; // 96 bits
pub const TAG_SIZE: usize = 16; // 128 bits
pub const MIN_CIPHER_LEN: usize = NONCE_SIZE + TAG_SIZE;

/// Mã hóa dữ liệu bằng thuật toán AES-256-GCM.
///
/// Kết quả trả về gồm Nonce 96-bit (12 bytes) ngẫu nhiên được gắn ở đầu,
/// tiếp theo là ciphertext và Authentication Tag 128-bit (16 bytes).
///
/// Cấu trúc đầu ra: `[ Nonce (12 bytes) | Ciphertext | Tag (16 bytes) ]`
///
/// # Arguments
/// * `plain` - Dữ liệu thô cần mã hóa.
/// * `key` - Khóa đối xứng 256-bit (32 bytes).
///
/// # Returns
/// * `Result<Vec<u8>, CryptoError>` - Mảng byte đã mã hóa hoặc lỗi.
pub fn encrypt_data(plain: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, CryptoError> {
    let cipher_key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(cipher_key);

    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let mut encrypted_data = cipher
        .encrypt(&nonce, plain)
        .map_err(|e| CryptoError::EncryptionError(e.to_string()))?;

    let mut output = Vec::with_capacity(NONCE_SIZE + encrypted_data.len());
    output.extend_from_slice(nonce.as_slice());
    output.append(&mut encrypted_data);

    Ok(output)
}

/// Tách Nonce và giải mã dữ liệu đã được mã hóa bởi `encrypt_data`.
///
/// # Arguments
/// * `cipher` - Mảng byte mã hóa có cấu trúc `[ Nonce (12B) | Ciphertext | Tag (16B) ]`.
/// * `key` - Khóa đối xứng 256-bit (32 bytes).
///
/// # Returns
/// * `Result<Vec<u8>, CryptoError>` - Dữ liệu thô ban đầu sau khi kiểm tra tính toàn vẹn thành công.
pub fn decrypt_data(cipher: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, CryptoError> {
    if cipher.len() < MIN_CIPHER_LEN {
        return Err(CryptoError::InvalidCiphertextLength {
            expected_min: MIN_CIPHER_LEN,
            actual: cipher.len(),
        });
    }

    let (nonce_bytes, ciphertext_with_tag) = cipher.split_at(NONCE_SIZE);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher_key = Key::<Aes256Gcm>::from_slice(key);
    let cipher_engine = Aes256Gcm::new(cipher_key);

    let decrypted_data = cipher_engine
        .decrypt(nonce, ciphertext_with_tag)
        .map_err(|e| CryptoError::DecryptionError(format!("Xác thực tag thất bại hoặc dữ liệu bị sửa đổi: {e}")))?;

    Ok(decrypted_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    #[test]
    fn test_encrypt_decrypt_success() {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);

        let plaintext = b"Kiem tra ma hoa toan ven du lieu P2P Mycelium Blockstore";
        let encrypted = encrypt_data(plaintext, &key).expect("Ma hoa thanh cong");

        assert_eq!(encrypted.len(), NONCE_SIZE + plaintext.len() + TAG_SIZE);

        let decrypted = decrypt_data(&encrypted, &key).expect("Giai ma thanh cong");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_decrypt_empty_payload() {
        let key = [42u8; 32];
        let plaintext = b"";

        let encrypted = encrypt_data(plaintext, &key).expect("Ma hoa chuoi rong thanh cong");
        assert_eq!(encrypted.len(), MIN_CIPHER_LEN);

        let decrypted = decrypt_data(&encrypted, &key).expect("Giai ma chuoi rong thanh cong");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let plaintext = b"Secret data";

        let encrypted = encrypt_data(plaintext, &key1).unwrap();
        let result = decrypt_data(&encrypted, &key2);

        assert!(result.is_err());
        match result.unwrap_err() {
            CryptoError::DecryptionError(_) => {}
            err => panic!("Expected DecryptionError, got: {err:?}"),
        }
    }

    #[test]
    fn test_decrypt_corrupted_ciphertext_fails() {
        let key = [7u8; 32];
        let plaintext = b"Tamper-proof payload";
        let mut encrypted = encrypt_data(plaintext, &key).unwrap();

        // Thay đổi 1 byte trong ciphertext
        let last_idx = encrypted.len() - 1;
        encrypted[last_idx] ^= 0xFF;

        let result = decrypt_data(&encrypted, &key);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_too_short_ciphertext() {
        let key = [0u8; 32];
        let short_cipher = vec![0u8; 10]; // Nhỏ hơn MIN_CIPHER_LEN (28 bytes)

        let result = decrypt_data(&short_cipher, &key);
        assert!(matches!(
            result,
            Err(CryptoError::InvalidCiphertextLength {
                expected_min: 28,
                actual: 10
            })
        ));
    }
}
