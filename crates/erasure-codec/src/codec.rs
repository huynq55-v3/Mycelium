use reed_solomon_erasure::galois_8::ReedSolomon;
use sha2::{Digest, Sha256};

use crate::error::CodecError;
use crate::types::{FileManifest, Shard};

/// Số lượng data shards mặc định ($K=10$).
pub const DATA_SHARDS: usize = 10;

/// Tổng số shards mặc định ($N=40$).
pub const TOTAL_SHARDS: usize = 40;

/// Số lượng parity shards mặc định ($M = N - K = 30$).
pub const PARITY_SHARDS: usize = TOTAL_SHARDS - DATA_SHARDS;

/// Hàm tiện ích tính toán SHA-256 trả về chuỗi Hex.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Mã hóa dữ liệu byte thành $N=40$ shards (10 data shards + 30 parity shards) sử dụng thuật toán Reed-Solomon.
///
/// Tự động đệm (padding) dữ liệu cho chia hết cho 10, sinh 30 parity shards,
/// băm SHA-256 từng shard và đóng gói thành `FileManifest` cùng danh sách `Vec<Shard>`.
///
/// # Arguments
/// * `data` - Dữ liệu thô cần phân đoạn và bảo vệ.
///
/// # Returns
/// * `Result<(FileManifest, Vec<Shard>), CodecError>` - Bộ kê khai FileManifest và 40 shards.
pub fn encode(data: &[u8]) -> Result<(FileManifest, Vec<Shard>), CodecError> {
    encode_with_name("unnamed", data)
}

/// Mã hóa dữ liệu với tên file xác định.
pub fn encode_with_name(
    file_name: &str,
    data: &[u8],
) -> Result<(FileManifest, Vec<Shard>), CodecError> {
    if data.is_empty() {
        return Err(CodecError::EmptyData);
    }

    let original_size = data.len();
    let original_hash = sha256_hex(data);

    // Tính kích thước mỗi shard sao cho chia đều cho DATA_SHARDS (10)
    let shard_size = (original_size + DATA_SHARDS - 1) / DATA_SHARDS;

    // Chuẩn bị 10 data shards
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(TOTAL_SHARDS);
    for i in 0..DATA_SHARDS {
        let start = i * shard_size;
        let mut shard = Vec::with_capacity(shard_size);
        if start < original_size {
            let end = (start + shard_size).min(original_size);
            shard.extend_from_slice(&data[start..end]);
        }
        // Đệm số 0 ở cuối nếu chưa đủ kích thước shard_size
        shard.resize(shard_size, 0);
        shards.push(shard);
    }

    // Khởi tạo 30 parity shards có cùng kích thước shard_size
    for _ in 0..PARITY_SHARDS {
        shards.push(vec![0u8; shard_size]);
    }

    // Thực thi thuật toán Reed-Solomon để tính toán 30 parity shards
    let r = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS)?;
    r.encode(&mut shards)?;

    // Băm SHA-256 từng shard và đóng gói
    let mut shard_hashes = Vec::with_capacity(TOTAL_SHARDS);
    let mut result_shards = Vec::with_capacity(TOTAL_SHARDS);

    for (index, shard_data) in shards.into_iter().enumerate() {
        let hash = sha256_hex(&shard_data);
        shard_hashes.push(hash.clone());
        result_shards.push(Shard {
            index,
            data: shard_data,
            hash,
        });
    }

    let manifest = FileManifest {
        file_name: file_name.to_string(),
        original_size,
        original_hash,
        k_data_shards: DATA_SHARDS,
        n_total_shards: TOTAL_SHARDS,
        shard_hashes,
    };

    Ok((manifest, result_shards))
}

/// Khôi phục lại dữ liệu gốc từ danh sách các shards nhận được (mảng $N=40$ phần tử, shard bị mất là `None`).
///
/// Chỉ cần thu thập đủ tối thiểu $K=10$ shards hợp lệ bất kỳ là lập tức khôi phục lại 100% dữ liệu gốc,
/// tự động cắt bỏ padding thừa về đúng `original_size` và kiểm tra toàn vẹn mã băm SHA-256.
///
/// # Arguments
/// * `manifest` - FileManifest chứa thông tin siêu dữ liệu của tệp gốc.
/// * `shards` - Mảng chứa đúng `n_total_shards` (40) phần tử `Option<Shard>`.
///
/// # Returns
/// * `Result<Vec<u8>, CodecError>` - Dữ liệu thô gốc sau khi khôi phục và xác thực toàn vẹn.
pub fn decode(
    manifest: &FileManifest,
    shards: Vec<Option<Shard>>,
) -> Result<Vec<u8>, CodecError> {
    if shards.len() != manifest.n_total_shards {
        return Err(CodecError::InvalidShardArrayLength {
            expected: manifest.n_total_shards,
            actual: shards.len(),
        });
    }

    // Đếm số lượng shard hiện có
    let available_count = shards.iter().filter(|s| s.is_some()).count();
    if available_count < manifest.k_data_shards {
        return Err(CodecError::InsufficientShards {
            required: manifest.k_data_shards,
            available: available_count,
        });
    }

    // Xác định kích thước shard từ một shard hợp lệ bất kỳ
    let shard_size = (manifest.original_size + manifest.k_data_shards - 1) / manifest.k_data_shards;

    // Chuẩn bị mảng `Option<Vec<u8>>` cho Reed-Solomon reconstruct
    let mut rs_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(manifest.n_total_shards);

    for (expected_idx, opt_shard) in shards.into_iter().enumerate() {
        match opt_shard {
            Some(shard) => {
                // Kiểm tra kích thước shard
                if shard.data.len() != shard_size {
                    return Err(CodecError::InvalidShardSize);
                }

                // Xác thực mã băm SHA-256 của shard
                let computed_hash = sha256_hex(&shard.data);
                if let Some(expected_hash) = manifest.shard_hashes.get(expected_idx) {
                    if &computed_hash != expected_hash || &shard.hash != expected_hash {
                        return Err(CodecError::InvalidShardHash {
                            index: expected_idx,
                            expected: expected_hash.clone(),
                            actual: computed_hash,
                        });
                    }
                }

                rs_shards.push(Some(shard.data));
            }
            None => {
                rs_shards.push(None);
            }
        }
    }

    // Khởi tạo engine Reed-Solomon và tái thiết lập các shard bị thiếu
    let parity_count = manifest.n_total_shards - manifest.k_data_shards;
    let r = ReedSolomon::new(manifest.k_data_shards, parity_count)?;
    
    // reconstruct_data chỉ cần phục hồi các data shards (0..k_data_shards)
    r.reconstruct_data(&mut rs_shards)?;

    // Ghép dữ liệu từ k_data_shards đầu tiên
    let mut recovered_data = Vec::with_capacity(manifest.k_data_shards * shard_size);
    for item in rs_shards.iter().take(manifest.k_data_shards) {
        if let Some(shard_bytes) = item {
            recovered_data.extend_from_slice(shard_bytes);
        } else {
            return Err(CodecError::InsufficientShards {
                required: manifest.k_data_shards,
                available: available_count,
            });
        }
    }

    // Cắt bỏ phần padding thừa về đúng original_size ban đầu
    recovered_data.truncate(manifest.original_size);

    // Xác minh mã băm toàn vẹn của dữ liệu sau khi khôi phục
    let recovered_hash = sha256_hex(&recovered_data);
    if recovered_hash != manifest.original_hash {
        return Err(CodecError::CorruptedDataIntegrity);
    }

    Ok(recovered_data)
}
