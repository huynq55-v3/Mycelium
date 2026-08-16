use erasure_codec::{
    decode, encode, encode_with_name, CodecError, FileManifest, DATA_SHARDS, TOTAL_SHARDS,
};
use rand::seq::SliceRandom;
use rand::RngCore;

#[test]
fn test_500kb_payload_loss_30_shards_recovery() {
    // 1. Tạo chuỗi dữ liệu giả lập 500KB (500 * 1024 = 512,000 bytes)
    let payload_size = 500 * 1024;
    let mut original_data = vec![0u8; payload_size];
    rand::thread_rng().fill_bytes(&mut original_data);

    // 2. Encode ra 40 shards
    let (manifest, shards) =
        encode_with_name("large_payload.bin", &original_data).expect("Encode 500KB thành công");

    assert_eq!(shards.len(), TOTAL_SHARDS);
    assert_eq!(manifest.n_total_shards, TOTAL_SHARDS);
    assert_eq!(manifest.k_data_shards, DATA_SHARDS);
    assert_eq!(manifest.original_size, payload_size);
    assert_eq!(manifest.shard_hashes.len(), TOTAL_SHARDS);

    // 3. Giả lập 30 shards bị mất, chỉ giữ lại 10 shards ngẫu nhiên bất kỳ
    let mut indices: Vec<usize> = (0..TOTAL_SHARDS).collect();
    indices.shuffle(&mut rand::thread_rng());

    let surviving_indices: Vec<usize> = indices.into_iter().take(DATA_SHARDS).collect();
    assert_eq!(surviving_indices.len(), 10);

    let mut sparse_shards: Vec<Option<erasure_codec::Shard>> = vec![None; TOTAL_SHARDS];
    for &idx in &surviving_indices {
        sparse_shards[idx] = Some(shards[idx].clone());
    }

    // Kiểm tra số lượng shards còn lại đúng bằng 10
    let count_available = sparse_shards.iter().filter(|s| s.is_some()).count();
    assert_eq!(count_available, 10);

    // 4. Decode và assert dữ liệu phục hồi khớp 100%
    let recovered_data = decode(&manifest, sparse_shards).expect("Khôi phục thành công từ 10 shards ngẫu nhiên");

    assert_eq!(
        recovered_data.len(),
        original_data.len(),
        "Độ dài dữ liệu khôi phục phải khớp với dữ liệu gốc"
    );
    assert_eq!(
        recovered_data, original_data,
        "Nội dung dữ liệu khôi phục phải khớp 100% với dữ liệu gốc"
    );
}

#[test]
fn test_small_and_odd_size_payloads() {
    let test_sizes = [1, 7, 10, 11, 33, 99, 1024, 7777];

    for size in test_sizes {
        let mut data = vec![0u8; size];
        rand::thread_rng().fill_bytes(&mut data);

        let (manifest, shards) = encode(&data).expect("Encode thành công");

        // Giữ lại đúng 10 shards ngẫu nhiên
        let mut indices: Vec<usize> = (0..TOTAL_SHARDS).collect();
        indices.shuffle(&mut rand::thread_rng());
        let keep_indices = &indices[..10];

        let mut sparse_shards = vec![None; TOTAL_SHARDS];
        for &idx in keep_indices {
            sparse_shards[idx] = Some(shards[idx].clone());
        }

        let recovered = decode(&manifest, sparse_shards).expect("Decode thành công");
        assert_eq!(recovered, data, "Dữ liệu phục hồi phải khớp với kích thước {}", size);
    }
}

#[test]
fn test_decode_insufficient_shards_fails() {
    let data = b"Mycelium P2P Storage Protocol Test Data";
    let (manifest, shards) = encode(data).unwrap();

    // Giả lập chỉ còn 9 shards (thiếu 1 shard so với yêu cầu K=10)
    let mut sparse_shards = vec![None; TOTAL_SHARDS];
    for (i, shard) in shards.into_iter().enumerate().take(9) {
        sparse_shards[i] = Some(shard);
    }

    let result = decode(&manifest, sparse_shards);
    assert!(result.is_err());
    match result.unwrap_err() {
        CodecError::InsufficientShards { required, available } => {
            assert_eq!(required, 10);
            assert_eq!(available, 9);
        }
        err => panic!("Kỳ vọng InsufficientShards, nhưng nhận: {:?}", err),
    }
}

#[test]
fn test_decode_tampered_shard_fails() {
    let data = b"Critical security payload for storage nodes";
    let (manifest, mut shards) = encode(data).unwrap();

    // Sửa đổi 1 byte trong shard đầu tiên
    shards[0].data[0] ^= 0xFF;

    let mut sparse_shards = vec![None; TOTAL_SHARDS];
    for (i, shard) in shards.into_iter().enumerate() {
        sparse_shards[i] = Some(shard);
    }

    let result = decode(&manifest, sparse_shards);
    assert!(result.is_err());
    match result.unwrap_err() {
        CodecError::InvalidShardHash { index, .. } => {
            assert_eq!(index, 0);
        }
        err => panic!("Kỳ vọng InvalidShardHash, nhưng nhận: {:?}", err),
    }
}

#[test]
fn test_manifest_json_serialization() {
    let data = b"Manifest JSON serde test payload";
    let (manifest, _) = encode_with_name("test.txt", data).unwrap();

    let json_str = manifest.to_json().expect("Serialize JSON thành công");
    let deserialized: FileManifest =
        FileManifest::from_json(&json_str).expect("Deserialize JSON thành công");

    assert_eq!(manifest, deserialized);
    assert_eq!(deserialized.file_name, "test.txt");
    assert_eq!(deserialized.k_data_shards, 10);
    assert_eq!(deserialized.n_total_shards, 40);
}
