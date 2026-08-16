use core_crypto::{
    compute_cid, decrypt_data, encrypt_data, Identity, SwarmKey, DID_PREFIX,
};
use tempfile::tempdir;

#[test]
fn test_did_generation_and_verification() {
    let identity = Identity::generate();
    let did = identity.to_did();

    // 1. Kiểm tra prefix DID
    assert!(
        did.starts_with("did:key:"),
        "DID phải bắt đầu bằng '{}'",
        DID_PREFIX
    );

    // 2. Kiểm tra độ dài hex (did:key: + 64 ký tự hex của Ed25519 public key 32 bytes)
    assert_eq!(
        did.len(),
        8 + 64,
        "Độ dài chuỗi DID không khớp với 32-byte public key hex"
    );

    // 3. Kiểm tra phần hex giải mã được
    let hex_part = &did[8..];
    let decoded_pk = hex::decode(hex_part).expect("Hex của DID phải hợp lệ");
    assert_eq!(decoded_pk.len(), 32);
    assert_eq!(decoded_pk, identity.public_key_bytes());
}

#[test]
fn test_data_encryption_and_decryption_integrity() {
    let mut key = [0u8; 32];
    for (i, item) in key.iter_mut().enumerate() {
        *item = (i * 7 + 13) as u8;
    }

    let payload = b"Du lieu phan doan blockstore p2p can bao mat bang AES-256-GCM";

    // Mã hóa
    let ciphertext = encrypt_data(payload, &key).expect("Mã hóa phải thành công");

    // Nonce 12 bytes + Payload len + GCM Tag 16 bytes
    assert_eq!(ciphertext.len(), 12 + payload.len() + 16);

    // Giải mã với đúng khóa
    let decrypted = decrypt_data(&ciphertext, &key).expect("Giải mã phải thành công");
    assert_eq!(decrypted, payload);

    // Giải mã với sai khóa -> Báo lỗi
    let mut wrong_key = key;
    wrong_key[0] ^= 0x01;
    let err_decrypt = decrypt_data(&ciphertext, &wrong_key);
    assert!(err_decrypt.is_err(), "Giải mã với sai khóa phải trả về lỗi");
}

#[test]
fn test_compute_cid_consistency() {
    let data_chunk = b"Mycelium distributed content storage chunk #4096";

    let cid1 = compute_cid(data_chunk);
    let cid2 = compute_cid(data_chunk);

    // Tính nhất quán
    assert_eq!(cid1, cid2);
    assert_eq!(cid1.len(), 64);

    // Kiểm tra tính nhạy cảm thay đổi dữ liệu (Avalanche effect)
    let mut modified_chunk = data_chunk.to_vec();
    modified_chunk[0] ^= 0x01;
    let cid_modified = compute_cid(&modified_chunk);
    assert_ne!(cid1, cid_modified);
}

#[test]
fn test_identity_file_export_import() {
    let dir = tempdir().expect("Tạo tempdir");
    let json_path = dir.path().join("identity.json");

    let original = Identity::generate();
    original
        .save_to_file(&json_path)
        .expect("Lưu identity ra file");

    let restored = Identity::load_from_file(&json_path).expect("Nạp identity từ file");

    assert_eq!(original.to_did(), restored.to_did());
    assert_eq!(original.secret_key_bytes(), restored.secret_key_bytes());

    // Kiểm tra ký và xác minh chéo giữa original và restored
    let message = b"Xac thuc danh tinh P2P node";
    let sig = original.sign(message);
    assert!(restored.verify(message, &sig).is_ok());
}

#[test]
fn test_swarm_key_behavior() {
    // 1. Private random swarm key
    let priv_swarm1 = SwarmKey::generate();
    let priv_swarm2 = SwarmKey::generate();
    assert_ne!(priv_swarm1, priv_swarm2);

    // 2. Public swarm key deterministic
    let pub_swarm1 = SwarmKey::public_default();
    let pub_swarm2 = SwarmKey::public_default();
    assert_eq!(pub_swarm1, pub_swarm2);
    assert_eq!(pub_swarm1.as_bytes().len(), 32);
}
