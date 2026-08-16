use sha2::{Digest, Sha256};

/// Tính toán Content Identifier (CID) cho dữ liệu byte bằng giải thuật SHA-256 (trả về chuỗi hex).
///
/// # Ví dụ
/// ```
/// use core_crypto::compute_cid;
///
/// let data = b"Hello Mycelium P2P Storage";
/// let cid = compute_cid(data);
/// assert_eq!(cid.len(), 64);
/// ```
pub fn compute_cid(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_cid_empty() {
        let cid = compute_cid(b"");
        // SHA-256 của chuỗi rỗng: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            cid,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_compute_cid_consistency() {
        let data = b"mycelium-storage-payload-chunk-001";
        let cid1 = compute_cid(data);
        let cid2 = compute_cid(data);
        assert_eq!(cid1, cid2);
        assert_eq!(cid1.len(), 64);
    }

    #[test]
    fn test_compute_cid_different_data() {
        let cid1 = compute_cid(b"data_a");
        let cid2 = compute_cid(b"data_b");
        assert_ne!(cid1, cid2);
    }
}
