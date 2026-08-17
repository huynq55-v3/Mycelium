use blockstore::BlockStore;
use quota_manager::QuotaManager;
use tempfile::tempdir;

#[test]
fn test_atomic_first_commit_flow() {
    let sixty_gb = 60 * 1024 * 1024 * 1024;
    let mut manager = QuotaManager::new(sixty_gb);

    // 1. Lần đầu tiên: calculate_required_ingest = 0 (cho phép up ngay trong khoảng 10-40MB)
    let twenty_mb = 20 * 1024 * 1024;
    assert_eq!(manager.calculate_required_ingest_for_upload(twenty_mb), 0);
    assert!(manager.validate_upload(twenty_mb).is_ok());

    // 2. Upload xong: Thiết lập Merit = 20MB, Shard = 80MB -> R = 4.0
    manager.record_upload(twenty_mb).unwrap();
    assert_eq!(manager.my_uploaded_bytes, twenty_mb);
    assert_eq!(manager.stored_shard_bytes, 80 * 1024 * 1024);
    assert_eq!(manager.current_r_ratio(), Some(4.0));

    // 3. Lần thứ 2: muốn up thêm 1GB thì cần nhận thêm 4GB shard
    let one_gb = 1024 * 1024 * 1024;
    assert_eq!(manager.calculate_required_ingest_for_upload(one_gb), 4 * 1024 * 1024 * 1024);
    assert!(manager.validate_upload(one_gb).is_err());

    // Nhận đủ 4GB shard -> Cho phép upload tiếp
    manager.record_stored_shard(4 * 1024 * 1024 * 1024);
    assert!(manager.validate_upload(one_gb).is_ok());
    manager.record_upload(one_gb).unwrap();
    assert_eq!(manager.current_r_ratio(), Some(4.0));
}

#[test]
fn test_can_store_incoming_shard_with_blockstore() {
    let blockstore = BlockStore::open_temporary().unwrap();
    let allocated_bytes = 1024 * 1024; // 1 MB
    let manager = QuotaManager::new(allocated_bytes);

    // Shard kích thước 500KB -> Lưu được
    assert!(manager.can_accept_shard(&blockstore, 500 * 1024, false));

    // Shard kích thước 2MB -> Vượt quá 1MB cam kết -> Không lưu được
    assert!(!manager.can_accept_shard(&blockstore, 2 * 1024 * 1024, false));
}

#[test]
fn test_quota_manager_save_and_load() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("quota.json");

    let mut manager = QuotaManager::new(60 * 1024 * 1024 * 1024);
    manager.record_upload(20 * 1024 * 1024).unwrap();

    manager.save_to_file(&file_path).unwrap();
    assert!(file_path.exists());

    let loaded = QuotaManager::load_from_file(&file_path).unwrap();
    assert_eq!(manager, loaded);
    assert_eq!(loaded.my_uploaded_bytes, 20 * 1024 * 1024);
    assert_eq!(loaded.stored_shard_bytes, 80 * 1024 * 1024);
}
