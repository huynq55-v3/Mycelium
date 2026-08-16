use blockstore::BlockStore;
use quota_manager::{QuotaError, QuotaManager, REDUNDANCY_FACTOR};
use tempfile::tempdir;

#[test]
fn test_quota_blocks_upload_exceeding_15gb_for_60gb_disk() {
    let sixty_gb = 60 * 1024 * 1024 * 1024;
    let manager = QuotaManager::new(sixty_gb);

    // Hạn mức là 60 / 4 = 15 GB
    let fifteen_gb = 15 * 1024 * 1024 * 1024;
    assert_eq!(manager.allowed_upload_capacity(), fifteen_gb);
    assert_eq!(REDUNDANCY_FACTOR, 4.0);

    // 1. Upload dưới hạn mức (10 GB) -> Thành công
    let ten_gb = 10 * 1024 * 1024 * 1024;
    assert!(manager.validate_upload(ten_gb).is_ok());

    // 2. Upload vượt quá 15 GB (15 GB + 1 byte) -> Bị chặn ngay từ đầu
    let exceed_file = fifteen_gb + 1;
    let result = manager.validate_upload(exceed_file);
    assert!(result.is_err());
    match result.unwrap_err() {
        QuotaError::UploadQuotaExceeded {
            requested_bytes,
            available_bytes,
            current_uploaded_bytes,
            allowed_capacity_bytes,
        } => {
            assert_eq!(requested_bytes, exceed_file);
            assert_eq!(available_bytes, fifteen_gb);
            assert_eq!(current_uploaded_bytes, 0);
            assert_eq!(allowed_capacity_bytes, fifteen_gb);
        }
        err => panic!("Kỳ vọng UploadQuotaExceeded, nhưng nhận: {:?}", err),
    }

    // 3. Upload nhiều lần lũy kế: 8 GB + 7 GB = 15 GB (Đạt đỉnh) -> Upload tiếp 1 byte nữa sẽ bị chặn
    let mut stateful_manager = QuotaManager::new(sixty_gb);
    let eight_gb = 8 * 1024 * 1024 * 1024;
    let seven_gb = 7 * 1024 * 1024 * 1024;

    stateful_manager.record_upload(eight_gb).unwrap();
    assert_eq!(stateful_manager.remaining_upload_capacity(), seven_gb);

    stateful_manager.record_upload(seven_gb).unwrap();
    assert_eq!(stateful_manager.remaining_upload_capacity(), 0);

    // Bây giờ upload thêm dù chỉ 1 byte cũng phải bị từ chối
    let fail_res = stateful_manager.record_upload(1);
    assert!(fail_res.is_err());
}

#[test]
fn test_can_store_incoming_shard_with_blockstore() {
    let blockstore = BlockStore::open_temporary().unwrap();
    // Giả sử phân bổ 1MB ổ cứng
    let allocated_bytes = 1024 * 1024; // 1 MB
    let manager = QuotaManager::new(allocated_bytes);

    // Shard kích thước 500KB -> Lưu được
    assert!(manager.can_store_incoming_shard(&blockstore, 500 * 1024));

    // Shard kích thước 2MB -> Vượt quá 1MB cam kết -> Không lưu được
    assert!(!manager.can_store_incoming_shard(&blockstore, 2 * 1024 * 1024));
}

#[test]
fn test_quota_manager_save_and_load() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("quota.json");

    let mut manager = QuotaManager::default_60gb();
    manager.record_upload(3 * 1024 * 1024 * 1024).unwrap();

    manager.save_to_file(&file_path).unwrap();
    assert!(file_path.exists());

    let loaded = QuotaManager::load_from_file(&file_path).unwrap();
    assert_eq!(manager, loaded);
    assert_eq!(loaded.my_uploaded_bytes, 3 * 1024 * 1024 * 1024);
}
