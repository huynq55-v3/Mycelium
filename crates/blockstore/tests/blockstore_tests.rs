use blockstore::BlockStore;
use tempfile::tempdir;

#[test]
fn test_blockstore_basic_crud() {
    let dir = tempdir().expect("Tạo tempdir");
    let store_path = dir.path().join("blocks");

    let store = BlockStore::open(&store_path).expect("Mở BlockStore");

    let shard_hash = "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";
    let shard_payload = b"Sample encrypted Reed-Solomon shard binary payload";

    // 1. Kiểm tra ban đầu
    assert!(!store.has_shard(shard_hash).unwrap());
    assert_eq!(store.get_shard(shard_hash).unwrap(), None);

    // 2. Put shard
    store.put_shard(shard_hash, shard_payload).unwrap();

    // 3. Has & Get shard
    assert!(store.has_shard(shard_hash).unwrap());
    let fetched = store.get_shard(shard_hash).unwrap().expect("Tìm thấy shard");
    assert_eq!(fetched, shard_payload);

    // 4. Kiểm tra disk usage
    let usage = store.current_disk_usage().unwrap();
    assert!(usage > 0);

    // 5. Delete shard
    let deleted = store.delete_shard(shard_hash).unwrap();
    assert!(deleted);
    assert!(!store.has_shard(shard_hash).unwrap());
}
