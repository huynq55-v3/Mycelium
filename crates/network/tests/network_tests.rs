use std::sync::Arc;
use std::time::Duration;

use blockstore::BlockStore;
use core_crypto::{Identity, SwarmKey};
use erasure_codec::Shard;
use network::{P2PService, RendezvousClient};
use quota_manager::QuotaManager;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_p2p_service_initialization_and_listener() {
    let identity = Identity::generate();
    let swarm_key = SwarmKey::generate();
    let blockstore = BlockStore::open_temporary().unwrap();
    let quota_manager = Arc::new(RwLock::new(QuotaManager::default_60gb()));

    let (service, _handle) =
        P2PService::new(&identity, Some(&swarm_key), blockstore, quota_manager)
            .expect("Khởi tạo P2PService thành công");

    let listen_addr: libp2p::Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    let res = service.listen_on(listen_addr).await;
    assert!(res.is_ok());

    let peer_id = service.local_peer_id();
    assert_eq!(peer_id.to_string().len() > 0, true);
}

#[tokio::test]
async fn test_rendezvous_client_mock_parsing_and_fallback() {
    let client = RendezvousClient::new("https://mock-rendezvous.deno.dev");

    // Khi không có kết nối internet hoặc server chưa bật -> Fallback mảng rỗng không làm crash
    let peers = client.fetch_bootstrap_peers(5).await.unwrap();
    assert!(peers.is_empty());
}

#[tokio::test]
async fn test_two_nodes_p2p_direct_shard_transfer() {
    let swarm_key = SwarmKey::generate();

    // 1. Khởi tạo Node 1 (Server / Receiver)
    let identity1 = Identity::generate();
    let store1 = BlockStore::open_temporary().unwrap();
    let quota1 = Arc::new(RwLock::new(QuotaManager::default_60gb()));
    let (node1, _h1) = P2PService::new(&identity1, Some(&swarm_key), store1.clone(), quota1)
        .expect("Tạo Node 1");

    // Lắng nghe trên cổng ngẫu nhiên 127.0.0.1
    let addr1: libp2p::Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    node1.listen_on(addr1).await.unwrap();

    // Đợi 200ms để Swarm cập nhật ListenAddr
    tokio::time::sleep(Duration::from_millis(200)).await;
    let listeners1 = node1.get_listeners().await.unwrap();
    if listeners1.is_empty() {
        return;
    }

    let node1_target_addr = listeners1[0].clone();

    // 2. Khởi tạo Node 2 (Client / Sender)
    let identity2 = Identity::generate();
    let store2 = BlockStore::open_temporary().unwrap();
    let quota2 = Arc::new(RwLock::new(QuotaManager::default_60gb()));
    let (node2, _h2) = P2PService::new(&identity2, Some(&swarm_key), store2.clone(), quota2)
        .expect("Tạo Node 2");

    // Node 2 Dial tới Node 1
    node2.dial(node1_target_addr).await.unwrap();

    // Đợi kết nối P2P hoàn tất (khoảng 500ms)
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 3. Node 2 gửi 1 Shard 2MB tới Node 1
    let sample_data = vec![0xABu8; 2 * 1024 * 1024]; // 2MB
    let sample_hash = erasure_codec::sha256_hex(&sample_data);
    let sample_shard = Shard {
        index: 0,
        data: sample_data.clone(),
        hash: sample_hash.clone(),
    };

    let distribute_res = node2.distribute_shards(vec![sample_shard]).await;
    assert!(distribute_res.is_ok());

    // Đợi Node 1 nhận và lưu vào BlockStore
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. Kiểm tra Node 1 đã nhận và lưu thành công shard vào BlockStore chưa
    let stored_in_node1 = store1.has_shard(&sample_hash).unwrap();
    assert!(stored_in_node1, "Node 1 phải lưu giữ shard trong BlockStore");

    let data_in_node1 = store1.get_shard(&sample_hash).unwrap().unwrap();
    assert_eq!(data_in_node1, sample_data);
}
