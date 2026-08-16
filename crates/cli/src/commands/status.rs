use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use blockstore::BlockStore;
use core_crypto::{Identity, SwarmKey};
use network::{P2PService, RendezvousClient};
use quota_manager::QuotaManager;
use tokio::sync::RwLock;

use crate::config::AppConfig;

pub async fn handle_status() -> Result<()> {
    let config_dir = AppConfig::config_dir()?;
    let config = AppConfig::load_or_default();

    println!("============================================================");
    println!("          📊 TRẠNG THÁI HỆ THỐNG MYCELIUM P2P DRIVE         ");
    println!("============================================================");

    // 1. Identity
    let identity_path = config_dir.join("identity.json");
    let identity_opt = if identity_path.exists() {
        Identity::load_from_file(&identity_path).ok()
    } else {
        None
    };

    if let Some(ref id) = identity_opt {
        println!("🆔 Identity DID : \x1b[1;32m{}\x1b[0m", id.to_did());
    } else {
        println!("🆔 Identity DID : \x1b[1;31mChưa khởi tạo (Chạy 'p2pdrive init')\x1b[0m");
    }

    // 2. Swarm Grid
    let swarm_key_path = config_dir.join("swarm.key");
    if swarm_key_path.exists() {
        if let Ok(key) = SwarmKey::load_from_file(&swarm_key_path) {
            println!("🔒 Swarm Grid    : \x1b[1;33mPrivate Network\x1b[0m (Key: {}...)", &key.to_hex()[..16]);
        } else {
            println!("🔒 Swarm Grid    : \x1b[1;31mPrivate Key Lỗi\x1b[0m");
        }
    } else {
        println!("🌐 Swarm Grid    : \x1b[1;34mPublic Open Network\x1b[0m");
    }

    // 3. Rendezvous Server Health
    let rendezvous = RendezvousClient::new(&config.rendezvous_url);
    print!("📡 Rendezvous    : {} ... ", config.rendezvous_url);
    match tokio::time::timeout(Duration::from_secs(3), rendezvous.fetch_bootstrap_peers(1)).await {
        Ok(Ok(_)) => println!("\x1b[1;32m[ONLINE]\x1b[0m"),
        _ => println!("\x1b[1;33m[UNREACHABLE / FALLBACK mDNS]\x1b[0m"),
    }

    // 4. BlockStore Status
    let blockstore_path = config_dir.join("blockstore");
    let (shard_count, disk_used_mb) = if blockstore_path.exists() {
        if let Ok(store) = BlockStore::open(&blockstore_path) {
            let count = store.count_shards();
            let used = store.current_disk_usage().unwrap_or(0) as f64 / (1024.0 * 1024.0);
            (count, used)
        } else {
            (0, 0.0)
        }
    } else {
        (0, 0.0)
    };
    println!("🗄️ BlockStore     : {} shards lưu trữ ({:.2} MB trên đĩa)", shard_count, disk_used_mb);

    // 5. Quota & Contribution
    let quota_path = config_dir.join("quota.json");
    let quota = if quota_path.exists() {
        QuotaManager::load_from_file(&quota_path).unwrap_or_else(|_| QuotaManager::default_60gb())
    } else {
        QuotaManager::default_60gb()
    };

    let allocated_gb = quota.allocated_disk_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let disk_used_gb = disk_used_mb / 1024.0;
    let allowed_upload_gb = quota.allowed_upload_capacity() as f64 / (1024.0 * 1024.0 * 1024.0);
    let uploaded_gb = quota.my_uploaded_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("💾 Ổ đĩa đóng góp: \x1b[1;36m{:.2} GB / {:.0} GB\x1b[0m ({:.1}%)",
        disk_used_gb, allocated_gb, (disk_used_gb / allocated_gb.max(1.0)) * 100.0);
    println!("🚀 Hạn mức tải lên: \x1b[1;32m{:.2} GB / {:.2} GB\x1b[0m (Tỷ lệ 1:4 an toàn)",
        uploaded_gb, allowed_upload_gb);

    // 6. Kiểm tra Active Peers (thử khởi tạo P2P listener ngắn hạn nếu có identity)
    if let Some(ref id) = identity_opt {
        let swarm_key = if swarm_key_path.exists() {
            SwarmKey::load_from_file(&swarm_key_path).ok()
        } else {
            None
        };
        let store = BlockStore::open_temporary()?;
        let qm = Arc::new(RwLock::new(quota.clone()));
        if let Ok((service, _)) = P2PService::new(id, swarm_key.as_ref(), store, qm) {
            let connected = service.get_connected_peers().await.unwrap_or_default();
            println!("👥 Peers kết nối : \x1b[1;35m{} peers\x1b[0m", connected.len());
        }
    }

    println!("============================================================");
    Ok(())
}
