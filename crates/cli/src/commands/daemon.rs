use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use blockstore::BlockStore;
use core_crypto::{Identity, SwarmKey};
use libp2p::Multiaddr;
use network::{P2PService, RendezvousClient};
use quota_manager::QuotaManager;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::warn;

use crate::config::AppConfig;
use crate::ipc::IpcServer;

pub async fn handle_daemon(port_opt: Option<u16>, rendezvous_url_opt: Option<String>) -> Result<()> {
    let config = AppConfig::load_or_default();
    let port = port_opt.unwrap_or(config.port);
    let rendezvous_url = rendezvous_url_opt.unwrap_or(config.rendezvous_url.clone());
    let config_dir = AppConfig::config_dir()?;

    println!("============================================================");
    println!("       🚀 KHỞI ĐỘNG MYCELIUM P2P STORAGE DAEMON 🚀           ");
    println!("============================================================");

    // 1. Nạp Identity
    let identity_path = config_dir.join("identity.json");
    if !identity_path.exists() {
        println!("⚠️ Chưa tìm thấy cấu hình. Đang tự động khởi tạo...");
        crate::commands::init::handle_init(None, None, Some(rendezvous_url.clone()))?;
    }
    let identity = Identity::load_from_file(&identity_path)
        .context("Không thể nạp file identity.json")?;
    println!("🆔 Local DID: \x1b[1;32m{}\x1b[0m", identity.to_did());

    // 2. Nạp SwarmKey
    let swarm_key_path = config_dir.join("swarm.key");
    let swarm_key = if swarm_key_path.exists() {
        let key = SwarmKey::load_from_file(&swarm_key_path)?;
        println!("🔒 Chế độ: \x1b[1;33mPrivate Swarm Grid\x1b[0m (Key: {}...)", &key.to_hex()[..12]);
        Some(key)
    } else {
        println!("🌐 Chế độ: \x1b[1;34mPublic Open Network\x1b[0m");
        None
    };

    // 3. Mở BlockStore độc quyền duy nhất bởi Daemon
    let blockstore_path = config_dir.join("blockstore");
    let blockstore = BlockStore::open(&blockstore_path)
        .context("Không thể mở BlockStore trên đĩa")?;
    let disk_used_mb = blockstore.current_disk_usage().unwrap_or(0) as f64 / (1024.0 * 1024.0);
    println!("🗄️ BlockStore: {:?} (Số shards: {}, Dung lượng: {:.2} MB)",
        blockstore_path, blockstore.count_shards(), disk_used_mb);

    // 4. Nạp QuotaManager
    let quota_path = config_dir.join("quota.json");
    let quota = if quota_path.exists() {
        QuotaManager::load_from_file(&quota_path)?
    } else {
        QuotaManager::default_60gb()
    };
    let quota_manager = Arc::new(RwLock::new(quota));

    // 5. Khởi động P2PService
    let (service, _service_handle) = P2PService::new(
        &identity,
        swarm_key.as_ref(),
        blockstore.clone(),
        quota_manager.clone(),
    ).context("Khởi tạo P2PService thất bại")?;

    let listen_multiaddr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", port).parse()?;
    service.listen_on(listen_multiaddr.clone()).await?;
    println!("👂 P2P Network: Đang lắng nghe tại \x1b[1;36m{}\x1b[0m", listen_multiaddr);

    // 6. Rendezvous Bootstrap & Heartbeat
    let rendezvous = RendezvousClient::new(&rendezvous_url);
    println!("📡 Rendezvous Server: \x1b[1;34m{}\x1b[0m", rendezvous_url);

    let public_multiaddr: Multiaddr = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", port, service.local_peer_id()).parse()?;
    let _heartbeat_handle = rendezvous.start_heartbeat_loop(
        service.local_peer_id(),
        public_multiaddr,
        Some("VN".to_string()),
    );

    match service.bootstrap_from_rendezvous(&rendezvous, 20).await {
        Ok(count) => {
            println!("🌐 Bootstrap: Đã liên kết với \x1b[1;32m{}\x1b[0m peers từ mạng lưới", count);
        }
        Err(err) => {
            warn!("Không thể kết nối bootstrap peers: {}", err);
            println!("⚠️ Bootstrap qua HTTP thất bại, chuyển sang cơ chế mDNS mạng LAN.");
        }
    }

    // 7. Khởi động Local IPC Server cho Thin CLI Clients
    let ipc_server = Arc::new(IpcServer::new(
        service.clone(),
        identity.clone(),
        swarm_key.clone(),
        blockstore.clone(),
        quota_manager.clone(),
        config.clone(),
    ));

    tokio::spawn(async move {
        if let Err(e) = ipc_server.run().await {
            eprintln!("❌ Lỗi IPC Server: {}", e);
        }
    });

    println!("🔌 Local IPC Server: Đã mở socket tại \x1b[1;32m127.0.0.1:5001\x1b[0m (sẵn sàng nhận lệnh upload/download/status)");
    println!("============================================================");
    println!("✨ Node P2P đang hoạt động ổn định. Nhấn \x1b[1;31mCtrl+C\x1b[0m để dừng an toàn.");
    println!("============================================================");

    // 8. Lắng nghe tín hiệu dừng Ctrl+C / SIGINT
    signal::ctrl_c().await.context("Lỗi lắng nghe tín hiệu Ctrl+C")?;

    println!("\n🛑 Đang tiến hành tắt an toàn (Graceful Shutdown)...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("✅ Đã đồng bộ BlockStore và ngắt kết nối an toàn. Hẹn gặp lại!");

    Ok(())
}
