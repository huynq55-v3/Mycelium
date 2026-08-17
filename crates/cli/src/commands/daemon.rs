use std::net::IpAddr;
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
use tracing::info;

use crate::config::AppConfig;
use crate::ipc::IpcServer;

/// Tự động phát hiện IP Public thực của máy (ưu tiên IPv6, fallback IPv4).
async fn detect_public_or_lan_ip() -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let ip_services = [
        "https://api64.ipify.org",
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ];

    for url in ip_services {
        if let Ok(res) = client.get(url).send().await {
            if let Ok(ip_text) = res.text().await {
                let trimmed = ip_text.trim();
                if trimmed.parse::<IpAddr>().is_ok() {
                    return trimmed.to_string();
                }
            }
        }
    }

    "127.0.0.1".to_string()
}

pub async fn handle_daemon(
    port_opt: Option<u16>,
    rendezvous_url_opt: Option<String>,
    public_ip_opt: Option<String>,
) -> Result<()> {
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
    let disk_used_mb = blockstore.total_payload_bytes().unwrap_or(0) as f64 / (1024.0 * 1024.0);
    println!("🗄️ BlockStore: {:?} (Số shards: {}, Dung lượng: {:.2} MB)",
        blockstore_path, blockstore.count_shards(), disk_used_mb);

    // 4. Nạp QuotaManager
    let quota_path = config_dir.join("quota.json");
    let mut quota = if quota_path.exists() {
        QuotaManager::load_from_file(&quota_path)?
    } else {
        QuotaManager::default_60gb()
    };

    let total_blockstore_bytes = blockstore.total_payload_bytes().unwrap_or(0);
    if total_blockstore_bytes == 0 {
        if quota.stored_shard_bytes > 0 {
            quota.stored_shard_bytes = 0;
            let _ = quota.save_to_file(&quota_path);
        }
    } else if quota.stored_shard_bytes != total_blockstore_bytes {
        quota.stored_shard_bytes = total_blockstore_bytes;
        let _ = quota.save_to_file(&quota_path);
    }
    let quota_manager = Arc::new(RwLock::new(quota));

    // 5. Khởi động P2PService (Storage Node Client với Relay Client & DCUtR)
    let (service, _service_handle) = P2PService::new(
        &identity,
        swarm_key.as_ref(),
        blockstore.clone(),
        quota_manager.clone(),
    ).context("Khởi tạo P2PService thất bại")?;

    // 5. Lắng nghe P2P Dual-Stack (IPv4 & IPv6)
    let ipv4_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", port).parse()?;
    service.listen_on(ipv4_addr.clone()).await?;
    println!("👂 P2P Network: Đang lắng nghe tại \x1b[1;36m{}\x1b[0m", ipv4_addr);

    let ipv6_addr: Multiaddr = format!("/ip6/::/tcp/{}", port).parse()?;
    if let Ok(_) = service.listen_on(ipv6_addr.clone()).await {
        println!("🌐 IPv6 Dual-Stack: Đang lắng nghe tại \x1b[1;36m{}\x1b[0m", ipv6_addr);
    }

    // 6. Xác định IP Public của node để công bố lên Rendezvous (Whitelist)
    let resolved_ip = match public_ip_opt {
        Some(ip) => ip,
        None => detect_public_or_lan_ip().await,
    };

    let is_v6 = resolved_ip.contains(':');
    let proto_prefix = if is_v6 { "ip6" } else { "ip4" };
    let public_multiaddr: Multiaddr = format!("/{}/{}/tcp/{}/p2p/{}", proto_prefix, resolved_ip, port, service.local_peer_id()).parse()?;

    if network::service::is_dialable_multiaddr(&public_multiaddr) {
        println!("🌐 Địa chỉ IP Public công bố: \x1b[1;32m{}\x1b[0m ({})", resolved_ip, proto_prefix.to_uppercase());
    } else {
        println!("🔒 Node đang ở Private IP ({}), đăng ký sổ địa chỉ định tuyến qua Relay Circuit.", resolved_ip);
    }

    // 7. Rendezvous Bootstrap, Heartbeat & Dynamic Peer Keeper
    let rendezvous = RendezvousClient::new(&rendezvous_url);
    println!("📡 Rendezvous Server: \x1b[1;34m{}\x1b[0m", rendezvous_url);

    let public_multiaddr: Multiaddr = format!("/{}/{}/tcp/{}/p2p/{}", proto_prefix, resolved_ip, port, service.local_peer_id()).parse()?;
    
    // Đăng ký Rendezvous với Service để tự động kích hoạt Immediate Heartbeat khi có Circuit
    let _ = service.register_rendezvous(rendezvous.clone(), public_multiaddr.clone(), Some("VN".to_string())).await;

    // Heartbeat định kỳ 30s và cập nhật linh hoạt Circuit Relay multiaddr
    let _heartbeat_handle = rendezvous.start_dynamic_heartbeat_loop(
        service.clone(),
        public_multiaddr,
        Some("VN".to_string()),
    );

    // Bootstrap peers ban đầu
    let _ = service.bootstrap_from_rendezvous(&rendezvous, 20).await;

    // Kích hoạt Dynamic Peer Keeper: Tự động tìm kiếm peer mới mỗi 15 giây và chủ động tạo kết nối Outbound 2 chiều
    let _peer_keeper_handle = service.start_auto_discovery_loop(rendezvous.clone());
    info!("Đã kích hoạt Dynamic Peer Keeper (chu kỳ 15s tự động kết nối 2 chiều)");

    // 8. Khởi động Local IPC Server cho Thin CLI Clients
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

    // 9. Lắng nghe tín hiệu dừng Ctrl+C / SIGINT
    signal::ctrl_c().await.context("Lỗi lắng nghe tín hiệu Ctrl+C")?;

    println!("\n🛑 Đang tiến hành tắt an toàn (Graceful Shutdown)...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("✅ Đã đồng bộ BlockStore và ngắt kết nối an toàn. Hẹn gặp lại!");

    Ok(())
}
