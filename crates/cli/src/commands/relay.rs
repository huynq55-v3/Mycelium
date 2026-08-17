use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use core_crypto::{Identity, SwarmKey};
use futures::StreamExt;
use libp2p::autonat::{self, Config as AutoNatConfig};
use libp2p::identify::{self, Config as IdentifyConfig};
use libp2p::identity::Keypair;
use libp2p::relay;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::Multiaddr;
use network::transport::build_transport;
use network::{
    behaviour::{MYCELIUM_RELAY_AGENT_VERSION, MYCELIUM_RELAY_PROTOCOL},
    RendezvousClient,
};
use tokio::signal;
use tracing::{debug, info, warn};

use crate::config::AppConfig;

pub const DEFAULT_RELAY_PORT: u16 = 4002;

use libp2p::ping;

/// Hành vi mạng chuyên dụng cho Dedicated Relay Server (Không Storage, Không Quota, Không DHT Pollution).
#[derive(NetworkBehaviour)]
pub struct PureRelayBehaviour {
    pub identify: identify::Behaviour,
    pub relay_server: relay::Behaviour,
    pub autonat: autonat::Behaviour,
    pub ping: ping::Behaviour,
}

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

pub async fn handle_relay(
    port_opt: Option<u16>,
    public_port_opt: Option<u16>,
    public_host_opt: Option<String>,
    rendezvous_url_opt: Option<String>,
    swarm_key_opt: Option<PathBuf>,
) -> Result<()> {
    let listen_port = port_opt.unwrap_or(DEFAULT_RELAY_PORT);
    let public_port = public_port_opt
        .or_else(|| std::env::var("PUBLIC_PORT").ok().and_then(|p| p.parse().ok()))
        .unwrap_or(listen_port);
    let public_host_resolved = public_host_opt
        .or_else(|| std::env::var("PUBLIC_HOST").ok());

    let config = AppConfig::load_or_default();
    let rendezvous_url = rendezvous_url_opt
        .or_else(|| std::env::var("RENDEZVOUS_URL").ok())
        .unwrap_or(config.rendezvous_url);
    let config_dir = AppConfig::config_dir()?;

    println!("============================================================");
    println!("     🌐 KHỞI ĐỘNG MYCELIUM P2P DEDICATED RELAY SERVER 🌐     ");
    println!("  (Trạm trung chuyển dữ liệu vượt NAT - Zero Storage Database) ");
    println!("============================================================");

    // 1. Quản lý Relay Identity độc lập
    let relay_identity_path = config_dir.join("relay_identity.json");
    let identity = if relay_identity_path.exists() {
        Identity::load_from_file(&relay_identity_path)?
    } else {
        let id = Identity::generate();
        let _ = id.save_to_file(&relay_identity_path);
        id
    };
    println!("🆔 Relay DID: \x1b[1;32m{}\x1b[0m", identity.to_did());

    // 2. Nạp SwarmKey (Hỗ trợ từ ENV SWARM_KEY hoặc file swarm.key)
    let swarm_key = if let Ok(key_hex) = std::env::var("SWARM_KEY") {
        if let Ok(key) = SwarmKey::from_hex(&key_hex) {
            println!("🔒 Swarm Grid: \x1b[1;33mPrivate Network\x1b[0m (Key từ ENV: {}...)", &key.to_hex()[..12]);
            Some(key)
        } else {
            None
        }
    } else {
        let swarm_key_path = swarm_key_opt.unwrap_or_else(|| config_dir.join("swarm.key"));
        if swarm_key_path.exists() {
            let key = SwarmKey::load_from_file(&swarm_key_path)?;
            println!("🔒 Swarm Grid: \x1b[1;33mPrivate Network\x1b[0m (Key: {}...)", &key.to_hex()[..12]);
            Some(key)
        } else {
            println!("🌐 Swarm Grid: \x1b[1;34mPublic Open Network\x1b[0m");
            None
        }
    };

    // 3. Khởi tạo Keypair & Transport
    let secret_bytes = identity.secret_key_bytes();
    let libp2p_secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(secret_bytes)
        .context("Lỗi chuyển đổi SecretKey")?;
    let keypair = Keypair::from(libp2p::identity::ed25519::Keypair::from(libp2p_secret));
    let local_peer_id = keypair.public().to_peer_id();

    let (transport, _relay_client) = build_transport(&keypair, swarm_key.as_ref())
        .context("Lỗi khởi tạo Transport")?;

    // 4. Khởi tạo Behaviour
    let identify_config = IdentifyConfig::new(
        MYCELIUM_RELAY_PROTOCOL.to_string(),
        keypair.public(),
    )
    .with_agent_version(MYCELIUM_RELAY_AGENT_VERSION.to_string());
    let identify = identify::Behaviour::new(identify_config);

    let relay_config = relay::Config {
        max_reservations: 256,
        max_circuits: 512,
        max_circuit_duration: Duration::from_secs(300),
        max_circuit_bytes: 1024 * 1024 * 500, // 500 MB per circuit
        reservation_rate_limiters: vec![],
        circuit_src_rate_limiters: vec![],
        ..Default::default()
    };
    let relay_server = relay::Behaviour::new(local_peer_id, relay_config);
    let autonat = autonat::Behaviour::new(local_peer_id, AutoNatConfig::default());
    let ping = ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(15)));

    let behaviour = PureRelayBehaviour {
        identify,
        relay_server,
        autonat,
        ping,
    };

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        local_peer_id,
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(86400 * 365)),
    );

    // 5. Lắng nghe Dual-Stack trên listen_port cục bộ
    let ipv4_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", listen_port).parse()?;
    swarm.listen_on(ipv4_addr.clone())?;
    println!("👂 Cổng lắng nghe nội bộ: \x1b[1;36m{}\x1b[0m", ipv4_addr);

    let ipv6_addr: Multiaddr = format!("/ip6/::/tcp/{}", listen_port).parse()?;
    if let Ok(_) = swarm.listen_on(ipv6_addr.clone()) {
        println!("🌐 IPv6 Dual-Stack: Đang lắng nghe tại \x1b[1;36m{}\x1b[0m", ipv6_addr);
    }

    // 6. Phát hiện IP/Host và gửi Heartbeat lên Rendezvous Server
    let host_or_ip = match public_host_resolved {
        Some(host) => host,
        None => detect_public_or_lan_ip().await,
    };

    let public_transport_addr: Multiaddr = if host_or_ip.contains('.') && host_or_ip.chars().any(|c| c.is_alphabetic()) {
        // Domain name (ngrok / custom domain) -> Dùng dns4
        format!("/dns4/{}/tcp/{}", host_or_ip, public_port).parse()?
    } else if host_or_ip.contains(':') {
        // IPv6
        format!("/ip6/{}/tcp/{}", host_or_ip, public_port).parse()?
    } else {
        // IPv4
        format!("/ip4/{}/tcp/{}", host_or_ip, public_port).parse()?
    };

    // Công bố địa chỉ public cho Swarm để libp2p Identify truyền tới các peer
    swarm.add_external_address(public_transport_addr.clone());

    let public_multiaddr: Multiaddr = format!("{}/p2p/{}", public_transport_addr, local_peer_id).parse()?;

    println!("🌐 Địa chỉ công bố ra ngoài: \x1b[1;32m{}:{}\x1b[0m", host_or_ip, public_port);

    let rendezvous = RendezvousClient::new(&rendezvous_url);
    println!("📡 Rendezvous Server: \x1b[1;34m{}\x1b[0m", rendezvous_url);

    let _heartbeat_handle = rendezvous.start_heartbeat_loop(
        local_peer_id,
        public_multiaddr.clone(),
        Some("RELAY".to_string()),
    );

    println!("📡 Multiaddr đăng ký: \x1b[1;33m{}\x1b[0m", public_multiaddr);
    println!("============================================================");
    println!("✨ Relay Node đang hoạt động 24/7. Nhấn \x1b[1;31mCtrl+C\x1b[0m để dừng.");
    println!("============================================================");

    // 7. Event loop chạy nền cho Relay Server
    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        debug!("Relay mở listener mới: {}", address);
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        info!("🤝 [RELAY] Peer đã kết nối thành công: {} ({:?})", peer_id, endpoint.get_remote_address());
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        info!("👋 [RELAY] Peer đã ngắt kết nối: {}", peer_id);
                    }
                    SwarmEvent::Behaviour(PureRelayBehaviourEvent::RelayServer(relay_event)) => {
                        match relay_event {
                            relay::Event::ReservationReqAccepted { src_peer_id, renewed } => {
                                info!("🎉 [RELAY] Đã cấp Reservation thành công cho peer: {} (renewed={})", src_peer_id, renewed);
                            }
                            relay::Event::ReservationReqDenied { src_peer_id } => {
                                warn!("❌ [RELAY] TỪ CHỐI cấp Reservation cho peer: {}", src_peer_id);
                            }
                            relay::Event::ReservationTimedOut { src_peer_id } => {
                                debug!("⏳ [RELAY] Reservation của peer {} đã hết hạn (Timed Out)", src_peer_id);
                            }
                            relay::Event::CircuitReqAccepted { src_peer_id, dst_peer_id } => {
                                info!("⚡ [RELAY] ĐANG BẮC CẦU THÔNG SUỐT: {} <=====> {}", src_peer_id, dst_peer_id);
                            }
                            relay::Event::CircuitReqDenied { src_peer_id, dst_peer_id } => {
                                warn!("❌ [RELAY] TỪ CHỐI bắc cầu Circuit: {} ----> {}", src_peer_id, dst_peer_id);
                            }
                            relay::Event::CircuitClosed { src_peer_id, dst_peer_id, error } => {
                                info!("🔌 [RELAY] Đã đóng cầu nối: {} <=====> {} (lý do: {:?})", src_peer_id, dst_peer_id, error);
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ = signal::ctrl_c() => {
                println!("\n🛑 Đang dừng Relay Server an toàn...");
                break;
            }
        }
    }

    Ok(())
}
