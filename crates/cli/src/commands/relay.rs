use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use core_crypto::{Identity, SwarmKey};
use futures::StreamExt;
use libp2p::autonat::{self, Config as AutoNatConfig};
use libp2p::identify::{self, Config as IdentifyConfig};
use libp2p::identity::Keypair;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{self, Config as KadConfig};
use libp2p::mdns::{self, Config as MdnsConfig};
use libp2p::relay;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::Multiaddr;
use network::transport::build_transport;
use network::{behaviour::MYCELIUM_AGENT_VERSION, behaviour::MYCELIUM_KAD_PROTOCOL, RendezvousClient};
use tokio::signal;
use tracing::{debug, info, trace};

use crate::config::AppConfig;

pub const DEFAULT_RELAY_PORT: u16 = 4002;

/// Hành vi mạng chuyên dụng cho Dedicated Relay Server (Không Storage, Không Quota).
#[derive(NetworkBehaviour)]
pub struct PureRelayBehaviour {
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
    pub relay_server: relay::Behaviour,
    pub autonat: autonat::Behaviour,
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
    rendezvous_url_opt: Option<String>,
    public_ip_opt: Option<String>,
    swarm_key_opt: Option<PathBuf>,
) -> Result<()> {
    let listen_port = port_opt.unwrap_or(DEFAULT_RELAY_PORT);
    let public_port = public_port_opt.unwrap_or(listen_port);
    let config = AppConfig::load_or_default();
    let rendezvous_url = rendezvous_url_opt.unwrap_or(config.rendezvous_url);
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
        id.save_to_file(&relay_identity_path)?;
        id
    };
    println!("🆔 Relay DID: \x1b[1;32m{}\x1b[0m", identity.to_did());

    // 2. Nạp SwarmKey
    let swarm_key_path = swarm_key_opt.unwrap_or_else(|| config_dir.join("swarm.key"));
    let swarm_key = if swarm_key_path.exists() {
        let key = SwarmKey::load_from_file(&swarm_key_path)?;
        println!("🔒 Swarm Grid: \x1b[1;33mPrivate Network\x1b[0m (Key: {}...)", &key.to_hex()[..12]);
        Some(key)
    } else {
        println!("🌐 Swarm Grid: \x1b[1;34mPublic Open Network\x1b[0m");
        None
    };

    // 3. Khởi tạo Keypair & Transport
    let secret_bytes = identity.secret_key_bytes();
    let libp2p_secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(secret_bytes)
        .context("Lỗi chuyển đổi SecretKey")?;
    let keypair = Keypair::from(libp2p::identity::ed25519::Keypair::from(libp2p_secret));
    let local_peer_id = keypair.public().to_peer_id();

    let transport = build_transport(&keypair, swarm_key.as_ref())
        .context("Lỗi khởi tạo Transport")?;

    // 4. Khởi tạo Behaviour
    let mut kad_config = KadConfig::new(MYCELIUM_KAD_PROTOCOL);
    kad_config.set_query_timeout(Duration::from_secs(20));
    let store = MemoryStore::new(local_peer_id);
    let kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);

    let mdns = mdns::tokio::Behaviour::new(MdnsConfig::default(), local_peer_id)
        .context("Lỗi khởi tạo mDNS")?;

    let identify_config = IdentifyConfig::new(
        "/mycelium/relay/1.0.0".to_string(),
        keypair.public(),
    )
    .with_agent_version(MYCELIUM_AGENT_VERSION.to_string());
    let identify = identify::Behaviour::new(identify_config);

    let relay_config = relay::Config {
        max_reservations: 256,
        max_circuits: 512,
        max_circuit_duration: Duration::from_secs(300),
        max_circuit_bytes: 1024 * 1024 * 500, // 500 MB per circuit
        ..Default::default()
    };
    let relay_server = relay::Behaviour::new(local_peer_id, relay_config);
    let autonat = autonat::Behaviour::new(local_peer_id, AutoNatConfig::default());

    let behaviour = PureRelayBehaviour {
        kademlia,
        mdns,
        identify,
        relay_server,
        autonat,
    };

    let mut swarm = Swarm::new(
        transport,
        behaviour,
        local_peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );

    // 5. Lắng nghe Dual-Stack trên listen_port cục bộ
    let ipv4_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", listen_port).parse()?;
    swarm.listen_on(ipv4_addr.clone())?;
    println!("👂 Cổng lắng nghe nội bộ: \x1b[1;36m{}\x1b[0m", ipv4_addr);

    let ipv6_addr: Multiaddr = format!("/ip6/::/tcp/{}", listen_port).parse()?;
    if let Ok(_) = swarm.listen_on(ipv6_addr.clone()) {
        println!("🌐 IPv6 Dual-Stack: Đang lắng nghe tại \x1b[1;36m{}\x1b[0m", ipv6_addr);
    }

    // 6. Phát hiện IP và gửi Heartbeat lên Rendezvous Server (sử dụng public_port từ ngrok/router)
    let resolved_ip = match public_ip_opt {
        Some(ip) => ip,
        None => detect_public_or_lan_ip().await,
    };
    let is_v6 = resolved_ip.contains(':');
    let proto_prefix = if is_v6 { "ip6" } else { "ip4" };
    println!("🌐 Địa chỉ công bố ra ngoài: \x1b[1;32m{}:{}\x1b[0m ({})", resolved_ip, public_port, proto_prefix.to_uppercase());

    let rendezvous = RendezvousClient::new(&rendezvous_url);
    println!("📡 Rendezvous Server: \x1b[1;34m{}\x1b[0m", rendezvous_url);

    let public_multiaddr: Multiaddr = format!("/{}/{}/tcp/{}/p2p/{}", proto_prefix, resolved_ip, public_port, local_peer_id).parse()?;
    
    let _heartbeat_handle = rendezvous.start_heartbeat_loop(
        local_peer_id,
        public_multiaddr.clone(),
        Some("RELAY-VN".to_string()),
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
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        info!("🤝 Peer kết nối vào Relay: {}", peer_id);
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        debug!("Peer ngắt kết nối khỏi Relay: {}", peer_id);
                    }
                    SwarmEvent::Behaviour(PureRelayBehaviourEvent::Identify(identify_event)) => {
                        if let libp2p::identify::Event::Received { peer_id, info, .. } = identify_event {
                            trace!("Relay Identify từ {}: {:?}", peer_id, info.listen_addrs);
                            for addr in info.listen_addrs {
                                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                            }
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
