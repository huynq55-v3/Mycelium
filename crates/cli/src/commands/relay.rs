use std::collections::{HashMap, HashSet};
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
use libp2p::request_response::{self, Behaviour as ReqRespBehaviour, Config as ReqRespConfig, Message, ProtocolSupport};
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::Multiaddr;
use network::behaviour::{MYCELIUM_RELAY_AGENT_VERSION, MYCELIUM_RELAY_PROTOCOL, MYCELIUM_STORAGE_PROTOCOL};
use network::protocol::{MyceliumStorageCodec, NodeStateReport, ShardRequest, ShardResponse, SwarmStateBroadcast};
use network::transport::build_transport;
use network::RendezvousClient;
use tokio::signal;
use tracing::{debug, info, warn};

use crate::config::AppConfig;

pub const DEFAULT_RELAY_PORT: u16 = 4002;

use libp2p::ping;

/// Hành vi mạng chuyên dụng cho Dedicated Relay Server kiêm Swarm State Coordinator.
#[derive(NetworkBehaviour)]
pub struct PureRelayBehaviour {
    pub identify: identify::Behaviour,
    pub relay_server: relay::Behaviour,
    pub autonat: autonat::Behaviour,
    pub ping: ping::Behaviour,
    pub request_response: ReqRespBehaviour<MyceliumStorageCodec>,
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

    // 1. Khởi tạo Relay Identity tạm thời (Ephemeral in-memory, không cần lưu đĩa)
    let identity = Identity::generate();
    println!("🆔 Relay DID: \x1b[1;32m{}\x1b[0m (Ephemeral)", identity.to_did());

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
        max_reservations: 1024,
        max_reservations_per_peer: 64,
        max_circuits: 2048,
        max_circuits_per_peer: 128,
        max_circuit_duration: Duration::from_secs(3600),
        max_circuit_bytes: 1024 * 1024 * 1024, // 1 GB per circuit
        reservation_rate_limiters: vec![],
        circuit_src_rate_limiters: vec![],
        ..Default::default()
    };
    let relay_server = relay::Behaviour::new(local_peer_id, relay_config);
    let autonat = autonat::Behaviour::new(local_peer_id, AutoNatConfig::default());
    let ping = ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(15)));
    let req_resp_config = ReqRespConfig::default()
        .with_request_timeout(Duration::from_secs(60));
    let protocols = [(MYCELIUM_STORAGE_PROTOCOL, ProtocolSupport::Full)];
    let request_response = ReqRespBehaviour::with_codec(MyceliumStorageCodec, protocols, req_resp_config);

    let behaviour = PureRelayBehaviour {
        identify,
        relay_server,
        autonat,
        ping,
        request_response,
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

fn duration_until_next_minute_slot(interval_secs: u64) -> Duration {
    let now = std::time::SystemTime::now();
    let since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    let seconds = since_epoch.as_secs();
    let nanos = since_epoch.subsec_nanos();
    let next_slot_secs = (seconds / interval_secs + 1) * interval_secs;
    let diff_secs = next_slot_secs - seconds;
    Duration::from_secs(diff_secs).saturating_sub(Duration::from_nanos(nanos as u64))
}

    let mut node_reports: HashMap<libp2p::PeerId, NodeStateReport> = HashMap::new();
    let mut connected_node_peers: HashSet<libp2p::PeerId> = HashSet::new();
    let mut active_reservations: HashSet<libp2p::PeerId> = HashSet::new();

    // Đồng bộ tuyệt đối theo mốc giây đầu tiên của mỗi phút (Wall-clock minute alignment)
    let initial_delay = duration_until_next_minute_slot(60);
    info!("⏰ [RELAY SWARM] Đang chờ {}s để đồng bộ chu kỳ Heartbeat bắt đầu từ đúng giây đầu tiên của phút...", initial_delay.as_secs());
    tokio::time::sleep(initial_delay).await;

    let mut relay_heartbeat_interval = tokio::time::interval(Duration::from_secs(60));
    relay_heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // 7. Event loop chạy nền cho Relay Server kiêm Swarm Coordinator
    loop {
        tokio::select! {
            _ = relay_heartbeat_interval.tick() => {
                let nodes: Vec<NodeStateReport> = node_reports.values().cloned().collect();
                if !nodes.is_empty() && !connected_node_peers.is_empty() {
                    let broadcast = SwarmStateBroadcast {
                        nodes,
                        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                    };
                    info!("📡 [RELAY SWARM] Phát sóng SwarmStateBroadcast ({} nodes) tới {} peers", broadcast.nodes.len(), connected_node_peers.len());
                    for peer in &connected_node_peers {
                        let req = ShardRequest::SyncSwarmState(broadcast.clone());
                        swarm.behaviour_mut().request_response.send_request(peer, req);
                    }
                }

                // Gia hạn Heartbeat cho các Client Node đang cắm qua Relay lên Rendezvous
                for client_peer in &active_reservations {
                    let client_circuit_addr = format!("{}/p2p-circuit/p2p/{}", public_multiaddr, client_peer);
                    let _ = rendezvous.send_heartbeat(&client_peer.to_string(), &[client_circuit_addr], Some("VN".to_string())).await;
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        debug!("Relay mở listener mới: {}", address);
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        info!("🤝 [RELAY] Peer đã kết nối thành công: {} ({:?})", peer_id, endpoint.get_remote_address());
                        connected_node_peers.insert(peer_id);
                        // Gửi ngay snapshot hiện có cho peer mới cắm vào
                        let nodes: Vec<NodeStateReport> = node_reports.values().cloned().collect();
                        if !nodes.is_empty() {
                            let broadcast = SwarmStateBroadcast {
                                nodes,
                                timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
                            };
                            let req = ShardRequest::SyncSwarmState(broadcast);
                            swarm.behaviour_mut().request_response.send_request(&peer_id, req);
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        info!("👋 [RELAY] Peer đã ngắt kết nối: {}", peer_id);
                        connected_node_peers.remove(&peer_id);
                        active_reservations.remove(&peer_id);
                        node_reports.remove(&peer_id);
                    }
                    SwarmEvent::Behaviour(PureRelayBehaviourEvent::RelayServer(relay_event)) => {
                        match relay_event {
                            relay::Event::ReservationReqAccepted { src_peer_id, renewed } => {
                                info!("🎉 [RELAY] Đã cấp Reservation thành công cho peer: {} (renewed={})", src_peer_id, renewed);
                                active_reservations.insert(src_peer_id);
                                let client_circuit_addr = format!("{}/p2p-circuit/p2p/{}", public_multiaddr, src_peer_id);
                                let rz = rendezvous.clone();
                                let src_str = src_peer_id.to_string();
                                tokio::spawn(async move {
                                    info!("📡 [RELAY] Đăng ký địa chỉ Circuit lên Rendezvous cho client {}: {}", src_str, client_circuit_addr);
                                    let _ = rz.send_heartbeat(&src_str, &[client_circuit_addr], Some("VN".to_string())).await;
                                });
                            }
                            relay::Event::ReservationReqDenied { src_peer_id } => {
                                warn!("❌ [RELAY] TỪ CHỐI cấp Reservation cho peer: {}", src_peer_id);
                            }
                            relay::Event::ReservationTimedOut { src_peer_id } => {
                                debug!("⏳ [RELAY] Reservation của peer {} đã hết hạn (Timed Out)", src_peer_id);
                                active_reservations.remove(&src_peer_id);
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
                    SwarmEvent::Behaviour(PureRelayBehaviourEvent::RequestResponse(req_event)) => {
                        if let request_response::Event::Message { peer, message } = req_event {
                            match message {
                                Message::Request { request, channel, .. } => {
                                    match request {
                                        ShardRequest::ReportState(report) => {
                                            info!("📊 [RELAY SWARM] Nhận báo cáo trạng thái từ Node {}: R={:?} ({} shards)", peer, report.r_ratio, report.held_shards.len());
                                            node_reports.insert(peer, report.clone());
                                            // Forward / Gossip sang các Relay hoặc Peer khác
                                            for other_peer in &connected_node_peers {
                                                if *other_peer != peer {
                                                    let fwd_req = ShardRequest::ReportState(report.clone());
                                                    swarm.behaviour_mut().request_response.send_request(other_peer, fwd_req);
                                                }
                                            }
                                            let _ = swarm.behaviour_mut().request_response.send_response(channel, ShardResponse::Ack);
                                        }
                                        _ => {
                                            let _ = swarm.behaviour_mut().request_response.send_response(channel, ShardResponse::Ack);
                                        }
                                    }
                                }
                                _ => {}
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
