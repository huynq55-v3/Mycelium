use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use blockstore::BlockStore;
use core_crypto::{Identity, SwarmKey};
use erasure_codec::Shard;
use futures::StreamExt;
use libp2p::core::ConnectedPoint;
use libp2p::identity::Keypair;
use libp2p::kad::RecordKey;
use libp2p::mdns::Event as MdnsEvent;
use libp2p::request_response::{self, Message};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId};
use quota_manager::QuotaManager;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, info, trace, warn};

use crate::behaviour::{MyceliumBehaviour, MyceliumBehaviourEvent};
use crate::error::NetworkError;
use crate::protocol::{
    PullResponse, PullShard, PushResponse, PushShard, ShardRequest, ShardResponse,
};
use crate::rendezvous::RendezvousClient;
use crate::transport::build_transport;

const PEER_DISCOVERY_INTERVAL: Duration = Duration::from_secs(15);

/// Các lệnh nội bộ gửi từ `P2PHandle` tới background event loop của `P2PService`.
enum P2PCommand {
    ListenOn {
        addr: Multiaddr,
        respond_to: oneshot::Sender<Result<Multiaddr, NetworkError>>,
    },
    Dial {
        addr: Multiaddr,
        respond_to: oneshot::Sender<Result<(), NetworkError>>,
    },
    BootstrapPeers {
        peers: Vec<Multiaddr>,
        respond_to: oneshot::Sender<Result<usize, NetworkError>>,
    },
    DistributeShards {
        shards: Vec<Shard>,
        file_cid: Option<String>,
        respond_to: oneshot::Sender<Result<(), NetworkError>>,
    },
    FetchShardsParallel {
        hashes: Vec<String>,
        target_count: usize,
        respond_to: oneshot::Sender<Result<Vec<Shard>, NetworkError>>,
    },
    GetConnectedPeers {
        respond_to: oneshot::Sender<Vec<PeerId>>,
    },
    GetKnownPeers {
        respond_to: oneshot::Sender<Vec<PeerId>>,
    },
    GetListeners {
        respond_to: oneshot::Sender<Vec<Multiaddr>>,
    },
    RegisterRendezvous {
        rendezvous: RendezvousClient,
        public_multiaddr: Multiaddr,
        region: Option<String>,
        respond_to: oneshot::Sender<Result<(), NetworkError>>,
    },
}

/// Handle giao tiếp bất đồng bộ, luồng an toàn với `P2PService` background task.
#[derive(Clone)]
pub struct P2PService {
    local_peer_id: PeerId,
    command_tx: mpsc::Sender<P2PCommand>,
    quota_manager: Arc<RwLock<QuotaManager>>,
    blockstore: BlockStore,
}

impl P2PService {
    /// Khởi tạo và kích hoạt `P2PService` background task.
    pub fn new(
        identity: &Identity,
        swarm_key: Option<&SwarmKey>,
        blockstore: BlockStore,
        quota_manager: Arc<RwLock<QuotaManager>>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), NetworkError> {
        let secret_bytes = identity.secret_key_bytes();
        let libp2p_secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(secret_bytes)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi chuyển đổi SecretKey: {e}")))?;
        let keypair = Keypair::from(libp2p::identity::ed25519::Keypair::from(libp2p_secret));
        let local_peer_id = keypair.public().to_peer_id();

        let (transport, relay_client) = build_transport(&keypair, swarm_key)?;
        let behaviour = MyceliumBehaviour::new(&keypair, relay_client)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi khởi tạo Behaviour: {e}")))?;

        // Chế độ On-Demand: Đóng kết nối nhàn rỗi sau 30 giây khi không còn tác vụ truyền file
        let swarm = Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            libp2p::swarm::Config::with_tokio_executor()
                .with_idle_connection_timeout(Duration::from_secs(30)),
        );

        let (command_tx, command_rx) = mpsc::channel(100);

        let service = Self {
            local_peer_id,
            command_tx,
            quota_manager: quota_manager.clone(),
            blockstore: blockstore.clone(),
        };

        let mut event_loop = P2PEventLoop::new(
            swarm,
            command_rx,
            blockstore,
            quota_manager,
            local_peer_id,
        );

        let handle = tokio::spawn(async move {
            event_loop.run().await;
        });

        Ok((service, handle))
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn blockstore(&self) -> &BlockStore {
        &self.blockstore
    }

    pub fn quota_manager(&self) -> &Arc<RwLock<QuotaManager>> {
        &self.quota_manager
    }

    pub async fn listen_on(&self, addr: Multiaddr) -> Result<Multiaddr, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::ListenOn {
                addr,
                respond_to: tx,
            })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))?
    }

    pub async fn dial(&self, addr: Multiaddr) -> Result<(), NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::Dial {
                addr,
                respond_to: tx,
            })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))?
    }

    pub async fn bootstrap_from_rendezvous(
        &self,
        rendezvous: &RendezvousClient,
        limit: usize,
    ) -> Result<usize, NetworkError> {
        let peers = rendezvous.fetch_bootstrap_peers(limit).await?;
        if peers.is_empty() {
            return Ok(0);
        }

        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::BootstrapPeers {
                peers,
                respond_to: tx,
            })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))?
    }

    /// Đăng ký thông tin Rendezvous để EventLoop tự động kích hoạt Immediate Heartbeat khi có Circuit address.
    pub async fn register_rendezvous(
        &self,
        rendezvous: RendezvousClient,
        public_multiaddr: Multiaddr,
        region: Option<String>,
    ) -> Result<(), NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::RegisterRendezvous {
                rendezvous,
                public_multiaddr,
                region,
                respond_to: tx,
            })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))?
    }

    /// Khởi động vòng lặp tự động duy trì kết nối và tìm kiếm Peer mới qua Rendezvous Server (mỗi 15 giây).
    pub fn start_auto_discovery_loop(
        &self,
        rendezvous: RendezvousClient,
    ) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(PEER_DISCOVERY_INTERVAL);
            loop {
                interval.tick().await;
                let _ = service.bootstrap_from_rendezvous(&rendezvous, 20).await;
            }
        })
    }

    pub async fn distribute_shards(&self, shards: Vec<Shard>) -> Result<(), NetworkError> {
        self.distribute_shards_with_file(shards, None).await
    }

    pub async fn distribute_shards_with_file(&self, shards: Vec<Shard>, file_cid: Option<String>) -> Result<(), NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::DistributeShards {
                shards,
                file_cid,
                respond_to: tx,
            })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))?
    }

    pub async fn fetch_shards_parallel(
        &self,
        hashes: Vec<String>,
        target_count: usize,
    ) -> Result<Vec<Shard>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::FetchShardsParallel {
                hashes,
                target_count,
                respond_to: tx,
            })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))?
    }

    pub async fn get_connected_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::GetConnectedPeers { respond_to: tx })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))
    }

    pub async fn get_known_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::GetKnownPeers { respond_to: tx })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))
    }

    pub async fn get_listeners(&self) -> Result<Vec<Multiaddr>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::GetListeners { respond_to: tx })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))
    }
}

struct PendingFetchRequest {
    target_count: usize,
    collected_shards: Vec<Shard>,
    collected_indices: HashSet<usize>,
    hashes_map: HashMap<String, usize>,
    respond_to: oneshot::Sender<Result<Vec<Shard>, NetworkError>>,
}

struct P2PEventLoop {
    swarm: Swarm<MyceliumBehaviour>,
    command_rx: mpsc::Receiver<P2PCommand>,
    blockstore: BlockStore,
    quota_manager: Arc<RwLock<QuotaManager>>,
    local_peer_id: PeerId,
    connected_peers: HashSet<PeerId>,
    discovered_relays: HashSet<PeerId>,
    reserved_relays: HashSet<PeerId>,
    pending_reservations: HashSet<PeerId>,
    known_relays: HashSet<Multiaddr>,
    known_peer_addrs: HashMap<PeerId, HashSet<Multiaddr>>,
    pending_fetches: HashMap<request_response::OutboundRequestId, usize>,
    pending_pushes: HashMap<request_response::OutboundRequestId, (PeerId, String, String)>,
    peer_shards: HashMap<PeerId, HashMap<String, String>>,
    peer_r_ratios: HashMap<PeerId, f64>,
    fetch_sessions: HashMap<usize, PendingFetchRequest>,
    next_fetch_session_id: usize,
    rendezvous_info: Option<(RendezvousClient, Multiaddr, Option<String>)>,
}

impl P2PEventLoop {
    fn new(
        swarm: Swarm<MyceliumBehaviour>,
        command_rx: mpsc::Receiver<P2PCommand>,
        blockstore: BlockStore,
        quota_manager: Arc<RwLock<QuotaManager>>,
        local_peer_id: PeerId,
    ) -> Self {
        Self {
            swarm,
            command_rx,
            blockstore,
            quota_manager,
            local_peer_id,
            connected_peers: HashSet::new(),
            discovered_relays: HashSet::new(),
            reserved_relays: HashSet::new(),
            pending_reservations: HashSet::new(),
            known_relays: HashSet::new(),
            known_peer_addrs: HashMap::new(),
            pending_fetches: HashMap::new(),
            pending_pushes: HashMap::new(),
            peer_shards: HashMap::new(),
            peer_r_ratios: HashMap::new(),
            fetch_sessions: HashMap::new(),
            next_fetch_session_id: 1,
            rendezvous_info: None,
        }
    }

    async fn run(&mut self) {
        info!("Bắt đầu P2P Event Loop cho node {}", self.local_peer_id);
        let mut rebalance_interval = tokio::time::interval(Duration::from_secs(15));
        rebalance_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut prune_interval = tokio::time::interval(Duration::from_secs(60));
        prune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rebalance_interval.tick() => {
                    self.perform_mesh_rebalance().await;
                }
                _ = prune_interval.tick() => {
                    self.perform_mesh_prune_redundancy().await;
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_command(cmd).await;
                }
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
            }
        }
    }

    async fn perform_mesh_rebalance(&mut self) {
        // 1. Gửi báo cáo trạng thái định kỳ lên các Relay Servers
        let qm_guard = self.quota_manager.read().await;
        let my_hashes = self.blockstore.list_shard_hashes().unwrap_or_default();
        let held_shards: Vec<crate::protocol::ShardItem> = my_hashes
            .iter()
            .map(|h| {
                let file_cid = self.blockstore.get_file_cid_for_shard(h).unwrap_or(None).unwrap_or_else(|| "default_file".to_string());
                crate::protocol::ShardItem {
                    file_cid,
                    shard_hash: h.clone(),
                }
            })
            .collect();

        let report = crate::protocol::NodeStateReport {
            peer_id: self.local_peer_id.to_string(),
            uploaded_bytes: qm_guard.my_uploaded_bytes,
            stored_bytes: qm_guard.stored_shard_bytes,
            r_ratio: qm_guard.current_r_ratio(),
            held_shards,
        };
        drop(qm_guard);

        for &relay in &self.reserved_relays {
            if relay != self.local_peer_id {
                let req = ShardRequest::ReportState(report.clone());
                let _ = self.swarm.behaviour_mut().request_response.send_request(&relay, req);
            }
        }

        let mut storage_peers: Vec<PeerId> = self
            .connected_peers
            .iter()
            .copied()
            .filter(|p| p != &self.local_peer_id && !self.reserved_relays.contains(p) && !self.discovered_relays.contains(p))
            .collect();

        if storage_peers.is_empty() {
            return;
        }

        // 2. Sắp xếp danh sách Peers theo thứ tự R tăng dần (Ưu tiên bơm Shard cho Node đói R < 4.0 trước)
        storage_peers.sort_by(|a, b| {
            let r_a = self.peer_r_ratios.get(a).copied().unwrap_or(0.0);
            let r_b = self.peer_r_ratios.get(b).copied().unwrap_or(0.0);
            r_a.partial_cmp(&r_b).unwrap_or(std::cmp::Ordering::Equal)
        });

        if my_hashes.is_empty() {
            return;
        }

        // 3. Cơ chế Round-Robin Chuẩn mực: Mỗi chu kỳ chỉ gửi ĐÚNG 1 SHARD cho mỗi Peer đang đói (R < 4.5)
        for &peer in &storage_peers {
            let peer_r = self.peer_r_ratios.get(&peer).copied().unwrap_or(0.0);
            if peer_r >= 4.5 {
                // Peer này đã no nê đạt trần cân bằng (R >= 4.5), không cần bơm thêm
                continue;
            }

            let in_flight = self.pending_pushes.values().filter(|(p, _, _)| p == &peer).count();
            if in_flight >= 1 {
                // Đang có 1 shard in-flight đang truyền, đợi truyền xong mới gửi shard tiếp theo
                continue;
            }

            // Tìm đúng 1 Shard tiếp theo mà peer này chưa lưu trữ
            for hash in &my_hashes {
                let already_has = self.peer_shards.get(&peer).map(|s| s.contains_key(hash)).unwrap_or(false);
                let already_in_flight = self.pending_pushes.values().any(|(p, h, _)| p == &peer && h == hash);
                if !already_has && !already_in_flight {
                    if let Ok(Some(data)) = self.blockstore.get_shard(hash) {
                        let shard_kb = data.len() as f64 / 1024.0;
                        let file_cid = self.blockstore.get_file_cid_for_shard(hash).unwrap_or(None);
                        let current_file_cid = file_cid.unwrap_or_else(|| "default_file".to_string());
                        let req = ShardRequest::Push(crate::protocol::PushShard {
                            hash: hash.clone(),
                            file_cid: Some(current_file_cid.clone()),
                            data,
                        });
                        let held_count = self.peer_shards.get(&peer)
                            .map(|s| s.values().filter(|cid| *cid == &current_file_cid).count())
                            .unwrap_or(0);
                        let short_cid = if current_file_cid.len() > 12 { &current_file_cid[..12] } else { &current_file_cid };
                        info!("📤 [Mesh Diffusion] Đang gửi 1 shard {}... ({:.2} KB) sang peer {} (R={:.2}, hiện giữ {} shards của file {})", &hash[..12], shard_kb, peer, peer_r, held_count, short_cid);
                        let req_id = self.swarm.behaviour_mut().request_response.send_request(&peer, req);
                        self.pending_pushes.insert(req_id, (peer, hash.clone(), current_file_cid));
                        break; // Mỗi chu kỳ chỉ gửi đúng 1 shard cho peer này
                    }
                }
            }
        }
    }

    /// Kiểm tra độ dôi dư Shard trên toàn mạng qua DHT. Nếu > 50 shards online, thu hồi định lượng T = Count - 50 từ các node no nê.
    async fn perform_mesh_prune_redundancy(&mut self) {
        let storage_peers: Vec<PeerId> = self
            .connected_peers
            .iter()
            .copied()
            .filter(|p| !self.reserved_relays.contains(p) && !self.discovered_relays.contains(p))
            .collect();

        if storage_peers.is_empty() {
            return;
        }

        // Lọc các peers có R cao (no nê: R > 4.5), sắp xếp theo R giảm dần (node no nhất xếp đầu)
        let mut full_peers: Vec<PeerId> = storage_peers
            .into_iter()
            .filter(|p| self.peer_r_ratios.get(p).copied().unwrap_or(0.0) > 4.5)
            .collect();

        full_peers.sort_by(|a, b| {
            let r_a = self.peer_r_ratios.get(a).copied().unwrap_or(0.0);
            let r_b = self.peer_r_ratios.get(b).copied().unwrap_or(0.0);
            r_b.partial_cmp(&r_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        if full_peers.is_empty() {
            return;
        }

        let shard_hashes = match self.blockstore.list_shard_hashes() {
            Ok(hashes) if !hashes.is_empty() => hashes,
            _ => return,
        };

        for hash in shard_hashes {
            let key = RecordKey::new(&hash.as_bytes());
            let _ = self.swarm.behaviour_mut().kademlia.get_providers(key);
        }
    }

    async fn handle_command(&mut self, cmd: P2PCommand) {
        match cmd {
            P2PCommand::RegisterRendezvous {
                rendezvous,
                public_multiaddr,
                region,
                respond_to,
            } => {
                self.rendezvous_info = Some((rendezvous, public_multiaddr, region));
                let _ = respond_to.send(Ok(()));
            }
            P2PCommand::ListenOn { addr, respond_to } => {
                match self.swarm.listen_on(addr) {
                    Ok(_) => {
                        let _ = respond_to.send(Ok(Multiaddr::empty()));
                    }
                    Err(e) => {
                        let _ = respond_to.send(Err(NetworkError::TransportError(e.to_string())));
                    }
                }
            }
            P2PCommand::Dial { addr, respond_to } => {
                match self.swarm.dial(addr) {
                    Ok(_) => {
                        let _ = respond_to.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = respond_to.send(Err(NetworkError::TransportError(e.to_string())));
                    }
                }
            }
            P2PCommand::BootstrapPeers { peers, respond_to } => {
                let mut added = 0;
                info!("📡 Bootstrap: Đang xử lý {} địa chỉ từ Rendezvous...", peers.len());

                for addr in peers {
                    if !is_dialable_multiaddr(&addr) {
                        continue;
                    }

                    if let Some(peer_id) = extract_peer_id(&addr) {
                        if peer_id == self.local_peer_id {
                            continue;
                        }

                        let addr_str = addr.to_string();
                        let is_circuit = addr_str.contains("/p2p-circuit/") || addr_str.contains("/p2p-circuit");

                        if !is_circuit {
                            // Địa chỉ công khai trực tiếp từ Rendezvous -> Đây là Relay Server
                            self.discovered_relays.insert(peer_id);
                            let relay_with_id = ensure_relay_peer_id(addr.clone(), &peer_id);
                            self.known_relays.insert(relay_with_id.clone());
                            info!("🔎 [Relay Discovery] Phát hiện Relay Server: {} ({})", peer_id, addr);

                            self.known_peer_addrs
                                .entry(peer_id)
                                .or_default()
                                .insert(addr.clone());

                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, addr.clone());

                            // Chủ động dial trực tiếp tới Relay Server nếu chưa kết nối
                            if !self.connected_peers.contains(&peer_id) {
                                info!("🔄 [Relay Connection] Đang quay số kết nối tới Relay Server: {} ({})", peer_id, addr);
                                let _ = self.swarm.dial(addr.clone());
                            }
                        } else {
                            // Địa chỉ Circuit định tuyến qua Relay -> Đây là Storage Peer
                            info!("🔎 [Storage Peer] Phát hiện Storage Peer trong mạng qua Circuit: {} ({})", peer_id, addr);
                            self.known_peer_addrs
                                .entry(peer_id)
                                .or_default()
                                .insert(addr.clone());

                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, addr.clone());

                            if !self.connected_peers.contains(&peer_id) {
                                info!("⚡ [Storage Peer] Nối cầu Circuit tới Storage Peer: {} ({})", peer_id, addr);
                                let _ = self.swarm.dial(addr.clone());
                            }
                        }

                        added += 1;
                    }
                }

                let _ = self.swarm.behaviour_mut().kademlia.bootstrap();
                let _ = respond_to.send(Ok(added));
            }
            P2PCommand::DistributeShards { shards, file_cid, respond_to } => {
                // 1. Luôn lưu toàn bộ shards vào BlockStore cục bộ của uploader kèm File_CID và công bố lên DHT
                let f_cid = file_cid.as_deref().unwrap_or("default_file");
                for shard in &shards {
                    let _ = self.blockstore.put_shard_with_file(&shard.hash, f_cid, &shard.data);
                    let key = RecordKey::new(&shard.hash.as_bytes());
                    let _ = self.swarm.behaviour_mut().kademlia.start_providing(key);
                }

                // 2. Phân tán ngay 1 shard cho mỗi peer kết nối (load balancing, không dồn cục làm nghẽn Relay)
                let storage_peers: Vec<PeerId> = self
                    .connected_peers
                    .iter()
                    .cloned()
                    .filter(|p| p != &self.local_peer_id && !self.reserved_relays.contains(p) && !self.discovered_relays.contains(p))
                    .collect();

                let target_peers = if storage_peers.is_empty() {
                    self.connected_peers.iter().cloned().filter(|p| p != &self.local_peer_id).collect()
                } else {
                    storage_peers
                };

                if !target_peers.is_empty() && !shards.is_empty() {
                    for (i, target_peer) in target_peers.iter().enumerate() {
                        let shard = &shards[i % shards.len()];
                        let req = ShardRequest::Push(PushShard {
                            hash: shard.hash.clone(),
                            file_cid: Some(f_cid.to_string()),
                            data: shard.data.clone(),
                        });
                        let req_id = self.swarm.behaviour_mut().request_response.send_request(target_peer, req);
                        self.pending_pushes.insert(req_id, (*target_peer, shard.hash.clone(), f_cid.to_string()));
                    }
                }

                let _ = respond_to.send(Ok(()));
            }
            P2PCommand::FetchShardsParallel {
                hashes,
                target_count,
                respond_to,
            } => {
                let mut collected_shards = Vec::new();
                let mut remaining_hashes = Vec::new();
                let mut hashes_map = HashMap::new();

                for (idx, hash) in hashes.iter().enumerate() {
                    hashes_map.insert(hash.clone(), idx);
                    if let Ok(Some(data)) = self.blockstore.get_shard(hash) {
                        collected_shards.push(Shard {
                            index: idx,
                            data,
                            hash: hash.clone(),
                        });
                    } else {
                        remaining_hashes.push(hash.clone());
                    }
                }

                if collected_shards.len() >= target_count {
                    let _ = respond_to.send(Ok(collected_shards));
                    return;
                }

                // Thử re-dial các known peers chưa kết nối (chỉ qua Relay Circuit cho Storage Peer)
                for (peer_id, addrs) in &self.known_peer_addrs {
                    if !self.connected_peers.contains(peer_id) {
                        let is_relay = self.discovered_relays.contains(peer_id);
                        for addr in addrs {
                            let s = addr.to_string();
                            if (is_relay || s.contains("/p2p-circuit/")) && is_dialable_multiaddr(addr) {
                                info!("🔄 Đang thử kết nối lại tới peer {} qua Relay ({})", peer_id, addr);
                                let _ = self.swarm.dial(addr.clone());
                            }
                        }
                    }
                }

                // Lọc bỏ các relay thuần túy, chỉ gửi yêu cầu kéo Shard tới các Storage Peer
                let storage_peers: Vec<PeerId> = self
                    .connected_peers
                    .iter()
                    .cloned()
                    .filter(|p| !self.reserved_relays.contains(p) && !self.discovered_relays.contains(p))
                    .collect();

                if storage_peers.is_empty() {
                    warn!("⚠️ Chưa có Storage Peer nào kết nối (chỉ có Relay). Đang chờ kết nối...");
                    let _ = respond_to.send(Err(NetworkError::NoPeersAvailable));
                    return;
                }

                let session_id = self.next_fetch_session_id;
                self.next_fetch_session_id += 1;

                let mut collected_indices = HashSet::new();
                for s in &collected_shards {
                    collected_indices.insert(s.index);
                }

                self.fetch_sessions.insert(
                    session_id,
                    PendingFetchRequest {
                        target_count,
                        collected_shards,
                        collected_indices,
                        hashes_map,
                        respond_to,
                    },
                );

                for hash in remaining_hashes {
                    for peer in &storage_peers {
                        let req = ShardRequest::Pull(PullShard { hash: hash.clone() });
                        let req_id = self
                            .swarm
                            .behaviour_mut()
                            .request_response
                            .send_request(peer, req);
                        self.pending_fetches.insert(req_id, session_id);
                    }
                }
            }
            P2PCommand::GetConnectedPeers { respond_to } => {
                let peers: Vec<PeerId> = self.connected_peers.iter().cloned().collect();
                let _ = respond_to.send(peers);
            }
            P2PCommand::GetKnownPeers { respond_to } => {
                let peers: Vec<PeerId> = self.known_peer_addrs.keys()
                    .filter(|p| !self.reserved_relays.contains(p) && !self.discovered_relays.contains(p))
                    .cloned()
                    .collect();
                let _ = respond_to.send(peers);
            }
            P2PCommand::GetListeners { respond_to } => {
                let listeners: Vec<Multiaddr> = self.swarm.listeners().cloned().collect();
                let _ = respond_to.send(listeners);
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<MyceliumBehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionEstablished {
                peer_id,
                endpoint,
                num_established,
                ..
            } => {
                if peer_id == self.local_peer_id {
                    return;
                }

                let is_relay = self.discovered_relays.contains(&peer_id) || self.reserved_relays.contains(&peer_id);
                if num_established.get() == 1 {
                    if is_relay {
                        info!("🤝 [Relay Connection] Đã kết nối thành công tới Relay Server: {}", peer_id);
                    } else {
                        info!("🎉 [Storage Peer] Đã thiết lập kết nối 2 chiều thành công với Storage Peer: {}", peer_id);
                    }
                } else {
                    debug!("🔗 Mở thêm luồng kết nối phụ (#{}) tới peer: {}", num_established, peer_id);
                }
                self.connected_peers.insert(peer_id);
                // Chỉ ghi nhận địa chỉ khi WE là bên chủ động Dial (vì đó là cổng lắng nghe thực của peer).
                // Khi là Listener (Inbound), send_back_addr là ephemeral source port ngẫu nhiên của socket OS, không phải cổng lắng nghe.
                if let ConnectedPoint::Dialer { address, .. } = &endpoint {
                    if is_dialable_multiaddr(address) {
                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, address.clone());
                        self.known_peer_addrs
                            .entry(peer_id)
                            .or_default()
                            .insert(address.clone());
                    }
                }
            }
            SwarmEvent::OutgoingConnectionError {
                peer_id, error, ..
            } => {
                if let Some(pid) = peer_id {
                    let is_relay = self.discovered_relays.contains(&pid) || self.reserved_relays.contains(&pid);
                    if is_relay {
                        warn!("⚠️ [Relay Connection] Không thể kết nối tới Relay Server {}: {}", pid, error);
                    } else {
                        warn!("⚠️ [Storage Peer] Không thể kết nối tới Storage Peer {}: {}", pid, error);
                    }
                    self.discovered_relays.remove(&pid);
                    self.reserved_relays.remove(&pid);
                    self.pending_reservations.remove(&pid);
                    self.known_peer_addrs.remove(&pid);
                    self.known_relays.retain(|r| extract_relay_peer_id(r) != Some(pid));
                    self.swarm.behaviour_mut().kademlia.remove_peer(&pid);
                } else {
                    warn!("⚠️ [LỖI KẾT NỐI OUTBOUND] Không thể kết nối tới địa chỉ ngoại vi: {}", error);
                }
            }
            SwarmEvent::IncomingConnectionError {
                send_back_addr,
                error,
                ..
            } => {
                debug!("❌ Lỗi kết nối Inbound từ {}: {}", send_back_addr, error);
            }
            SwarmEvent::ListenerError { error, .. } => {
                warn!("⚠️ [LỖI LISTENER] {}", error);
            }
            SwarmEvent::Dialing { peer_id, .. } => {
                if let Some(pid) = peer_id {
                    let is_relay = self.discovered_relays.contains(&pid) || self.reserved_relays.contains(&pid);
                    if is_relay {
                        info!("📞 [Relay Connection] Đang quay số kết nối tới Relay Server: {}", pid);
                    } else {
                        info!("📞 [Storage Peer] Đang quay số kết nối tới Storage Peer: {}", pid);
                    }
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                let is_relay = self.discovered_relays.contains(&peer_id) || self.reserved_relays.contains(&peer_id);
                if is_relay {
                    info!("👋 [Relay Connection] Đã ngắt kết nối với Relay Server: {}", peer_id);
                } else {
                    info!("👋 [Storage Peer] Đã ngắt kết nối với Storage Peer: {}", peer_id);
                }
                self.connected_peers.remove(&peer_id);
                self.reserved_relays.remove(&peer_id);
                self.pending_reservations.remove(&peer_id);
            }
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::Mdns(mdns_event)) => match mdns_event {
                MdnsEvent::Discovered(list) => {
                    for (peer_id, multiaddr) in list {
                        if peer_id != self.local_peer_id && is_dialable_multiaddr(&multiaddr) {
                            debug!("mDNS phát hiện peer LAN: {} tại {}", peer_id, multiaddr);
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, multiaddr.clone());
                            let _ = self.swarm.dial(multiaddr);
                        }
                    }
                }
                MdnsEvent::Expired(_) => {}
            },
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::RequestResponse(req_event)) => {
                self.handle_req_resp_event(req_event).await;
            }
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::Identify(identify_event)) => {
                if let libp2p::identify::Event::Received { peer_id, info, .. } = identify_event {
                    trace!("Nhận thông tin Identify từ {}: {:?}", peer_id, info.listen_addrs);

                    for addr in &info.listen_addrs {
                        if is_dialable_multiaddr(addr) {
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, addr.clone());
                            self.known_peer_addrs
                                .entry(peer_id)
                                .or_default()
                                .insert(addr.clone());
                        }
                    }

                    // Tự phát hiện Relay duy nhất bằng từ khóa "relay" do node tự công bố
                    let is_relay = info.protocol_version.to_lowercase().contains("relay")
                        || info.agent_version.to_lowercase().contains("relay")
                        || info.protocols.iter().any(|p| p.to_string().to_lowercase().contains("relay"));

                    if is_relay {
                        self.discovered_relays.insert(peer_id);
                        info!("🔎 [Relay Identify] Xác nhận peer {} là Relay Server (protocol={}, agent={})", 
                            peer_id, info.protocol_version, info.agent_version);

                        // Tìm địa chỉ dialable cụ thể của Relay (không chứa /p2p-circuit)
                        let candidate_addr = self.known_peer_addrs.get(&peer_id)
                            .and_then(|addrs| addrs.iter().find(|a| !a.to_string().contains("/p2p-circuit")).cloned())
                            .or_else(|| {
                                info.listen_addrs.iter()
                                    .find(|a| is_dialable_multiaddr(a) && !a.to_string().contains("/p2p-circuit"))
                                    .cloned()
                            });

                        if let Some(base_addr) = candidate_addr {
                            let relay_with_id = ensure_relay_peer_id(base_addr, &peer_id);
                            self.known_relays.insert(relay_with_id.clone());

                            // Chỉ gửi yêu cầu nếu node chưa có bất kỳ Relay Circuit nào đang hoạt động
                            if self.reserved_relays.is_empty() && self.pending_reservations.is_empty() {
                                let circuit_addr = relay_with_id.with(libp2p::multiaddr::Protocol::P2pCircuit);
                                info!("🔗 [Identify] Chọn Relay {} làm trạm kết nối Circuit chính: {}", peer_id, circuit_addr);
                                match self.swarm.listen_on(circuit_addr) {
                                    Ok(_) => {
                                        self.pending_reservations.insert(peer_id);
                                    }
                                    Err(e) => {
                                        warn!("❌ [Identify] Lỗi đăng ký circuit_addr trên Relay {}: {}", peer_id, e);
                                    }
                                }
                            } else if !self.reserved_relays.is_empty() {
                                debug!("ℹ️ Node đã có Relay Circuit hoạt động, bỏ qua không tạo thêm circuit trên Relay {}", peer_id);
                            }
                        } else {
                            warn!("⚠️ Phát hiện Relay {} nhưng chưa có địa chỉ mạng cụ thể để tạo reservation", peer_id);
                        }
                    } else {
                        info!("🔎 [Storage Peer] Xác nhận peer {} là Storage Peer (agent={})", peer_id, info.agent_version);
                    }
                }
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                let addr_str = address.to_string();
                if addr_str.contains("/p2p-circuit/") || addr_str.contains("/p2p-circuit") {
                    println!("🎉 Đã đăng ký thành công Relay Circuit: \x1b[1;32m{}\x1b[0m", address);
                    info!("🎉 Đã đăng ký thành công Relay Circuit: {}", address);

                    if let Some(relay_id) = extract_relay_peer_id(&address) {
                        self.pending_reservations.remove(&relay_id);
                        self.reserved_relays.insert(relay_id);
                    }

                    // KHI ĐÃ CÓ RESERVATION XÁC NHẬN: Nối cầu Circuit tới các Storage Peers!
                    for relay_addr in &self.known_relays {
                        if let Some(relay_id) = extract_peer_id(relay_addr) {
                            let relay_with_id = ensure_relay_peer_id(relay_addr.clone(), &relay_id);
                            for (known_peer, addrs) in &mut self.known_peer_addrs {
                                if *known_peer != self.local_peer_id && *known_peer != relay_id && !self.connected_peers.contains(known_peer) {
                                    let inferred_circuit = relay_with_id
                                        .clone()
                                        .with(libp2p::multiaddr::Protocol::P2pCircuit)
                                        .with(libp2p::multiaddr::Protocol::P2p(*known_peer));

                                    addrs.insert(inferred_circuit.clone());
                                    self.swarm
                                        .behaviour_mut()
                                        .kademlia
                                        .add_address(known_peer, inferred_circuit.clone());

                                    info!("⚡ Nối cầu Circuit tới Storage Peer: {} ({})", known_peer, inferred_circuit);
                                    let _ = self.swarm.dial(inferred_circuit);
                                }
                            }
                        }
                    }
                } else {
                    info!("Node đang lắng nghe tại Multiaddr: {}", address);
                }
            }
            _ => {}
        }
    }

    async fn handle_req_resp_event(
        &mut self,
        event: request_response::Event<ShardRequest, ShardResponse>,
    ) {
        match event {
            request_response::Event::Message {
                peer,
                message:
                    Message::Request {
                        request_id: _,
                        request,
                        channel,
                    },
            } => match request {
                ShardRequest::Push(push) => {
                    let shard_len = push.data.len() as u64;
                    let already_has = self.blockstore.has_shard(&push.hash).unwrap_or(false);
                    if already_has {
                        let resp = ShardResponse::Push(PushResponse {
                            accepted: true,
                            reason: None,
                        });
                        let _ = self
                            .swarm
                            .behaviour_mut()
                            .request_response
                            .send_response(channel, resp);
                        return;
                    }

                    let quota_guard = self.quota_manager.read().await;

                    if quota_guard.can_accept_shard(&self.blockstore, shard_len, true) {
                        drop(quota_guard);

                        let file_cid = push.file_cid.as_deref().unwrap_or("default_file");
                        if let Err(e) = self.blockstore.put_shard_with_file(&push.hash, file_cid, &push.data) {
                            error!("Lỗi khi ghi shard vào blockstore: {}", e);
                            let resp = ShardResponse::Push(PushResponse {
                                accepted: false,
                                reason: Some(format!("Lỗi I/O: {e}")),
                            });
                            let _ = self
                                .swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, resp);
                            return;
                        }

                        // Ghi nhận tăng stored_shard_bytes trong QuotaManager
                        {
                            let mut qm = self.quota_manager.write().await;
                            qm.record_stored_shard(shard_len);
                            if let Some(home) = dirs::home_dir() {
                                let quota_file = home.join(".p2pdrive").join("quota.json");
                                let _ = qm.save_to_file(&quota_file);
                            }
                        }

                        let key = RecordKey::new(&push.hash.as_bytes());
                        let _ = self.swarm.behaviour_mut().kademlia.start_providing(key);

                        let f_cid = push.file_cid.as_deref().unwrap_or("default_file");
                        let local_file_shards = self.blockstore.count_shards_for_file(f_cid).unwrap_or(0);
                        let short_cid = if f_cid.len() > 12 { &f_cid[..12] } else { f_cid };
                        info!("📥 [Mesh Cache] Đã nhận và lưu trữ thành công shard cache {} ({:.2} KB) từ peer {} (Hiện giữ: {} shards của file {})", &push.hash[..12], shard_len as f64 / 1024.0, peer, local_file_shards, short_cid);
                        let resp = ShardResponse::Push(PushResponse {
                            accepted: true,
                            reason: None,
                        });
                        let _ = self
                            .swarm
                            .behaviour_mut()
                            .request_response
                            .send_response(channel, resp);
                    } else {
                        warn!("⚠️ Từ chối lưu shard {} ({:.2} KB) từ peer {} vì vượt quá hạn mức ổ cứng hoặc trần cache (R > 5.0)", push.hash, shard_len as f64 / 1024.0, peer);
                        let resp = ShardResponse::Push(PushResponse {
                            accepted: false,
                            reason: Some("Vượt quá hạn mức ổ cứng phân bổ hoặc trần cache".to_string()),
                        });
                        let _ = self
                            .swarm
                            .behaviour_mut()
                            .request_response
                            .send_response(channel, resp);
                    }
                }
                ShardRequest::Pull(pull) => {
                    let data = self.blockstore.get_shard(&pull.hash).unwrap_or(None);
                    debug!(
                        "Phản hồi PullShard {} cho peer {}: tìm thấy = {}",
                        pull.hash,
                        peer,
                        data.is_some()
                    );
                    let resp = ShardResponse::Pull(PullResponse { data });
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, resp);
                }
                ShardRequest::Prune(prune) => {
                    if let Ok(Some(data)) = self.blockstore.get_shard(&prune.hash) {
                        let shard_len = data.len() as u64;
                        let mut quota_guard = self.quota_manager.write().await;

                        if quota_guard.my_uploaded_bytes > 0 {
                            let next_stored = quota_guard.stored_shard_bytes.saturating_sub(shard_len);
                            let can_prune = if quota_guard.my_uploaded_bytes == 0 {
                                true
                            } else {
                                (next_stored as f64 / quota_guard.my_uploaded_bytes as f64) >= 4.0
                            };

                            if can_prune {
                                let _ = self.blockstore.delete_shard(&prune.hash);
                                quota_guard.record_pruned_shard(shard_len);
                                info!("Đã thu hồi shard thừa {} theo yêu cầu mạng", prune.hash);
                                let resp = ShardResponse::Prune(crate::protocol::PruneResponse {
                                    pruned: true,
                                    reason: None,
                                });
                                let _ = self
                                    .swarm
                                    .behaviour_mut()
                                    .request_response
                                    .send_response(channel, resp);
                                return;
                            } else {
                                debug!("Từ chối xóa shard {} vì sẽ làm R < 4.0", prune.hash);
                                let resp = ShardResponse::Prune(crate::protocol::PruneResponse {
                                    pruned: false,
                                    reason: Some("Không thể xóa: R sẽ tụt dưới 4.0".to_string()),
                                });
                                let _ = self
                                    .swarm
                                    .behaviour_mut()
                                    .request_response
                                    .send_response(channel, resp);
                                return;
                            }
                        }
                    }

                    let resp = ShardResponse::Prune(crate::protocol::PruneResponse {
                        pruned: false,
                        reason: Some("Không tìm thấy shard trên node".to_string()),
                    });
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, resp);
                }
                ShardRequest::ReportState(_) => {
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, ShardResponse::Ack);
                }
                ShardRequest::SyncSwarmState(broadcast) => {
                    // Cập nhật trạng thái toàn mạng từ Relay
                    for node in &broadcast.nodes {
                        if let Ok(pid) = node.peer_id.parse::<PeerId>() {
                            if pid != self.local_peer_id {
                                if let Some(r) = node.r_ratio {
                                    self.peer_r_ratios.insert(pid, r);
                                }
                                let mut shard_map: HashMap<String, String> = HashMap::new();
                                for s in &node.held_shards {
                                    shard_map.insert(s.shard_hash.clone(), s.file_cid.clone());
                                }
                                self.peer_shards.insert(pid, shard_map);
                            }
                        }
                    }
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, ShardResponse::Ack);

                    // Kiểm tra và thu hồi Shard thừa định lượng theo File (> 50 shards của file trên toàn mạng)
                    if let Ok(my_hashes) = self.blockstore.list_shard_hashes() {
                        let mut quota_guard = self.quota_manager.write().await;
                        for hash in my_hashes {
                            let file_cid = self.blockstore.get_file_cid_for_shard(&hash).unwrap_or(None).unwrap_or_else(|| "default_file".to_string());
                            let total_file_shards: usize = broadcast
                                .nodes
                                .iter()
                                .map(|n| n.held_shards.iter().filter(|s| s.file_cid == file_cid).count())
                                .sum();

                            if total_file_shards > 50 {
                                let redundant_count = total_file_shards - 50;
                                let mut nodes_with_file: Vec<&crate::protocol::NodeStateReport> = broadcast
                                    .nodes
                                    .iter()
                                    .filter(|n| n.held_shards.iter().any(|s| s.file_cid == file_cid))
                                    .collect();

                                nodes_with_file.sort_by(|a, b| {
                                    let r_a = a.r_ratio.unwrap_or(0.0);
                                    let r_b = b.r_ratio.unwrap_or(0.0);
                                    r_b.partial_cmp(&r_a).unwrap_or(std::cmp::Ordering::Equal)
                                });

                                let top_full_nodes: Vec<&crate::protocol::NodeStateReport> = nodes_with_file
                                    .into_iter()
                                    .take(redundant_count)
                                    .filter(|n| n.r_ratio.unwrap_or(0.0) > 4.5)
                                    .collect();

                                let am_in_top = top_full_nodes.iter().any(|n| n.peer_id == self.local_peer_id.to_string());
                                if am_in_top {
                                    if let Ok(Some(data)) = self.blockstore.get_shard(&hash) {
                                        let shard_len = data.len() as u64;
                                        if quota_guard.my_uploaded_bytes > 0 {
                                            let next_stored = quota_guard.stored_shard_bytes.saturating_sub(shard_len);
                                            let next_r = next_stored as f64 / quota_guard.my_uploaded_bytes as f64;
                                            if next_r >= 4.0 {
                                                let _ = self.blockstore.delete_shard(&hash);
                                                quota_guard.record_pruned_shard(shard_len);
                                                info!("🗑️ [File Prune] Đã tự động thu hồi shard thừa {} của file {} (toàn mạng có {} shards của file này, R_mới={:.3})", &hash[..12], &file_cid[..file_cid.len().min(12)], total_file_shards, next_r);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                ShardRequest::QueryStats(_) => {
                    let qm = self.quota_manager.read().await;
                    let resp = ShardResponse::Stats(crate::protocol::PeerStatsResponse {
                        peer_id: self.local_peer_id.to_string(),
                        my_uploaded_bytes: qm.my_uploaded_bytes,
                        stored_shard_bytes: qm.stored_shard_bytes,
                        r_ratio: qm.current_r_ratio(),
                    });
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, resp);
                }
            },
            request_response::Event::Message {
                peer,
                message:
                    Message::Response {
                        request_id,
                        response,
                    },
            } => {
                if let ShardResponse::Stats(ref stats) = response {
                    let r = stats.r_ratio.unwrap_or(0.0);
                    self.peer_r_ratios.insert(peer, r);
                    info!("📊 [Peer Stats] Peer {} công bố R = {:.3} (Upload: {:.2} MB, Stored: {:.2} MB)", peer, r, stats.my_uploaded_bytes as f64 / 1048576.0, stats.stored_shard_bytes as f64 / 1048576.0);
                }

                if let ShardResponse::Push(PushResponse { accepted, reason }) = &response {
                    if let Some((target_peer, hash, file_cid)) = self.pending_pushes.remove(&request_id) {
                        if *accepted {
                            self.peer_shards.entry(target_peer).or_default().insert(hash, file_cid.clone());
                            let count = self.peer_shards.get(&target_peer)
                                .map(|s| s.values().filter(|cid| *cid == &file_cid).count())
                                .unwrap_or(0);
                            let short_cid = if file_cid.len() > 12 { &file_cid[..12] } else { &file_cid };
                            info!("✅ [Mesh Diffusion] Peer {} đã chấp nhận và lưu trữ shard cache! (Hiện giữ: {} shards của file {})", target_peer, count, short_cid);
                        } else {
                            // Ghi nhận shard này đã xử lý với peer để không gửi lặp lại
                            self.peer_shards.entry(target_peer).or_default().insert(hash, file_cid);
                            if let Some(r_str) = reason {
                                if r_str.contains("trần cache") || r_str.contains("R > 5.0") || r_str.contains("hạn mức") {
                                    self.peer_r_ratios.insert(target_peer, 5.0);
                                }
                                warn!("⚠️ [Mesh Diffusion] Peer {} từ chối nhận shard cache: {}", target_peer, r_str);
                            }
                        }
                    }
                }

                if let Some(session_id) = self.pending_fetches.remove(&request_id) {
                    if let ShardResponse::Pull(PullResponse { data: Some(data) }) = response {
                        if let Some(session) = self.fetch_sessions.get_mut(&session_id) {
                            let hash = erasure_codec::sha256_hex(&data);
                            if let Some(&idx) = session.hashes_map.get(&hash) {
                                if !session.collected_indices.contains(&idx) {
                                    session.collected_indices.insert(idx);
                                    session.collected_shards.push(Shard {
                                        index: idx,
                                        data,
                                        hash,
                                    });

                                    if session.collected_shards.len() >= session.target_count {
                                        if let Some(finished_session) =
                                             self.fetch_sessions.remove(&session_id)
                                        {
                                            let _ = finished_session
                                                .respond_to
                                                .send(Ok(finished_session.collected_shards));
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Peer không có shard này
                        let has_remaining = self.pending_fetches.values().any(|&sid| sid == session_id);
                        if !has_remaining {
                            if let Some(session) = self.fetch_sessions.remove(&session_id) {
                                if session.collected_shards.len() >= session.target_count {
                                    let _ = session.respond_to.send(Ok(session.collected_shards));
                                } else {
                                    let _ = session.respond_to.send(Err(NetworkError::InsufficientShardsFound {
                                        target: session.target_count,
                                        collected: session.collected_shards.len(),
                                    }));
                                }
                            }
                        }
                    }
                }
            }
            request_response::Event::OutboundFailure { peer, request_id, error, .. } => {
                warn!("⚠️ [Mesh Diffusion] Lỗi gửi yêu cầu Shard sang peer {} (request_id={}): {:?}", peer, request_id, error);
                self.pending_pushes.remove(&request_id);
                if let Some(session_id) = self.pending_fetches.remove(&request_id) {
                    let has_remaining = self.pending_fetches.values().any(|&sid| sid == session_id);
                    if !has_remaining {
                        if let Some(session) = self.fetch_sessions.remove(&session_id) {
                            if session.collected_shards.len() >= session.target_count {
                                let _ = session.respond_to.send(Ok(session.collected_shards));
                            } else {
                                let _ = session.respond_to.send(Err(NetworkError::InsufficientShardsFound {
                                    target: session.target_count,
                                    collected: session.collected_shards.len(),
                                }));
                            }
                        }
                    }
                }
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                warn!("⚠️ [Mesh Cache] Lỗi nhận luồng Shard từ peer {}: {:?}", peer, error);
            }
            _ => {}
        }
    }
}

/// Kiểm tra xem địa chỉ IPv4 có phải là Public IP hợp lệ hay không (Whitelist).
pub fn is_public_ipv4(ip: &std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    // 0.0.0.0/8 (Unspecified / Default route)
    if octets[0] == 0 { return false; }
    // 10.0.0.0/8 (Private LAN)
    if octets[0] == 10 { return false; }
    // 127.0.0.0/8 (Loopback localhost)
    if octets[0] == 127 { return false; }
    // 169.254.0.0/16 (Link-local APIPA)
    if octets[0] == 169 && octets[1] == 254 { return false; }
    // 172.16.0.0/12 (Private LAN & Docker bridges 172.17.x, 172.18.x, etc.)
    if octets[0] == 172 && (16..=31).contains(&octets[1]) { return false; }
    // 192.168.0.0/16 (Private LAN Router)
    if octets[0] == 192 && octets[1] == 168 { return false; }
    // 100.64.0.0/10 (Carrier-Grade NAT / Shared Address Space)
    if octets[0] == 100 && (64..=127).contains(&octets[1]) { return false; }
    // 198.18.0.0/15 (Benchmarking)
    if octets[0] == 198 && (18..=19).contains(&octets[1]) { return false; }
    // >= 224.0.0.0 (Multicast, Reserved, Broadcast)
    if octets[0] >= 224 { return false; }
    
    true
}

/// Kiểm tra xem địa chỉ IPv6 có phải là Public Global Unicast IP hợp lệ hay không (Whitelist).
pub fn is_public_ipv6(ip: &std::net::Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() { return false; }
    let segs = ip.segments();
    // fe80::/10 (Link-local)
    if (segs[0] & 0xffc0) == 0xfe80 { return false; }
    // fc00::/7 (Unique Local Address - Private LAN)
    if (segs[0] & 0xfe00) == 0xfc00 { return false; }
    // ff00::/8 (Multicast)
    if (segs[0] & 0xff00) == 0xff00 { return false; }
    
    true
}

/// WHITELIST: Chỉ chấp nhận các địa chỉ công khai hợp lệ để broadcast/đăng ký sổ địa chỉ và quay số:
/// 1. Địa chỉ Relay Circuit (`/p2p-circuit`)
/// 2. Tên miền Public DNS (`/dns/`, `/dns4/`, `/dns6/`)
/// 3. Địa chỉ Public IPv4 (đã qua kiểm tra Whitelist `is_public_ipv4`)
/// 4. Địa chỉ Public IPv6 (đã qua kiểm tra Whitelist `is_public_ipv6`)
pub fn is_dialable_multiaddr(addr: &Multiaddr) -> bool {
    let mut has_valid_public_transport = false;

    for proto in addr.iter() {
        match proto {
            libp2p::multiaddr::Protocol::P2pCircuit => {
                has_valid_public_transport = true;
            }
            libp2p::multiaddr::Protocol::Dns(_)
            | libp2p::multiaddr::Protocol::Dns4(_)
            | libp2p::multiaddr::Protocol::Dns6(_) => {
                has_valid_public_transport = true;
            }
            libp2p::multiaddr::Protocol::Ip4(ip) => {
                if is_public_ipv4(&ip) {
                    has_valid_public_transport = true;
                } else {
                    return false; // Chứa IPv4 private/loopback/docker -> Loại bỏ hoàn toàn khỏi Whitelist
                }
            }
            libp2p::multiaddr::Protocol::Ip6(ip) => {
                if is_public_ipv6(&ip) {
                    has_valid_public_transport = true;
                } else {
                    return false; // Chứa IPv6 private/link-local -> Loại bỏ hoàn toàn khỏi Whitelist
                }
            }
            _ => {}
        }
    }

    has_valid_public_transport
}

fn extract_relay_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    let mut before_circuit = None;
    for protocol in addr.iter() {
        match protocol {
            libp2p::multiaddr::Protocol::P2p(peer_id) => {
                if before_circuit.is_none() {
                    before_circuit = Some(peer_id);
                }
            }
            libp2p::multiaddr::Protocol::P2pCircuit => {
                return before_circuit;
            }
            _ => {}
        }
    }
    before_circuit
}

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    let mut last = None;
    for protocol in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(peer_id) = protocol {
            last = Some(peer_id);
        }
    }
    last
}

fn ensure_relay_peer_id(mut addr: Multiaddr, relay_id: &PeerId) -> Multiaddr {
    let s = addr.to_string();
    if !s.contains(&relay_id.to_string()) {
        addr.push(libp2p::multiaddr::Protocol::P2p(*relay_id));
    }
    addr
}
