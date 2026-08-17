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
    GetListeners {
        respond_to: oneshot::Sender<Vec<Multiaddr>>,
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

        let swarm = Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            libp2p::swarm::Config::with_tokio_executor(),
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
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::DistributeShards {
                shards,
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
    reserved_relays: HashSet<PeerId>,
    known_peer_addrs: HashMap<PeerId, HashSet<Multiaddr>>,
    pending_fetches: HashMap<request_response::OutboundRequestId, usize>,
    fetch_sessions: HashMap<usize, PendingFetchRequest>,
    next_fetch_session_id: usize,
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
            reserved_relays: HashSet::new(),
            known_peer_addrs: HashMap::new(),
            pending_fetches: HashMap::new(),
            fetch_sessions: HashMap::new(),
            next_fetch_session_id: 1,
        }
    }

    async fn run(&mut self) {
        info!("Bắt đầu P2P Event Loop cho node {}", self.local_peer_id);

        loop {
            tokio::select! {
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_command(cmd).await;
                }
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: P2PCommand) {
        match cmd {
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

                for addr in peers {
                    if !is_dialable_multiaddr(&addr) {
                        continue;
                    }

                    if let Some(peer_id) = extract_peer_id(&addr) {
                        if peer_id == self.local_peer_id {
                            continue;
                        }

                        self.known_peer_addrs
                            .entry(peer_id)
                            .or_default()
                            .insert(addr.clone());

                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, addr.clone());

                        // Nếu chưa kết nối tới peer này, chủ động dial
                        if !self.connected_peers.contains(&peer_id) {
                            debug!("Chủ động kết nối tới peer: {} ({})", peer_id, addr);
                            let _ = self.swarm.dial(addr.clone());
                        }

                        // Nếu đây là địa chỉ Relay thuần túy (không phải circuit), tự động đăng ký Reservation
                        let addr_str = addr.to_string();
                        let is_relay_server = (addr_str.contains("ngrok")
                            || addr_str.contains("relay")
                            || addr_str.contains("/tcp/4002"))
                            && !addr_str.contains("/p2p-circuit/");

                        if is_relay_server && !self.reserved_relays.contains(&peer_id) {
                            let circuit_addr = addr.with(libp2p::multiaddr::Protocol::P2pCircuit);
                            debug!("Đang đăng ký Relay Circuit Reservation tại: {}", circuit_addr);
                            if let Ok(_) = self.swarm.listen_on(circuit_addr) {
                                self.reserved_relays.insert(peer_id);
                            }
                        }

                        added += 1;
                    }
                }

                let _ = self.swarm.behaviour_mut().kademlia.bootstrap();
                let _ = respond_to.send(Ok(added));
            }
            P2PCommand::DistributeShards { shards, respond_to } => {
                let connected: Vec<PeerId> = self.connected_peers.iter().cloned().collect();
                if connected.is_empty() {
                    for shard in &shards {
                        let _ = self.blockstore.put_shard(&shard.hash, &shard.data);
                        let key = RecordKey::new(&shard.hash.as_bytes());
                        let _ = self.swarm.behaviour_mut().kademlia.start_providing(key);
                    }
                    let _ = respond_to.send(Ok(()));
                    return;
                }

                let total_peers = connected.len();
                for (i, shard) in shards.into_iter().enumerate() {
                    let target_peer = connected[i % total_peers];
                    let req = ShardRequest::Push(PushShard {
                        hash: shard.hash.clone(),
                        data: shard.data,
                    });
                    self.swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&target_peer, req);
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

                // Thử re-dial các known peers chưa kết nối (cả direct và qua Relay Circuit)
                for (peer_id, addrs) in &self.known_peer_addrs {
                    if !self.connected_peers.contains(peer_id) {
                        for addr in addrs {
                            if is_dialable_multiaddr(addr) {
                                debug!("Đang thử kết nối lại tới known peer {} ({})", peer_id, addr);
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
                    .filter(|p| !self.reserved_relays.contains(p))
                    .collect();

                let connected_targets = if storage_peers.is_empty() {
                    self.connected_peers.iter().cloned().collect::<Vec<_>>()
                } else {
                    storage_peers
                };

                if connected_targets.is_empty() {
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
                    for peer in &connected_targets {
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
            P2PCommand::GetListeners { respond_to } => {
                let listeners: Vec<Multiaddr> = self.swarm.listeners().cloned().collect();
                let _ = respond_to.send(listeners);
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<MyceliumBehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                info!("🎉 Đã thiết lập kết nối 2 chiều thành công với peer: {}", peer_id);
                self.connected_peers.insert(peer_id);
                if let ConnectedPoint::Dialer { address, .. } = endpoint {
                    if is_dialable_multiaddr(&address) {
                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, address.clone());
                        self.known_peer_addrs
                            .entry(peer_id)
                            .or_default()
                            .insert(address.clone());
                    }

                    // Khi đã kết nối thành công tới peer, nếu peer này có thể là Relay và chưa tạo reservation:
                    if !self.reserved_relays.contains(&peer_id) {
                        let addr_str = address.to_string();
                        let is_potential_relay = addr_str.contains("ngrok")
                            || addr_str.contains("relay")
                            || addr_str.contains("/dns")
                            || addr_str.contains("/tcp/4002");

                        if is_potential_relay {
                            let circuit_addr = address.with(libp2p::multiaddr::Protocol::P2pCircuit);
                            debug!("Đang đăng ký Relay Circuit Reservation tại: {}", circuit_addr);
                            match self.swarm.listen_on(circuit_addr) {
                                Ok(_) => {
                                    self.reserved_relays.insert(peer_id);
                                }
                                Err(e) => {
                                    debug!("Không thể đăng ký Circuit Reservation trên {}: {}", peer_id, e);
                                }
                            }
                        }
                    }
                }
            }
            SwarmEvent::OutgoingConnectionError {
                peer_id, error, ..
            } => {
                debug!("❌ Lỗi kết nối Outbound tới peer {:?}: {}", peer_id, error);
            }
            SwarmEvent::IncomingConnectionError {
                send_back_addr,
                error,
                ..
            } => {
                debug!("❌ Lỗi kết nối Inbound từ {}: {}", send_back_addr, error);
            }
            SwarmEvent::Dialing { peer_id, .. } => {
                debug!("📞 Đang thực hiện dial tới peer: {:?}", peer_id);
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                debug!("Ngắt kết nối với peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
                self.reserved_relays.remove(&peer_id);
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
                    for addr in info.listen_addrs {
                        if is_dialable_multiaddr(&addr) {
                            let s = addr.to_string();
                            // Không nạp loopback từ identify của remote peer
                            if s.contains("/ip4/127.0.0.1/") || s.contains("/ip6/::1/") {
                                continue;
                            }
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, addr.clone());
                            self.known_peer_addrs
                                .entry(peer_id)
                                .or_default()
                                .insert(addr);
                        }
                    }
                }
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                let addr_str = address.to_string();
                if addr_str.contains("/p2p-circuit/") || addr_str.contains("/p2p-circuit") {
                    info!("🎉 Đã đăng ký thành công Relay Circuit: {}", address);
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
                    let quota_guard = self.quota_manager.read().await;

                    if quota_guard.can_store_incoming_shard(&self.blockstore, shard_len) {
                        drop(quota_guard);

                        if let Err(e) = self.blockstore.put_shard(&push.hash, &push.data) {
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

                        let key = RecordKey::new(&push.hash.as_bytes());
                        let _ = self.swarm.behaviour_mut().kademlia.start_providing(key);

                        debug!("Đã nhận và lưu trữ shard {} từ peer {}", push.hash, peer);
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
                        warn!("Từ chối lưu shard {} vì vượt quá hạn mức ổ cứng", push.hash);
                        let resp = ShardResponse::Push(PushResponse {
                            accepted: false,
                            reason: Some("Vượt quá hạn mức ổ cứng phân bổ".to_string()),
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
            },
            request_response::Event::Message {
                peer: _,
                message:
                    Message::Response {
                        request_id,
                        response,
                    },
            } => {
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
            request_response::Event::OutboundFailure { request_id, error, .. } => {
                debug!("Lỗi gửi yêu cầu Shard (request_id={}): {}", request_id, error);
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
            _ => {}
        }
    }
}

fn is_dialable_multiaddr(addr: &Multiaddr) -> bool {
    let s = addr.to_string();
    if s.contains("/ip6/fe80:") {
        return false;
    }
    if s.contains("/ip4/0.0.0.0/") || s.contains("/ip6/::/") {
        return false;
    }
    true
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
