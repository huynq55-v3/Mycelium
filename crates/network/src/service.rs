use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
        // Chuyển đổi Ed25519 dalek key thành libp2p keypair
        let secret_bytes = identity.secret_key_bytes();
        let libp2p_secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(secret_bytes)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi chuyển đổi SecretKey: {e}")))?;
        let keypair = Keypair::from(libp2p::identity::ed25519::Keypair::from(libp2p_secret));
        let local_peer_id = keypair.public().to_peer_id();

        // Xây dựng Transport và Swarm Behaviour
        let transport = build_transport(&keypair, swarm_key)?;
        let behaviour = MyceliumBehaviour::new(&keypair)
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

        // Chạy Background Event Loop trên Tokio task
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

    /// Lấy PeerId của node cục bộ.
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// Lấy tham chiếu `BlockStore` cục bộ.
    pub fn blockstore(&self) -> &BlockStore {
        &self.blockstore
    }

    /// Lấy tham chiếu `QuotaManager`.
    pub fn quota_manager(&self) -> &Arc<RwLock<QuotaManager>> {
        &self.quota_manager
    }

    /// Lắng nghe trên một Multiaddr chỉ định (ví dụ `/ip4/0.0.0.0/tcp/4001`).
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

    /// Kết nối tới một địa chỉ Multiaddr.
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

    /// Nạp danh sách peers từ Rendezvous Server và thêm vào Kademlia Routing Table.
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

    /// Phân phối các mảnh shards (thông thường 40 shards từ erasure-codec) đến các peer trong mạng P2P.
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

    /// Truy vấn song song mạng lưới để gom đủ tối thiểu `target_count` (10) shards sớm nhất thì dừng lại ngay.
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

    /// Lấy danh sách các PeerId đang kết nối trực tiếp.
    pub async fn get_connected_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::GetConnectedPeers { respond_to: tx })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))
    }

    /// Lấy danh sách các địa chỉ đang lắng nghe (Listeners).
    pub async fn get_listeners(&self) -> Result<Vec<Multiaddr>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(P2PCommand::GetListeners { respond_to: tx })
            .await
            .map_err(|e| NetworkError::ChannelClosed(e.to_string()))?;
        rx.await.map_err(|e| NetworkError::ChannelClosed(e.to_string()))
    }
}

/// Trạng thái yêu cầu song song đang chờ phản hồi từ mạng
struct PendingFetchRequest {
    target_count: usize,
    collected_shards: Vec<Shard>,
    collected_indices: HashSet<usize>,
    hashes_map: HashMap<String, usize>, // hash -> original_index
    respond_to: oneshot::Sender<Result<Vec<Shard>, NetworkError>>,
}

/// Background Event Loop của `P2PService`.
struct P2PEventLoop {
    swarm: Swarm<MyceliumBehaviour>,
    command_rx: mpsc::Receiver<P2PCommand>,
    blockstore: BlockStore,
    quota_manager: Arc<RwLock<QuotaManager>>,
    local_peer_id: PeerId,
    connected_peers: HashSet<PeerId>,
    pending_fetches: HashMap<request_response::OutboundRequestId, usize>, // req_id -> fetch_session_id
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
            pending_fetches: HashMap::new(),
            fetch_sessions: HashMap::new(),
            next_fetch_session_id: 1,
        }
    }

    async fn run(&mut self) {
        info!("Bắt đầu P2P Event Loop cho node {}", self.local_peer_id);

        loop {
            tokio::select! {
                // 1. Nhận lệnh từ P2PHandle
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_command(cmd).await;
                }

                // 2. Xử lý các sự kiện Swarm P2P
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
                    if let Some(peer_id) = extract_peer_id(&addr) {
                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, addr.clone());
                        let _ = self.swarm.dial(addr);
                        added += 1;
                    }
                }
                let _ = self.swarm.behaviour_mut().kademlia.bootstrap();
                let _ = respond_to.send(Ok(added));
            }
            P2PCommand::DistributeShards { shards, respond_to } => {
                let connected: Vec<PeerId> = self.connected_peers.iter().cloned().collect();
                if connected.is_empty() {
                    // Nếu chưa có peer kết nối, lưu cục bộ vào blockstore
                    for shard in &shards {
                        let _ = self.blockstore.put_shard(&shard.hash, &shard.data);
                        let key = RecordKey::new(&shard.hash.as_bytes());
                        let _ = self.swarm.behaviour_mut().kademlia.start_providing(key);
                    }
                    let _ = respond_to.send(Ok(()));
                    return;
                }

                // Phân phối luân phiên các shards tới các peers
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

                // 1. Kiểm tra trong local blockstore trước
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

                // Nếu đã đủ target_count ngay trong local blockstore, trả về ngay lập tức
                if collected_shards.len() >= target_count {
                    let _ = respond_to.send(Ok(collected_shards));
                    return;
                }

                let connected: Vec<PeerId> = self.connected_peers.iter().cloned().collect();
                if connected.is_empty() {
                    let _ = respond_to.send(Err(NetworkError::NoPeersAvailable));
                    return;
                }

                // Tạo Fetch Session mới
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

                // Gửi truy vấn Pull song song tới tất cả các peers đã kết nối
                for hash in remaining_hashes {
                    for peer in &connected {
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
                info!("Đã kết nối thành công với peer: {}", peer_id);
                self.connected_peers.insert(peer_id);
                if let ConnectedPoint::Dialer { address, .. } = endpoint {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, address);
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                debug!("Đã ngắt kết nối với peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
            }
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::Mdns(mdns_event)) => match mdns_event {
                MdnsEvent::Discovered(list) => {
                    for (peer_id, multiaddr) in list {
                        debug!("mDNS phát hiện peer LAN: {} tại {}", peer_id, multiaddr);
                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, multiaddr.clone());
                        let _ = self.swarm.dial(multiaddr);
                    }
                }
                MdnsEvent::Expired(list) => {
                    for (peer_id, _) in list {
                        trace!("mDNS peer hết hạn: {}", peer_id);
                    }
                }
            },
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::RequestResponse(req_event)) => {
                self.handle_req_resp_event(req_event).await;
            }
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::Kademlia(kad_event)) => {
                trace!("Kademlia event: {:?}", kad_event);
            }
            SwarmEvent::Behaviour(MyceliumBehaviourEvent::Identify(identify_event)) => {
                trace!("Identify event: {:?}", identify_event);
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Node đang lắng nghe tại Multiaddr: {}", address);
            }
            _ => {}
        }
    }

    async fn handle_req_resp_event(
        &mut self,
        event: request_response::Event<ShardRequest, ShardResponse>,
    ) {
        match event {
            // 1. Nhận Request từ một peer khác gửi đến
            request_response::Event::Message {
                peer,
                message:
                    Message::Request {
                        request_id: _,
                        request,
                        channel,
                    },
            } => match request {
                // Nhận PushShard: kiểm tra QuotaManager -> lưu BlockStore -> thông báo DHT
                ShardRequest::Push(push) => {
                    let shard_len = push.data.len() as u64;
                    let quota_guard = self.quota_manager.read().await;

                    if quota_guard.can_store_incoming_shard(&self.blockstore, shard_len) {
                        drop(quota_guard);

                        // Lưu shard vào BlockStore
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

                        // Thông báo DHT rằng node này là provider của shard hash
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
                // Nhận PullShard: đọc từ BlockStore -> gửi byte dữ liệu
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

            // 2. Nhận Response từ yêu cầu Outbound mà node đã gửi đi trước đó
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

                                    // Nếu đã thu thập đủ target_count -> hoàn thành session ngay lập tức
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
                    }
                }
            }
            _ => {}
        }
    }
}

/// Helper trích xuất PeerId từ Multiaddr nếu có đuôi `/p2p/<peer_id>`.
fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    for protocol in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(peer_id) = protocol {
            return Some(peer_id);
        }
    }
    None
}
