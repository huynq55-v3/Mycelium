use std::time::Duration;

use libp2p::autonat::{self, Config as AutoNatConfig};
use libp2p::identify::{self, Config as IdentifyConfig};
use libp2p::identity::Keypair;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{self, Config as KadConfig};
use libp2p::mdns::{self, Config as MdnsConfig};
use libp2p::relay;
use libp2p::request_response::{Behaviour as ReqRespBehaviour, Config as ReqRespConfig, ProtocolSupport};
use libp2p::swarm::NetworkBehaviour;
use libp2p::StreamProtocol;

use crate::protocol::MyceliumStorageCodec;

pub const MYCELIUM_STORAGE_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/mycelium/storage/1.0.0");
pub const MYCELIUM_KAD_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/mycelium/kad/1.0.0");
pub const MYCELIUM_AGENT_VERSION: &str = "mycelium/p2p-drive/0.1.0";
pub const MYCELIUM_RELAY_PROTOCOL: &str = "/mycelium/relay/1.0.0";
pub const MYCELIUM_RELAY_AGENT_VERSION: &str = "mycelium-relay/0.1.0";

/// Tập hợp các hành vi mạng (Network Behaviour) của node Mycelium P2P.
/// Tích hợp đầy đủ Circuit Relay v2 Client, AutoNAT, Kademlia DHT và mô hình kết nối On-Demand.
#[derive(NetworkBehaviour)]
pub struct MyceliumBehaviour {
    /// Định tuyến phân tán Kademlia DHT để tìm kiếm node và công bố provider records.
    pub kademlia: kad::Behaviour<MemoryStore>,
    /// Khám phá các peer trong mạng LAN nội bộ bằng mDNS.
    pub mdns: mdns::tokio::Behaviour,
    /// Giao thức Request-Response truyền nhận Shard (Push/Pull/Prune) hỗ trợ payload tới 64MB.
    pub request_response: ReqRespBehaviour<MyceliumStorageCodec>,
    /// Xác thực danh tính và thông tin của peer kết nối.
    pub identify: identify::Behaviour,
    /// Circuit Relay v2 Client (cho phép node đăng ký và chuyển tiếp qua Relay).
    pub relay_client: relay::client::Behaviour,
    /// Tự động phát hiện trạng thái NAT (Public hay Private).
    pub autonat: autonat::Behaviour,
}

impl MyceliumBehaviour {
    /// Khởi tạo `MyceliumBehaviour` với cấu hình tối ưu cho mạng P2P Drive.
    pub fn new(
        local_key: &Keypair,
        relay_client: relay::client::Behaviour,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let local_peer_id = local_key.public().to_peer_id();

        // 1. Cấu hình Kademlia DHT
        let mut kad_config = KadConfig::new(MYCELIUM_KAD_PROTOCOL);
        kad_config.set_query_timeout(Duration::from_secs(20));
        let store = MemoryStore::new(local_peer_id);
        let kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);

        // 2. Cấu hình mDNS (LAN discovery)
        let mdns = mdns::tokio::Behaviour::new(MdnsConfig::default(), local_peer_id)?;

        // 3. Cấu hình Request-Response với Codec 64MB
        let req_resp_config = ReqRespConfig::default()
            .with_request_timeout(Duration::from_secs(60));
        let protocols = [(MYCELIUM_STORAGE_PROTOCOL, ProtocolSupport::Full)];
        let request_response = ReqRespBehaviour::with_codec(MyceliumStorageCodec, protocols, req_resp_config);

        // 4. Cấu hình Identify
        let identify_config = IdentifyConfig::new(
            "/mycelium/id/1.0.0".to_string(),
            local_key.public(),
        )
        .with_agent_version(MYCELIUM_AGENT_VERSION.to_string());
        let identify = identify::Behaviour::new(identify_config);

        // 5. Cấu hình AutoNAT
        let autonat = autonat::Behaviour::new(
            local_peer_id,
            AutoNatConfig::default(),
        );

        Ok(Self {
            kademlia,
            mdns,
            request_response,
            identify,
            relay_client,
            autonat,
        })
    }
}
