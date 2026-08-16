use thiserror::Error;

/// Các lỗi có thể phát sinh trong tầng mạng P2P `network`.
#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("Lỗi P2P Transport / Swarm: {0}")]
    TransportError(String),

    #[error("Lỗi khởi tạo Private Network Pre-shared Key (Pnet): {0}")]
    PnetError(String),

    #[error("Lỗi giao thức Kademlia DHT: {0}")]
    KademliaError(String),

    #[error("Lỗi giao thức Request-Response: {0}")]
    RequestResponseError(String),

    #[error("Lỗi giải mã/chuyển đổi Multiaddr: {0}")]
    MultiaddrError(#[from] libp2p::core::multiaddr::Error),

    #[error("Lỗi định dạng PeerId: {0}")]
    PeerIdError(#[from] libp2p::identity::ParseError),

    #[error("Lỗi giao tiếp nội bộ qua Kênh (Channel): {0}")]
    ChannelClosed(String),

    #[error("Không tìm thấy đủ shards từ mạng lưới: yêu cầu {target}, thu thập được {collected}")]
    InsufficientShardsFound { target: usize, collected: usize },

    #[error("Không có peer nào khả dụng trong mạng để phân phối shards")]
    NoPeersAvailable,

    #[error("Lỗi HTTP Client khi gọi Rendezvous Server: {0}")]
    HttpClientError(#[from] reqwest::Error),

    #[error("Lỗi BlockStore: {0}")]
    BlockStoreError(#[from] blockstore::BlockStoreError),

    #[error("Lỗi QuotaManager: {0}")]
    QuotaError(#[from] quota_manager::QuotaError),

    #[error("Quá thời gian chờ phản hồi từ mạng P2P (Timeout)")]
    Timeout,
}
