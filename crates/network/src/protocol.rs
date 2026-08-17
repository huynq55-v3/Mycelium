use serde::{Deserialize, Serialize};

/// Thông điệp gửi lên một mảnh shard (`PushShard`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushShard {
    /// Mã băm SHA-256 của shard.
    pub hash: String,
    /// Mã định danh File (File CID) mà shard này thuộc về.
    pub file_cid: Option<String>,
    /// Dữ liệu nhị phân của shard.
    pub data: Vec<u8>,
}

/// Phản hồi sau khi tiếp nhận `PushShard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushResponse {
    /// Node nhận có chấp thuận lưu trữ hay không (phụ thuộc QuotaManager).
    pub accepted: bool,
    /// Lý do nếu bị từ chối.
    pub reason: Option<String>,
}

/// Thông điệp yêu cầu tải về một mảnh shard (`PullShard`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullShard {
    /// Mã băm SHA-256 của shard cần tải.
    pub hash: String,
}

/// Phản hồi dữ liệu cho `PullShard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullResponse {
    /// Dữ liệu byte của shard (hoặc `None` nếu node không lưu giữ).
    pub data: Option<Vec<u8>>,
}

/// Thông điệp yêu cầu thu hồi / xóa bỏ shard thừa (`PruneShard`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneShard {
    /// Mã băm SHA-256 của shard cần thu hồi.
    pub hash: String,
}

/// Phản hồi sau khi tiếp nhận `PruneShard`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneResponse {
    /// Node đã xóa thành công shard hay không.
    pub pruned: bool,
    /// Lý do nếu không xóa (ví dụ: làm R tụt dưới 4.0).
    pub reason: Option<String>,
}

/// Thông điệp yêu cầu kiểm tra thông số Quota và chỉ số R của Peer (`QueryPeerStats`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPeerStats;

/// Phản hồi thông số Quota và chỉ số R của Peer (`PeerStatsResponse`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerStatsResponse {
    pub peer_id: String,
    pub my_uploaded_bytes: u64,
    pub stored_shard_bytes: u64,
    pub r_ratio: Option<f64>,
}

/// Mục Shard định danh gồm File CID và Shard Hash (`ShardItem`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardItem {
    pub file_cid: String,
    pub shard_hash: String,
}

/// Báo cáo trạng thái đầy đủ của Node gửi lên Relay (`NodeStateReport`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeStateReport {
    pub peer_id: String,
    pub uploaded_bytes: u64,
    pub stored_bytes: u64,
    pub r_ratio: Option<f64>,
    pub held_shards: Vec<ShardItem>,
}

/// Bản tin phát sóng trạng thái toàn mạng từ Relay Server (`SwarmStateBroadcast`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwarmStateBroadcast {
    pub nodes: Vec<NodeStateReport>,
    pub timestamp: u64,
}

/// Định nghĩa thông điệp Request tổng hợp của giao thức lưu trữ P2P Mycelium.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardRequest {
    Push(PushShard),
    Pull(PullShard),
    Prune(PruneShard),
    QueryStats(QueryPeerStats),
    ReportState(NodeStateReport),
    SyncSwarmState(SwarmStateBroadcast),
}

/// Định nghĩa thông điệp Response tổng hợp của giao thức lưu trữ P2P Mycelium.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardResponse {
    Push(PushResponse),
    Pull(PullResponse),
    Prune(PruneResponse),
    Stats(PeerStatsResponse),
    Ack,
}

pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB

#[derive(Debug, Clone, Default)]
pub struct MyceliumStorageCodec;

#[async_trait::async_trait]
impl libp2p::request_response::Codec for MyceliumStorageCodec {
    type Protocol = libp2p::StreamProtocol;
    type Request = ShardRequest;
    type Response = ShardResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        use futures::AsyncReadExt;
        let mut len_bytes = [0u8; 4];
        io.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Payload {} vượt quá giới hạn 64MB", len),
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        serde_cbor::from_slice(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        use futures::AsyncReadExt;
        let mut len_bytes = [0u8; 4];
        io.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Payload {} vượt quá giới hạn 64MB", len),
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        serde_cbor::from_slice(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        use futures::AsyncWriteExt;
        let bytes = serde_cbor::to_vec(&req)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        io.write_all(&len.to_be_bytes()).await?;
        io.write_all(&bytes).await?;
        io.flush().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        use futures::AsyncWriteExt;
        let bytes = serde_cbor::to_vec(&res)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        io.write_all(&len.to_be_bytes()).await?;
        io.write_all(&bytes).await?;
        io.flush().await?;
        Ok(())
    }
}
