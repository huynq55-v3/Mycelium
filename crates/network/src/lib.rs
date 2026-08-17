//! # P2P Network Module for Mycelium Storage
//!
//! Tầng mạng P2P hiệu năng cao dựa trên `rust-libp2p` và `tokio`:
//! - **Private Network (Pnet)**: Cô lập mạng với `SwarmKey` mật mã từ `core-crypto`.
//! - **Discovery**: `libp2p-mdns` (mạng nội bộ LAN) & `libp2p-kad` (Kademlia DHT).
//! - **Giao thức truyền dữ liệu**: `libp2p-request-response` (CBOR) với `PushShard` và `PullShard`.
//! - **Rendezvous Client**: Kết nối Bootstrap / Rendezvous server trên Deno Deploy (fallback mDNS an toàn).
//! - **P2PService**: Quản lý background loop Tokio task, tích hợp `BlockStore` và `QuotaManager`.

pub mod behaviour;
pub mod error;
pub mod protocol;
pub mod rendezvous;
pub mod service;
pub mod transport;
pub mod vfs;

// Re-exports
pub use behaviour::{MyceliumBehaviour, MYCELIUM_STORAGE_PROTOCOL};
pub use error::NetworkError;
pub use protocol::{
    PullResponse, PullShard, PushResponse, PushShard, ShardRequest, ShardResponse,
};
pub use rendezvous::{HeartbeatRequest, PeersResponse, RendezvousClient};
pub use service::P2PService;
pub use transport::build_transport;
pub use vfs::{DirectoryNode, FileNode, VfsEntry, VirtualTree};
