use std::io;

use core_crypto::SwarmKey;
use futures::TryFutureExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::upgrade::Version;
use libp2p::identity::Keypair;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::{noise, tcp, yamux, PeerId, Transport};

use crate::error::NetworkError;

/// Xây dựng Transport hoàn chỉnh cho node P2P Mycelium:
/// - **Lớp 1 (Network Layer)**: TCP Tokio Transport.
/// - **Lớp 2 (Private Swarm)**: `libp2p-pnet` bắt tay bảo mật bằng `SwarmKey` (32 bytes Pre-shared Key).
///   Bất kỳ node nào không có cùng `SwarmKey` sẽ bị ngắt kết nối ngay lập tức ở tầng handshake.
/// - **Lớp 3 (Encryption & Auth)**: Noise Protocol Framework (Ed25519 authentication + ChaCha20-Poly1305 / AES-GCM).
/// - **Lớp 4 (Multiplexing)**: Yamux multiplexer.
pub fn build_transport(
    keypair: &Keypair,
    swarm_key: Option<&SwarmKey>,
) -> Result<Boxed<(PeerId, StreamMuxerBox)>, NetworkError> {
    let tcp_transport = tcp::tokio::Transport::default();

    // Áp dụng lớp bảo vệ Private Network (libp2p-pnet) nếu có SwarmKey
    if let Some(sk) = swarm_key {
        let psk = PreSharedKey::new(*sk.as_bytes());
        let pnet_config = PnetConfig::new(psk);

        let noise_config = noise::Config::new(keypair)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi cấu hình Noise: {e}")))?;
        let yamux_config = yamux::Config::default();

        let transport = tcp_transport
            .and_then(move |socket, _| {
                pnet_config
                    .handshake(socket)
                    .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))
            })
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux_config)
            .boxed();

        Ok(transport)
    } else {
        // Public Network không dùng Pnet
        let noise_config = noise::Config::new(keypair)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi cấu hình Noise: {e}")))?;
        let yamux_config = yamux::Config::default();

        let transport = tcp_transport
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux_config)
            .boxed();

        Ok(transport)
    }
}
