use std::io;

use core_crypto::SwarmKey;
use futures::TryFutureExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::core::upgrade::Version;
use libp2p::identity::Keypair;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::relay::client::{self, Behaviour as RelayClientBehaviour};
use libp2p::{noise, tcp, yamux, PeerId, Transport};

use crate::error::NetworkError;

/// Xây dựng Transport hoàn chỉnh cho node P2P Mycelium:
/// - **Direct TCP Transport**: Kết nối mạng trực tiếp (LAN / Public IP / IPv4 / IPv6).
/// - **Circuit Relay v2 Client Transport**: Tự động chuyển tiếp qua Relay khi bị chặn bởi NAT/Firewall.
/// - **Private Swarm (libp2p-pnet)**: Bảo vệ mạng riêng bằng `SwarmKey` 32-byte.
/// - **Noise Protocol**: Mã hóa và xác thực chữ ký Ed25519.
/// - **Yamux**: Ghép kênh kết nối (Multiplexing).
pub fn build_transport(
    keypair: &Keypair,
    swarm_key: Option<&SwarmKey>,
) -> Result<(Boxed<(PeerId, StreamMuxerBox)>, RelayClientBehaviour), NetworkError> {
    let local_peer_id = keypair.public().to_peer_id();
    let (relay_transport, relay_behaviour) = client::new(local_peer_id);

    let tcp_transport = tcp::tokio::Transport::default();

    // Áp dụng lớp bảo vệ Private Network (libp2p-pnet) nếu có SwarmKey
    if let Some(sk) = swarm_key {
        let psk = PreSharedKey::new(*sk.as_bytes());
        let pnet_config = PnetConfig::new(psk);

        let noise_config = noise::Config::new(keypair)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi cấu hình Noise: {e}")))?;
        let yamux_config = yamux::Config::default();

        let secured_tcp = tcp_transport
            .and_then(move |socket, _| {
                pnet_config
                    .handshake(socket)
                    .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))
            })
            .boxed();

        let transport = OrTransport::new(relay_transport, secured_tcp)
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux_config)
            .boxed();

        Ok((transport, relay_behaviour))
    } else {
        let noise_config = noise::Config::new(keypair)
            .map_err(|e| NetworkError::TransportError(format!("Lỗi cấu hình Noise: {e}")))?;
        let yamux_config = yamux::Config::default();

        let transport = OrTransport::new(relay_transport, tcp_transport)
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux_config)
            .boxed();

        Ok((transport, relay_behaviour))
    }
}
