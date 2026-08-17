use std::io;

use core_crypto::SwarmKey;
use futures::TryFutureExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::core::upgrade::Version;
use libp2p::identity::Keypair;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::relay::client::{self, Behaviour as RelayClientBehaviour};
use libp2p::{dns, noise, tcp, yamux, PeerId, Transport};

use crate::error::NetworkError;

/// Xây dựng Transport chuẩn mực cho node P2P Mycelium:
/// - **Ưu tiên 1 (Left - Direct TCP)**: Direct TCP / DNS qua PNet -> Noise -> Yamux.
/// - **Ưu tiên 2 (Right - Relay Circuit)**: Circuit Relay v2 Client qua Noise -> Yamux.
pub fn build_transport(
    keypair: &Keypair,
    swarm_key: Option<&SwarmKey>,
) -> Result<(Boxed<(PeerId, StreamMuxerBox)>, RelayClientBehaviour), NetworkError> {
    let local_peer_id = keypair.public().to_peer_id();
    let (relay_transport, relay_behaviour) = client::new(local_peer_id);

    // 1. Cấu hình Noise & Yamux
    let noise_config = noise::Config::new(keypair)
        .map_err(|e| NetworkError::TransportError(format!("Lỗi cấu hình Noise: {e}")))?;
    let yamux_config = yamux::Config::default();

    // 2. Nâng cấp Relay Circuit Transport độc lập
    let upgraded_relay = relay_transport
        .upgrade(Version::V1Lazy)
        .authenticate(noise_config.clone())
        .multiplex(yamux_config.clone());

    // 3. DNS + Direct TCP Transport
    let tcp_transport = tcp::tokio::Transport::default();
    let dns_tcp = dns::tokio::Transport::system(tcp_transport)
        .map_err(|e| NetworkError::TransportError(format!("Lỗi khởi tạo DNS Transport: {e}")))?;

    if let Some(sk) = swarm_key {
        let psk = PreSharedKey::new(*sk.as_bytes());
        let pnet_config = PnetConfig::new(psk);

        // Nâng cấp Direct TCP với PNet + Noise + Yamux
        let upgraded_tcp = dns_tcp
            .and_then(move |socket, _| {
                pnet_config
                    .handshake(socket)
                    .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))
            })
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux_config);

        // Đặt upgraded_relay làm nhánh ưu tiên hàng đầu (Left) để mọi địa chỉ chứa /p2p-circuit/ được Relay Transport xử lý chính xác
        let transport = OrTransport::new(upgraded_relay, upgraded_tcp)
            .map(|either_output, _| match either_output {
                futures::future::Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                futures::future::Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            })
            .boxed();

        Ok((transport, relay_behaviour))
    } else {
        let upgraded_tcp = dns_tcp
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux_config);

        let transport = OrTransport::new(upgraded_relay, upgraded_tcp)
            .map(|either_output, _| match either_output {
                futures::future::Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                futures::future::Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            })
            .boxed();

        Ok((transport, relay_behaviour))
    }
}
