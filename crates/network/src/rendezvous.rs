use std::str::FromStr;
use std::time::Duration;

use libp2p::{Multiaddr, PeerId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::error::NetworkError;

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30); // 30 giây

/// Payload gửi lên Rendezvous Server qua endpoint `/heartbeat`.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub peer_id: String,
    pub multiaddrs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

/// Dữ liệu trả về từ `/peers`.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeersResponse {
    pub peers: Vec<String>,
    #[serde(default)]
    pub total_active: usize,
    #[serde(default)]
    pub returned: usize,
}

/// HTTP Client tương tác với Deno Deploy Rendezvous / Bootstrap Server.
#[derive(Clone)]
pub struct RendezvousClient {
    endpoint_url: String,
    client: Client,
}

impl RendezvousClient {
    /// Khởi tạo `RendezvousClient` với URL chỉ định và cấu hình timeout 5 giây.
    pub fn new(endpoint_url: &str) -> Self {
        let trimmed_url = endpoint_url.trim_end_matches('/').to_string();
        let client = Client::builder()
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            endpoint_url: trimmed_url,
            client,
        }
    }

    /// Lấy danh sách các active peer multiaddr từ Rendezvous server (cold-start bootstrap).
    pub async fn fetch_bootstrap_peers(&self, limit: usize) -> Result<Vec<Multiaddr>, NetworkError> {
        let url = format!("{}/peers?limit={}", self.endpoint_url, limit);
        debug!("Đang truy vấn bootstrap peers từ Rendezvous: {}", url);

        let response = match self.client.get(&url).send().await {
            Ok(res) => res,
            Err(err) => {
                warn!(
                    "Không thể kết nối tới Rendezvous server ({}): {}. Kích hoạt chế độ Fallback mDNS/Local.",
                    url, err
                );
                return Ok(Vec::new());
            }
        };

        if !response.status().is_success() {
            warn!(
                "Rendezvous server trả về mã lỗi HTTP {}. Bỏ qua bootstrap từ xa.",
                response.status()
            );
            return Ok(Vec::new());
        }

        let peers_dto = match response.json::<PeersResponse>().await {
            Ok(dto) => dto,
            Err(err) => {
                warn!("Không thể giải mã danh sách peers từ Rendezvous: {}", err);
                return Ok(Vec::new());
            }
        };

        let mut multiaddrs = Vec::with_capacity(peers_dto.peers.len());
        for addr_str in peers_dto.peers {
            match Multiaddr::from_str(&addr_str) {
                Ok(addr) => multiaddrs.push(addr),
                Err(e) => {
                    warn!("Bỏ qua multiaddr không hợp lệ '{}': {}", addr_str, e);
                }
            }
        }

        info!(
            "Nhận thành công {} bootstrap peers từ Rendezvous server",
            multiaddrs.len()
        );
        Ok(multiaddrs)
    }

    /// Gửi heartbeat danh sách multiaddr lên server.
    pub async fn send_heartbeat(
        &self,
        peer_id: &str,
        multiaddrs: &[String],
        region: Option<String>,
    ) -> Result<bool, NetworkError> {
        let url = format!("{}/heartbeat", self.endpoint_url);
        let body = HeartbeatRequest {
            peer_id: peer_id.to_string(),
            multiaddrs: multiaddrs.to_vec(),
            region,
        };

        let res = self.client.post(&url).json(&body).send().await?;
        Ok(res.status().is_success())
    }

    /// Khởi chạy vòng lặp gửi heartbeat đơn giản định kỳ.
    pub fn start_heartbeat_loop(
        &self,
        local_peer_id: PeerId,
        public_multiaddr: Multiaddr,
        region: Option<String>,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.clone();
        let peer_id_str = local_peer_id.to_string();
        let addrs = vec![public_multiaddr.to_string()];

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            info!(
                "Đã khởi động heartbeat loop tới {} (chu kỳ 30 giây)",
                client.endpoint_url
            );

            loop {
                interval.tick().await;

                debug!("Đang gửi heartbeat định kỳ tới Rendezvous...");
                match client
                    .send_heartbeat(&peer_id_str, &addrs, region.clone())
                    .await
                {
                    Ok(true) => {
                        debug!("Gửi heartbeat thành công cho node {}", peer_id_str);
                    }
                    Ok(false) => {
                        warn!("Rendezvous server từ chối heartbeat cho node {}", peer_id_str);
                    }
                    Err(err) => {
                        warn!(
                            "Gặp sự cố khi gửi heartbeat tới Rendezvous ({}): {}. Sẽ thử lại sau 30 giây.",
                            client.endpoint_url, err
                        );
                    }
                }
            }
        })
    }

    /// Khởi chạy vòng lặp gửi heartbeat định kỳ, tự động gửi cả Direct IP và Relay Circuit addresses.
    pub fn start_dynamic_heartbeat_loop(
        &self,
        service: crate::service::P2PService,
        direct_multiaddr: Multiaddr,
        region: Option<String>,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.clone();
        let local_peer_id = service.local_peer_id();
        let peer_id_str = local_peer_id.to_string();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            info!(
                "Đã khởi động Dynamic Heartbeat Loop tới {} (chu kỳ 30s)",
                client.endpoint_url
            );

            loop {
                interval.tick().await;

                let listeners = service.get_listeners().await.unwrap_or_default();
                let mut addrs = vec![direct_multiaddr.to_string()];

                for l in listeners {
                    let s = l.to_string();
                    if s.contains("/p2p-circuit/") || s.contains("/p2p-circuit") {
                        let formatted = if s.ends_with(&peer_id_str) {
                            s
                        } else {
                            format!("{}/p2p/{}", s.trim_end_matches('/'), peer_id_str)
                        };
                        if !addrs.contains(&formatted) {
                            addrs.push(formatted);
                        }
                    }
                }

                debug!("Đang gửi heartbeat tới Rendezvous với multiaddrs: {:?}", addrs);
                match client
                    .send_heartbeat(&peer_id_str, &addrs, region.clone())
                    .await
                {
                    Ok(true) => {
                        debug!("Gửi heartbeat thành công cho node {}", peer_id_str);
                    }
                    Ok(false) => {
                        warn!("Rendezvous server từ chối heartbeat cho node {}", peer_id_str);
                    }
                    Err(err) => {
                        warn!(
                            "Gặp sự cố khi gửi heartbeat tới Rendezvous: {}",
                            err
                        );
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rendezvous_parse_mock_response() {
        let p1 = PeerId::random();
        let p2 = PeerId::random();
        let p3 = PeerId::random();

        let mock_json = format!(
            r#"{{
                "peers": [
                    "/ip4/127.0.0.1/tcp/4001/p2p/{}",
                    "/ip4/192.168.1.100/tcp/4001/p2p/{}",
                    "/dns4/bootstrap.mycelium.network/tcp/4001/p2p/{}"
                ],
                "total_active": 3,
                "returned": 3
            }}"#,
            p1, p2, p3
        );

        let parsed: PeersResponse = serde_json::from_str(&mock_json).unwrap();
        assert_eq!(parsed.peers.len(), 3);

        let mut multiaddrs = Vec::new();
        for addr_str in parsed.peers {
            let m: Multiaddr = addr_str.parse().unwrap();
            multiaddrs.push(m);
        }

        assert_eq!(multiaddrs.len(), 3);
        assert!(multiaddrs[0].to_string().contains("127.0.0.1"));
        assert!(multiaddrs[1].to_string().contains("192.168.1.100"));
        assert!(multiaddrs[2].to_string().contains("bootstrap.mycelium.network"));
    }

    #[tokio::test]
    async fn test_rendezvous_fallback_on_unreachable_server() {
        // Test fallback khi server không tồn tại / port đóng
        let client = RendezvousClient::new("http://127.0.0.1:59999");
        let result = client.fetch_bootstrap_peers(10).await;

        // Phải thành công và trả về mảng rỗng (Resilience fallback)
        assert!(result.is_ok());
        let peers = result.unwrap();
        assert!(peers.is_empty());
    }
}
