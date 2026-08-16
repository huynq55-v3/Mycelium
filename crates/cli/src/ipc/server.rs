use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use blockstore::BlockStore;
use core_crypto::{compute_cid, decrypt_data, encrypt_data, Identity, SwarmKey};
use erasure_codec::{decode, encode_with_name};
use network::{P2PService, RendezvousClient};
use quota_manager::QuotaManager;
use rand::RngCore;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::ipc::protocol::{DaemonStatusInfo, IpcRequest, IpcResponse, DEFAULT_IPC_ADDR};
use crate::manifest::EncryptedFilePackage;

/// IPC Server lắng nghe trên `127.0.0.1:5001` bên trong `p2pdrive daemon`.
pub struct IpcServer {
    service: P2PService,
    identity: Identity,
    swarm_key: Option<SwarmKey>,
    blockstore: BlockStore,
    quota_manager: Arc<RwLock<QuotaManager>>,
    config: AppConfig,
}

impl IpcServer {
    pub fn new(
        service: P2PService,
        identity: Identity,
        swarm_key: Option<SwarmKey>,
        blockstore: BlockStore,
        quota_manager: Arc<RwLock<QuotaManager>>,
        config: AppConfig,
    ) -> Self {
        Self {
            service,
            identity,
            swarm_key,
            blockstore,
            quota_manager,
            config,
        }
    }

    /// Khởi chạy IPC listener loop trên Tokio task.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(DEFAULT_IPC_ADDR)
            .await
            .with_context(|| format!("Không thể mở IPC socket tại {}", DEFAULT_IPC_ADDR))?;

        info!("IPC Server đang lắng nghe các lệnh CLI tại {}", DEFAULT_IPC_ADDR);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("Nhận kết nối IPC mới từ {}", addr);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream).await {
                            error!("Lỗi xử lý IPC connection từ {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    warn!("Lỗi khi accept IPC connection: {}", e);
                }
            }
        }
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let (reader, mut writer) = stream.split();
        let mut buf_reader = BufReader::new(reader);
        let mut line = String::new();

        if buf_reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }

        let request: IpcRequest = match serde_json::from_str(line.trim()) {
            Ok(req) => req,
            Err(e) => {
                let resp = IpcResponse::Error(format!("Invalid IPC Request: {e}"));
                let mut out = serde_json::to_string(&resp)?;
                out.push('\n');
                writer.write_all(out.as_bytes()).await?;
                return Ok(());
            }
        };

        match request {
            IpcRequest::Upload { file_path } => {
                self.handle_upload(PathBuf::from(file_path), &mut writer).await?;
            }
            IpcRequest::Download {
                manifest_path,
                output_path,
            } => {
                self.handle_download(
                    PathBuf::from(manifest_path),
                    PathBuf::from(output_path),
                    &mut writer,
                )
                .await?;
            }
            IpcRequest::GetStatus => {
                self.handle_status(&mut writer).await?;
            }
        }

        Ok(())
    }

    async fn send_response<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        response: &IpcResponse,
    ) -> Result<()> {
        let mut out = serde_json::to_string(response)?;
        out.push('\n');
        writer.write_all(out.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn handle_upload<W: AsyncWriteExt + Unpin>(
        &self,
        file_path: PathBuf,
        writer: &mut W,
    ) -> Result<()> {
        if !file_path.exists() {
            Self::send_response(
                writer,
                &IpcResponse::Error(format!("Tệp tin nguồn không tồn tại: {:?}", file_path)),
            )
            .await?;
            return Ok(());
        }

        let file_metadata = fs::metadata(&file_path)?;
        let file_size = file_metadata.len();
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unnamed.bin")
            .to_string();

        // 1. Kiểm tra Quota
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "quota_check".to_string(),
                current: 0,
                total: 100,
                message: "Đang kiểm tra hạn mức lưu trữ...".to_string(),
            },
        )
        .await?;

        let qm_guard = self.quota_manager.read().await;
        if let Err(e) = qm_guard.validate_upload(file_size) {
            Self::send_response(
                writer,
                &IpcResponse::Error(format!("Vượt quá hạn ngạch cho phép: {e}")),
            )
            .await?;
            return Ok(());
        }
        drop(qm_guard);

        // 2. Đọc file
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "reading".to_string(),
                current: 20,
                total: 100,
                message: "Đang đọc dữ liệu tệp vào bộ nhớ...".to_string(),
            },
        )
        .await?;

        let mut file = File::open(&file_path)?;
        let mut raw_data = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut raw_data)?;
        let original_hash = erasure_codec::sha256_hex(&raw_data);

        // 3. Mã hóa AES-256-GCM
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "encrypting".to_string(),
                current: 40,
                total: 100,
                message: "Đang mã hóa AES-256-GCM bảo mật đầu cuối...".to_string(),
            },
        )
        .await?;

        let mut enc_key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut enc_key);

        let encrypted_payload = match encrypt_data(&raw_data, &enc_key) {
            Ok(payload) => payload,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Lỗi mã hóa dữ liệu: {e}")),
                )
                .await?;
                return Ok(());
            }
        };
        let encrypted_cid = compute_cid(&encrypted_payload);

        // 4. Phân đoạn Reed-Solomon
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "erasure_coding".to_string(),
                current: 60,
                total: 100,
                message: "Đang phân rã Reed-Solomon (K=10, N=40 shards)...".to_string(),
            },
        )
        .await?;

        let (erasure_manifest, shards) = match encode_with_name(&file_name, &encrypted_payload) {
            Ok(res) => res,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Lỗi phân đoạn Reed-Solomon: {e}")),
                )
                .await?;
                return Ok(());
            }
        };

        // 5. Phân phối Shards qua P2PService
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "distributing".to_string(),
                current: 80,
                total: 100,
                message: "Đang phân tán 40 shards lên mạng lưới P2P DHT...".to_string(),
            },
        )
        .await?;

        if let Err(e) = self.service.distribute_shards(shards).await {
            Self::send_response(
                writer,
                &IpcResponse::Error(format!("Lỗi phân tán shards: {e}")),
            )
            .await?;
            return Ok(());
        }

        // 6. Cập nhật Quota
        let mut qm = self.quota_manager.write().await;
        let _ = qm.record_upload(file_size);
        if let Ok(config_dir) = AppConfig::config_dir() {
            let _ = qm.save_to_file(config_dir.join("quota.json"));
        }
        drop(qm);

        // 7. Lưu Manifest file
        let manifest_filename = format!("{}.manifest.json", file_name);
        let manifest_path = file_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&manifest_filename);

        let package = EncryptedFilePackage {
            file_name: file_name.clone(),
            original_size: file_size as usize,
            original_hash,
            encrypted_cid: encrypted_cid.clone(),
            encryption_key_hex: hex::encode(enc_key),
            erasure_manifest,
        };

        package.save_to_file(&manifest_path)?;

        // Phản hồi thành công
        Self::send_response(
            writer,
            &IpcResponse::UploadSuccess {
                file_name,
                original_size: file_size as usize,
                encrypted_cid,
                manifest_path: manifest_path.to_string_lossy().to_string(),
            },
        )
        .await?;

        Ok(())
    }

    async fn handle_download<W: AsyncWriteExt + Unpin>(
        &self,
        manifest_path: PathBuf,
        output_path: PathBuf,
        writer: &mut W,
    ) -> Result<()> {
        if !manifest_path.exists() {
            Self::send_response(
                writer,
                &IpcResponse::Error(format!("File manifest không tồn tại: {:?}", manifest_path)),
            )
            .await?;
            return Ok(());
        }

        let package = match EncryptedFilePackage::load_from_file(&manifest_path) {
            Ok(pkg) => pkg,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Không thể đọc file manifest: {e}")),
                )
                .await?;
                return Ok(());
            }
        };

        // 1. Gom song song 10 shards từ DHT
        let target_count = package.erasure_manifest.k_data_shards; // 10 shards
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "fetching_shards".to_string(),
                current: 20,
                total: 100,
                message: format!("Đang truy vấn song song K={} shards từ DHT...", target_count),
            },
        )
        .await?;

        let shard_hashes = package.erasure_manifest.shard_hashes.clone();
        let collected_shards = match self
            .service
            .fetch_shards_parallel(shard_hashes, target_count)
            .await
        {
            Ok(shards) => shards,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Không thể thu thập đủ 10 shards từ mạng lưới: {e}")),
                )
                .await?;
                return Ok(());
            }
        };

        // 2. Giải mã Reed-Solomon
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "decoding_rs".to_string(),
                current: 60,
                total: 100,
                message: "Đang tái tạo dữ liệu qua Reed-Solomon decode...".to_string(),
            },
        )
        .await?;

        let mut sparse_shards = vec![None; package.erasure_manifest.n_total_shards];
        for shard in collected_shards {
            let idx = shard.index;
            if idx < sparse_shards.len() {
                sparse_shards[idx] = Some(shard);
            }
        }

        let recovered_encrypted_data = match decode(&package.erasure_manifest, sparse_shards) {
            Ok(data) => data,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Lỗi giải mã Reed-Solomon: {e}")),
                )
                .await?;
                return Ok(());
            }
        };

        // 3. Giải mã AES-256-GCM
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "decrypting".to_string(),
                current: 85,
                total: 100,
                message: "Đang giải mã AES-256-GCM bằng Secret Key...".to_string(),
            },
        )
        .await?;

        let key_bytes = match hex::decode(&package.encryption_key_hex) {
            Ok(bytes) if bytes.len() == 32 => bytes,
            _ => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error("Khóa AES trong manifest không hợp lệ".to_string()),
                )
                .await?;
                return Ok(());
            }
        };
        let mut enc_key = [0u8; 32];
        enc_key.copy_from_slice(&key_bytes);

        let decrypted_raw_data = match decrypt_data(&recovered_encrypted_data, &enc_key) {
            Ok(data) => data,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Lỗi giải mã AES: {e}")),
                )
                .await?;
                return Ok(());
            }
        };

        // Xác thực mã băm dữ liệu gốc
        let recovered_hash = erasure_codec::sha256_hex(&decrypted_raw_data);
        if recovered_hash != package.original_hash {
            Self::send_response(
                writer,
                &IpcResponse::Error(
                    "Mã băm dữ liệu khôi phục không khớp với dữ liệu gốc ban đầu!".to_string(),
                ),
            )
            .await?;
            return Ok(());
        }

        // 4. Ghi ra output_path
        if let Some(parent) = output_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut out_file = match File::create(&output_path) {
            Ok(f) => f,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Không thể tạo tệp đích {:?}: {e}", output_path)),
                )
                .await?;
                return Ok(());
            }
        };
        out_file.write_all(&decrypted_raw_data)?;
        out_file.flush()?;

        // Phản hồi thành công
        Self::send_response(
            writer,
            &IpcResponse::DownloadSuccess {
                file_name: package.file_name,
                bytes_written: decrypted_raw_data.len(),
                output_path: output_path.to_string_lossy().to_string(),
            },
        )
        .await?;

        Ok(())
    }

    async fn handle_status<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        let qm = self.quota_manager.read().await;
        let allocated_gb = qm.allocated_disk_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let upload_limit_gb = qm.allowed_upload_capacity() as f64 / (1024.0 * 1024.0 * 1024.0);
        let upload_used_gb = qm.my_uploaded_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        drop(qm);

        let shard_count = self.blockstore.count_shards();
        let disk_used_mb = self.blockstore.current_disk_usage().unwrap_or(0) as f64 / (1024.0 * 1024.0);

        let connected_peers = self.service.get_connected_peers().await.unwrap_or_default();
        let listeners = self.service.get_listeners().await.unwrap_or_default();

        let rendezvous = RendezvousClient::new(&self.config.rendezvous_url);
        let rendezvous_online = match tokio::time::timeout(
            Duration::from_secs(2),
            rendezvous.fetch_bootstrap_peers(1),
        )
        .await
        {
            Ok(Ok(_)) => true,
            _ => false,
        };

        let swarm_key_preview = self.swarm_key.as_ref().map(|k| k.to_hex()[..16].to_string());

        let status_info = DaemonStatusInfo {
            did: self.identity.to_did(),
            is_private_swarm: self.swarm_key.is_some(),
            swarm_key_preview,
            rendezvous_url: self.config.rendezvous_url.clone(),
            rendezvous_online,
            shard_count,
            disk_used_mb,
            allocated_gb,
            upload_used_gb,
            upload_limit_gb,
            connected_peers_count: connected_peers.len(),
            listen_addrs: listeners.into_iter().map(|a| a.to_string()).collect(),
        };

        Self::send_response(writer, &IpcResponse::StatusSuccess(status_info)).await?;
        Ok(())
    }
}
