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
            IpcRequest::Commit { paths, message } => {
                self.handle_commit(paths, message, &mut writer).await?;
            }
            IpcRequest::VfsList { path } => {
                self.handle_vfs_list(path, &mut writer).await?;
            }
            IpcRequest::VfsTree { path } => {
                self.handle_vfs_tree(path, &mut writer).await?;
            }
            IpcRequest::VfsRemove { path } => {
                self.handle_vfs_remove(path, &mut writer).await?;
            }
            IpcRequest::VfsDownload { vfs_path, output_path } => {
                self.handle_vfs_download(vfs_path, output_path, &mut writer).await?;
            }
            IpcRequest::Dump { private_key, output_dir, vfs_path } => {
                self.handle_dump(private_key, output_dir, vfs_path, &mut writer).await?;
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

        if let Err(e) = self.service.distribute_shards_with_file(shards, Some(encrypted_cid.clone())).await {
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

        // 1. Tự động kiểm tra và kết nối nhanh tới các peer từ Rendezvous
        let rendezvous = RendezvousClient::new(&self.config.rendezvous_url);
        let _ = self.service.bootstrap_from_rendezvous(&rendezvous, 20).await;

        let mut connected = self.service.get_connected_peers().await.unwrap_or_default();
        if connected.len() < 2 {
            Self::send_response(
                writer,
                &IpcResponse::Progress {
                    step: "connecting_peers".to_string(),
                    current: 5,
                    total: 100,
                    message: "Đang đồng bộ và mở kết nối 2 chiều tới các peer qua Relay...".to_string(),
                },
            )
            .await?;

            // Đợi tối đa 6 giây cho tới khi có ít nhất 2 peers (Relay + Storage node) kết nối
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                connected = self.service.get_connected_peers().await.unwrap_or_default();
                if connected.len() >= 2 {
                    break;
                }
            }
        }

        // 2. Gom song song 10 shards từ DHT
        let target_count = package.erasure_manifest.k_data_shards; // 10 shards
        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "fetching_shards".to_string(),
                current: 20,
                total: 100,
                message: format!("Đang truy vấn song song K={} shards từ mạng P2P...", target_count),
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

        // 3. Giải mã Reed-Solomon
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

        // 4. Giải mã AES-256-GCM
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

        // 5. Ghi ra output_path
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

    fn vfs_tree_path(&self) -> PathBuf {
        AppConfig::config_dir()
            .unwrap_or_else(|_| PathBuf::from(".p2pdrive"))
            .join("vfs_tree.enc")
    }

    fn drive_dir_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        home.join("MyceliumDrive")
    }

    fn load_vfs_tree(&self) -> network::VirtualTree {
        let path = self.vfs_tree_path();
        if path.exists() {
            if let Ok(bytes) = fs::read(&path) {
                if let Ok(tree) = network::VirtualTree::decrypt_tree(&bytes, &self.identity) {
                    return tree;
                }
            }
        }
        network::VirtualTree::new(&self.identity.to_did())
    }

    fn save_vfs_tree(&self, tree: &network::VirtualTree) -> Result<()> {
        let path = self.vfs_tree_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let enc_bytes = tree
            .encrypt_tree(&self.identity)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        fs::write(path, enc_bytes)?;
        Ok(())
    }

    fn collect_disk_files(dir: &Path, prefix: &str, out: &mut Vec<(String, PathBuf)>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = entry.file_name().to_string_lossy().to_string();
                if path.is_file() {
                    let rel = if prefix.is_empty() {
                        format!("/{}", file_name)
                    } else {
                        format!("{}/{}", prefix, file_name)
                    };
                    out.push((rel, path));
                } else if path.is_dir() {
                    let next_prefix = if prefix.is_empty() {
                        format!("/{}", file_name)
                    } else {
                        format!("{}/{}", prefix, file_name)
                    };
                    Self::collect_disk_files(&path, &next_prefix, out);
                }
            }
        }
    }

    fn detect_dirty_files(&self, tree: &network::VirtualTree) -> Vec<String> {
        let mut dirty = Vec::new();
        let drive_path = self.drive_dir_path();
        if !drive_path.exists() {
            let _ = fs::create_dir_all(&drive_path);
        }

        let mut disk_files = Vec::new();
        Self::collect_disk_files(&drive_path, "", &mut disk_files);

        for (vfs_path, path_on_disk) in &disk_files {
            match tree.find_file(vfs_path) {
                Some(file_node) => {
                    if let Ok(meta) = fs::metadata(path_on_disk) {
                        if meta.len() != file_node.size {
                            dirty.push(format!("Modified: {vfs_path}"));
                        }
                    }
                }
                None => {
                    dirty.push(format!("Added: {vfs_path}"));
                }
            }
        }

        for (vfs_path, _) in tree.list_all_files() {
            let rel = vfs_path.trim_start_matches('/');
            let disk_file = drive_path.join(rel);
            if !disk_file.exists() {
                dirty.push(format!("Deleted: {vfs_path}"));
            }
        }

        dirty
    }

    async fn handle_commit<W: AsyncWriteExt + Unpin>(
        &self,
        paths: Vec<String>,
        _message: Option<String>,
        writer: &mut W,
    ) -> Result<()> {
        let mut tree = self.load_vfs_tree();
        let drive_path = self.drive_dir_path();
        let mut committed_files = Vec::new();
        let mut total_bytes = 0u64;

        // Nếu paths rỗng, quét toàn bộ ~/MyceliumDrive/
        let target_paths = if paths.is_empty() {
            let mut list = Vec::new();
            let mut disk_files = Vec::new();
            Self::collect_disk_files(&drive_path, "", &mut disk_files);
            for (rel, _) in disk_files {
                list.push(rel);
            }
            list
        } else {
            paths
        };

        // Kiểm tra xem có phải First Atomic Commit (Merit=0, Shard=0) không
        let is_first_atomic_commit = {
            let qm = self.quota_manager.read().await;
            qm.my_uploaded_bytes == 0 && qm.stored_shard_bytes == 0
        };

        if is_first_atomic_commit {
            let mut total_batch_size = 0u64;
            for p in &target_paths {
                let clean_p = if p.starts_with('/') { p.clone() } else { format!("/{}", p) };
                let disk_path = if Path::new(&p).is_absolute() && Path::new(&p).exists() {
                    PathBuf::from(&p)
                } else {
                    drive_path.join(clean_p.trim_start_matches('/'))
                };
                if disk_path.exists() {
                    if let Ok(m) = fs::metadata(&disk_path) {
                        total_batch_size += m.len();
                    }
                }
            }

            if total_batch_size < quota_manager::FIRST_COMMIT_MIN_BYTES
                || total_batch_size > quota_manager::FIRST_COMMIT_MAX_BYTES
            {
                let total_mb = total_batch_size as f64 / (1024.0 * 1024.0);
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!(
                        "Lần commit đầu tiên yêu cầu tổng dung lượng từ 10 MB đến 40 MB để kích hoạt hệ sinh thái P2P (Hiện tại: {:.2} MB). Vui lòng thêm/bớt tệp tin phù hợp vào ~/MyceliumDrive.",
                        total_mb
                    )),
                ).await?;
                return Ok(());
            }
        }

        for p in target_paths {
            let clean_p = if p.starts_with('/') { p.clone() } else { format!("/{}", p) };
            let disk_path = if Path::new(&p).is_absolute() && Path::new(&p).exists() {
                PathBuf::from(&p)
            } else {
                drive_path.join(clean_p.trim_start_matches('/'))
            };

            if !disk_path.exists() {
                // Nếu file bị xóa trên đĩa -> Xóa khỏi VFS Tree
                if let Ok(Some(network::VfsEntry::File(f))) = tree.remove_path(&clean_p) {
                    let mut qm = self.quota_manager.write().await;
                    qm.record_delete(f.size);
                    if let Ok(p) = AppConfig::config_dir() {
                        let _ = qm.save_to_file(&p.join("quota.json"));
                    }
                    committed_files.push(format!("Deleted: {}", clean_p));
                }
                continue;
            }

            let meta = fs::metadata(&disk_path)?;
            let file_size = meta.len();

            // 1. Kiểm tra Quota / Atomic Ingest (chỉ kiểm tra nếu không phải first atomic batch)
            if !is_first_atomic_commit {
                let qm_guard = self.quota_manager.read().await;
                let needed_ingest = qm_guard.calculate_required_ingest_for_upload(file_size);
                drop(qm_guard);

                if needed_ingest > 0 {
                    Self::send_response(
                        writer,
                        &IpcResponse::Progress {
                            step: "atomic_ingest".to_string(),
                            current: 10,
                            total: 100,
                            message: format!("Đang nhận {} MB shard từ mạng để mở khóa upload...", needed_ingest / (1024 * 1024)),
                        },
                    ).await?;
                }
            }

            // 2. Đọc file
            let mut file = File::open(&disk_path)?;
            let mut raw_data = Vec::with_capacity(file_size as usize);
            file.read_to_end(&mut raw_data)?;

            // 3. Sinh AES-256 Key ngẫu nhiên và mã hóa
            let mut enc_key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut enc_key);
            let encrypted_data = match encrypt_data(&raw_data, &enc_key) {
                Ok(data) => data,
                Err(e) => {
                    Self::send_response(writer, &IpcResponse::Error(format!("Lỗi mã hóa: {e}"))).await?;
                    return Ok(());
                }
            };
            let enc_cid = compute_cid(&encrypted_data);

            // 4. Erasure Coding 1:4 (k=10, n=40 default trong erasure-codec)
            let (manifest, shards) = match encode_with_name(&clean_p, &encrypted_data) {
                Ok(res) => res,
                Err(e) => {
                    Self::send_response(writer, &IpcResponse::Error(format!("Lỗi Erasure Coding: {e}"))).await?;
                    return Ok(());
                }
            };

            // 5. Phân tán Shard qua P2P Network
            Self::send_response(
                writer,
                &IpcResponse::Progress {
                    step: "distributing".to_string(),
                    current: 60,
                    total: 100,
                    message: format!("Đang phân tán {} shards cho {}...", manifest.n_total_shards, clean_p),
                },
            ).await?;

            if let Err(e) = self.service.distribute_shards_with_file(shards, Some(enc_cid.clone())).await {
                Self::send_response(writer, &IpcResponse::Error(format!("Lỗi phân tán shards: {e}"))).await?;
                return Ok(());
            }

            // 6. Cập nhật VirtualTree
            let file_node = network::FileNode {
                name: clean_p.split('/').last().unwrap_or("file").to_string(),
                size: file_size,
                encrypted_cid: enc_cid,
                encryption_key_hex: hex::encode(enc_key),
                k_data_shards: manifest.k_data_shards,
                n_total_shards: manifest.n_total_shards,
                shard_hashes: manifest.shard_hashes,
                updated_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
            };

            tree.insert_file(&clean_p, file_node).map_err(|e| anyhow::anyhow!("{e}"))?;
            
            if !is_first_atomic_commit {
                let mut qm = self.quota_manager.write().await;
                let _ = qm.record_upload(file_size);
                if let Ok(p) = AppConfig::config_dir() {
                    let _ = qm.save_to_file(&p.join("quota.json"));
                }
            }

            committed_files.push(format!("Committed: {}", clean_p));
            total_bytes += file_size;

            Self::send_response(
                writer,
                &IpcResponse::Progress {
                    step: "completed".to_string(),
                    current: 100,
                    total: 100,
                    message: format!("Cập nhật Cây ảo (VirtualTree) & Hoàn tất cho {}!", clean_p),
                },
            ).await?;
        }

        if is_first_atomic_commit && total_bytes > 0 {
            let mut qm = self.quota_manager.write().await;
            qm.my_uploaded_bytes = total_bytes;
            qm.stored_shard_bytes = total_bytes.saturating_mul(4);
            if let Ok(p) = AppConfig::config_dir() {
                let _ = qm.save_to_file(&p.join("quota.json"));
            }
        }

        self.save_vfs_tree(&tree)?;
        let r_ratio = self.quota_manager.read().await.current_r_ratio();

        Self::send_response(
            writer,
            &IpcResponse::CommitSuccess {
                committed_files,
                total_bytes,
                r_ratio,
            },
        ).await?;

        Ok(())
    }

    async fn handle_vfs_list<W: AsyncWriteExt + Unpin>(&self, _path: Option<String>, writer: &mut W) -> Result<()> {
        let tree = self.load_vfs_tree();
        let files = tree.list_all_files();
        let entries = files
            .into_iter()
            .map(|(p, f)| format!("{} ({:.2} MB)", p, f.size as f64 / (1024.0 * 1024.0)))
            .collect();

        Self::send_response(writer, &IpcResponse::VfsListSuccess { entries }).await?;
        Ok(())
    }

    async fn handle_vfs_tree<W: AsyncWriteExt + Unpin>(&self, _path: Option<String>, writer: &mut W) -> Result<()> {
        let tree = self.load_vfs_tree();
        let tree_rendered = tree.render_tree();
        Self::send_response(writer, &IpcResponse::VfsTreeSuccess { tree_rendered }).await?;
        Ok(())
    }

    async fn handle_vfs_remove<W: AsyncWriteExt + Unpin>(&self, path: String, writer: &mut W) -> Result<()> {
        let mut tree = self.load_vfs_tree();
        let clean_p = if path.starts_with('/') { path.clone() } else { format!("/{}", path) };

        match tree.remove_path(&clean_p) {
            Ok(Some(entry)) => {
                let freed_bytes = match entry {
                    network::VfsEntry::File(f) => {
                        self.quota_manager.write().await.record_delete(f.size);
                        f.size
                    }
                    network::VfsEntry::Dir(_) => 0,
                };

                self.save_vfs_tree(&tree)?;
                let r_ratio = self.quota_manager.read().await.current_r_ratio();

                Self::send_response(
                    writer,
                    &IpcResponse::VfsRemoveSuccess {
                        path: clean_p,
                        freed_bytes,
                        r_ratio,
                    },
                ).await?;
            }
            _ => {
                Self::send_response(writer, &IpcResponse::Error(format!("Không tìm thấy đường dẫn '{}'", clean_p))).await?;
            }
        }

        Ok(())
    }

    async fn handle_vfs_download<W: AsyncWriteExt + Unpin>(
        &self,
        vfs_path: String,
        output_path: Option<String>,
        writer: &mut W,
    ) -> Result<()> {
        let tree = self.load_vfs_tree();
        let clean_p = if vfs_path.starts_with('/') { vfs_path.clone() } else { format!("/{}", vfs_path) };

        let file_node = match tree.find_file(&clean_p) {
            Some(f) => f.clone(),
            None => {
                Self::send_response(writer, &IpcResponse::Error(format!("Tệp '{}' không tồn tại trong Drive", clean_p))).await?;
                return Ok(());
            }
        };

        let target_out = match output_path {
            Some(p) => PathBuf::from(p),
            None => {
                let file_name = clean_p.split('/').last().unwrap_or("downloaded.bin");
                PathBuf::from(file_name)
            }
        };

        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "fetching_shards".to_string(),
                current: 20,
                total: 100,
                message: format!("Đang kéo tối thiểu {}/{} shards từ P2P Network...", file_node.k_data_shards, file_node.n_total_shards),
            },
        ).await?;

        let collected_shards = match self
            .service
            .fetch_shards_parallel(file_node.shard_hashes.clone(), file_node.k_data_shards)
            .await
        {
            Ok(shards) => shards,
            Err(e) => {
                Self::send_response(writer, &IpcResponse::Error(format!("Lỗi khi kéo shards: {e}"))).await?;
                return Ok(());
            }
        };

        let manifest = erasure_codec::FileManifest {
            file_name: file_node.name.clone(),
            original_size: 0,
            original_hash: file_node.encrypted_cid.clone(),
            k_data_shards: file_node.k_data_shards,
            n_total_shards: file_node.n_total_shards,
            shard_hashes: file_node.shard_hashes.clone(),
        };

        let mut sparse_shards = vec![None; file_node.n_total_shards];
        for shard in collected_shards {
            let idx = shard.index;
            if idx < sparse_shards.len() {
                sparse_shards[idx] = Some(shard);
            }
        }

        let recovered_encrypted = match decode(&manifest, sparse_shards) {
            Ok(data) => data,
            Err(e) => {
                Self::send_response(writer, &IpcResponse::Error(format!("Lỗi giải mã Reed-Solomon: {e}"))).await?;
                return Ok(());
            }
        };

        let key_bytes = hex::decode(&file_node.encryption_key_hex)?;
        let mut enc_key = [0u8; 32];
        enc_key.copy_from_slice(&key_bytes);

        let decrypted_data = match decrypt_data(&recovered_encrypted, &enc_key) {
            Ok(data) => data,
            Err(e) => {
                Self::send_response(writer, &IpcResponse::Error(format!("Lỗi giải mã AES: {e}"))).await?;
                return Ok(());
            }
        };

        if let Some(parent) = target_out.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut out_f = File::create(&target_out)?;
        out_f.write_all(&decrypted_data)?;
        out_f.flush()?;

        Self::send_response(
            writer,
            &IpcResponse::DownloadSuccess {
                file_name: file_node.name,
                bytes_written: decrypted_data.len(),
                output_path: target_out.to_string_lossy().to_string(),
            },
        ).await?;

        Ok(())
    }

    async fn handle_dump<W: AsyncWriteExt + Unpin>(
        &self,
        private_key: Option<String>,
        output_dir: String,
        vfs_path: Option<String>,
        writer: &mut W,
    ) -> Result<()> {
        // 1. Phục hồi Identity từ private_key (hex, file, hoặc mặc định từ Daemon)
        let identity = match private_key {
            Some(ref k) if !k.is_empty() => {
                if let Ok(id) = Identity::from_secret_hex(k) {
                    id
                } else if let Ok(id) = Identity::load_from_file(&PathBuf::from(k)) {
                    id
                } else {
                    Self::send_response(
                        writer,
                        &IpcResponse::Error(format!("Không thể nạp khóa bí mật từ: '{}'. Vui lòng kiểm tra chuỗi hex hoặc đường dẫn file identity.json hợp lệ.", k)),
                    ).await?;
                    return Ok(());
                }
            }
            _ => self.identity.clone(),
        };

        // 2. Đọc file vfs_tree.enc
        let tree_file = match vfs_path {
            Some(p) => PathBuf::from(p),
            None => self.vfs_tree_path(),
        };

        if !tree_file.exists() {
            Self::send_response(
                writer,
                &IpcResponse::Error(format!("Không tìm thấy file cây thư mục ảo tại {:?}", tree_file)),
            ).await?;
            return Ok(());
        }

        let encrypted_vfs_bytes = match fs::read(&tree_file) {
            Ok(bytes) => bytes,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Không thể đọc file vfs_tree.enc: {e}")),
                ).await?;
                return Ok(());
            }
        };

        // 3. Giải mã VirtualTree bằng Identity
        let tree = match network::VirtualTree::decrypt_tree(&encrypted_vfs_bytes, &identity) {
            Ok(t) => t,
            Err(e) => {
                Self::send_response(
                    writer,
                    &IpcResponse::Error(format!("Giải mã VirtualTree thất bại! Khóa bí mật không khớp với danh tính đã mã hóa cây thư mục này ({e}).")),
                ).await?;
                return Ok(());
            }
        };

        let files = tree.list_all_files();
        if files.is_empty() {
            Self::send_response(
                writer,
                &IpcResponse::Error("Cây thư mục ảo không chứa tệp tin nào để khôi phục".to_string()),
            ).await?;
            return Ok(());
        }

        let out_dir = PathBuf::from(&output_dir);
        let _ = fs::create_dir_all(&out_dir);

        let total_files = files.len();
        let mut restored_count = 0;
        let mut total_bytes = 0u64;

        Self::send_response(
            writer,
            &IpcResponse::Progress {
                step: "dump_start".to_string(),
                current: 0,
                total: total_files as u64,
                message: format!("Bắt đầu khôi phục {} tệp tin cho DID {}...", total_files, identity.to_did()),
            },
        ).await?;

        for (idx, (vfs_path, file_node)) in files.iter().enumerate() {
            Self::send_response(
                writer,
                &IpcResponse::Progress {
                    step: "dumping_file".to_string(),
                    current: (idx + 1) as u64,
                    total: total_files as u64,
                    message: format!("({}/{}) Đang kéo shards và giải mã {} ({:.2} MB)...", idx + 1, total_files, vfs_path, file_node.size as f64 / (1024.0 * 1024.0)),
                },
            ).await?;

            let collected_shards = match self
                .service
                .fetch_shards_parallel(file_node.shard_hashes.clone(), file_node.k_data_shards)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    warn!("Không thể kéo đủ shards cho file {}: {}", vfs_path, e);
                    continue;
                }
            };

            let manifest = erasure_codec::FileManifest {
                file_name: file_node.name.clone(),
                original_size: file_node.size as usize,
                original_hash: String::new(),
                k_data_shards: file_node.k_data_shards,
                n_total_shards: file_node.n_total_shards,
                shard_hashes: file_node.shard_hashes.clone(),
            };

            let mut sparse_shards = vec![None; file_node.n_total_shards];
            for shard in collected_shards {
                let s_idx = shard.index;
                if s_idx < sparse_shards.len() {
                    sparse_shards[s_idx] = Some(shard);
                }
            }

            let recovered_encrypted_data = match decode(&manifest, sparse_shards) {
                Ok(data) => data,
                Err(e) => {
                    warn!("Lỗi decode Reed-Solomon cho file {}: {}", vfs_path, e);
                    continue;
                }
            };

            let enc_key = match hex::decode(&file_node.encryption_key_hex) {
                Ok(k) if k.len() == 32 => {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&k);
                    arr
                }
                _ => {
                    warn!("Khóa mã hóa file {} không hợp lệ", vfs_path);
                    continue;
                }
            };

            let decrypted_data = match decrypt_data(&recovered_encrypted_data, &enc_key) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Lỗi giải mã AES-GCM cho file {}: {}", vfs_path, e);
                    continue;
                }
            };

            let clean_rel_path = vfs_path.trim_start_matches('/');
            let target_out = out_dir.join(clean_rel_path);

            if let Some(parent) = target_out.parent() {
                let _ = fs::create_dir_all(parent);
            }

            if let Ok(mut out_f) = File::create(&target_out) {
                if out_f.write_all(&decrypted_data).is_ok() {
                    let _ = out_f.flush();
                    restored_count += 1;
                    total_bytes += decrypted_data.len() as u64;
                }
            }
        }

        Self::send_response(
            writer,
            &IpcResponse::DumpSuccess {
                restored_files_count: restored_count,
                total_bytes,
                output_dir,
            },
        ).await?;

        Ok(())
    }

    async fn handle_status<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        let tree = self.load_vfs_tree();
        let total_tree_size: u64 = tree.list_all_files().iter().map(|(_, f)| f.size).sum();
        let total_blockstore_bytes = self.blockstore.total_payload_bytes().unwrap_or(0);

        let mut qm = self.quota_manager.write().await;
        let mut need_save = false;

        // 1. Đồng bộ my_uploaded_bytes với VFS Tree
        if total_tree_size == 0 {
            if qm.my_uploaded_bytes > 0 {
                qm.my_uploaded_bytes = 0;
                need_save = true;
            }
        } else if qm.my_uploaded_bytes != total_tree_size {
            qm.my_uploaded_bytes = total_tree_size;
            need_save = true;
        }

        // 2. Đồng bộ stored_shard_bytes chính xác với BlockStore thực tế
        if total_blockstore_bytes == 0 {
            if qm.stored_shard_bytes > 0 {
                qm.stored_shard_bytes = 0;
                need_save = true;
            }
        } else if qm.stored_shard_bytes != total_blockstore_bytes {
            qm.stored_shard_bytes = total_blockstore_bytes;
            need_save = true;
        }

        if need_save {
            if let Ok(p) = AppConfig::config_dir() {
                let _ = qm.save_to_file(&p.join("quota.json"));
            }
        }

        let merit_mb = qm.my_uploaded_bytes as f64 / (1024.0 * 1024.0);
        let stored_shards_mb = qm.stored_shard_bytes as f64 / (1024.0 * 1024.0);
        let r_ratio = qm.current_r_ratio();
        let r_status = match r_ratio {
            None => "⚡ Sẵn sàng cho First Atomic Commit (R = N/A)".to_string(),
            Some(r) if r < 4.0 => "🔴 Đang đói Shard (R < 4.0 - Cần nhận thêm Shard)".to_string(),
            Some(r) if r <= 5.0 => "🟢 Cân bằng lý tưởng (4.0 <= R <= 5.0 - Sẵn sàng Upload)".to_string(),
            Some(_) => "🟡 No nê / Chạm trần Cache (R > 5.0 - Tạm dừng nhận Cache)".to_string(),
        };
        drop(qm);

        let shard_count = self.blockstore.count_shards();
        let disk_used_mb = self.blockstore.total_payload_bytes().unwrap_or(0) as f64 / (1024.0 * 1024.0);

        let connected_peers = self.service.get_connected_peers().await.unwrap_or_default();
        let known_peers = self.service.get_known_peers().await.unwrap_or_default();
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
        let tree = self.load_vfs_tree();
        let dirty_files = self.detect_dirty_files(&tree);

        let status_info = DaemonStatusInfo {
            did: self.identity.to_did(),
            is_private_swarm: self.swarm_key.is_some(),
            swarm_key_preview,
            rendezvous_url: self.config.rendezvous_url.clone(),
            rendezvous_online,
            shard_count,
            disk_used_mb,
            merit_mb,
            stored_shards_mb,
            r_ratio,
            r_status,
            connected_peers_count: connected_peers.len(),
            known_peers_count: known_peers.len(),
            listen_addrs: listeners.into_iter().map(|a| a.to_string()).collect(),
            dirty_files,
        };

        Self::send_response(writer, &IpcResponse::StatusSuccess(status_info)).await?;
        Ok(())
    }
}
