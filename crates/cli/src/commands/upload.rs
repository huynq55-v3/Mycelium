use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use blockstore::BlockStore;
use core_crypto::{compute_cid, encrypt_data, Identity, SwarmKey};
use erasure_codec::encode_with_name;
use indicatif::{ProgressBar, ProgressStyle};
use network::{P2PService, RendezvousClient};
use quota_manager::QuotaManager;
use rand::RngCore;
use tokio::sync::RwLock;

use crate::config::AppConfig;
use crate::manifest::EncryptedFilePackage;

pub async fn handle_upload(file_path: PathBuf) -> Result<()> {
    if !file_path.exists() {
        bail!("Tệp tin nguồn không tồn tại: {:?}", file_path);
    }

    let file_metadata = fs::metadata(&file_path)
        .with_context(|| format!("Không thể đọc thông tin tệp {:?}", file_path))?;
    let file_size = file_metadata.len();
    let file_name = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed.bin")
        .to_string();

    println!("============================================================");
    println!("           📤 TẢI LÊN TỆP TIN VÀO MẠNG P2P DRIVE            ");
    println!("============================================================");
    println!("📄 Tên tệp: \x1b[1;36m{}\x1b[0m", file_name);
    println!("📊 Kích thước: \x1b[1;33m{} bytes\x1b[0m ({:.2} MB)", file_size, file_size as f64 / (1024.0 * 1024.0));

    let config_dir = AppConfig::config_dir()?;
    let config = AppConfig::load_or_default();

    // 1. Kiểm tra hạn ngạch tải lên QuotaManager
    let quota_path = config_dir.join("quota.json");
    let mut quota_manager = if quota_path.exists() {
        QuotaManager::load_from_file(&quota_path)?
    } else {
        QuotaManager::default_60gb()
    };

    if let Err(e) = quota_manager.validate_upload(file_size) {
        println!("\x1b[1;31m❌ Tải lên bị từ chối:\x1b[0m {}", e);
        bail!("Vượt quá hạn ngạch cho phép");
    }

    // 2. Đọc dữ liệu tệp
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .expect("style"),
    );
    pb.set_message("Đang đọc dữ liệu tệp vào bộ nhớ...");
    pb.enable_steady_tick(Duration::from_millis(80));

    let mut file = File::open(&file_path)?;
    let mut raw_data = Vec::with_capacity(file_size as usize);
    file.read_to_end(&mut raw_data)?;
    let original_hash = erasure_codec::sha256_hex(&raw_data);

    // 3. Sinh khóa ngẫu nhiên AES-256 (32 bytes) và mã hóa đối xứng
    pb.set_message("Đang mã hóa AES-256-GCM bảo mật đầu cuối (End-to-End Encryption)...");
    let mut enc_key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut enc_key);

    let encrypted_payload = encrypt_data(&raw_data, &enc_key)
        .map_err(|e| anyhow::anyhow!("Lỗi mã hóa dữ liệu: {}", e))?;
    let encrypted_cid = compute_cid(&encrypted_payload);

    // 4. Bẻ phân đoạn Reed-Solomon (10 Data + 30 Parity = 40 Shards)
    pb.set_message("Đang áp dụng Reed-Solomon Erasure Coding (K=10, N=40)...");
    let (erasure_manifest, shards) = encode_with_name(&file_name, &encrypted_payload)
        .map_err(|e| anyhow::anyhow!("Lỗi phân đoạn Reed-Solomon: {}", e))?;

    pb.finish_with_message("✅ Mã hóa và phân đoạn hoàn tất.");

    // 5. Kết nối P2P Node để phân tán 40 Shards
    let pb_dist = ProgressBar::new(shards.len() as u64);
    pb_dist.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} shards ({msg})")
            .expect("style"),
    );
    pb_dist.set_message("Đang phân phối shards lên mạng P2P");

    let identity_path = config_dir.join("identity.json");
    let identity = if identity_path.exists() {
        Identity::load_from_file(&identity_path)?
    } else {
        Identity::generate()
    };

    let swarm_key_path = config_dir.join("swarm.key");
    let swarm_key = if swarm_key_path.exists() {
        Some(SwarmKey::load_from_file(&swarm_key_path)?)
    } else {
        None
    };

    let blockstore_path = config_dir.join("blockstore");
    let blockstore = BlockStore::open(&blockstore_path)?;
    let qm_arc = Arc::new(RwLock::new(quota_manager.clone()));

    let (service, _handle) = P2PService::new(
        &identity,
        swarm_key.as_ref(),
        blockstore.clone(),
        qm_arc,
    )?;

    // Bootstrap từ Rendezvous nếu có
    let rendezvous = RendezvousClient::new(&config.rendezvous_url);
    let _ = service.bootstrap_from_rendezvous(&rendezvous, 10).await;

    // Phân phối shards lên mạng
    service.distribute_shards(shards).await?;
    pb_dist.finish_with_message("✅ Hoàn thành phân tán 40 shards");

    // 6. Ghi nhận hạn ngạch đã dùng và lưu lại
    quota_manager.record_upload(file_size)?;
    quota_manager.save_to_file(&quota_path)?;

    // 7. Xuất file manifest
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

    println!("============================================================");
    println!("🎉 TẢI LÊN THÀNH CÔNG!");
    println!("🔑 Content Identifier (CID): \x1b[1;32m{}\x1b[0m", encrypted_cid);
    println!("📜 File Manifest: \x1b[1;33m{:?}\x1b[0m", manifest_path);
    println!("💡 Hãy giữ cẩn thận file manifest để giải mã và tải về sau này!");
    println!("============================================================");

    Ok(())
}
