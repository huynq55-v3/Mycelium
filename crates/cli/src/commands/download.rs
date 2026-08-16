use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use blockstore::BlockStore;
use core_crypto::{decrypt_data, Identity, SwarmKey};
use erasure_codec::decode;
use indicatif::{ProgressBar, ProgressStyle};
use network::{P2PService, RendezvousClient};
use quota_manager::QuotaManager;
use tokio::sync::RwLock;

use crate::config::AppConfig;
use crate::manifest::EncryptedFilePackage;

pub async fn handle_download(manifest_path: PathBuf, output_path: PathBuf) -> Result<()> {
    if !manifest_path.exists() {
        bail!("File manifest không tồn tại: {:?}", manifest_path);
    }

    println!("============================================================");
    println!("          📥 TẢI VỀ VÀ KHÔI PHỤC TỆP TIN TỪ P2P             ");
    println!("============================================================");

    // 1. Đọc file manifest
    let package = EncryptedFilePackage::load_from_file(&manifest_path)?;
    println!("📄 Tên tệp gốc: \x1b[1;36m{}\x1b[0m", package.file_name);
    println!("📊 Kích thước kỳ vọng: \x1b[1;33m{} bytes\x1b[0m", package.original_size);
    println!("🔑 CID: \x1b[1;34m{}\x1b[0m", package.encrypted_cid);

    let config_dir = AppConfig::config_dir()?;
    let config = AppConfig::load_or_default();

    // 2. Khởi tạo P2P Service
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
    let quota_manager = Arc::new(RwLock::new(QuotaManager::default_60gb()));

    let (service, _handle) = P2PService::new(
        &identity,
        swarm_key.as_ref(),
        blockstore.clone(),
        quota_manager,
    )?;

    // Bootstrap peers
    let rendezvous = RendezvousClient::new(&config.rendezvous_url);
    let _ = service.bootstrap_from_rendezvous(&rendezvous, 15).await;

    // 3. Gom song song tối thiểu 10 shards từ mạng lưới
    let target_count = package.erasure_manifest.k_data_shards; // 10 shards
    let pb = ProgressBar::new(target_count as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.green/black}] {pos}/{len} shards ({msg})")
            .expect("style"),
    );
    pb.set_message("Đang truy vấn song song K=10 shards từ DHT...");

    let shard_hashes = package.erasure_manifest.shard_hashes.clone();
    let collected_shards = service
        .fetch_shards_parallel(shard_hashes, target_count)
        .await
        .context("Không thể thu thập đủ 10 shards từ mạng lưới P2P")?;

    pb.set_position(collected_shards.len() as u64);
    pb.finish_with_message(format!(
        "✅ Đã thu thập đủ {}/{} shards!",
        collected_shards.len(),
        target_count
    ));

    // 4. Chuẩn bị mảng 40 Options cho Reed-Solomon decode
    let mut sparse_shards = vec![None; package.erasure_manifest.n_total_shards];
    for shard in collected_shards {
        let idx = shard.index;
        if idx < sparse_shards.len() {
            sparse_shards[idx] = Some(shard);
        }
    }

    // 5. Giải mã Reed-Solomon để khôi phục dữ liệu mã hóa gốc
    let pb_dec = ProgressBar::new_spinner();
    pb_dec.set_message("Đang tái tạo dữ liệu qua Reed-Solomon decode...");
    pb_dec.enable_steady_tick(Duration::from_millis(80));

    let recovered_encrypted_data = decode(&package.erasure_manifest, sparse_shards)
        .map_err(|e| anyhow::anyhow!("Lỗi giải mã Reed-Solomon: {}", e))?;

    // 6. Giải mã AES-256-GCM
    pb_dec.set_message("Đang giải mã AES-256-GCM bằng Secret Key...");
    let key_bytes = hex::decode(&package.encryption_key_hex)
        .context("Khóa AES trong manifest không hợp lệ")?;
    if key_bytes.len() != 32 {
        bail!("Độ dài khóa AES không đúng 32 bytes");
    }
    let mut enc_key = [0u8; 32];
    enc_key.copy_from_slice(&key_bytes);

    let decrypted_raw_data = decrypt_data(&recovered_encrypted_data, &enc_key)
        .map_err(|e| anyhow::anyhow!("Lỗi giải mã AES: {}", e))?;

    // Xác thực tính toàn vẹn của dữ liệu gốc
    let recovered_hash = erasure_codec::sha256_hex(&decrypted_raw_data);
    if recovered_hash != package.original_hash {
        bail!("Mã băm dữ liệu khôi phục không khớp với dữ liệu gốc ban đầu!");
    }

    pb_dec.finish_with_message("✅ Giải mã và xác thực tính toàn vẹn 100% thành công!");

    // 7. Ghi dữ liệu ra tệp đích
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out_file = File::create(&output_path)
        .with_context(|| format!("Không thể tạo tệp đích tại {:?}", output_path))?;
    out_file.write_all(&decrypted_raw_data)?;
    out_file.flush()?;

    println!("============================================================");
    println!("🎉 KHÔI PHỤC VÀ TẢI VỀ THÀNH CÔNG!");
    println!("📁 Đã lưu tại: \x1b[1;32m{:?}\x1b[0m", output_path);
    println!("📊 Dung lượng: \x1b[1;33m{} bytes\x1b[0m", decrypted_raw_data.len());
    println!("============================================================");

    Ok(())
}
