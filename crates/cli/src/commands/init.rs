use std::path::PathBuf;

use anyhow::{Context, Result};
use core_crypto::{Identity, SwarmKey};
use quota_manager::QuotaManager;
use tracing::info;

use crate::config::{AppConfig, DEFAULT_CONTRIBUTE_GB, DEFAULT_PORT, DEFAULT_RENDEZVOUS_URL};

pub fn handle_init(
    swarm_key_path: Option<PathBuf>,
    contribute_gb: Option<u64>,
    rendezvous_url: Option<String>,
) -> Result<()> {
    let config_dir = AppConfig::config_dir()?;
    std::fs::create_dir_all(&config_dir)
        .with_context(|| format!("Không thể tạo thư mục cấu hình tại {:?}", config_dir))?;

    println!("============================================================");
    println!("     🌿 KHỞI TẠO NỀN TẢNG NODE MYCELIUM P2P DRIVE 🌿         ");
    println!("============================================================");
    println!("📁 Thư mục cấu hình: {:?}", config_dir);

    // 1. Quản lý Identity
    let identity_path = config_dir.join("identity.json");
    let identity = if identity_path.exists() {
        println!("🔑 Tìm thấy Identity hiện có tại {:?}", identity_path);
        Identity::load_from_file(&identity_path)?
    } else {
        println!("✨ Đang sinh cặp khóa Ed25519 Identity mới...");
        let id = Identity::generate();
        id.save_to_file(&identity_path)?;
        println!("✅ Đã lưu Identity vào {:?}", identity_path);
        id
    };

    println!("🆔 Decentralized Identifier (DID):");
    println!("   \x1b[1;32m{}\x1b[0m", identity.to_did());

    // 2. Quản lý Swarm Key
    let swarm_key_dest = config_dir.join("swarm.key");
    let (swarm_key, is_private) = if let Some(path) = swarm_key_path {
        println!("🔒 Đang nạp Private Swarm Key từ {:?}", path);
        let key = SwarmKey::load_from_file(&path)
            .with_context(|| format!("Không thể đọc Swarm Key từ {:?}", path))?;
        key.save_to_file(&swarm_key_dest)?;
        println!("✅ Đã thiết lập Private Swarm Key tại {:?}", swarm_key_dest);
        (key, true)
    } else if swarm_key_dest.exists() {
        let key = SwarmKey::load_from_file(&swarm_key_dest)?;
        (key, true)
    } else {
        println!("🌐 Chưa chỉ định Swarm Key riêng -> Tự động sinh Private Swarm Key mới.");
        let key = SwarmKey::generate();
        key.save_to_file(&swarm_key_dest)?;
        println!("✅ Đã lưu Swarm Key mới tại {:?}", swarm_key_dest);
        (key, true)
    };

    println!("🔑 Swarm Key Hex: {}...", &swarm_key.to_hex()[..16]);

    // 3. Quản lý Quota
    let gb = contribute_gb.unwrap_or(DEFAULT_CONTRIBUTE_GB);
    let allocated_bytes = gb * 1024 * 1024 * 1024;
    let quota_path = config_dir.join("quota.json");

    let quota_manager = if quota_path.exists() {
        let mut qm = QuotaManager::load_from_file(&quota_path)?;
        qm.allocated_disk_bytes = allocated_bytes;
        qm.save_to_file(&quota_path)?;
        qm
    } else {
        let qm = QuotaManager::new(allocated_bytes);
        qm.save_to_file(&quota_path)?;
        qm
    };

    let allowed_upload_gb = quota_manager.allowed_upload_capacity() as f64 / (1024.0 * 1024.0 * 1024.0);
    println!("💾 Cam kết chia sẻ ổ cứng: \x1b[1;36m{} GB\x1b[0m", gb);
    println!("🚀 Quyền tải lên an toàn (Tỷ lệ 1:4): \x1b[1;32m{:.2} GB\x1b[0m", allowed_upload_gb);

    // 4. Lưu AppConfig
    let rendezvous = rendezvous_url.unwrap_or_else(|| DEFAULT_RENDEZVOUS_URL.to_string());
    let config = AppConfig {
        rendezvous_url: rendezvous.clone(),
        port: DEFAULT_PORT,
        contribute_gb: gb,
        is_private_swarm: is_private,
    };
    config.save()?;

    println!("📡 Rendezvous Bootstrap URL: \x1b[1;34m{}\x1b[0m", rendezvous);
    println!("============================================================");
    println!("🎉 Khởi tạo hoàn tất! Bạn có thể bắt đầu node bằng lệnh:");
    println!("   \x1b[1;33mp2pdrive daemon\x1b[0m");
    println!("============================================================");

    info!("Khởi tạo node hoàn tất");
    Ok(())
}
