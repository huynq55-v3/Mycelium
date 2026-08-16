use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG_DIR: &str = ".p2pdrive";
pub const DEFAULT_CONFIG_FILE: &str = "config.json";
pub const DEFAULT_RENDEZVOUS_URL: &str = "https://p2p-rendezvous.deno.dev";
pub const DEFAULT_PORT: u16 = 4001;
pub const DEFAULT_CONTRIBUTE_GB: u64 = 60;

/// Cấu hình chung của node P2P Drive lưu trữ tại `~/.p2pdrive/config.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub rendezvous_url: String,
    pub port: u16,
    pub contribute_gb: u64,
    pub is_private_swarm: bool,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            rendezvous_url: DEFAULT_RENDEZVOUS_URL.to_string(),
            port: DEFAULT_PORT,
            contribute_gb: DEFAULT_CONTRIBUTE_GB,
            is_private_swarm: false,
        }
    }
}

impl AppConfig {
    /// Lấy đường dẫn thư mục gốc `~/.p2pdrive`.
    pub fn config_dir() -> Result<PathBuf> {
        let home = dirs::home_dir().context("Không tìm thấy thư mục Home người dùng")?;
        Ok(home.join(DEFAULT_CONFIG_DIR))
    }

    /// Lấy đường dẫn file `~/.p2pdrive/config.json`.
    pub fn config_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join(DEFAULT_CONFIG_FILE))
    }

    /// Nạp cấu hình từ đĩa hoặc trả về cấu hình mặc định nếu chưa tồn tại.
    pub fn load_or_default() -> Self {
        if let Ok(path) = Self::config_path() {
            if path.exists() {
                if let Ok(mut file) = File::open(&path) {
                    let mut content = String::new();
                    if file.read_to_string(&mut content).is_ok() {
                        if let Ok(cfg) = serde_json::from_str(&content) {
                            return cfg;
                        }
                    }
                }
            }
        }
        Self::default()
    }

    /// Lưu cấu hình ra file `config.json`.
    pub fn save(&self) -> Result<()> {
        let dir = Self::config_dir()?;
        fs::create_dir_all(&dir)?;

        let path = Self::config_path()?;
        let json = serde_json::to_string_pretty(self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}
