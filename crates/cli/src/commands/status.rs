use anyhow::{bail, Result};

use crate::ipc::protocol::{IpcRequest, IpcResponse};
use crate::ipc::IpcClient;

pub async fn handle_status() -> Result<()> {
    println!("============================================================");
    println!("          📊 TRẠNG THÁI HỆ THỐNG MYCELIUM P2P DRIVE         ");
    println!("============================================================");

    let mut final_error = None;

    IpcClient::send_request(IpcRequest::GetStatus, |response| {
        match response {
            IpcResponse::StatusSuccess(info) => {
                println!("🆔 Identity DID  : \x1b[1;32m{}\x1b[0m", info.did);

                if info.is_private_swarm {
                    let preview = info.swarm_key_preview.as_deref().unwrap_or("********");
                    println!("🔒 Swarm Grid     : \x1b[1;33mPrivate Network\x1b[0m (Key: {}...)", preview);
                } else {
                    println!("🌐 Swarm Grid     : \x1b[1;34mPublic Open Network\x1b[0m");
                }

                let status_str = if info.rendezvous_online {
                    "\x1b[1;32m[ONLINE]\x1b[0m"
                } else {
                    "\x1b[1;33m[UNREACHABLE / mDNS LOCAL]\x1b[0m"
                };
                println!("📡 Rendezvous     : {} {}", info.rendezvous_url, status_str);

                println!("🗄️ BlockStore      : {} shards lưu trữ ({:.2} MB trên đĩa)",
                    info.shard_count, info.disk_used_mb);

                println!("📦 Đã Upload (Merit) : \x1b[1;32m{:.2} MB\x1b[0m", info.merit_mb);
                println!("🛡️ Shard Đang Giữ    : \x1b[1;36m{:.2} MB\x1b[0m", info.stored_shards_mb);
                let r_ratio_str = match info.r_ratio {
                    Some(r) => format!("\x1b[1;33m{:.3}\x1b[0m (Shard / Merit)", r),
                    None => "\x1b[1;33mN/A\x1b[0m".to_string(),
                };
                println!("📈 Chỉ số cống hiến R: {} | {}", r_ratio_str, info.r_status);

                if info.known_peers_count > 0 {
                    println!("👥 Peers trong mạng : \x1b[1;35m{} active\x1b[0m / \x1b[1;32m{} storage peers\x1b[0m (On-Demand)", 
                        info.connected_peers_count, info.known_peers_count);
                } else {
                    println!("👥 Peers kết nối   : \x1b[1;35m{} peers\x1b[0m", info.connected_peers_count);
                }

                if !info.dirty_files.is_empty() {
                    println!("------------------------------------------------------------");
                    println!("📝 Tệp tin có thay đổi chưa Commit (~/MyceliumDrive):");
                    for f in &info.dirty_files {
                        if f.starts_with("Added:") {
                            println!("   \x1b[1;32m+\x1b[0m {}", f);
                        } else if f.starts_with("Modified:") {
                            println!("   \x1b[1;33m~\x1b[0m {}", f);
                        } else if f.starts_with("Deleted:") {
                            println!("   \x1b[1;31m-\x1b[0m {}", f);
                        } else {
                            println!("   - {}", f);
                        }
                    }
                    println!("💡 Gõ \x1b[1;36mp2pdrive commit\x1b[0m để đẩy thay đổi lên mạng P2P.");
                }

                if !info.listen_addrs.is_empty() {
                    println!("👂 Địa chỉ lắng nghe:");
                    for addr in info.listen_addrs {
                        println!("   - {}", addr);
                    }
                }

                println!("============================================================");
                Ok(false)
            }
            IpcResponse::Error(msg) => {
                final_error = Some(msg);
                Ok(false)
            }
            _ => Ok(true),
        }
    })
    .await?;

    if let Some(err) = final_error {
        bail!("{}", err);
    }

    Ok(())
}
