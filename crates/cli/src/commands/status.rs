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

                let disk_used_gb = info.disk_used_mb / 1024.0;
                let percent_disk = (disk_used_gb / info.allocated_gb.max(1.0)) * 100.0;
                println!("💾 Ổ đĩa đóng góp : \x1b[1;36m{:.2} GB / {:.0} GB\x1b[0m ({:.1}%)",
                    disk_used_gb, info.allocated_gb, percent_disk);

                println!("🚀 Hạn mức tải lên : \x1b[1;32m{:.2} GB / {:.2} GB\x1b[0m (Tỷ lệ 1:4 an toàn)",
                    info.upload_used_gb, info.upload_limit_gb);

                println!("👥 Peers kết nối  : \x1b[1;35m{} peers\x1b[0m", info.connected_peers_count);

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
