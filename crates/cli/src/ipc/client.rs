use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::ipc::protocol::{IpcRequest, IpcResponse, DEFAULT_IPC_ADDR};

/// Thin IPC Client gửi yêu cầu đến `p2pdrive daemon` đang chạy ngầm.
pub struct IpcClient;

impl IpcClient {
    /// Kết nối tới daemon qua Localhost TCP.
    ///
    /// Nếu daemon chưa bật, trả về thông báo lỗi hướng dẫn người dùng chạy `p2pdrive daemon`.
    pub async fn connect() -> Result<TcpStream> {
        match tokio::time::timeout(
            Duration::from_secs(2),
            TcpStream::connect(DEFAULT_IPC_ADDR),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            _ => {
                bail!(
                    "\x1b[1;31mLỗi: p2pdrive daemon chưa được khởi động.\x1b[0m\n\
                     Vui lòng khởi động daemon trong một terminal khác bằng lệnh:\n\
                     \x1b[1;33m   p2pdrive daemon\x1b[0m"
                );
            }
        }
    }

    /// Gửi một yêu cầu và nhận dòng phản hồi từ daemon.
    pub async fn send_request<F>(request: IpcRequest, mut on_response: F) -> Result<()>
    where
        F: FnMut(IpcResponse) -> Result<bool>, // Trả về true nếu tiếp tục lắng nghe, false nếu dừng
    {
        let mut stream = Self::connect().await?;
        let (reader, mut writer) = stream.split();

        // Gửi Request dưới dạng JSON-line
        let mut req_str = serde_json::to_string(&request)
            .context("Không thể serialize IPC Request sang JSON")?;
        req_str.push('\n');
        writer.write_all(req_str.as_bytes()).await?;
        writer.flush().await?;

        // Đọc các Response liên tiếp (dạng JSON-lines)
        let mut buf_reader = BufReader::new(reader);
        let mut line = String::new();

        while buf_reader.read_line(&mut line).await? > 0 {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let response: IpcResponse = serde_json::from_str(trimmed)
                    .with_context(|| format!("Không thể giải mã IPC Response: {}", trimmed))?;

                let should_continue = on_response(response)?;
                if !should_continue {
                    break;
                }
            }
            line.clear();
        }

        Ok(())
    }
}
