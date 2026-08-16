# Mycelium P2P Rendezvous / Bootstrap Server

P2P Bootstrap & Rendezvous server gọn nhẹ viết bằng TypeScript chạy trên [Deno Deploy](https://deno.com/deploy) sử dụng **Deno KV** (Edge-native, zero database maintenance).

## Chức năng
- Nhận heartbeat từ các node đang online qua `POST /heartbeat` (TTL 15 phút).
- Cung cấp danh sách các peer đang hoạt động qua `GET /peers?limit=10`, chủ động xáo trộn (Fisher-Yates) và ưu tiên trả về peer khác khu vực địa lý để chống hiện tượng phân mảnh mạng (Network Partitioning / Split-brain).
- Hỗ trợ CORS đầy đủ cho Web/Desktop/CLI clients.

---

## Hướng dẫn Deploy nhanh lên Deno Deploy (2 Bước)

### Cách 1: Deploy qua Giao diện Web (Không cần cài đặt, mất 1 phút)
1. Đăng nhập vào [dash.deno.com](https://dash.deno.com/) bằng tài khoản GitHub của bạn.
2. Bấm **"New Project"** -> Chọn **"Playground"** hoặc **"Deploy from URL / Blank"**.
3. Dán toàn bộ nội dung từ file [`main.ts`](./main.ts) vào trình soạn thảo trực tuyến.
4. Bấm **"Save & Deploy"**.
5. Bật Deno KV: Vào tab **"Settings"** -> mục **"KV Databases"** -> Đảm bảo database đã được liên kết (mặc định sẵn có trên Deno Deploy).

### Cách 2: Deploy tự động qua GitHub Repository
1. Push mã nguồn dự án Mycelium lên GitHub.
2. Truy cập [dash.deno.com](https://dash.deno.com/) -> Bấm **"New Project"**.
3. Chọn Repository `Mycelium` từ danh sách.
4. Thiết lập:
   - **Entrypoint**: `services/rendezvous/main.ts`
   - **Production Branch**: `main`
5. Bấm **"Deploy Project"**. Deno Deploy sẽ tự động build và cấp phát URL dạng `https://<ten-project>.deno.dev`.

---

## Kiểm tra sau khi Deploy
- **Health Check**: `curl https://<ten-project>.deno.dev/health`
- **Lấy danh sách Peers**: `curl https://<ten-project>.deno.dev/peers?limit=10`
