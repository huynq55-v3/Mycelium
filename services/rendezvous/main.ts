/**
 * Mycelium P2P Rendezvous / Bootstrap Server
 * Deno Deploy with Deno KV (Zero-dependency, Edge Native)
 *
 * Endpoints:
 *  - GET  /health           : Trả về trạng thái & số lượng peer đang hoạt động
 *  - POST /heartbeat        : Đăng ký / làm mới trạng thái active của một node (TTL 15 phút)
 *  - GET  /peers?limit=10   : Lấy danh sách multiaddr active (ưu tiên cross-region, Fisher-Yates shuffle)
 */

interface PeerRecord {
  peer_id: string;
  multiaddr: string;
  region: string;
  last_seen: number;
}

interface HeartbeatPayload {
  peer_id: string;
  multiaddr: string;
  region?: string;
}

const PEER_TTL_MS = 15 * 60 * 1000; // 15 phút TTL
const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
};

// Khởi tạo Deno KV
const kv = await Deno.openKv();

/**
 * Hàm xáo trộn mảng ngẫu nhiên (Fisher-Yates Shuffle)
 */
function shuffleArray<T>(array: T[]): T[] {
  const arr = [...array];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

/**
 * Trả về response JSON kèm CORS
 */
function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...CORS_HEADERS,
    },
  });
}

/**
 * Validate định dạng Multiaddr cơ bản:
 * Bắt đầu bằng /ip4/ hoặc /ip6/ hoặc /dns4/ hoặc /dns6/ và chứa /p2p/
 */
function isValidMultiaddr(addr: string): boolean {
  if (typeof addr !== "string") return false;
  const isIpOrDns =
    addr.startsWith("/ip4/") ||
    addr.startsWith("/ip6/") ||
    addr.startsWith("/dns4/") ||
    addr.startsWith("/dns6/") ||
    addr.startsWith("/dns/");
  const hasP2p = addr.includes("/p2p/") || addr.includes("/ipfs/");
  return isIpOrDns && hasP2p;
}

/**
 * Dọn dẹp các peer quá hạn (lazy cleanup hoặc query filter)
 */
async function getActivePeers(): Promise<PeerRecord[]> {
  const now = Date.now();
  const entries = kv.list<PeerRecord>({ prefix: ["peers"] });
  const active: PeerRecord[] = [];

  for await (const entry of entries) {
    const peer = entry.value;
    if (now - peer.last_seen <= PEER_TTL_MS) {
      active.push(peer);
    } else {
      // Xóa peer đã hết hạn khỏi KV để tiết kiệm dung lượng
      await kv.delete(entry.key);
    }
  }

  return active;
}

Deno.serve(async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const method = req.method;

  // 1. Xử lý CORS Preflight OPTIONS
  if (method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: CORS_HEADERS,
    });
  }

  // 2. Health check endpoint (GET / hoặc GET /health)
  if (method === "GET" && (url.pathname === "/" || url.pathname === "/health")) {
    const activePeers = await getActivePeers();
    return jsonResponse({
      status: "ok",
      service: "Mycelium P2P Rendezvous Server",
      active_peers_count: activePeers.length,
      timestamp: Date.now(),
      ttl_minutes: PEER_TTL_MS / 60000,
    });
  }

  // 3. Heartbeat endpoint (POST /heartbeat)
  if (method === "POST" && url.pathname === "/heartbeat") {
    try {
      const payload: HeartbeatPayload = await req.json();

      if (!payload.peer_id || typeof payload.peer_id !== "string") {
        return jsonResponse({ error: "Missing or invalid 'peer_id'" }, 400);
      }

      if (!isValidMultiaddr(payload.multiaddr)) {
        return jsonResponse(
          {
            error:
              "Invalid multiaddr format. Must start with /ip4/, /ip6/, or /dns/ and contain /p2p/<peer_id>",
          },
          400
        );
      }

      // Phát hiện Region qua Cloudflare/Deno geo headers hoặc payload fallback
      const geoRegion =
        req.headers.get("cf-ipcountry") ||
        req.headers.get("x-client-geo-region") ||
        payload.region ||
        "GLOBAL";

      const record: PeerRecord = {
        peer_id: payload.peer_id,
        multiaddr: payload.multiaddr,
        region: geoRegion.toUpperCase(),
        last_seen: Date.now(),
      };

      // Lưu vào Deno KV với key ["peers", peer_id]
      await kv.set(["peers", payload.peer_id], record, {
        expireIn: PEER_TTL_MS,
      });

      return jsonResponse({
        status: "registered",
        peer_id: record.peer_id,
        region: record.region,
        expires_in_seconds: PEER_TTL_MS / 1000,
      });
    } catch (err) {
      return jsonResponse(
        { error: "Failed to parse JSON body", details: String(err) },
        400
      );
    }
  }

  // 4. Lấy danh sách peer active (GET /peers)
  if (method === "GET" && url.pathname === "/peers") {
    const limitParam = parseInt(url.searchParams.get("limit") || `${DEFAULT_LIMIT}`);
    const limit = Math.min(Math.max(limitParam || DEFAULT_LIMIT, 1), MAX_LIMIT);

    // Xác định client region để ưu tiên cross-region
    const clientRegion = (
      url.searchParams.get("region") ||
      req.headers.get("cf-ipcountry") ||
      "GLOBAL"
    ).toUpperCase();

    const activePeers = await getActivePeers();

    // Tách peer thành 2 nhóm: khác vùng (cross-region) và cùng vùng (local-region)
    const foreignPeers: PeerRecord[] = [];
    const localPeers: PeerRecord[] = [];

    for (const p of activePeers) {
      if (p.region !== clientRegion && clientRegion !== "GLOBAL") {
        foreignPeers.push(p);
      } else {
        localPeers.push(p);
      }
    }

    // Xáo trộn ngẫu nhiên từng nhóm bằng Fisher-Yates shuffle
    const shuffledForeign = shuffleArray(foreignPeers);
    const shuffledLocal = shuffleArray(localPeers);

    // Ưu tiên cross-region lên đầu danh sách để chống Network Partitioning
    const merged = [...shuffledForeign, ...shuffledLocal];
    const selected = merged.slice(0, limit);

    return jsonResponse({
      peers: selected.map((p) => p.multiaddr),
      total_active: activePeers.length,
      returned: selected.length,
      client_region: clientRegion,
    });
  }

  // Fallback 404
  return jsonResponse({ error: "Endpoint not found" }, 404);
});
