/**
 * Mycelium P2P Rendezvous / Bootstrap Server
 * Deno Deploy with Deno KV (Zero-dependency, Edge Native)
 *
 * Tự động phát hiện IP Public thực của client qua Cloudflare / Proxy headers
 * để các máy khác nhau trên toàn cầu có thể kết nối trực tiếp với nhau.
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

function shuffleArray<T>(array: T[]): T[] {
  const arr = [...array];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...CORS_HEADERS,
    },
  });
}

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
 * Trích xuất IP Public thực tế của Client từ Cloudflare / Proxy headers
 */
function extractClientPublicIp(req: Request): string | null {
  const cfIp = req.headers.get("cf-connecting-ip");
  if (cfIp) return cfIp.trim();

  const forwarded = req.headers.get("x-forwarded-for");
  if (forwarded) {
    const firstIp = forwarded.split(",")[0].trim();
    if (firstIp && firstIp !== "127.0.0.1" && firstIp !== "::1") {
      return firstIp;
    }
  }

  const realIp = req.headers.get("x-real-ip");
  if (realIp) return realIp.trim();

  return null;
}

/**
 * Chuẩn hóa Multiaddr: Thay thế 127.0.0.1 hoặc 0.0.0.0 bằng IP Public thực tế nếu có
 */
function resolveMultiaddrWithPublicIp(rawMultiaddr: string, publicIp: string | null): string {
  if (!publicIp) return rawMultiaddr;

  // Nếu client gửi 127.0.0.1 hoặc 0.0.0.0 -> Thay bằng Public IP thật
  if (rawMultiaddr.startsWith("/ip4/127.0.0.1/") || rawMultiaddr.startsWith("/ip4/0.0.0.0/")) {
    return rawMultiaddr
      .replace("/ip4/127.0.0.1/", `/ip4/${publicIp}/`)
      .replace("/ip4/0.0.0.0/", `/ip4/${publicIp}/`);
  }

  return rawMultiaddr;
}

async function getActivePeers(): Promise<PeerRecord[]> {
  const now = Date.now();
  const entries = kv.list<PeerRecord>({ prefix: ["peers"] });
  const active: PeerRecord[] = [];

  for await (const entry of entries) {
    const peer = entry.value;
    if (now - peer.last_seen <= PEER_TTL_MS) {
      active.push(peer);
    } else {
      await kv.delete(entry.key);
    }
  }

  return active;
}

Deno.serve(async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const method = req.method;

  // 1. CORS Preflight
  if (method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: CORS_HEADERS,
    });
  }

  // 2. Health check
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

  // 3. Heartbeat (POST /heartbeat)
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

      // Phát hiện IP Public và tự động thay thế 127.0.0.1
      const clientPublicIp = extractClientPublicIp(req);
      const resolvedMultiaddr = resolveMultiaddrWithPublicIp(payload.multiaddr, clientPublicIp);

      const geoRegion =
        req.headers.get("cf-ipcountry") ||
        req.headers.get("x-client-geo-region") ||
        payload.region ||
        "GLOBAL";

      const record: PeerRecord = {
        peer_id: payload.peer_id,
        multiaddr: resolvedMultiaddr,
        region: geoRegion.toUpperCase(),
        last_seen: Date.now(),
      };

      await kv.set(["peers", payload.peer_id], record, {
        expireIn: PEER_TTL_MS,
      });

      return jsonResponse({
        status: "registered",
        peer_id: record.peer_id,
        multiaddr: record.multiaddr,
        detected_public_ip: clientPublicIp,
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

    const clientRegion = (
      url.searchParams.get("region") ||
      req.headers.get("cf-ipcountry") ||
      "GLOBAL"
    ).toUpperCase();

    const activePeers = await getActivePeers();

    const foreignPeers: PeerRecord[] = [];
    const localPeers: PeerRecord[] = [];

    for (const p of activePeers) {
      if (p.region !== clientRegion && clientRegion !== "GLOBAL") {
        foreignPeers.push(p);
      } else {
        localPeers.push(p);
      }
    }

    const shuffledForeign = shuffleArray(foreignPeers);
    const shuffledLocal = shuffleArray(localPeers);

    const merged = [...shuffledForeign, ...shuffledLocal];
    const selected = merged.slice(0, limit);

    return jsonResponse({
      peers: selected.map((p) => p.multiaddr),
      total_active: activePeers.length,
      returned: selected.length,
      client_region: clientRegion,
    });
  }

  return jsonResponse({ error: "Endpoint not found" }, 404);
});
