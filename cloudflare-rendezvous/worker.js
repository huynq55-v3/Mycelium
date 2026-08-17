/**
 * Cloudflare Worker - Mycelium P2P Rendezvous Server
 * Hỗ trợ lưu trữ danh bạ Peers tốc độ cao (KV / In-Memory)
 * Hạn mức: 100,000 requests / ngày MIỄN PHÍ!
 */

const PEER_TTL_SECONDS = 120; // Peer tự hết hạn sau 120s nếu không gửi heartbeat
const peersStore = new Map(); // In-memory cache

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const method = request.method;

    // CORS Headers
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Peer-Id",
      "Content-Type": "application/json",
    };

    if (method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const clientIp = request.headers.get("CF-Connecting-IP") || "127.0.0.1";
    const clientRegion = request.cf?.country || request.cf?.colo || "GLOBAL";

    // 1. Endpoint: POST /heartbeat hoặc POST /
    if (method === "POST" && (url.pathname === "/heartbeat" || url.pathname === "/")) {
      try {
        const body = await request.json();
        const peerId = body.peer_id || request.headers.get("X-Peer-Id");
        let addresses = body.addresses || [];

        if (!peerId) {
          return new Response(JSON.stringify({ error: "Missing peer_id" }), {
            status: 400,
            headers: corsHeaders,
          });
        }

        // Tự động thay thế địa chỉ 0.0.0.0 bằng IP thực của Client nếu cần
        addresses = addresses.map((addr) => {
          if (addr.includes("/ip4/0.0.0.0/")) {
            return addr.replace("/ip4/0.0.0.0/", `/ip4/${clientIp}/`);
          }
          return addr;
        });

        const now = Date.now();
        const peerRecord = {
          peer_id: peerId,
          addresses,
          region: body.region || clientRegion,
          last_seen: now,
          expires_at: now + PEER_TTL_SECONDS * 1000,
        };

        // Lưu vào in-memory store
        peersStore.set(peerId, peerRecord);

        // Lưu vào KV nếu có binding (dành cho multi-edge sync)
        if (env && env.MYCELIUM_KV) {
          ctx.waitUntil(
            env.MYCELIUM_KV.put(`peer:${peerId}`, JSON.stringify(peerRecord), {
              expirationTtl: PEER_TTL_SECONDS,
            })
          );
        }

        return new Response(
          JSON.stringify({
            status: "ok",
            client_ip: clientIp,
            region: clientRegion,
            expires_in: PEER_TTL_SECONDS,
          }),
          { status: 200, headers: corsHeaders }
        );
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 400,
          headers: corsHeaders,
        });
      }
    }

    // 2. Endpoint: GET /peers hoặc GET /
    if (method === "GET" && (url.pathname === "/peers" || url.pathname === "/" || url.pathname === "/bootstrap")) {
      const now = Date.now();
      const requestingPeerId = request.headers.get("X-Peer-Id") || url.searchParams.get("exclude");
      const limit = parseInt(url.searchParams.get("limit") || "20", 10);

      // Dọn dẹp các peer đã hết hạn
      for (const [id, record] of peersStore.entries()) {
        if (record.expires_at < now) {
          peersStore.delete(id);
        }
      }

      let activePeers = Array.from(peersStore.values());

      // Lọc bỏ chính người đang yêu cầu
      if (requestingPeerId) {
        activePeers = activePeers.filter((p) => p.peer_id !== requestingPeerId);
      }

      // Trích xuất danh sách Multiaddr
      const peerAddrs = [];
      for (const p of activePeers) {
        for (const addr of p.addresses) {
          if (!peerAddrs.includes(addr)) {
            peerAddrs.push(addr);
          }
        }
      }

      return new Response(
        JSON.stringify({
          peers: peerAddrs.slice(0, limit),
          total_active: activePeers.length,
          returned: peerAddrs.slice(0, limit).length,
          client_region: clientRegion,
        }),
        { status: 200, headers: corsHeaders }
      );
    }

    return new Response(
      JSON.stringify({
        name: "Mycelium P2P Rendezvous Server (Cloudflare Worker)",
        version: "1.0.0",
        active_peers: peersStore.size,
      }),
      { status: 200, headers: corsHeaders }
    );
  },
};
