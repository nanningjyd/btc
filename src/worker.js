// btc-monitor Worker：Cron 采集（每分钟 6 个 10s 采样点）+ API + 内嵌静态资源托管
// 构建说明：build.py 会把 public/ 下的文件内联进 __INLINE_ASSETS__ 占位符后部署。
const SOURCES = ["binance", "okx", "polymarket"];
const BINANCE_MAP = { BTCUSDT: "btc", ETHUSDT: "eth", SOLUSDT: "sol", BNBUSDT: "bnb", DOGEUSDT: "doge" };
const PM_MAP = { "btc/usd": "btc", "eth/usd": "eth", "sol/usd": "sol", "xrp/usd": "xrp", "bnb/usd": "bnb", "doge/usd": "doge" };
const PM_BINANCE_MAP = { btcusdt: "btc", ethusdt: "eth", solusdt: "sol", xrpusdt: "xrp", bnbusdt: "bnb", dogeusdt: "doge" };
// Polymarket RTDS 聚合消息没有 symbol 字段时，按价格区间推断币种
const PM_PRICE_RANGES = [
  { min: 50000, max: 200000, sym: "btc/usd" },
  { min: 1500, max: 5000, sym: "eth/usd" },
  { min: 50, max: 200, sym: "sol/usd" },
  { min: 300, max: 1000, sym: "bnb/usd" },
  { min: 0.5, max: 5, sym: "xrp/usd" },
  { min: 0.0001, max: 0.5, sym: "doge/usd" },
];
const TZ = 8 * 3600 * 1000;

const INLINE_ASSETS = __INLINE_ASSETS__;

function bucketStart(ts) { return Math.floor(ts / 10000) * 10000; }

function waitMs(ms) {
  if (typeof scheduler !== "undefined" && scheduler.wait) {
    return scheduler.wait(ms).catch(() => new Promise((r) => setTimeout(r, ms)));
  }
  return new Promise((r) => setTimeout(r, ms));
}

function jresp(obj, status) {
  return new Response(JSON.stringify(obj), {
    status: status || 200,
    headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" },
  });
}

async function fetchOKX() {
  const hosts = ["https://www.okx.com", "https://aws.okx.com", "https://app.okx.com"];
  let lastErr = null;
  for (const host of hosts) {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const resp = await fetch(host + "/api/v5/market/tickers?instType=SPOT", { signal: AbortSignal.timeout(8000) });
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        const j = await resp.json();
        if (!j.data) throw new Error("empty data");
        const out = {};
        for (const it of j.data) { const c = BINANCE_MAP_SYMBOL(it.instId); if (c) out[c] = parseFloat(it.last); }
        if (Object.keys(out).length < 5) throw new Error("incomplete");
        return out;
      } catch (e) { lastErr = e; if (attempt === 0) await waitMs(700); }
    }
  }
  throw lastErr || new Error("okx all hosts failed");
}

function BINANCE_MAP_SYMBOL(instId) {
  return { "BTC-USDT": "btc", "ETH-USDT": "eth", "SOL-USDT": "sol", "BNB-USDT": "bnb", "DOGE-USDT": "doge" }[instId];
}

// 币安 WebSocket 流（CF 出口 REST 被 geo 封锁，WS 可用）+ 自动重连（最多 3 次）
function startBinanceStream() {
  const latest = {};
  const diag = { opened: false, err: null, reconnects: 0 };
  let ws = null, closed = false, attempts = 0;
  function connect() {
    if (closed || attempts >= 3) return;
    attempts++;
    (async () => {
      try {
        const streams = Object.keys(BINANCE_MAP).map((s) => s.toLowerCase() + "@miniTicker").join("/");
        const resp = await fetch("https://stream.binance.com:9443/stream?streams=" + streams, { headers: { Upgrade: "websocket" } });
        ws = resp.webSocket;
        if (!ws) throw new Error("no ws, status " + resp.status);
        ws.accept();
        diag.opened = true;
        ws.addEventListener("message", (ev) => {
          try {
            const m = JSON.parse(ev.data);
            const d = m.data || m;
            const coin = BINANCE_MAP[d.s];
            if (coin && d.c != null) latest[coin] = { v: parseFloat(d.c), t: d.E || Date.now() };
          } catch (e) {}
        });
        ws.addEventListener("close", () => { if (!closed) { diag.reconnects++; setTimeout(connect, 1000); } });
      } catch (e) { diag.err = String((e && e.message) || e); if (!closed && attempts < 3) setTimeout(connect, 1000); }
    })();
  }
  connect();
  return {
    snapshot() {
      const now = Date.now();
      const out = {};
      for (const c in latest) { if (now - latest[c].t < 20000) out[c] = latest[c].v; }
      return out;
    },
    diag() { return Object.assign({ n: Object.keys(latest).length }, diag); },
    stop() { closed = true; try { if (ws) ws.close(); } catch (e) {} },
  };
}

// Polymarket RTDS 流：crypto_prices(币安行情) + crypto_prices_chainlink(聚合格式，无 symbol)
function startPolymarketStream() {
  const chainlink = {};
  const topicBinance = {};
  const seen = new Set();
  const diag = { opened: false, msgs: 0, parsed: 0, err: null };
  const rawMsgs = [];
  const msgHeaders = [];
  let ws = null, closed = false, pingTimer = null;
  (async () => {
    try {
      const resp = await fetch("https://ws-live-data.polymarket.com", { headers: { Upgrade: "websocket" } });
      ws = resp.webSocket;
      if (!ws) throw new Error("no ws, status " + resp.status);
      ws.accept();
      diag.opened = true;
      ws.addEventListener("message", (ev) => {
        diag.msgs++;
        try {
          const raw = typeof ev.data === "string" ? ev.data : "";
          if (!raw || raw === "PONG") return;
          if (rawMsgs.length < 3) { rawMsgs.push(String(raw).slice(0, 1500)); msgHeaders.push(String(raw).slice(0, 400)); }
          let msg;
          try { msg = JSON.parse(raw); } catch (e) { return; }
          let items = [];
          if (msg.payload && msg.payload.data && Array.isArray(msg.payload.data)) items = msg.payload.data;
          else if (msg.payload) items = Array.isArray(msg.payload) ? msg.payload : [msg.payload];
          else items = [msg];
          for (const p of items) {
            const v = parseFloat(p.value != null ? p.value : p.price);
            if (!isFinite(v)) continue;
            const t = p.timestamp ? Number(p.timestamp) : Date.now();
            const sym = String(p.symbol || "").toLowerCase().trim();
            let normalized = "";
            if (sym) {
              normalized = sym;
              if (/^btcusdt$/i.test(sym)) normalized = "btc/usd";
              else if (/^ethusdt$/i.test(sym)) normalized = "eth/usd";
              else if (/^solusdt$/i.test(sym)) normalized = "sol/usd";
              else if (/^xrpusdt$/i.test(sym)) normalized = "xrp/usd";
              else if (/^bnbusdt$/i.test(sym)) normalized = "bnb/usd";
              else if (/^dogeusdt$/i.test(sym)) normalized = "doge/usd";
            } else {
              for (const pr of PM_PRICE_RANGES) {
                if (v >= pr.min && v <= pr.max) { normalized = pr.sym; break; }
              }
            }
            if (!normalized) continue;
            const isLink = PM_MAP[normalized];
            const isTopic = PM_BINANCE_MAP[sym || normalized];
            if (isLink || isTopic) {
              if (isLink) chainlink[isLink] = { v, t };
              if (isTopic) topicBinance[isTopic] = { v, t };
              seen.add(normalized || sym);
              diag.parsed++;
            }
          }
        } catch (e) {}
      });
      ws.addEventListener("close", () => { closed = true; });
      ws.send(JSON.stringify({
        action: "subscribe",
        subscriptions: [
          { topic: "crypto_prices", type: "update", filters: "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt" },
          { topic: "crypto_prices_chainlink", type: "update", filters: '{"symbol":"btc/usd"}' },
          { topic: "crypto_prices_chainlink", type: "update", filters: '{"symbol":"eth/usd"}' },
          { topic: "crypto_prices_chainlink", type: "update", filters: '{"symbol":"sol/usd"}' },
        ],
      }));
      pingTimer = setInterval(() => { try { if (!closed) ws.send("PING"); } catch (e) {} }, 5000);
    } catch (e) { diag.err = String((e && e.message) || e); closed = true; }
  })();
  return {
    snapshot() {
      const now = Date.now();
      const out = {};
      for (const coin in chainlink) { if (now - chainlink[coin].t < 30000) out[coin] = chainlink[coin].v; }
      for (const coin in topicBinance) { if (!(coin in out) && now - topicBinance[coin].t < 30000) out[coin] = topicBinance[coin].v; }
      return out;
    },
    seenSymbols() { return Array.from(seen); },
    diag() { return Object.assign({}, diag); },
    rawMsgs() { return rawMsgs; },
    msgHeaders() { return msgHeaders; },
    stop() { if (pingTimer) clearInterval(pingTimer); try { if (ws && !closed) ws.close(); } catch (e) {} },
  };
}

async function insertBars(env, source, rows) {
  if (!rows.length) return;
  const ph = rows.map(() => "(?,?,?,?,?,?,?,?)").join(",");
  const params = [];
  for (const r of rows) params.push(r[0], source, r[1], r[2], r[3], r[4], r[5], r[6]);
  await env.DB.prepare("INSERT OR REPLACE INTO bars (ts,source,btc,eth,sol,bnb,doge,xrp) VALUES " + ph).bind(...params).run();
}

// 一轮采集：约 60 秒，覆盖 6 个连续 10s 桶。
async function sampleRound(env) {
  const N = parseInt(env.SAMPLES_PER_RUN || "6", 10);
  const samples = [];
  const bns = startBinanceStream();
  let pm = startPolymarketStream();
  await waitMs(2000); // 等待币安 WS 建立并收到首批数据
  let pmRetried = false;
  for (let i = 0; i < N; i++) {
    // Polymarket 流自愈：第 3 个采样点仍零消息则重建一次连接（RTDS 偶发拒流）
    if (!pmRetried && i === 2 && pm.diag().msgs === 0) {
      pmRetried = true;
      try { pm.stop(); } catch (e) {}
      pm = startPolymarketStream();
      await waitMs(2500);
    }
    const now = Date.now();
    let bucketEnd = bucketStart(now) + 10000;
    let target = bucketEnd - 800;
    if (target <= now) { target += 10000; bucketEnd += 10000; }
    await waitMs(target - now);
    const ts = bucketEnd - 10000;
    let o = null;
    try { o = await fetchOKX(); } catch (e) {}
    samples.push({ ts, b: bns.snapshot(), o, p: pm.snapshot() });
  }
  bns.stop(); pm.stop();

  const rowsBySource = { binance: [], okx: [], polymarket: [] };
  const report = { binance: { ok: 0, err: null }, okx: { ok: 0, err: null }, polymarket: { ok: 0, err: null } };
  for (const s of samples) {
    if (s.b && Object.keys(s.b).length) {
      rowsBySource.binance.push([s.ts, s.b.btc ?? null, s.b.eth ?? null, s.b.sol ?? null, s.b.bnb ?? null, s.b.doge ?? null, null]);
      report.binance.ok++;
    } else if (!report.binance.err) report.binance.err = "ws empty";
    if (s.o && Object.keys(s.o).length) {
      rowsBySource.okx.push([s.ts, s.o.btc ?? null, s.o.eth ?? null, s.o.sol ?? null, s.o.bnb ?? null, s.o.doge ?? null, null]);
      report.okx.ok++;
    } else if (!report.okx.err) report.okx.err = "rest empty/429";
    if (s.p && Object.keys(s.p).length) {
      rowsBySource.polymarket.push([s.ts, s.p.btc ?? null, s.p.eth ?? null, s.p.sol ?? null, s.p.bnb ?? null, s.p.doge ?? null, s.p.xrp ?? null]);
      report.polymarket.ok++;
    }
  }
  if (!report.polymarket.ok) report.polymarket.err = "no updates in window";
  report.diags = { binance: bns.diag(), okx: null, polymarket: pm.diag() };

  for (const src of SOURCES) {
    const now = Date.now();
    try {
      if (rowsBySource[src].length) await insertBars(env, src, rowsBySource[src]);
      await env.DB.prepare("INSERT OR REPLACE INTO health (source,last_ok,last_err,updated) VALUES (?,?,?,?)")
        .bind(src, rowsBySource[src].length ? now : null, report[src].err, now).run();
    } catch (e) {
      console.error("d1 write failed", src, e);
    }
  }
  const seen = pm.seenSymbols();
  if (seen.length) {
    try { await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('pm_symbols',?)").bind(seen.join(",")).run(); } catch (e) {}
  }
  report.pmSymbols = seen;
  const pRaw = pm.rawMsgs();
  if (pRaw.length) {
    try { await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('pm_raw_msgs',?)").bind(JSON.stringify(pRaw)).run(); } catch (e) {}
  }
  const pHdr = pm.msgHeaders();
  if (pHdr.length) {
    try { await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('pm_msg_headers',?)").bind(JSON.stringify(pHdr)).run(); } catch (e) {}
  }
  try {
    await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('last_diag',?)")
      .bind(JSON.stringify({ t: Date.now(), binance: bns.diag(), polymarket: pm.diag() })).run();
  } catch (e) {}
  return report;
}

async function cleanup(env) {
  const cutoff = Date.now() - 48 * 3600 * 1000;
  await env.DB.prepare("DELETE FROM bars WHERE ts < ?").bind(cutoff).run();
}

// ---------- VPNGate 服务器列表（评分：归一化原始分 + 延迟惩罚 + 速度奖励 + 在线时长 - 日志惩罚） ----------
const VPNGATE_API_URL = "https://www.vpngate.net/api/iphone/";
const VPNGATE_CACHE_TTL = 120000;
const _vpngateCache = { servers: [], fetchedAt: 0 };
let _vpngateDebug = null;
const _pmConnectivityCache = { reachable: null, checkedAt: 0 };

async function fetchVPNGateServers() {
  const now = Date.now();
  if (now - _vpngateCache.fetchedAt < VPNGATE_CACHE_TTL && _vpngateCache.servers.length > 0) {
    return _vpngateCache.servers;
  }
  try {
    const resp = await fetch(VPNGATE_API_URL, { signal: AbortSignal.timeout(15000) });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const text = await resp.text();
    _vpngateDebug = { http_ok: true, raw_len: text.length, raw_head: text.slice(0, 300) };
    const rawLines = text.trim().split("\n").filter((l) => l && l.trim());
    let headerIdx = rawLines.findIndex((l) => l.startsWith("#"));
    if (headerIdx < 0) headerIdx = rawLines.findIndex((l) => !l.startsWith("*"));
    if (headerIdx < 0) throw new Error("no header");
    const headers = rawLines[headerIdx].replace(/^#/, "").trim().split(",");
    const dataLines = rawLines.slice(headerIdx + 1).filter((l) => l && !l.startsWith("*") && !l.startsWith("#"));
    if (dataLines.length < 1) throw new Error("empty response");
    _vpngateDebug.header_len = headers.length;
    _vpngateDebug.data_lines = dataLines.length;
    const servers = [];
    let shortRows = 0, missingKey = 0;
    for (const dl of dataLines) {
      const vals = dl.split(",");
      if (vals.length < 10) { shortRows++; continue; }
      const row = {};
      for (let j = 0; j < headers.length; j++) row[headers[j].trim()] = (vals[j] || "").trim();
      if (!row.IP || !row.Score) { missingKey++; continue; }
      const rawScore = parseFloat(row.Score) || 0;
      const ping = parseInt(row.Ping) || 0;
      const speed = parseFloat(row.Speed) || 0;
      const uptime = parseFloat(row.Uptime) || 0;
      const users = parseInt(row.TotalUsers) || 0;
      const sessions = parseInt(row.NumVpnSessions) || 0;
      const logType = (row.LogType || "n").toLowerCase();
      let finalScore = Math.min(50, Math.log10((rawScore || 1) + 1) * 12);
      if (ping > 0) finalScore += Math.max(0, 25 - ping * 0.8);
      else finalScore += 10;
      finalScore += Math.min(15, Math.log10((speed || 1) + 1) * 2);
      if (uptime > 0) finalScore += Math.min(10, Math.log10(uptime + 1) * 0.8);
      if (logType === "y") finalScore -= 15;
      servers.push({
        hostname: row.HostName || "",
        ip: row.IP || "",
        score: Math.round(Math.max(0, Math.min(100, finalScore)) * 10) / 10,
        ping, speed, uptime, users, sessions,
        country_long: row.CountryLong || "",
        country: row.CountryShort || "",
        log_type: logType,
        operator: row.Operator || "",
        message: row.Message || "",
      });
    }
    servers.sort((a, b) => b.score - a.score);
    _vpngateCache = { servers, fetchedAt: now };
    _vpngateDebug.short_rows = shortRows;
    _vpngateDebug.missing_key_rows = missingKey;
    _vpngateDebug.parsed = servers.length;
    return servers;
  } catch (e) {
    console.error("VPNGate fetch error:", e.message);
    _vpngateDebug = Object.assign(_vpngateDebug || {}, { http_ok: false, err: e.message });
    if (_vpngateCache.servers.length > 0) return _vpngateCache.servers;
    throw e;
  }
}

async function checkPolymarketConnectivity() {
  const now = Date.now();
  if (now - _pmConnectivityCache.checkedAt < 60000 && _pmConnectivityCache.reachable !== null) {
    return _pmConnectivityCache.reachable;
  }
  try {
    await fetch("https://ws-live-data.polymarket.com", { method: "HEAD", mode: "no-cors", signal: AbortSignal.timeout(8000) });
    _pmConnectivityCache = { reachable: true, checkedAt: now };
    return true;
  } catch (e) {
    _pmConnectivityCache = { reachable: false, checkedAt: now };
    return false;
  }
}

let sigCache = null;
let d1DownUntil = 0;
const EXT_VER = "1.6.7"; // gate-bridge-ext 扩展版本（推送扩展时同步修改） // D1 配额耗尽时的降级标记（60 秒后重试） // /api/signals 隔离级缓存（保护 D1 读取配额）

async function handleApi(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  if (path === "/api/version") return jresp({ ver: EXT_VER, t: Date.now() });
    if (path === "/api/healthz") return jresp({ ok: true, now: Date.now() });
  if (path === "/api/status") {
    const bars = await env.DB.prepare("SELECT source, COUNT(*) AS cnt, MAX(ts) AS last_ts FROM bars GROUP BY source").all();
    const health = await env.DB.prepare("SELECT * FROM health").all();
    const metaRows = await env.DB.prepare("SELECT * FROM meta").all();
    const sources = {};
    for (const s of SOURCES) sources[s] = { cnt: 0, last_ts: null, last_ok: null, last_err: null };
    for (const r of bars.results) if (sources[r.source]) { sources[r.source].cnt = r.cnt; sources[r.source].last_ts = r.last_ts; }
    for (const r of health.results) if (sources[r.source]) { sources[r.source].last_ok = r.last_ok; sources[r.source].last_err = r.last_err; }
    const meta = {};
    for (const r of metaRows.results) meta[r.k] = r.v;
    return jresp({ now: Date.now(), sources, meta });
  }
  if (path === "/api/bars") {
    const source = url.searchParams.get("source") || "binance";
    if (!SOURCES.includes(source)) return jresp({ error: "bad source" }, 400);
    const now = Date.now();
    let start, end;
    const date = url.searchParams.get("date");
    if (date && /^\d{4}-\d{2}-\d{2}$/.test(date)) {
      start = Date.parse(date + "T00:00:00+08:00");
      if (!isFinite(start)) return jresp({ error: "bad date" }, 400);
      end = start + 86400000;
    } else {
      start = Math.floor((now + TZ) / 86400000) * 86400000 - TZ;
      end = now + 60000;
    }
    const res = await env.DB.prepare("SELECT ts,btc,eth,sol,bnb,doge,xrp FROM bars WHERE source=?1 AND ts>=?2 AND ts<?3 ORDER BY ts")
      .bind(source, start, end).all();
    const rows = res.results.map((r) => [r.ts, r.btc, r.eth, r.sol, r.bnb, r.doge, r.xrp]);
    return jresp({ source, start, end, count: rows.length, rows });
  }
  if (path === "/api/pm-rest-test") {
    const result = { started_at: new Date().toISOString(), endpoints: {}, exit_ip: null };
    try {
      const ipResp = await fetch("https://api.ipify.org?format=json", { headers: { Accept: "application/json" } });
      result.exit_ip = (await ipResp.json()).ip || null;
    } catch (e) {}
    const urls = [
      "https://gamma-api.polymarket.com/markets?limit=1&active=true",
      "https://gamma-api.polymarket.com/events?limit=1",
      "https://clob.polymarket.com/markets?limit=1",
    ];
    for (const u of urls) {
      try {
        const t0 = Date.now();
        const r = await fetch(u, { headers: { Accept: "application/json" } });
        let body = null;
        try { body = await r.text(); } catch (e) {}
        result.endpoints[u] = { ok: true, status: r.status, duration_ms: Date.now() - t0, body_preview: (body || "").slice(0, 400) };
      } catch (e) {
        result.endpoints[u] = { ok: false, error: String((e && e.message) || e) };
      }
    }
    return jresp(result);
  }
  if (path === "/api/pm-ws-debug") {
    const metaRows = await env.DB.prepare("SELECT k,v FROM meta WHERE k IN ('last_diag','pm_raw_msgs','pm_symbols','pm_msg_headers')").all();
    const meta = {};
    for (const r of metaRows.results) meta[r.k] = r.v;
    return jresp({ now: Date.now(), meta });
  }
  if (path === "/api/collect") {
    const secret = url.searchParams.get("secret");
    if (!env.COLLECT_SECRET || secret !== env.COLLECT_SECRET) return jresp({ error: "forbidden" }, 403);
    const report = await sampleRound(env);
    return jresp({ ok: true, report });
  }
  if (path === "/api/put") {
    // 浏览器实时数据回传（页面打开期间由前端推送，弥补 CF 出口无法访问币安/OKX）
    if (request.method !== "POST") return jresp({ error: "post only" }, 405);
    const body = await request.json().catch(() => null);
    if (!body || body.token !== env.PUT_TOKEN || !Array.isArray(body.bars) || !body.bars.length) return jresp({ error: "forbidden" }, 403);
    const now = Date.now();
    const rowsBySource = { binance: [], okx: [], polymarket: [] };
    for (const b of body.bars.slice(0, 60)) {
      if (!rowsBySource[b.source]) continue;
      const ts = Number(b.ts);
      if (!isFinite(ts) || ts < now - 600000 || ts > now + 60000) continue;
      const row = Array.isArray(b.row) ? b.row.slice(0, 6) : null;
      if (!row) continue;
      let okRow = true;
      for (let i = 0; i < 6; i++) { if (row[i] != null && typeof row[i] !== "number") { okRow = false; break; } }
      if (!okRow) continue;
      rowsBySource[b.source].push([ts, row[0] ?? null, row[1] ?? null, row[2] ?? null, row[3] ?? null, row[4] ?? null, row[5] ?? null]);
    }
    let stored = 0;
    for (const src of SOURCES) {
      if (rowsBySource[src].length) { await insertBars(env, src, rowsBySource[src]); stored += rowsBySource[src].length; }
      await env.DB.prepare("INSERT OR REPLACE INTO health (source,last_ok,last_err,updated) VALUES (?,?,?,?)")
        .bind(src, rowsBySource[src].length ? now : null, null, now).run();
    }
    return jresp({ ok: true, received: body.bars.length, stored });
  }
  if (path === "/api/backfill") {
    // 历史数据补齐：浏览器用币安 1s K 线重建 10s 桶后批量回写（INSERT OR IGNORE 只补缺失桶）
    if (request.method !== "POST") return jresp({ error: "post only" }, 405);
    const body = await request.json().catch(() => null);
    if (!body || body.token !== env.PUT_TOKEN || body.source !== "binance" || !Array.isArray(body.rows) || !body.rows.length) {
      return jresp({ error: "forbidden" }, 403);
    }
    const now = Date.now();
    const minTs = now - 49 * 3600 * 1000; // 仅接受保留窗口内的数据
    const vals = [];
    for (const r of body.rows.slice(0, 500)) {
      if (!Array.isArray(r) || r.length < 6) continue;
      const ts = Number(r[0]);
      if (!isFinite(ts) || Math.floor(ts / 10000) * 10000 !== ts) continue; // 必须是 10s 对齐的桶起点
      if (ts < minTs || ts > now + 60000) continue;
      const coins = [];
      let okRow = true;
      for (let i = 1; i <= 5; i++) {
        if (r[i] == null) { coins.push("NULL"); continue; }
        const n = Number(r[i]);
        if (!isFinite(n) || n <= 0) { okRow = false; break; }
        coins.push(String(n));
      }
      if (!okRow) continue;
      vals.push("(" + ts + ",'binance'," + coins.join(",") + ",NULL)");
    }
    if (!vals.length) return jresp({ ok: true, received: body.rows.length, written: 0 });
    try {
      const res = await env.DB.prepare("INSERT OR IGNORE INTO bars (ts,source,btc,eth,sol,bnb,doge,xrp) VALUES " + vals.join(",")).run();
      return jresp({ ok: true, received: body.rows.length, written: (res.meta && res.meta.changes) || 0 });
    } catch (e) {
      return jresp({ error: String((e && e.message) || e).slice(0, 200) }, 500);
    }
  }
  if (path === "/api/probe") {
    const probes = {};
    const targets = [
      { name: "polymarket", url: "https://ws-live-data.polymarket.com", timeout: 5000 },
      { name: "binance", url: "https://api.binance.com", timeout: 5000 },
      { name: "okx", url: "https://www.okx.com", timeout: 5000 },
    ];
    for (const t of targets) {
      try {
        await fetch(t.url, { method: "HEAD", mode: "no-cors", signal: AbortSignal.timeout(t.timeout) });
        probes[t.name] = { ok: true };
      } catch (e) {
        probes[t.name] = { ok: false, err: e.message };
      }
    }
    return jresp({ now: Date.now(), probes });
  }
  if (path === "/api/vpngate-servers") {
    try {
      const [servers, pmReach] = await Promise.all([fetchVPNGateServers(), checkPolymarketConnectivity()]);
      const topServer = servers[0] || null;
      return jresp({
        servers: servers.slice(0, 30),
        polymarket_reachable: pmReach,
        current_server: topServer ? topServer.hostname + " (" + topServer.ip + ")" : null,
        last_updated: new Date().toISOString(),
        debug: _vpngateDebug,
      });
    } catch (e) {
      return jresp({ error: e.message, servers: _vpngateCache.servers.slice(0, 30) }, 500);
    }
  }
  if (path === "/api/wf-markets") {
    // 钱包共振监控：批量解析当前/下一窗口的 updown 市场（浏览器经同源代理访问，规避直连问题）
    const slugs = (url.searchParams.get("slugs") || "").split(",").filter(Boolean).slice(0, 24);
    const results = await Promise.all(slugs.map(async (slug) => {
      try {
        const r = await fetch("https://gamma-api.polymarket.com/events?slug=" + slug, { signal: AbortSignal.timeout(9000) });
        const j = await r.json();
        if (j && j.length && j[0].markets && j[0].markets[0] && j[0].markets[0].conditionId) {
          const m = j[0].markets[0];
          let tokens = [];
          try { tokens = JSON.parse(m.clobTokenIds || "[]"); } catch (e) {}
          return { slug, cid: m.conditionId, end: m.endDate, up: tokens[0] || null, dn: tokens[1] || null };
        }
      } catch (e) {}
      return null;
    }));
    return jresp({ now: Date.now(), markets: results.filter(Boolean) });
  }
  if (path === "/api/wf-trades") {
    const cid = url.searchParams.get("market");
    if (!cid || !/^0x[0-9a-f]{64}$/i.test(cid)) return jresp({ error: "market required" }, 400);
    try {
      const r = await fetch("https://data-api.polymarket.com/trades?market=" + cid + "&limit=100", { signal: AbortSignal.timeout(9000) });
      const j = await r.json();
      return jresp(j);
    } catch (e) {
      return jresp({ error: String((e && e.message) || e).slice(0, 100) }, 502);
    }
  }
  if (path === "/api/bars1s") {
    // 数据直通车：经 CF 出口拉币安 1s K线（本地代理断线时的备用数据通道）
    const sym = (url.searchParams.get("sym") || "BTCUSDT").toUpperCase().replace(/[^A-Z0-9]/g, "");
    let t = Number(url.searchParams.get("start") || 0);
    const pages = Math.min(45, Number(url.searchParams.get("pages") || 40));
    if (!/^[A-Z0-9]{5,12}$/.test(sym) || !t) return jresp({ error: "bad params" }, 400);
    const closes = [];
    let last = t;
    const hosts = ["https://data-api.binance.vision", "https://api.binance.com"];
    for (let i = 0; i < pages; i++) {
      let j = null;
      for (const h of hosts) {
        try {
          const r = await fetch(h + "/api/v3/klines?symbol=" + sym + "&interval=1s&startTime=" + t + "&limit=1000", { signal: AbortSignal.timeout(8000) });
          if (r.status !== 200) continue;
          const jj = await r.json();
          if (Array.isArray(jj) && jj.length) { j = jj; break; }
        } catch (e) {}
      }
      if (!j) break;
      for (const k of j) closes.push([k[0], Number(k[4])]);
      t = j[j.length - 1][6] + 1;
      last = t;
      if (t >= Date.now() - 2000) break;
    }
    return jresp({ sym, start: Number(url.searchParams.get("start") || 0), next: last, n: closes.length, closes });
  }
  if (path === "/api/tickers") {
    // 全币种实时价（CF 出口 → 币安行情端点），供扩展后台版 S2P 使用
    const hosts = ["https://data-api.binance.vision", "https://api.binance.com"];
    for (const h of hosts) {
      try {
        const r = await fetch(h + "/api/v3/ticker/price", { signal: AbortSignal.timeout(8000) });
        if (r.status !== 200) continue;
        const j = await r.json();
        const out = {};
        for (const t of j) out[t.symbol] = Number(t.price);
        return jresp(out);
      } catch (e) {}
    }
    return jresp({ error: "upstream failed" }, 502);
  }
  if (path === "/api/signals") {
    // 扩展桥信号通道：内存主路径（多端轮询共享）+ D1 尽力而为（配额耗尽时自动降级，不阻塞）
    if (request.method === "POST") {
      const body = await request.json().catch(() => null);
      if (!body || body.token !== env.PUT_TOKEN || !Array.isArray(body.list)) return jresp({ error: "forbidden" }, 403);
      const payload = { t: Date.now(), list: body.list.slice(0, 20) };
      sigCache = { t: payload.t, data: payload, cmd: sigCache ? sigCache.cmd : null };
      try {
        await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('signals',?)")
          .bind(JSON.stringify(payload)).run();
      } catch (e) { d1DownUntil = Date.now() + 60000; }
      return jresp({ ok: true, stored: payload.list.length });
    }
    if (sigCache && Date.now() - sigCache.t < 1500) {
      const stale = Date.now() - sigCache.data.t > 120000;
      return jresp({ now: Date.now(), signals: stale ? [] : sigCache.data.list, sigTs: sigCache.data.t, cmd: sigCache.cmd });
    }
    let parsed = null, command = null;
    if (!d1DownUntil || Date.now() > d1DownUntil) {
      try {
        const sig = await env.DB.prepare("SELECT v FROM meta WHERE k='signals'").all();
        const cmd = await env.DB.prepare("SELECT v FROM meta WHERE k='commands'").all();
        try { parsed = JSON.parse((sig.results[0] || {}).v || "null"); } catch (e) {}
        try { command = JSON.parse((cmd.results[0] || {}).v || "null"); } catch (e) {}
      } catch (e) { d1DownUntil = Date.now() + 30000; }
    }
    const stale = !parsed || Date.now() - parsed.t > 120000;
    sigCache = { t: Date.now(), data: { t: parsed ? parsed.t : 0, list: stale ? [] : (parsed.list || []) }, cmd: command };
    return jresp({ now: Date.now(), signals: sigCache.data.list, sigTs: sigCache.data.t, cmd: command });
  }
  if (path === "/api/commands") {
    // 模式一控制通道：管理员（ZCode/运维）下发指令；页面脚本执行后回传结果
    if (request.method === "POST") {
      const body = await request.json().catch(() => null);
      if (!body || !body.action) return jresp({ error: "forbidden or bad params" }, 403);
      // 结果回传（页面脚本执行后调用，token 用 report 标识）
      if (body.action === "result") {
        await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('cmd_result',?)")
          .bind(JSON.stringify({ id: body.id, out: body.out, t: Date.now() })).run();
        return jresp({ ok: true });
      }
      if (body.token !== env.PUT_TOKEN) return jresp({ error: "forbidden" }, 403);
      const cmd = { id: Date.now(), action: body.action, params: body.params || {}, t: Date.now() };
      await env.DB.prepare("INSERT OR REPLACE INTO meta (k,v) VALUES ('commands',?)").bind(JSON.stringify(cmd)).run();
      return jresp({ ok: true, cmd });
    }
    const cmd = await env.DB.prepare("SELECT v FROM meta WHERE k='commands'").all();
    let command = null;
    try { command = JSON.parse((cmd.results[0] || {}).v || "null"); } catch (e) {}
    if (url.searchParams.get("result")) {
      let result = null;
      try { result = JSON.parse(((await env.DB.prepare("SELECT v FROM meta WHERE k='cmd_result'").all()).results[0] || {}).v || "null"); } catch (e) {}
      return jresp({ cmd: command, result });
    }
    return jresp({ cmd: command });
  }
  if (path === "/api/wf-book") {
    // 策略信号：代理 CLOB 订单簿（返回最优买卖价）
    const token = url.searchParams.get("token") || "";
    if (!/^[0-9]{40,90}$/.test(token)) return jresp({ error: "token required" }, 400);
    try {
      const r = await fetch("https://clob.polymarket.com/book?token_id=" + token, { signal: AbortSignal.timeout(8000) });
      const j = await r.json();
      const bids = j.bids || [], asks = j.asks || [];
      return jresp({
        bid: bids.length ? Number(bids[bids.length - 1].price) : null,
        ask: asks.length ? Number(asks[0].price) : null,
        bidSz: bids.length ? Number(bids[bids.length - 1].size) : 0,
        askSz: asks.length ? Number(asks[0].size) : 0,
      });
    } catch (e) {
      return jresp({ error: String((e && e.message) || e).slice(0, 100) }, 502);
    }
  }
  if (path === "/api/backfill-okx") {
    // OKX 历史回填：OKX 1m K线（CF 出口直连 OKX）→ 按“币种列”更新 okx 行，不覆盖其他币已有值
    const token = url.searchParams.get("token") || "";
    const coin = (url.searchParams.get("coin") || "BTC-USDT").toUpperCase();
    const date = url.searchParams.get("date") || "";
    if (token !== env.PUT_TOKEN) return jresp({ error: "forbidden" }, 403);
    if (!/^[A-Z]{2,6}-USDT$/.test(coin) || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return jresp({ error: "bad params" }, 400);
    const start = Date.parse(date + "T00:00:00+08:00");
    if (!isFinite(start)) return jresp({ error: "bad date" }, 400);
    const end = start + 86400000;
    const col = { "BTC-USDT": "btc", "ETH-USDT": "eth", "SOL-USDT": "sol", "BNB-USDT": "bnb", "DOGE-USDT": "doge" }[coin];
    if (!col) return jresp({ error: "unsupported coin" }, 400);
    let cursor = end;
    const vals = [];
    for (let page = 0; page < 20 && cursor > start; page++) {
      let j = null;
      try {
        const r = await fetch("https://www.okx.com/api/v5/market/history-candles?instId=" + coin + "&bar=1m&after=" + cursor + "&limit=100", { signal: AbortSignal.timeout(8000) });
        j = await r.json();
      } catch (e) { break; }
      const dd = (j && j.data) || [];
      if (!dd.length) break;
      for (const k of dd) {
        const ts = Number(k[0]);
        if (ts >= start && ts < end) {
          const c = Number(k[4]);
          if (isFinite(c) && c > 0) vals.push([ts, c]);
        }
      }
      cursor = Number(dd[dd.length - 1][0]);
      if (cursor <= start) break;
    }
    let written = 0;
    for (let i = 0; i < vals.length; i += 300) {
      const chunk = vals.slice(i, i + 300);
      const sql = "INSERT INTO bars (ts,source," + col + ") VALUES " +
        chunk.map(([ts, c]) => "(" + ts + ",'okx'," + c + ")").join(",") +
        " ON CONFLICT(source,ts) DO UPDATE SET " + col + "=excluded." + col + " WHERE bars." + col + " IS NULL";
      try {
        const r = await env.DB.prepare(sql).run();
        written += (r.meta && r.meta.changes) || 0;
      } catch (e) {}
    }
    return jresp({ ok: true, coin, date, candles: vals.length, written });
  }
  return null;
}

// ---------- 静态资源（构建时内联） ----------
function serveStatic(path) {
  const mapping = {
    "/": "index.html",
    "/index.html": "index.html",
    "/app.js": "app.js",
    "/echarts.min.js": "echarts.min.js",
    "/engine.js": "engine.js",
    "/vpngate": "vpngate.html",
    "/vpngate.html": "vpngate.html",
  };
  const filename = mapping[path];
  if (!filename || !(filename in INLINE_ASSETS)) return null;
  const content = INLINE_ASSETS[filename];
  const ct = filename.endsWith(".js") ? "application/javascript" : filename.endsWith(".html") ? "text/html" : "text/plain";
  // HTML 每次都回源（内含带版本号的 JS 引用），JS 可缓存 1 天
  const cc = filename.endsWith(".html") ? "no-store" : "public, max-age=86400";
  return new Response(content, {
    headers: { "Content-Type": ct + "; charset=utf-8", "Cache-Control": cc },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path.startsWith("/api/")) {
      try {
        const apiResp = await handleApi(request, env);
        if (apiResp) return apiResp;
      } catch (e) {
        return jresp({ error: String((e && e.message) || e) }, 500);
      }
      return jresp({ error: "not found" }, 404);
    }
    const staticResp = serveStatic(path);
    if (staticResp) return staticResp;
    return serveStatic("/");
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(sampleRound(env));
    if (event.cron === "5 16 * * *") {
      ctx.waitUntil(cleanup(env));
    }
  },
};
