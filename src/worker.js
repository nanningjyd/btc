// btc-monitor Worker：Cron 采集（每分钟 6 个 10s 采样点）+ API + 静态资源托管
const SOURCES = ["binance", "okx", "polymarket"];
const BINANCE_MAP = { BTCUSDT: "btc", ETHUSDT: "eth", SOLUSDT: "sol", BNBUSDT: "bnb", DOGEUSDT: "doge" };
const PM_MAP = { "btc/usd": "btc", "eth/usd": "eth", "sol/usd": "sol", "xrp/usd": "xrp", "bnb/usd": "bnb", "doge/usd": "doge" };
const PM_BINANCE_MAP = { btcusdt: "btc", ethusdt: "eth", solusdt: "sol", xrpusdt: "xrp", bnbusdt: "bnb", dogeusdt: "doge" };
const TZ = 8 * 3600 * 1000;

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
    headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" },
  });
}

let binanceHost = null; // 记住本 isolate 内可用的主机（CF 出口可能被 geo 封锁）
const BINANCE_HOSTS = ["https://data-api.binance.vision", "https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com"];

async function fetchBinance() {
  const symbols = JSON.stringify(Object.keys(BINANCE_MAP));
  const hosts = binanceHost ? [binanceHost].concat(BINANCE_HOSTS.filter((h) => h !== binanceHost)) : BINANCE_HOSTS;
  let lastErr = null;
  for (const host of hosts) {
    try {
      const resp = await fetch(host + "/api/v3/ticker/price?symbols=" + encodeURIComponent(symbols), {
        signal: AbortSignal.timeout(6000),
      });
      if (!resp.ok) throw new Error("HTTP " + resp.status + " @" + host.replace("https://", ""));
      const arr = await resp.json();
      const out = {};
      for (const it of arr) { const c = BINANCE_MAP[it.symbol]; if (c) out[c] = parseFloat(it.price); }
      if (Object.keys(out).length < 5) throw new Error("incomplete @" + host.replace("https://", ""));
      binanceHost = host;
      return out;
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error("binance all hosts failed");
}

async function fetchOKX() {
  const hosts = ["https://www.okx.com", "https://aws.okx.com", "https://app.okx.com"];
  let lastErr = null;
  for (const host of hosts) {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const resp = await fetch(host + "/api/v5/market/tickers?instType=SPOT", {
          signal: AbortSignal.timeout(8000),
        });
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

// 币安 WebSocket 流（REST 被 CF 出口 geo 封锁，WS 可用）+ 自动重连
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

// OKX WebSocket 流（REST 时常 429）
function startOKXStream() {
  const latest = {};
  const diag = { opened: false, err: null };
  let ws = null, closed = false, pingTimer = null;
  (async () => {
    try {
      const resp = await fetch("https://ws.okx.com:8443/ws/v5/public", { headers: { Upgrade: "websocket" } });
      ws = resp.webSocket;
      if (!ws) throw new Error("no ws, status " + resp.status);
      ws.accept();
      diag.opened = true;
      ws.addEventListener("message", (ev) => {
        const raw = typeof ev.data === "string" ? ev.data : "";
        if (raw === "pong") return;
        try {
          const m = JSON.parse(raw);
          if (m.arg && m.data && m.data[0]) {
            const coin = BINANCE_MAP_SYMBOL(m.arg.instId);
            if (coin && m.data[0].last) latest[coin] = { v: parseFloat(m.data[0].last), t: Number(m.data[0].ts) || Date.now() };
          }
        } catch (e) {}
      });
      ws.addEventListener("close", () => { closed = true; });
      ws.send(JSON.stringify({ op: "subscribe", args: ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "DOGE-USDT"].map((i) => ({ channel: "tickers", instId: i })) }));
      pingTimer = setInterval(() => { try { if (!closed) ws.send("ping"); } catch (e) {} }, 15000);
    } catch (e) { diag.err = String((e && e.message) || e); closed = true; }
  })();
  return {
    snapshot() {
      const now = Date.now();
      const out = {};
      for (const c in latest) { if (now - latest[c].t < 20000) out[c] = latest[c].v; }
      return out;
    },
    diag() { return Object.assign({ n: Object.keys(latest).length }, diag); },
    stop() { if (pingTimer) clearInterval(pingTimer); try { if (ws && !closed) ws.close(); } catch (e) {} },
  };
}
function startPolymarketStream() {
  const chainlink = {};
  const topicBinance = {};
  const seen = new Set();
  const diag = { opened: false, msgs: 0, parsed: 0, err: null };
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
          const msg = JSON.parse(raw);
          const topic = msg.topic || "";
          let items = [];
          if (msg.payload) items = Array.isArray(msg.payload) ? msg.payload : [msg.payload];
          else if (msg.symbol) items = [msg];
          for (const p of items) {
            const sym = String(p.symbol || "").toLowerCase();
            if (!sym) continue;
            const v = parseFloat(p.value != null ? p.value : p.price);
            if (!isFinite(v)) continue;
            const t = p.timestamp ? Number(p.timestamp) : Date.now();
            const isLink = PM_MAP[sym];
            const isTopic = PM_BINANCE_MAP[sym];
            if (topic === "crypto_prices_chainlink" && isLink) { chainlink[isLink] = { v, t }; seen.add(sym); diag.parsed++; }
            else if (topic === "crypto_prices" && isTopic) { topicBinance[isTopic] = { v, t }; seen.add(sym); diag.parsed++; }
            else if (!topic && (isLink || isTopic)) { (isLink ? chainlink : topicBinance)[isLink || isTopic] = { v, t }; seen.add(sym); diag.parsed++; }
          }
        } catch (e) {}
      });
      ws.addEventListener("close", () => { closed = true; });
      ws.send(JSON.stringify({
        action: "subscribe",
        subscriptions: [
          { topic: "crypto_prices_chainlink", type: "update", filters: "btc/usd,eth/usd,sol/usd,xrp/usd,bnb/usd,doge/usd" },
          { topic: "crypto_prices", type: "update", filters: "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt" },
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
// 币安用 WS（REST 被 geo 封锁）；OKX 用 REST 单次全量（WS tickers 推送频率过高会打爆免费版 CPU）；Polymarket 用 RTDS WS。
async function sampleRound(env) {
  const N = parseInt(env.SAMPLES_PER_RUN || "6", 10);
  const samples = [];
  const bns = startBinanceStream();
  const pm = startPolymarketStream();
  await waitMs(2000); // 等待币安 WS 建立并收到首批数据
  for (let i = 0; i < N; i++) {
    const now = Date.now();
    let bucketEnd = bucketStart(now) + 10000;
    let target = bucketEnd - 800;
    if (target <= now) { target += 10000; bucketEnd += 10000; } // 本桶采样点已过 → 跳到下一桶
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
    if (s.b && Object.keys(s.b).length) { rowsBySource.binance.push([s.ts, s.b.btc ?? null, s.b.eth ?? null, s.b.sol ?? null, s.b.bnb ?? null, s.b.doge ?? null, null]); report.binance.ok++; }
    else if (!report.binance.err) report.binance.err = "ws empty";
    if (s.o && Object.keys(s.o).length) { rowsBySource.okx.push([s.ts, s.o.btc ?? null, s.o.eth ?? null, s.o.sol ?? null, s.o.bnb ?? null, s.o.doge ?? null, null]); report.okx.ok++; }
    else if (!report.okx.err) report.okx.err = "rest empty/429";
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

async function handleApi(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
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
      for (let i = 0; i < 6; i++) { const v = row[i]; if (v != null && !isFinite(Number(v))) { okRow = false; break; } row[i] = v == null ? null : Number(v); }
      if (!okRow) continue;
      rowsBySource[b.source].push([ts, row[0], row[1], row[2], row[3], row[4], row[5]]);
    }
    let written = 0;
    for (const src of SOURCES) {
      if (!rowsBySource[src].length) continue;
      try { await insertBars(env, src, rowsBySource[src]); written += rowsBySource[src].length; } catch (e) {}
    }
    return jresp({ ok: true, written });
  }
  if (path === "/api/probe") {
    const secret = url.searchParams.get("secret");
    if (!env.COLLECT_SECRET || secret !== env.COLLECT_SECRET) return jresp({ error: "forbidden" }, 403);
    const out = { binance: {}, now: Date.now() };
    for (const h of BINANCE_HOSTS) {
      try {
        const r = await fetch(h + "/api/v3/ping", { signal: AbortSignal.timeout(6000) });
        out.binance[h.replace("https://", "")] = r.status;
      } catch (e) { out.binance[h.replace("https://", "")] = String((e && e.message) || e).slice(0, 60); }
    }
    try {
      const r = await fetch("https://data-api.binance.vision/api/v3/ping", {
        headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json" },
        signal: AbortSignal.timeout(6000),
      });
      out.binance["vision+UA"] = r.status;
    } catch (e) { out.binance["vision+UA"] = String((e && e.message) || e).slice(0, 60); }
    try {
      const r = await fetch("https://www.okx.com/api/v5/market/tickers?instType=SPOT", { signal: AbortSignal.timeout(8000) });
      const j = await r.json();
      out.okx = { status: r.status, instruments: (j.data || []).length };
    } catch (e) { out.okx = String((e && e.message) || e).slice(0, 60); }
    try {
      const r = await fetch("https://aws.okx.com/api/v5/market/tickers?instType=SPOT", { signal: AbortSignal.timeout(8000) });
      const j = await r.json();
      out.okxAws = { status: r.status, instruments: (j.data || []).length };
    } catch (e) { out.okxAws = String((e && e.message) || e).slice(0, 60); }
    try {
      const r = await fetch("https://stream.binance.com:9443/stream?streams=btcusdt@miniTicker", { headers: { Upgrade: "websocket" }, signal: AbortSignal.timeout(8000) });
      const wsx = r.webSocket;
      if (wsx) { wsx.accept(); out.binanceWS = { status: r.status, note: "ws ok", msg: await new Promise((res) => { const t = setTimeout(() => res("timeout"), 6000); wsx.addEventListener("message", (ev) => { clearTimeout(t); try { wsx.close(); } catch (e) {} res(String(ev.data).slice(0, 120)); }); }) }; }
      else out.binanceWS = { status: r.status, note: "no websocket" };
    } catch (e) { out.binanceWS = String((e && e.message) || e).slice(0, 60); }
    try {
      const r = await fetch("https://ws.okx.com:8443/ws/v5/public", { headers: { Upgrade: "websocket" }, signal: AbortSignal.timeout(8000) });
      const wsx = r.webSocket;
      if (wsx) {
        wsx.accept();
        out.okxWS = { status: r.status, msg: await new Promise((res) => {
          const t = setTimeout(() => res("timeout"), 7000);
          wsx.addEventListener("message", (ev) => { clearTimeout(t); try { wsx.close(); } catch (e) {} res(String(ev.data).slice(0, 120)); });
          wsx.send(JSON.stringify({ op: "subscribe", args: [{ channel: "tickers", instId: "BTC-USDT" }] }));
        }) };
      } else out.okxWS = { status: r.status, note: "no websocket" };
    } catch (e) { out.okxWS = String((e && e.message) || e).slice(0, 60); }
    const pm = startPolymarketStream();
    await waitMs(9000);
    out.polymarket = { diag: pm.diag(), seen: pm.seenSymbols(), snapshot: pm.snapshot() };
    pm.stop();
    return jresp(out);
  }
  return jresp({ error: "not found" }, 404);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      try { return await handleApi(request, env); }
      catch (e) { return jresp({ error: String((e && e.message) || e) }, 500); }
    }
    return env.ASSETS.fetch(request);
  },
  async scheduled(controller, env, ctx) {
    if (controller.cron === "5 16 * * *") { await cleanup(env); return; }
    try { await sampleRound(env); } catch (e) { console.error("sampleRound failed", e); }
  },
};
