// 本地常驻采集器：经 10808 代理连币安/OKX/Polymarket WebSocket，10s 聚合后推回 Cloudflare D1
// 运行：node collector/collector.js   （可用环境变量 PROXY 覆盖代理地址）
const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");
const PUT_URL = "https://btc.hhxx.eu.org/api/put";
const PUT_TOKEN = "pmtk_9f3ac41e7d2b8056";
const PROXY = process.env.PROXY || "http://127.0.0.1:10808";
const agent = new HttpsProxyAgent(PROXY);
const BINANCE_MAP = { BTCUSDT: "btc", ETHUSDT: "eth", SOLUSDT: "sol", BNBUSDT: "bnb", DOGEUSDT: "doge" };
const OKX_MAP = { "BTC-USDT": "btc", "ETH-USDT": "eth", "SOL-USDT": "sol", "BNB-USDT": "bnb", "DOGE-USDT": "doge" };
const PM_MAP = { "btc/usd": "btc", "eth/usd": "eth", "sol/usd": "sol", "xrp/usd": "xrp", "bnb/usd": "bnb", "doge/usd": "doge" };
const PM_BINANCE_MAP = { btcusdt: "btc", ethusdt: "eth", solusdt: "sol", xrpusdt: "xrp", bnbusdt: "bnb", dogeusdt: "doge" };
const IDX = { btc: 0, eth: 1, sol: 2, bnb: 3, doge: 4, xrp: 5 };
const latest = { binance: {}, okx: {}, polymarket: {} };
const state = { binance: "init", okx: "init", polymarket: "init" };
function log(...a) { console.log(new Date().toLocaleTimeString("zh-CN", { hour12: false }), ...a); }
function connectBinance() {
  try {
    const streams = Object.keys(BINANCE_MAP).map((s) => s.toLowerCase() + "@miniTicker").join("/");
    const ws = new WebSocket("wss://stream.binance.com:9443/stream?streams=" + streams, { agent });
    ws.on("open", () => { state.binance = "open"; log("币安 WS 已连接"); });
    ws.on("message", (d) => {
      try { const m = JSON.parse(d); const dd = m.data || m; const c = BINANCE_MAP[dd.s]; if (c) latest.binance[c] = { v: parseFloat(dd.c), t: dd.E || Date.now() }; } catch (e) {}
    });
    ws.on("close", () => { state.binance = "closed"; setTimeout(connectBinance, 5000); });
    ws.on("error", (e) => { state.binance = "err:" + ((e && e.message) || e).slice(0, 60); try { ws.close(); } catch (_) {} });
  } catch (e) { setTimeout(connectBinance, 5000); }
}
function connectOKX() {
  try {
    const ws = new WebSocket("wss://ws.okx.com:8443/ws/v5/public", { agent });
    ws.on("open", () => {
      state.okx = "open"; log("OKX WS 已连接");
      ws.send(JSON.stringify({ op: "subscribe", args: Object.keys(OKX_MAP).map((i) => ({ channel: "tickers", instId: i })) }));
    });
    ws.on("message", (d) => {
      const raw = String(d); if (raw === "pong") return;
      try { const m = JSON.parse(raw); if (m.arg && m.data && m.data[0]) { const c = OKX_MAP[m.arg.instId]; if (c) latest.okx[c] = { v: parseFloat(m.data[0].last), t: Number(m.data[0].ts) || Date.now() }; } } catch (e) {}
    });
    ws.on("close", () => { state.okx = "closed"; setTimeout(connectOKX, 5000); });
    ws.on("error", (e) => { state.okx = "err:" + ((e && e.message) || e).slice(0, 60); try { ws.close(); } catch (_) {} });
    const ping = setInterval(() => { try { if (ws.readyState === 1) ws.send("ping"); else clearInterval(ping); } catch (e) { clearInterval(ping); } }, 15000);
  } catch (e) { setTimeout(connectOKX, 5000); }
}
function connectPM() {
  try {
    const ws = new WebSocket("wss://ws-live-data.polymarket.com", { agent });
    ws.on("open", () => {
      state.polymarket = "open"; log("Polymarket RTDS 已连接");
      ws.send(JSON.stringify({
        action: "subscribe",
        subscriptions: [
          { topic: "crypto_prices_chainlink", type: "update", filters: "btc/usd,eth/usd,sol/usd,xrp/usd,bnb/usd,doge/usd" },
          { topic: "crypto_prices", type: "update", filters: "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt" },
        ],
      }));
    });
    ws.on("message", (d) => {
      const raw = String(d); if (!raw || raw === "PONG") return;
      try {
        const m = JSON.parse(raw); const topic = m.topic || "";
        let items = [];
        if (m.payload) items = Array.isArray(m.payload) ? m.payload : [m.payload];
        else if (m.symbol) items = [m];
        for (const p of items) {
          const sym = String(p.symbol || "").toLowerCase();
          const v = parseFloat(p.value != null ? p.value : p.price);
          if (!isFinite(v)) continue;
          const c = topic === "crypto_prices" ? PM_BINANCE_MAP[sym] : PM_MAP[sym];
          if (c) latest.polymarket[c] = { v, t: p.timestamp ? Number(p.timestamp) : Date.now() };
        }
      } catch (e) {}
    });
    ws.on("close", () => { state.polymarket = "closed"; setTimeout(connectPM, 5000); });
    ws.on("error", (e) => { state.polymarket = "err:" + ((e && e.message) || e).slice(0, 60); try { ws.close(); } catch (_) {} });
    const ping = setInterval(() => { try { if (ws.readyState === 1) ws.send("PING"); else clearInterval(ping); } catch (e) { clearInterval(ping); } }, 5000);
  } catch (e) { setTimeout(connectPM, 5000); }
}
function bucketStart(ts) { return Math.floor(ts / 10000) * 10000; }
const buf = [];
let flushing = false;
setInterval(() => {
  const ts = bucketStart(Date.now()) - 10000;
  for (const src of ["binance", "okx", "polymarket"]) {
    const ll = latest[src];
    const row = [null, null, null, null, null, null];
    let has = false;
    for (const coin in ll) { const o = ll[coin]; if (o && Date.now() - o.t < 20000) { row[IDX[coin]] = o.v; has = true; } }
    if (has) buf.push({ source: src, ts, row });
  }
  flush();
}, 10000);
async function flush() {
  if (flushing || !buf.length) return;
  flushing = true;
  const bars = buf.splice(0, 60);
  try {
    const r = await fetch(PUT_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: PUT_TOKEN, bars }),
      signal: AbortSignal.timeout(15000),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) { buf.unshift(...bars); }
    else log("推送", bars.length, "条 (币安", bars.filter((b) => b.source === "binance").length, "/OKX", bars.filter((b) => b.source === "okx").length, "/PM", bars.filter((b) => b.source === "polymarket").length + ")");
  } catch (e) { buf.unshift(...bars); }
  flushing = false;
}
connectBinance(); connectOKX(); connectPM();
setInterval(() => log("状态", JSON.stringify(state)), 60000);
log("本地采集器已启动，代理", PROXY, "→ 推送目标", PUT_URL);
