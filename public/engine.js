// 分析引擎 Web Worker：实时采集(WS) + 10s 聚合 + ZigZag 转折识别 + 滞后匹配 + 概率统计
const SOURCES = ["binance", "okx", "polymarket"];
const COIN_IDX = { btc: 0, eth: 1, sol: 2, bnb: 3, doge: 4, xrp: 5 };
const IDX_COIN = ["btc", "eth", "sol", "bnb", "doge", "xrp"];
const TZ = 8 * 3600 * 1000;
const BINANCE_MAP = { BTCUSDT: "btc", ETHUSDT: "eth", SOLUSDT: "sol", BNBUSDT: "bnb", DOGEUSDT: "doge" };
const OKX_MAP = { "BTC-USDT": "btc", "ETH-USDT": "eth", "SOL-USDT": "sol", "BNB-USDT": "bnb", "DOGE-USDT": "doge" };
const PM_MAP = { "btc/usd": "btc", "eth/usd": "eth", "sol/usd": "sol", "xrp/usd": "xrp", "bnb/usd": "bnb", "doge/usd": "doge" };
const PM_BINANCE_MAP = { btcusdt: "btc", ethusdt: "eth", solusdt: "sol", xrpusdt: "xrp", bnbusdt: "bnb", dogeusdt: "doge" };
let params = { threshold: 0.15, before: -60, after: 300, minGap: 30, mode2: false };
let analysisDate = "today"; // today | yesterday
let started = false;
const data = {}; // source -> { map: Map(ts->row[6]), keys: 升序 ts 数组 }
for (const s of SOURCES) data[s] = { map: new Map(), keys: [] };
const liveLatest = { binance: {}, okx: {}, polymarket: {} };
// Chainlink 多所聚合价（Polymarket 结算源）：最新值 + 环形缓冲（算 TWAP / 窗口起点价）
const clLatest = {};
const clBuf = {};
const CL_RANGES = [
  { min: 50000, max: 200000, c: "btc" },
  { min: 1500, max: 5000, c: "eth" },
  { min: 50, max: 200, c: "sol" },
  { min: 300, max: 1000, c: "bnb" },
  { min: 0.5, max: 5, c: "xrp" },
  { min: 0.0001, max: 0.5, c: "doge" },
];
function clCoinByRange(v) {
  for (const r of CL_RANGES) { if (v >= r.min && v <= r.max) return r.c; }
  return null;
}
const connState = { binance: "init", okx: "init", polymarket: "init" };
const lastPivotIds = {}; // source_coin -> Set(id)，用于新拐点检测（预测事件）
const mode2Last = {}; // source_coin -> 上次信号时间
function bucketStart(ts) { return Math.floor(ts / 10000) * 10000; }
function post(msg) { self.postMessage(msg); }
function setConn(src, st) { if (connState[src] !== st) { connState[src] = st; post({ type: "conn", source: src, state: st }); } }
// ---------- 数据管理 ----------
function loadData(src, rows) {
  const d = data[src];
  for (const r of rows) d.map.set(r[0], r.slice(1));
  d.keys = Array.from(d.map.keys()).sort((a, b) => a - b);
}
function setBar(src, ts, row) {
  const d = data[src];
  if (!d.map.has(ts)) {
    d.map.set(ts, row);
    if (!d.keys.length || ts > d.keys[d.keys.length - 1]) d.keys.push(ts);
    else {
      let i = d.keys.length - 1;
      while (i >= 0 && d.keys[i] > ts) i--;
      d.keys.splice(i + 1, 0, ts);
    }
  } else d.map.set(ts, row);
}
function windowRange() {
  const now = Date.now();
  const startToday = Math.floor((now + TZ) / 86400000) * 86400000 - TZ;
  return analysisDate === "yesterday" ? [startToday - 86400000, startToday] : [startToday, now + 20000];
}
function buildPoints(src, coin) {
  const d = data[src];
  const [a, b] = windowRange();
  const idx = COIN_IDX[coin];
  const pts = [];
  for (const ts of d.keys) {
    if (ts < a) continue;
    if (ts >= b) break;
    const row = d.map.get(ts);
    if (row && row[idx] != null) pts.push({ t: ts, v: row[idx] });
  }
  return pts;
}
// ---------- ZigZag 转折点 ----------
function zigzag(pts) {
  const thr = params.threshold;
  const minGapMs = params.minGap * 1000;
  const pivots = [];
  const n = pts.length;
  if (n < 3) return pivots;
  let dir = 0, extI = 0, lowI = 0, highI = 0;
  const tryPush = (p) => {
    const last = pivots[pivots.length - 1];
    if (last && p.t - last.t < minGapMs) {
      const better = p.type === "bottom" ? p.v < last.v : p.v > last.v;
      if (better) pivots[pivots.length - 1] = p;
      return;
    }
    pivots.push(p);
  };
  for (let i = 1; i < n; i++) {
    const p = pts[i].v;
    if (dir === 1) {
      const e = pts[extI].v;
      if (p > e) extI = i;
      else if ((e - p) / e * 100 >= thr) { tryPush({ type: "top", t: pts[extI].t, v: e, confirmedT: pts[i].t }); dir = -1; extI = i; }
    } else if (dir === -1) {
      const e = pts[extI].v;
      if (p < e) extI = i;
      else if ((p - e) / e * 100 >= thr) { tryPush({ type: "bottom", t: pts[extI].t, v: e, confirmedT: pts[i].t }); dir = 1; extI = i; }
    } else {
      if (p < pts[lowI].v) lowI = i;
      if (p > pts[highI].v) highI = i;
      const low = pts[lowI].v, high = pts[highI].v;
      if (low > 0 && (p - low) / low * 100 >= thr) { tryPush({ type: "bottom", t: pts[lowI].t, v: low, confirmedT: pts[i].t }); dir = 1; extI = i; }
      else if ((high - p) / high * 100 >= thr) { tryPush({ type: "top", t: pts[highI].t, v: high, confirmedT: pts[i].t }); dir = -1; extI = i; }
    }
  }
  return pivots;
}
// ---------- 滞后匹配 ----------
function matchLags(lp, fp) {
  const before = params.before * 1000, after = params.after * 1000;
  const res = [];
  const used = new Set();
  for (const l of lp) {
    let best = null, bestAbs = Infinity;
    for (const f of fp) {
      if (f.type !== l.type || used.has(f)) continue;
      const d = f.t - l.t;
      if (d >= before && d <= after) {
        const ab = Math.abs(d);
        if (ab < bestAbs) { bestAbs = ab; best = f; }
      }
    }
    if (best) { used.add(best); res.push({ leaderT: l.t, leaderConfirm: l.confirmedT, type: l.type, followerT: best.t, lag: Math.round((best.t - l.t) / 100) / 10 }); }
    else res.push({ leaderT: l.t, leaderConfirm: l.confirmedT, type: l.type, followerT: null, lag: null });
  }
  return res;
}
// ---------- 分析主流程 ----------
function analyze(src) {
  const [a, b] = windowRange();
  const d = data[src];
  const avail = {};
  for (const coin of IDX_COIN) {
    let c = 0;
    for (const ts of d.keys) {
      if (ts < a) continue;
      if (ts >= b) break;
      const row = d.map.get(ts);
      if (row && row[COIN_IDX[coin]] != null) c++;
    }
    avail[coin] = c >= 10;
  }
  const coins = IDX_COIN.filter((c) => avail[c]);
  const pivotsByCoin = {};
  for (const coin of coins) pivotsByCoin[coin] = zigzag(buildPoints(src, coin));
  const leaders = ["btc", "eth"].filter((c) => avail[c]);
  const followerList = ["sol"];
  if (avail["bnb"]) followerList.push("bnb");
  else if (avail["xrp"]) followerList.push("xrp");
  if (avail["doge"] && followerList.indexOf("doge") < 0) followerList.push("doge");
  const matches = [];
  const stats = {};
  const predictEvents = [];
  for (const L of leaders) {
    const lp = pivotsByCoin[L] || [];
    const ids = new Set(lp.map((p) => p.t + "_" + p.type));
    const prev = lastPivotIds[src + "_" + L];
    const freshPivots = prev ? lp.filter((p) => !prev.has(p.t + "_" + p.type)) : [];
    lastPivotIds[src + "_" + L] = ids;
    for (const F of followerList) {
      const fp = pivotsByCoin[F] || [];
      const ms = matchLags(lp, fp);
      for (const type of ["bottom", "top"]) {
        const ms2 = ms.filter((m) => m.type === type);
        const matched = ms2.filter((m) => m.lag != null).map((m) => m.lag).sort((x, y) => x - y);
        const st = { n: matched.length, total: ms2.length, rate: ms2.length ? matched.length / ms2.length : 0, mean: null, median: null, min: null, max: null, hist: null };
        if (matched.length) {
          st.mean = +(matched.reduce((s, v) => s + v, 0) / matched.length).toFixed(1);
          st.median = matched[Math.floor(matched.length / 2)];
          st.min = matched[0];
          st.max = matched[matched.length - 1];
          const buckets = { "<0": 0, "0-10": 0, "10-30": 0, "30-60": 0, "60-120": 0, "120-300": 0, ">300": 0 };
          for (const l of matched) {
            if (l < 0) buckets["<0"]++;
            else if (l <= 10) buckets["0-10"]++;
            else if (l <= 30) buckets["10-30"]++;
            else if (l <= 60) buckets["30-60"]++;
            else if (l <= 120) buckets["60-120"]++;
            else if (l <= 300) buckets["120-300"]++;
            else buckets[">300"]++;
          }
          st.hist = buckets;
        }
        stats[L + "|" + F + "|" + type] = st;
      }
      let seq = 0;
      for (const m of ms) matches.push({ leader: L, follower: F, seq: ++seq, leaderT: m.leaderT, leaderConfirm: m.leaderConfirm, type: m.type, followerT: m.followerT, lag: m.lag });
    }
    // 新确认的主流币拐点 → 预测事件
    if (freshPivots.length && analysisDate === "today") {
      for (const p of freshPivots) {
        const preds = [];
        for (const F of followerList) {
          const st = stats[L + "|" + F + "|" + p.type];
          if (st && st.n >= 1) {
            preds.push({ follower: F, lag: st.median, predTime: p.t + st.median * 1000, rate: st.rate, n: st.n, sufficient: st.n >= 3 });
          } else {
            preds.push({ follower: F, lag: null, predTime: null, rate: 0, n: 0, sufficient: false });
          }
        }
        predictEvents.push({ source: src, leader: L, type: p.type, pivotT: p.t, price: p.v, preds });
      }
    }
  }
  // 模式2：BTC/ETH 15 分钟 ±1% 触发
  const signals = [];
  if (params.mode2 && analysisDate === "today") {
    for (const L of leaders) {
      const pts = buildPoints(src, L);
      if (pts.length < 90) continue;
      const last = pts[pts.length - 1];
      const t15 = last.t - 900000;
      let ref = null;
      for (let i = pts.length - 1; i >= 0; i--) { if (pts[i].t <= t15) { ref = pts[i].v; break; } }
      if (ref == null) continue;
      const chg = (last.v - ref) / ref * 100;
      const key = src + "_" + L;
      if (Math.abs(chg) >= 1 && (!mode2Last[key] || last.t - mode2Last[key] > 900000)) {
        mode2Last[key] = last.t;
        signals.push({ coin: L, t: last.t, chg: +chg.toFixed(2), dir: chg > 0 ? "up" : "down" });
      }
    }
  }
  post({ type: "analysis", source: src, date: analysisDate, coins, leaders, followers: followerList, pivotsByCoin, matches, stats, signals });
  for (const ev of predictEvents) post({ type: "predict", ...ev });
}
function emitSeries(src) {
  const [a, b] = windowRange();
  const d = data[src];
  const times = [], rows = [];
  for (const ts of d.keys) {
    if (ts < a) continue;
    if (ts >= b) break;
    times.push(ts);
    rows.push(d.map.get(ts));
  }
  post({ type: "series", source: src, date: analysisDate, times, rows });
}
// ---------- 实时 WS 连接 ----------
function connectBinance() {
  try {
    const streams = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "dogeusdt"].map((s) => s + "@miniTicker").join("/");
    const ws = new WebSocket("wss://stream.binance.com:9443/stream?streams=" + streams);
    ws.onopen = () => setConn("binance", "open");
    ws.onclose = () => { setConn("binance", "closed"); setTimeout(connectBinance, 8000); };
    ws.onerror = () => { try { ws.close(); } catch (e) {} };
    ws.onmessage = (ev) => {
      try {
        const m = JSON.parse(ev.data);
        const dd = m.data || m;
        const coin = BINANCE_MAP[dd.s];
        if (coin) liveLatest.binance[coin] = { v: parseFloat(dd.c), t: dd.E || Date.now() };
      } catch (e) {}
    };
  } catch (e) { setTimeout(connectBinance, 8000); }
}
function connectOKX() {
  try {
    const ws = new WebSocket("wss://ws.okx.com:8443/ws/v5/public");
    let opened = false;
    ws.onopen = () => {
      opened = true;
      setConn("okx", "open");
      ws.send(JSON.stringify({ op: "subscribe", args: Object.keys(OKX_MAP).map((i) => ({ channel: "tickers", instId: i })) }));
    };
    ws.onclose = () => { setConn("okx", "closed"); setTimeout(connectOKX, 8000); };
    ws.onerror = () => { try { ws.close(); } catch (e) {} };
    ws.onmessage = (ev) => {
      const raw = ev.data;
      if (raw === "pong") return;
      try {
        const m = JSON.parse(raw);
        if (m.arg && m.data && m.data[0]) {
          const coin = OKX_MAP[m.arg.instId];
          if (coin && m.data[0].last) liveLatest.okx[coin] = { v: parseFloat(m.data[0].last), t: Number(m.data[0].ts) || Date.now() };
        }
      } catch (e) {}
    };
    const ping = setInterval(() => { try { if (ws.readyState === 1) ws.send("ping"); else clearInterval(ping); } catch (e) { clearInterval(ping); } }, 20000);
  } catch (e) { setTimeout(connectOKX, 8000); }
}
function connectPM() {
  try {
    const ws = new WebSocket("wss://ws-live-data.polymarket.com");
    ws.onopen = () => {
      setConn("polymarket", "open");
      ws.send(JSON.stringify({
        action: "subscribe",
        subscriptions: [
          { topic: "crypto_prices", type: "update", filters: "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt" },
          { topic: "crypto_prices_chainlink", type: "update", filters: '{"symbol":"btc/usd"}' },
          { topic: "crypto_prices_chainlink", type: "update", filters: '{"symbol":"eth/usd"}' },
          { topic: "crypto_prices_chainlink", type: "update", filters: '{"symbol":"sol/usd"}' },
        ],
      }));
    };
    ws.onclose = () => { setConn("polymarket", "closed"); setTimeout(connectPM, 8000); };
    ws.onerror = () => { try { ws.close(); } catch (e) {} };
    ws.onmessage = (ev) => {
      const raw = ev.data;
      if (!raw || raw === "PONG") return;
      try {
        const msg = JSON.parse(raw);
        const topic = msg.topic || "";
        // Chainlink 聚合格式：{payload:{data:[{timestamp,value}...]}} 无 symbol —— 按价格区间推断币种，
        // 并把整段秒级历史一次性入缓冲（页面打开几秒即有完整 TWAP 覆盖）
        if (topic === "crypto_prices_chainlink" && msg.payload && msg.payload.data && Array.isArray(msg.payload.data)) {
          for (const it of msg.payload.data) {
            const v = parseFloat(it.value != null ? it.value : it.price);
            if (!isFinite(v) || v <= 0) continue;
            const t = it.timestamp ? Number(it.timestamp) : Date.now();
            const c = clCoinByRange(v);
            if (!c) continue;
            clLatest[c] = { v, t };
            const buf = clBuf[c] || (clBuf[c] = []);
            const last = buf.length ? buf[buf.length - 1] : null;
            if (!last || t - last.t >= 900) {
              buf.push({ v, t });
              const cutoff = t - 1000 * 1000;
              while (buf.length > 2 && buf[0].t < cutoff) buf.shift();
            }
          }
        }
        let items = [];
        if (msg.payload) items = Array.isArray(msg.payload) ? msg.payload : [msg.payload];
        else if (msg.symbol) items = [msg];
        for (const p of items) {
          const sym = String(p.symbol || "").toLowerCase();
          if (!sym) continue;
          const v = parseFloat(p.value != null ? p.value : p.price);
          if (!isFinite(v)) continue;
          const t = p.timestamp ? Number(p.timestamp) : Date.now();
          const coin = (topic === "crypto_prices" ? PM_BINANCE_MAP[sym] : PM_MAP[sym]) || (!topic ? (PM_MAP[sym] || PM_BINANCE_MAP[sym]) : null);
          if (coin) liveLatest.polymarket[coin] = { v, t };
          // Chainlink 多所聚合流（Polymarket 结算源）单独存储 + 环形缓冲算 TWAP
          if ((topic === "crypto_prices_chainlink" || (!topic && PM_MAP[sym])) && PM_MAP[sym]) {
            const c = PM_MAP[sym];
            const q = { v, t };
            clLatest[c] = q;
            const buf = clBuf[c] || (clBuf[c] = []);
            const last = buf.length ? buf[buf.length - 1] : null;
            if (!last || t - last.t >= 900) { // 同秒去重
              buf.push(q);
              const cutoff = t - 1000 * 1000; // 保留 ~1000 秒
              while (buf.length > 2 && buf[0].t < cutoff) buf.shift();
            }
          }
        }
      } catch (e) {}
    };
    const ping = setInterval(() => { try { if (ws.readyState === 1) ws.send("PING"); else clearInterval(ping); } catch (e) { clearInterval(ping); } }, 5000);
  } catch (e) { setTimeout(connectPM, 8000); }
}
// ---------- 10s 聚合（对齐到桶边界后 300ms 收桶） ----------
function doTick() {
  const now = Date.now();
  const ts = bucketStart(now) - 10000;
  for (const src of SOURCES) {
    const ll = liveLatest[src];
    if (!Object.keys(ll).length) continue;
    const row = [null, null, null, null, null, null];
    let has = false;
    for (const coin in ll) {
      const o = ll[coin];
      if (o && now - o.t < 20000) { row[COIN_IDX[coin]] = o.v; has = true; }
    }
    if (!has) continue;
    setBar(src, ts, row);
    if (analysisDate === "today") post({ type: "bar", source: src, ts, row });
    analyze(src);
  }
}
function scheduleTick() {
  const now = Date.now();
  const next = bucketStart(now) + 10000 + 300;
  setTimeout(() => { doTick(); scheduleTick(); }, next - now);
}
// ---------- 消息入口 ----------
// ---------- Chainlink TWAP 计算与推送（每秒） ----------
function clWindowStart(buf, winSec, now) {
  const wStart = Math.floor(now / (winSec * 1000)) * winSec * 1000;
  let best = null, bestD = 1e9;
  for (const q of buf) {
    const d = Math.abs(q.t - wStart);
    if (d <= 5000 && d < bestD) { best = q.v; bestD = d; }
  }
  if (best !== null) return { price: best, approx: false };
  // 近似：用缓冲区里最早值（页面加载晚于窗口开始）
  return { price: buf.length ? buf[0].v : null, approx: true };
}
function postChainlink() {
  const now = Date.now();
  const coins = {};
  for (const c in clLatest) {
    const buf = clBuf[c] || [];
    const recent = buf.filter((q) => q.t >= now - 61000);
    const span = recent.length ? recent[recent.length - 1].t - recent[0].t : 0;
    const twap = span >= 45000 ? recent.reduce((s, q) => s + q.v, 0) / recent.length : null;
    const s5 = clWindowStart(buf, 300, now);
    const s15 = clWindowStart(buf, 900, now);
    coins[c] = {
      p: clLatest[c].v, t: clLatest[c].t,
      twap,
      start5: s5.price, approx5: s5.approx,
      gap5: twap != null && s5.price ? (twap - s5.price) / s5.price * 1e4 : null,
      start15: s15.price, approx15: s15.approx,
      gap15: twap != null && s15.price ? (twap - s15.price) / s15.price * 1e4 : null,
    };
  }
  post({ type: "chainlink", coins, now });
}
setInterval(postChainlink, 1000);

  // ---------- 六源 BTC 1 秒价格（Chainlink/Binance/OKX 已有 + Coinbase/Kraken WS + Uniswap 链上池） ----------
  const mexSrc = {};            // coinbase/kraken/uniswap -> {v, t, rtt, msgs}
  const MEX_COIN = "btc";
  function connectCoinbase() {
    try {
      const ws = new WebSocket("wss://ws-feed.exchange.coinbase.com");
      ws.onopen = () => ws.send(JSON.stringify({ type: "subscribe", product_ids: ["BTC-USD"], channels: ["ticker"] }));
      ws.onmessage = (ev) => {
        try { const m = JSON.parse(ev.data); if (m.type === "ticker" && m.price) mexSrc.coinbase = { v: parseFloat(m.price), t: Date.now() }; } catch (e) {}
      };
      ws.onclose = () => setTimeout(connectCoinbase, 8000);
      ws.onerror = () => { try { ws.close(); } catch (e) {} };
    } catch (e) { setTimeout(connectCoinbase, 8000); }
  }
  function connectKraken() {
    try {
      const ws = new WebSocket("wss://ws.kraken.com/v2");
      ws.onopen = () => ws.send(JSON.stringify({ method: "subscribe", params: { channel: "ticker", symbol: ["BTC/USD"], snapshot: false } }));
      ws.onmessage = (ev) => {
        try { const m = JSON.parse(ev.data); if (m.channel === "ticker" && m.data && m.data[0] && m.data[0].last) mexSrc.kraken = { v: parseFloat(m.data[0].last), t: Date.now() }; } catch (e) {}
      };
      ws.onclose = () => setTimeout(connectKraken, 8000);
      ws.onerror = () => { try { ws.close(); } catch (e) {} };
    } catch (e) { setTimeout(connectKraken, 8000); }
  }
  // Uniswap V3 WBTC/USDC 0.05% 池：factory.getPool 解析池地址 → slot0 轮询
  const UNI = {
    factory: "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    wbtc: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    usdc: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    rpcs: ["https://cloudflare-eth.com", "https://ethereum-rpc.publicnode.com", "https://eth.llamarpc.com", "https://rpc.ankr.com/eth"],
    pool: null, idx: 0,
  };
  async function uniRpc(to, data) {
    const url = UNI.rpcs[UNI.idx % UNI.rpcs.length]; UNI.idx++;
    const t0 = Date.now();
    const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_call", params: [{ to, data }, "latest"] }) });
    const j = await r.json();
    if (j.error) throw new Error(j.error.message);
    return { result: j.result, rtt: Date.now() - t0 };
  }
  async function uniInit() {
    const data = "0x1698ee82" + UNI.wbtc.slice(2).padStart(64, "0") + UNI.usdc.slice(2).padStart(64, "0") + (500).toString(16).padStart(64, "0");
    const r = await uniRpc(UNI.factory, data);
    UNI.pool = "0x" + r.result.slice(26);
  }
  let uniBusy = false;
  async function uniPoll() {
    if (uniBusy) return;
    uniBusy = true;
    try {
      if (!UNI.pool) await uniInit();
      if (!UNI.pool) return;
      const t0 = Date.now();
      const r = await uniRpc(UNI.pool, "0x3850c7bd"); // slot0()
      const sqrtP = BigInt(r.result.slice(2, 66));
      const raw = Number(sqrtP * sqrtP) / Number(1n << 192n);
      const price = raw * 100; // 10^(dec0-dec2)=10^(8-6)
      if (isFinite(price) && price > 1000) mexSrc.uniswap = { v: price, t: Date.now(), rtt: Date.now() - t0 };
    } catch (e) { /* 换 RPC 重试 */ }
    finally { uniBusy = false; }
  }
  setInterval(uniPoll, 1500);
  uniPoll();
  function mexPost() {
    const now = Date.now();
    const map = {
      chainlink: clLatest.btc ? { v: clLatest.btc.v, t: clLatest.btc.t } : null,
      binance: liveLatest.binance.btc ? { v: liveLatest.binance.btc.v, t: liveLatest.binance.btc.t } : null,
      okx: liveLatest.okx.btc ? { v: liveLatest.okx.btc.v, t: liveLatest.okx.btc.t } : null,
      coinbase: mexSrc.coinbase, kraken: mexSrc.kraken, uniswap: mexSrc.uniswap,
    };
    const srcs = {};
    for (const k in map) {
      const v = map[k];
      if (v && now - v.t < 10000) { const o = { p: v.v, t: v.t }; if (v.rtt != null) o.rtt = v.rtt; if (mexRtt[k] != null) o.rtt = mexRtt[k]; srcs[k] = o; }
    }
    post({ type: "mex", srcs, now });
  }
  setInterval(mexPost, 1000);
  connectCoinbase();
  connectKraken();
  // 全源 RTT 探测（轮换，每源约 30 秒一次；REST 探测同时是连通性诊断）
  const MEX_PROBES = [
    { key: "binance", url: "https://data-api.binance.vision/api/v3/time" },
    { key: "coinbase", url: "https://api.coinbase.com/v2/time" },
    { key: "kraken", url: "https://api.kraken.com/0/public/Time" },
    { key: "okx", url: "https://www.okx.com/api/v5/public/time" },
    { key: "chainlink", url: "https://gamma-api.polymarket.com/markets?limit=1" },
  ];
  let probeIdx = 0;
  setInterval(async () => {
    const p = MEX_PROBES[probeIdx % MEX_PROBES.length]; probeIdx++;
    const t0 = Date.now();
    try {
      await fetch(p.url, { cache: "no-store", signal: AbortSignal.timeout(5000) });
      mexRtt[p.key] = Date.now() - t0;
    } catch (e) { mexRtt[p.key] = -1; }
  }, 5000);

  self.onmessage = (e) => {
  const m = e.data;
  if (m.type === "init") {
    params = Object.assign(params, m.params || {});
    if (!started) { started = true; connectBinance(); connectOKX(); connectPM(); scheduleTick(); }
  } else if (m.type === "history") {
    loadData(m.source, m.rows);
    for (const k in lastPivotIds) delete lastPivotIds[k];
    emitSeries(m.source);
    analyze(m.source);
  } else if (m.type === "params") {
    params = Object.assign(params, m.params);
    for (const k in lastPivotIds) delete lastPivotIds[k];
    for (const s of SOURCES) analyze(s);
  } else if (m.type === "date") {
    analysisDate = m.mode;
    for (const k in lastPivotIds) delete lastPivotIds[k];
    for (const s of SOURCES) { emitSeries(s); analyze(s); }
  }
};
