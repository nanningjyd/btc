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
          { topic: "crypto_prices_chainlink", type: "update", filters: "btc/usd,eth/usd,sol/usd,xrp/usd,bnb/usd,doge/usd" },
          { topic: "crypto_prices", type: "update", filters: "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt" },
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
        let items = [];
        if (msg.payload) items = Array.isArray(msg.payload) ? msg.payload : [msg.payload];
        else if (msg.symbol) items = [msg];
        for (const p of items) {
          const sym = String(p.symbol || "").toLowerCase();
          if (!sym) continue;
          const v = parseFloat(p.value != null ? p.value : p.price);
          if (!isFinite(v)) continue;
          const coin = (topic === "crypto_prices" ? PM_BINANCE_MAP[sym] : PM_MAP[sym]) || (!topic ? (PM_MAP[sym] || PM_BINANCE_MAP[sym]) : null);
          if (coin) liveLatest.polymarket[coin] = { v, t: p.timestamp ? Number(p.timestamp) : Date.now() };
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
