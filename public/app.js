// 主线程：数据加载/缓存 + 渲染（ECharts 图表、预测卡片、统计表、lag直方图）
const APP_VER = "20260830-MEX";
window.addEventListener("error", (e) => { window.__lastJsErr = String(e.message || e).slice(0, 90); });
const SOURCES = ["binance", "okx", "polymarket"];
const SRC_LABEL = { binance: "币安", okx: "OKX", polymarket: "Polymarket" };
const COIN_LABEL = { btc: "BTC", eth: "ETH", sol: "SOL", bnb: "BNB", doge: "DOGE", xrp: "XRP" };
const COIN_COLOR = { btc: "#f7931a", eth: "#627eea", sol: "#9945ff", bnb: "#f0b90b", doge: "#c9a633", xrp: "#25a768" };
const IDX_COIN = ["btc", "eth", "sol", "bnb", "doge", "xrp"];
const TYPE_LABEL = { bottom: "谷底", top: "谷顶" };
const TZ = 8 * 3600 * 1000;

const DEFAULT_PARAMS = { threshold: 0.15, before: -60, after: 300, minGap: 30, mode2: false };
const PUT_TOKEN = "pmtk_9f3ac41e7d2b8056"; // 浏览器实时数据回传令牌（页面打开期间把三源10s数据写回D1）
// 币种显示开关（localStorage 持久化；每次渲染都会带上该状态，新数据刷新不会重置）
const DEFAULT_COINVIS = { btc: true, eth: true, sol: true, bnb: true, doge: true, xrp: true };
function loadCoinVis() {
  try { return Object.assign({}, DEFAULT_COINVIS, JSON.parse(localStorage.getItem("btcmon_coinvis") || "{}")); }
  catch (e) { return Object.assign({}, DEFAULT_COINVIS); }
}
let coinVisible = loadCoinVis();
function saveCoinVis() { localStorage.setItem("btcmon_coinvis", JSON.stringify(coinVisible)); }
let params = loadParams();
let activeTab = "binance";
let viewDate = "today";
let yesterdayLoaded = false;
let meta = { pmSymbols: null };
let statusInfo = null;
const pageStart = Date.now();

const seriesStore = {};   // source -> { times:[], rows:[] }
const analysisStore = {}; // source -> 最新 analysis
const cardsStore = {};    // source -> [predict事件]
const connStore = { binance: "init", okx: "init", polymarket: "init" };

// ---------- 工具 ----------
function loadParams() {
  try { return Object.assign({}, DEFAULT_PARAMS, JSON.parse(localStorage.getItem("btcmon_params") || "{}")); }
  catch (e) { return Object.assign({}, DEFAULT_PARAMS); }
}
function saveParams() { localStorage.setItem("btcmon_params", JSON.stringify(params)); }
function fetchJSON(url, timeout) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), timeout || 15000);
  return fetch(url, { signal: ctl.signal }).then((r) => { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); }).finally(() => clearTimeout(t));
}
function fmtT(ts) {
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, "0");
  return p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
}
function fmtDT(ts) {
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, "0");
  return (d.getMonth() + 1) + "-" + p(d.getDate()) + " " + p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
}
function yesterdayStr() {
  const startToday = Math.floor((Date.now() + TZ) / 86400000) * 86400000 - TZ;
  const d = new Date(startToday - 86400000);
  const p = (n) => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate());
}
function todayStr() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate());
}

// ---------- IndexedDB 缓存（离线兜底） ----------
function idb() {
  return new Promise((res, rej) => {
    const r = indexedDB.open("btc-monitor", 1);
    r.onupgradeneeded = () => r.result.createObjectStore("cache");
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
}
async function cachePut(key, val) {
  try { const db = await idb(); const tx = db.transaction("cache", "readwrite"); tx.objectStore("cache").put({ t: Date.now(), val }, key); } catch (e) {}
}
async function cacheGet(key) {
  try {
    const db = await idb();
    return await new Promise((res, rej) => {
      const rq = db.transaction("cache").objectStore("cache").get(key);
      rq.onsuccess = () => res(rq.result || null);
      rq.onerror = () => rej(rq.error);
    });
  } catch (e) { return null; }
}

// ---------- Web Worker ----------
const worker = new Worker("engine.js?v=" + APP_VER);
worker.onmessage = (e) => {
  const m = e.data;
  if (m.type === "series") {
    seriesStore[m.source] = { times: m.times, rows: m.rows };
    if (m.source === activeTab) scheduleRender();
  } else if (m.type === "bar") {
    window.__lastBarTs = Date.now();
    const s = seriesStore[m.source];
    pushQueue.push({ source: m.source, ts: m.ts, row: m.row });
    if (!s || !s.times) return;
    const last = s.times.length ? s.times[s.times.length - 1] : 0;
    if (m.ts > last) { s.times.push(m.ts); s.rows.push(m.row); }
    else if (m.ts === last) s.rows[s.rows.length - 1] = m.row;
    if (m.source === activeTab) scheduleRender();
  } else if (m.type === "analysis") {
    analysisStore[m.source] = m;
    if (m.source === activeTab) scheduleRender();
  } else if (m.type === "predict") {
    const arr = cardsStore[m.source] || (cardsStore[m.source] = []);
    arr.unshift(m);
    if (arr.length > 30) arr.pop();
    if (m.source === activeTab) renderCards();
  } else if (m.type === "conn") {
    connStore[m.source] = m.state;
    renderConnChip();
  } else if (m.type === "chainlink") {
    clStore.coins = m.coins;
    clStore.lastTs = Date.now();
    renderChainlink();
  } else if (m.type === "mex") {
    for (const k in m.srcs) {
      const arr = mexBuf[k] || (mexBuf[k] = []);
      const lastT = arr.length ? arr[arr.length - 1][0] : 0;
      if (m.srcs[k].t > lastT) {
        arr.push([m.srcs[k].t, m.srcs[k].p]);
        while (arr.length > 420) arr.shift();
      }
      if (m.srcs[k].rtt != null) mexRtt[k] = m.srcs[k].rtt;
    }
    if (m.diag) { mexDiag.errs = m.diag.errs || {}; mexDiag.pmConn = m.diag.pmConn || null; }
    renderMex();
  }
};

// ---------- 图表 ----------
let chart = null, histChart = null;
let renderTimer = null;
function scheduleRender() {
  if (renderTimer) return;
  renderTimer = setTimeout(() => { renderTimer = null; renderAll(); }, 1500);
}

function coinsFor(src) {
  const an = analysisStore[src];
  if (an && an.coins && an.coins.length) return an.coins;
  return ["btc", "eth", "sol", "bnb", "doge"];
}

// 图例选中状态：由 coinVisible 驱动，随每次渲染携带，保证开关不被数据刷新重置
function legendSelected() {
  const sel = {};
  for (const c of coinsFor(activeTab)) sel[COIN_LABEL[c]] = coinVisible[c] !== false;
  return sel;
}

let coinTogglesKey = "";
function initCoinToggles() {
  const el = document.getElementById("coinToggles");
  if (!el) return;
  // 事件委托只绑一次；币种集合不变时不重建 DOM，避免高频渲染替换按钮导致点击落空
  if (!el.dataset.bound) {
    el.dataset.bound = "1";
    el.addEventListener("click", (e) => {
      const b = e.target.closest("button[data-coin]");
      if (!b) return;
      const c = b.getAttribute("data-coin");
      coinVisible[c] = coinVisible[c] === false;
      saveCoinVis();
      renderCoinToggles();
      renderChart();
    });
  }
  const key = activeTab + "|" + coinsFor(activeTab).join(",");
  if (key === coinTogglesKey) return;
  coinTogglesKey = key;
  renderCoinToggles();
}

function renderCoinToggles() {
  const el = document.getElementById("coinToggles");
  if (!el) return;
  el.innerHTML = coinsFor(activeTab).map((c) =>
    '<button class="coinbtn' + (coinVisible[c] !== false ? " on" : "") + '" data-coin="' + c + '" style="--c:' + COIN_COLOR[c] + '">' +
    '<span class="dot" style="background:' + (coinVisible[c] !== false ? COIN_COLOR[c] : "#3a4450") + '"></span>' + COIN_LABEL[c] + "</button>"
  ).join("");
}

function renderChart() {
  const src = activeTab;
  const s = seriesStore[src];
  const an = analysisStore[src];
  if (!chart) {
    chart = echarts.init(document.getElementById("mainChart"), null, { renderer: "canvas" });
    // 用户点击图例隐藏/显示币种 → 同步回开关状态并持久化（渲染时始终携带，刷新数据不重置）
    chart.on("legendselectchanged", (e) => {
      const labelToCoin = {};
      for (const c of IDX_COIN) labelToCoin[COIN_LABEL[c]] = c;
      const c = labelToCoin[e.name];
      if (c) { coinVisible[c] = !!e.selected[e.name]; saveCoinVis(); renderCoinToggles(); }
    });
  }
  const el = document.getElementById("mainChart");
  if (!s || !s.times || !s.times.length) {
    chart.clear();
    chart.setOption({ title: { text: "暂无数据（云采集未运行或网络不通）", left: "center", top: "middle", textStyle: { color: "#6b7684", fontSize: 13 } } });
    return;
  }
  const coins = coinsFor(src);
  const base = {};
  for (const c of coins) {
    const idx = IDX_COIN.indexOf(c);
    for (let i = 0; i < s.times.length; i++) {
      const v = s.rows[i] ? s.rows[i][idx] : null;
      if (v != null) { base[c] = v; break; }
    }
  }
  const series = [];
  for (const c of coins) {
    if (!base[c]) continue;
    const idx = IDX_COIN.indexOf(c);
    const data = [];
    for (let i = 0; i < s.times.length; i++) {
      const v = s.rows[i] ? s.rows[i][idx] : null;
      data.push(v == null ? [s.times[i], null] : [s.times[i], +(((v / base[c]) - 1) * 100).toFixed(3)]);
    }
    series.push({
      name: COIN_LABEL[c], type: "line", showSymbol: false, sampling: "lttb", connectNulls: false,
      lineStyle: { width: 1.3 }, color: COIN_COLOR[c], data, large: true,
    });
  }
  // 拐点散点（谷底红▲ 谷顶绿▼）；隐藏的币种不显示拐点
  const bottoms = [], tops = [];
  if (an && an.pivotsByCoin) {
    for (const coin in an.pivotsByCoin) {
      if (!coins.includes(coin) || !base[coin] || coinVisible[coin] === false) continue;
      for (const p of an.pivotsByCoin[coin]) {
        const y = +(((p.v / base[coin]) - 1) * 100).toFixed(3);
        (p.type === "bottom" ? bottoms : tops).push({ value: [p.t, y], name: COIN_LABEL[coin] });
      }
    }
  }
  series.push({ name: "谷底", type: "scatter", symbol: "triangle", symbolSize: 9, itemStyle: { color: "#e04b4b" }, data: bottoms, z: 5 });
  series.push({ name: "谷顶", type: "scatter", symbol: "triangle", symbolRotate: 180, symbolSize: 9, itemStyle: { color: "#22a06b" }, data: tops, z: 5 });
  // 模式2 信号竖线
  const markLines = [];
  if (an && an.signals) {
    for (const sg of an.signals) {
      markLines.push({ xAxis: sg.t, label: { formatter: COIN_LABEL[sg.coin] + " " + (sg.chg > 0 ? "+" : "") + sg.chg + "%", color: sg.chg > 0 ? "#e04b4b" : "#22a06b" }, lineStyle: { color: sg.chg > 0 ? "#e04b4b" : "#22a06b", type: "dashed", width: 1 } });
    }
  }
  if (markLines.length && series.length) series[0].markLine = { silent: true, symbol: "none", data: markLines };

  // getOption() 在图表被 clear()/未初始化时可能返回 undefined，必须判空，否则渲染中断、图表空白失效
  const prevOpt = typeof chart.getOption === "function" ? chart.getOption() : null;
  const prevZoom = prevOpt && prevOpt.dataZoom;
  const zoomVal = prevZoom && prevZoom.length ? [prevZoom[0].start, prevZoom[0].end] : null;
  chart.setOption({
    animation: false,
    backgroundColor: "transparent",
    title: { text: SRC_LABEL[src] + " · " + (viewDate === "today" ? "今天" : "昨天 " + yesterdayStr()) + " · 当日00:00基准", left: 8, top: 4, textStyle: { color: "#8b97a5", fontSize: 12, fontWeight: 400 } },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#171d25", borderColor: "#28313d", textStyle: { color: "#d8dee6", fontSize: 12 },
      valueFormatter: (v) => (v == null ? "-" : (v > 0 ? "+" : "") + v.toFixed(2) + "%"),
    },
    legend: { top: 4, right: 8, textStyle: { color: "#8b97a5", fontSize: 12 }, itemWidth: 16, itemHeight: 8, selected: legendSelected() },
    grid: { left: 56, right: 16, top: 34, bottom: 52 },
    xAxis: { type: "time", axisLine: { lineStyle: { color: "#28313d" } }, axisLabel: { color: "#6b7684", formatter: (v) => fmtT(v) }, splitLine: { show: false } },
    yAxis: { type: "value", axisLabel: { color: "#6b7684", formatter: "{value}%" }, splitLine: { lineStyle: { color: "#1c232c" } } },
    dataZoom: [
      { type: "inside" },
      { type: "slider", height: 18, bottom: 8, borderColor: "#28313d", backgroundColor: "#141920", fillerColor: "rgba(24,95,165,.18)", textStyle: { color: "#6b7684" }, start: zoomVal ? zoomVal[0] : 0, end: zoomVal ? zoomVal[1] : 100 },
    ],
    series,
  }, { notMerge: true });
}

// ---------- 预测卡片 ----------
function renderCards() {
  const arr = cardsStore[activeTab] || [];
  const el = document.getElementById("cardList");
  if (!arr.length) { el.innerHTML = '<div class="empty">等待拐点确认…</div>'; return; }
  let html = "";
  for (const ev of arr) {
    const cls = ev.type === "bottom" ? "up" : "down";
    const sign = ev.type === "bottom" ? "▲" : "▼";
    html += '<div class="card"><div class="hd"><b class="' + cls + '">' + COIN_LABEL[ev.leader] + " " + sign + " " + TYPE_LABEL[ev.type] + '</b><span>' + fmtT(ev.pivotT) + " · 确认 " + fmtT(ev.pivotT) + '</span></div>';
    for (const p of ev.preds) {
      const rate = p.n ? Math.round(p.rate * 100) + "%" : "—";
      const lagTxt = p.lag != null ? (p.lag >= 0 ? "+" : "") + p.lag + "s" : "样本不足";
      const cd = p.predTime && p.predTime > Date.now() ? '<span class="cd" data-predtime="' + p.predTime + '">倒计时…</span>' : (p.predTime ? '<span class="muted">已到点</span>' : "");
      html += '<div class="pred"><span>' + COIN_LABEL[p.follower] + ' <span class="muted">预计 ' + lagTxt + " → " + (p.predTime ? fmtT(p.predTime) : "—") + '</span></span><span>' + rate + ' <span class="muted">n=' + p.n + "</span> " + cd + "</span></div>";
    }
    html += "</div>";
  }
  el.innerHTML = html;
}
setInterval(() => {
  document.querySelectorAll("[data-predtime]").forEach((el) => {
    const t = Number(el.getAttribute("data-predtime"));
    const d = Math.round((t - Date.now()) / 1000);
    if (d > 0) el.textContent = "倒计时 " + d + "s";
    else el.textContent = "已到点";
  });
}, 1000);

// ---------- 统计表 ----------
function renderStats() {
  const an = analysisStore[activeTab];
  const el = document.getElementById("statTable");
  if (!an || !an.stats) { el.innerHTML = ""; return; }
  const rows = [];
  for (const key in an.stats) {
    const st = an.stats[key];
    const [L, F, type] = key.split("|");
    rows.push('<tr><td>' + COIN_LABEL[L] + "→" + COIN_LABEL[F] + " · " + TYPE_LABEL[type] +
      "</td><td>" + st.n + "/" + st.total + "</td><td>" + Math.round(st.rate * 100) + "%</td><td>" +
      (st.mean != null ? st.mean : "—") + "</td><td>" + (st.median != null ? st.median : "—") + "</td><td>" +
      (st.min != null ? st.min : "—") + "</td><td>" + (st.max != null ? st.max : "—") + "</td></tr>");
  }
  el.innerHTML = '<thead><tr><th>组合</th><th>次数</th><th>跟随率</th><th>均值s</th><th>中位s</th><th>最小s</th><th>最大s</th></tr></thead><tbody>' +
    (rows.length ? rows.join("") : '<tr><td colspan="7" class="muted">暂无拐点</td></tr>') + "</tbody>";
}

// ---------- 时间差记录表 ----------
function renderMatches() {
  const an = analysisStore[activeTab];
  const el = document.getElementById("matchTable");
  if (!an || !an.matches) { el.innerHTML = ""; return; }
  const fType = document.getElementById("fType").value;
  const fLeader = document.getElementById("fLeader").value;
  const fFollower = document.getElementById("fFollower").value;
  // 按组合统计当天第 N 次
  const seqMap = {};
  const sorted = an.matches.slice().sort((a, b) => a.leaderT - b.leaderT);
  for (const m of sorted) {
    const key = m.leader + "|" + m.follower + "|" + m.type;
    seqMap[key] = (seqMap[key] || 0) + 1;
    m.no = seqMap[key];
  }
  const rows = [];
  for (const m of sorted.reverse()) {
    if (fType && m.type !== fType) continue;
    if (fLeader && m.leader !== fLeader) continue;
    if (fFollower && m.follower !== fFollower) continue;
    if (rows.length >= 300) break;
    const timeCol = viewDate === "yesterday" ? fmtDT(m.leaderT) : fmtT(m.leaderT);
    rows.push("<tr><td>" + COIN_LABEL[m.leader] + " " + TYPE_LABEL[m.type] + "</td><td>" + timeCol + "</td><td>" +
      COIN_LABEL[m.follower] + "</td><td>" + (m.followerT ? (viewDate === "yesterday" ? fmtDT(m.followerT) : fmtT(m.followerT)) : "—") + "</td><td>" +
      (m.lag != null ? ((m.lag >= 0 ? "+" : "") + m.lag) : '<span class="muted">未跟随</span>') + "</td><td>#" + m.no + "</td></tr>");
  }
  el.innerHTML = '<thead><tr><th>主流币拐点</th><th>时间</th><th>跟随币</th><th>跟随时间</th><th>lag(s)</th><th>当日第N次</th></tr></thead><tbody>' +
    (rows.length ? rows.join("") : '<tr><td colspan="6" class="muted">暂无记录</td></tr>') + "</tbody>";
}

// ---------- lag 直方图 ----------
function renderHist() {
  const an = analysisStore[activeTab];
  const sel = document.getElementById("histSel");
  if (!an || !an.stats) { if (histChart) histChart.clear(); return; }
  const keys = Object.keys(an.stats).sort();
  if (sel.dataset.keys !== keys.join(",")) {
    sel.dataset.keys = keys.join(",");
    sel.innerHTML = keys.map((k) => {
      const [L, F, type] = k.split("|");
      return '<option value="' + k + '">' + COIN_LABEL[L] + "→" + COIN_LABEL[F] + " " + TYPE_LABEL[type] + "</option>";
    }).join("");
  }
  const key = sel.value || keys[0];
  if (!key) return;
  const st = an.stats[key];
  if (!histChart) histChart = echarts.init(document.getElementById("histChart"));
  if (!st.hist) { histChart.clear(); histChart.setOption({ title: { text: "样本不足", left: "center", top: "middle", textStyle: { color: "#6b7684", fontSize: 12 } } }); return; }
  const cats = ["<0", "0-10", "10-30", "30-60", "60-120", "120-300", ">300"];
  histChart.setOption({
    animation: false, backgroundColor: "transparent",
    tooltip: { trigger: "axis", textStyle: { fontSize: 12 } },
    grid: { left: 40, right: 10, top: 24, bottom: 24 },
    xAxis: { type: "category", data: cats, axisLabel: { color: "#6b7684", fontSize: 10 }, axisLine: { lineStyle: { color: "#28313d" } } },
    yAxis: { type: "value", axisLabel: { color: "#6b7684" }, splitLine: { lineStyle: { color: "#1c232c" } } },
    series: [{ type: "bar", data: cats.map((c) => st.hist[c]), itemStyle: { color: "#378add" }, barWidth: "60%" }],
  });
}

// ---------- 状态徽标 ----------
function renderConnChip() {
  const st = connStore[activeTab];
  const cls = st === "open" ? "ok" : st === "init" ? "mid" : "bad";
  document.getElementById("chipConn").innerHTML = '实时 <span class="dot ' + cls + '"></span>' + (st === "open" ? "已连接" : st === "init" ? "连接中" : "断开(需代理)");
}
function renderCloudChip() {
  const el = document.getElementById("chipCloud");
  if (!statusInfo || !statusInfo.sources) { el.innerHTML = "云采集 <b>…</b>"; return; }
  const s = statusInfo.sources[activeTab] || {};
  let txt;
  if (s.last_ts) {
    const age = Math.round((statusInfo.now - s.last_ts) / 1000);
    txt = age < 180 ? "正常" : "中断(" + Math.floor(age / 60) + "分前)";
  } else txt = "无数据";
  const ok = s.last_ts && (statusInfo.now - s.last_ts) < 180;
  el.innerHTML = '云采集 <b class="' + (ok ? "" : "warn") + '">' + txt + " · " + (s.cnt || 0) + "点</b>";
}
function renderBadge() {
  const el = document.getElementById("badge");
  const parts = [];
  const an = analysisStore.polymarket;
  if (activeTab === "polymarket") {
    if (meta.pmSymbols) {
      const syms = meta.pmSymbols.split(",");
      const has = (s) => syms.indexOf(s) >= 0;
      parts.push("Polymarket(Chainlink) 覆盖: " + syms.map((s) => s.replace("/usd", "").toUpperCase()).join("/"));
      if (!has("bnb/usd") && has("xrp/usd")) parts.push('<span class="warn">BNB 缺失 → XRP 替补</span>');
      if (!has("doge/usd")) parts.push('<span class="warn">DOGE 无数据流</span>');
    }
  }
  if (an && an.signals && an.signals.length) {
    for (const sg of an.signals.slice(-3)) {
      parts.push('<span class="' + (sg.chg > 0 ? "red" : "green") + '">[模式2] ' + COIN_LABEL[sg.coin] + " 15min " + (sg.chg > 0 ? "+" : "") + sg.chg + "% @ " + fmtT(sg.t) + "</span>");
    }
  }
  el.innerHTML = parts.join(" · ");
}

// ---------- 渲染入口 ----------
function renderAll() {
  initCoinToggles();
  renderChart();
  renderCards();
  renderStats();
  renderMatches();
  renderHist();
  renderConnChip();
  renderCloudChip();
  renderBadge();
}

// ---------- 数据加载 ----------
async function loadDay(mode) {
  const dateQ = mode === "yesterday" ? "&date=" + yesterdayStr() : "";
  await Promise.all(SOURCES.map(async (src) => {
    let rows = null;
    try {
      const j = await fetchJSON("/api/bars?source=" + src + dateQ, 20000);
      rows = j.rows;
      cachePut(src + "_" + (mode === "yesterday" ? yesterdayStr() : todayStr()), rows);
    } catch (e) {
      const c = await cacheGet(src + "_" + (mode === "yesterday" ? yesterdayStr() : todayStr()));
      if (c && c.val) rows = c.val;
    }
    if (rows) worker.postMessage({ type: "history", source: src, rows });
  }));
}
async function pollStatus() {
  try {
    statusInfo = await fetchJSON("/api/status", 10000);
    if (statusInfo.meta && statusInfo.meta.pm_symbols) meta.pmSymbols = statusInfo.meta.pm_symbols;
    renderCloudChip();
    renderBadge();
  } catch (e) {}
}

// ---------- 浏览器实时数据回传 D1（弥补云端无法直连币安/OKX） ----------
const pushQueue = [];
async function flushPush() {
  if (!pushQueue.length) return;
  const bars = pushQueue.splice(0, 60);
  try {
    await fetch("/api/put", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: PUT_TOKEN, bars }),
    });
  } catch (e) {
    if (pushQueue.length < 500) pushQueue.unshift(...bars.slice(0, 60 - pushQueue.length));
  }
}
setInterval(flushPush, 30000);

// ---------- 数据补齐（币安 1s K线 → 重建缺失的 10s 桶 → 回写 D1） ----------
const BF_SYMS = { btc: "BTCUSDT", eth: "ETHUSDT", sol: "SOLUSDT", bnb: "BNBUSDT", doge: "DOGEUSDT" };
const BF_HOSTS = ["https://api.binance.com", "https://data-api.binance.vision", "https://api1.binance.com"];
let backfillRunning = false;

function bfStatus(html) { const el = document.getElementById("bfStatus"); if (el) el.innerHTML = html; }

async function bfFetchKlines(sym, startMs) {
  // 最多 1000 根 1s K线，返回 [openTime, closePrice]；所有主机失败则抛错
  let lastErr = null;
  for (const host of BF_HOSTS) {
    for (let attempt = 0; attempt < 2; attempt++) {
      const ctl = new AbortController();
      const t = setTimeout(() => ctl.abort(), 15000);
      try {
        const r = await fetch(host + "/api/v3/klines?symbol=" + sym + "&interval=1s&startTime=" + startMs + "&limit=1000", { signal: ctl.signal });
        if (!r.ok) throw new Error("HTTP " + r.status);
        const arr = await r.json();
        return arr.map((k) => [k[0], parseFloat(k[4])]);
      } catch (e) { lastErr = e; }
      finally { clearTimeout(t); }
    }
  }
  throw lastErr || new Error("klines failed");
}

async function runBackfill() {
  if (backfillRunning) return;
  backfillRunning = true;
  const btn = document.getElementById("btnBackfill");
  btn.disabled = true;
  try {
    bfStatus("检测缺口…");
    const now = Date.now();
    const winStart = Math.floor((now - 48 * 3600 * 1000) / 10000) * 10000;
    const have = new Set();
    for (const ds of [todayStr(), yesterdayStr()]) {
      try {
        const j = await fetchJSON("/api/bars?source=binance&date=" + ds, 20000);
        for (const r of j.rows) if (r[0] >= winStart) have.add(r[0]);
      } catch (e) {}
    }
    const missing = [];
    for (let t = winStart; t <= now - 10000; t += 10000) if (!have.has(t)) missing.push(t);
    if (!missing.length) { bfStatus("近48小时无缺口 ✓"); return; }
    const totalMissing = missing.length;
    bfStatus("缺口 " + totalMissing + " 点，抓取币安1sK线…");

    const closes = {}; // bucket -> {btc,eth,sol,bnb,doge}
    let reqDone = 0, lastErr = null;
    const reqTotal = Math.max(5, Math.ceil(missing.length / 900) * 5);
    let i = 0;
    while (i < missing.length) {
      const start = missing[i];              // 1s K线窗口起点（1000 秒）
      const winEnd = start + 1000 * 1000;
      for (const c in BF_SYMS) {
        let rows = null;
        try { rows = await bfFetchKlines(BF_SYMS[c], start); } catch (e) { lastErr = e; }
        if (rows) {
          for (const [ot, cl] of rows) {
            if (!isFinite(cl)) continue;
            const b = Math.floor(ot / 10000) * 10000;
            // 只填窗口内完整覆盖的缺失桶；同桶多条记录取最后一条（贴近桶末价格）
            if (b >= start && b + 10000 <= winEnd && !have.has(b)) {
              const o = closes[b] || (closes[b] = {});
              o[c] = cl;
            }
          }
        }
        reqDone++;
        bfStatus("补齐中 " + Math.min(99, Math.round((reqDone / reqTotal) * 100)) + "%（" + reqDone + " 请求）");
      }
      while (i < missing.length && missing[i] < winEnd) i++;
    }
    const tsList = Object.keys(closes).map(Number).sort((a, b) => a - b);
    let written = 0, batches = 0;
    for (let p = 0; p < tsList.length; p += 400) {
      const rows = tsList.slice(p, p + 400).map((ts) => {
        const o = closes[ts];
        return [ts, o.btc ?? null, o.eth ?? null, o.sol ?? null, o.bnb ?? null, o.doge ?? null];
      });
      try {
        const resp = await fetch("/api/backfill", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token: PUT_TOKEN, source: "binance", rows }),
        });
        const j = await resp.json().catch(() => ({}));
        if (j.written) written += j.written;
        if (j.error) lastErr = j.error;
      } catch (e) { lastErr = e; }
      batches++;
      bfStatus("写回数据库 " + batches + " 批（已补 " + written + " 点）");
    }
    // OKX 源回填：OKX 官方 1m K线（Worker 代理），按币种逐列补，不覆盖其他币已有值
    const okxCoins = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "DOGE-USDT"];
    let okxW = 0;
    for (const ds of [yesterdayStr(), todayStr()]) {
      for (const c of okxCoins) {
        bfStatus("OKX 回填 " + c + " " + ds + "…");
        try {
          const resp = await fetch("/api/backfill-okx?token=" + PUT_TOKEN + "&coin=" + c + "&date=" + ds, { method: "POST" });
          const j = await resp.json().catch(() => ({}));
          okxW += j.written || 0;
        } catch (e) {}
      }
    }
    bfStatus("完成：币安补 " + written + "/" + totalMissing + " 点 · OKX 补 " + okxW + " 点" +
      (lastErr ? '<span class="warn">（部分失败可再点一次）</span>' : " ✓"));
    await loadDay("today");
    if (yesterdayLoaded) await loadDay("yesterday");
    renderAll();
  } catch (e) {
    bfStatus('<span class="warn">补齐失败：' + String(e && e.message || e).slice(0, 80) + "</span>");
  } finally {
    backfillRunning = false;
    btn.disabled = false;
  }
}

// ---------- Chainlink 多所聚合价面板（Polymarket 结算源） ----------
const clStore = { coins: null };
function fmtPx(v) {
  if (v == null || !isFinite(v)) return "—";
  if (v >= 1000) return v.toFixed(1);
  if (v >= 100) return v.toFixed(2);
  if (v >= 1) return v.toFixed(3);
  return v.toFixed(4);
}
function latestSpot(src, coin) {
  const s = seriesStore[src];
  if (!s || !s.rows || !s.rows.length) return null;
  const idx = IDX_COIN.indexOf(coin);
  for (let i = s.rows.length - 1; i >= 0; i--) {
    const v = s.rows[i] ? s.rows[i][idx] : null;
    if (v != null) return v;
  }
  return null;
}
function favBadge(gap) {
  if (gap == null || !isFinite(gap)) return '<span class="muted">—</span>';
  const cls = gap >= 0 ? "up" : "dn";
  const lab = gap >= 0 ? "↑Up" : "↓Down";
  const op = Math.abs(gap) >= 2 ? 1 : Math.abs(gap) >= 0.5 ? 0.75 : 0.45;
  return '<span class="cl-fav ' + cls + '" style="opacity:' + op + '">' + lab + ' ' + (gap >= 0 ? "+" : "") + gap.toFixed(1) + "</span>";
}
function renderChainlink() {
  const el = document.getElementById("clTable");
  if (!el || !clStore.coins) return;
  let html = '<thead><tr><th>币种</th><th>聚合价</th><th>币安</th><th>OKX</th><th>聚-币安<br>(bps)</th><th>TWAP60s</th><th>5m起点</th><th>5m领先<br>(bps)</th><th>15m领先<br>(bps)</th></tr></thead><tbody>';
  let any = false;
  for (const c of ["btc", "eth", "sol", "bnb", "xrp", "doge"]) {
    const d = clStore.coins[c];
    if (!d) continue;
    any = true;
    const bn = latestSpot("binance", c);
    const ok = latestSpot("okx", c);
    const dev = (d.p != null && bn) ? (d.p - bn) / bn * 1e4 : null;
    html += "<tr><td><b>" + COIN_LABEL[c] + "</b></td><td>" + fmtPx(d.p) + "</td><td>" + fmtPx(bn) + "</td><td>" + fmtPx(ok) + "</td>" +
      "<td class='cl-gap'>" + (dev == null ? "—" : (dev >= 0 ? "+" : "") + dev.toFixed(1)) + "</td>" +
      "<td>" + fmtPx(d.twap) + "</td>" +
      "<td class='cl-gap'>" + (d.start5 == null ? "—" : fmtPx(d.start5) + (d.approx5 ? "~" : "")) + "</td>" +
      "<td>" + favBadge(d.gap5) + "</td><td>" + favBadge(d.gap15) + "</td></tr>";
  }
  if (!any) html += '<tr><td colspan="9" class="muted">等待 Chainlink 数据流（BTC/ETH/SOL 实测有流；BNB/DOGE/XRP 官方未推送）…</td></tr>';
  el.innerHTML = html + "</tbody>";
  const left = 300 - Math.floor((Date.now() % 300000) / 1000);
  const upd = document.getElementById("clUpdated");
  if (upd) {
    const age = clStore.lastTs ? Math.round((Date.now() - clStore.lastTs) / 1000) : null;
    upd.textContent = "链上流 " + (age == null ? "未收到数据" : age + "s前") + " · 本5m窗口剩 " + left + "s";
  }
}

// ---------- 六源 BTC 1 秒价格面板（Chainlink/Binance/OKX/Coinbase/Kraken/Uniswap） ----------
const MEX_SRCS = [
  { key: "chainlink", label: "Chainlink", color: "#2c6ecb" },
  { key: "binance", label: "Binance", color: "#f7931a" },
  { key: "okx", label: "OKX", color: "#00b8d4" },
  { key: "coinbase", label: "Coinbase", color: "#0052ff" },
  { key: "kraken", label: "Kraken", color: "#8a2be2" },
  { key: "uniswap", label: "Uniswap", color: "#ff007a" },
];
const MEX_DEF = { mode: "dev", width: 1.5, thr: 5, colors: {} };
let mexCfg = (() => { try { return Object.assign({}, MEX_DEF, JSON.parse(localStorage.getItem("mexCfg") || "{}")); } catch (e) { return Object.assign({}, MEX_DEF); } })();
mexCfg.colors = Object.assign({}, mexCfg.colors);
function saveMexCfg() { localStorage.setItem("mexCfg", JSON.stringify(mexCfg)); }
const mexBuf = {};
const mexRtt = {};
const mexDiag = { errs: {}, pmConn: null };
let mexChart = null;
let mexOptSync = false;

function mexColor(key) { return mexCfg.colors[key] || (MEX_SRCS.find((s2) => s2.key === key) || {}).color || "#378add"; }
function clAt(t) {
  const arr = mexBuf.chainlink;
  if (!arr || !arr.length) return null;
  let lo = 0, hi = arr.length - 1, best = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid][0] <= t) { best = arr[mid]; lo = mid + 1; } else hi = mid - 1;
  }
  return (best && t - best[0] <= 10000) ? best[1] : null;
}
function mexBase(srcKey) {
  const arr = mexBuf[srcKey];
  return arr && arr.length ? arr[0][1] : null;
}
function mexPlotVal(srcKey, t, p) {
  const mode = mexCfg.mode;
  if (mode === "raw") return +p.toFixed(2);
  if (mode === "dev") { const b = mexBase(srcKey); return b ? +(((p / b) - 1) * 100).toFixed(4) : null; }
  const c = clAt(t);
  return c ? +((p - c) / c * 1e4).toFixed(1) : null;
}
function renderMex() {
  const box = document.getElementById("mexChart");
  if (!box) return;
  if (!mexChart) mexChart = echarts.init(box, null, { renderer: "canvas" });
  const mode = mexCfg.mode;
  const info = document.getElementById("mexModeInfo");
  if (info) info.textContent = mode === "dev" ? "偏离%模式" : mode === "bps" ? "价差bps模式(vs Chainlink)" : "原始价格模式";
  const series = [];
  const marks = [];
  const thr = Number(mexCfg.thr) || 5;
  for (const src of MEX_SRCS) {
    const arr = mexBuf[src.key];
    if (!arr || arr.length < 2) continue;
    const base = mexBase(src.key);
    const data = [];
    for (let i = 0; i < arr.length; i++) {
      const t = arr[i][0], p = arr[i][1];
      const y = mexPlotVal(src.key, t, p);
      data.push(y == null ? [t, null] : [t, y]);
      if (i > 0 && arr[i - 1][1] > 0) {
        const step = Math.abs(p - arr[i - 1][1]) / arr[i - 1][1] * 1e4;
        if (step >= thr && y != null) marks.push({ value: [t, y], symbolSize: 9, itemStyle: { color: mexColor(src.key), borderColor: "#fff", borderWidth: 1 } });
      }
    }
    const line = { name: src.label, type: "line", showSymbol: false, sampling: "lttb", connectNulls: false,
      lineStyle: { width: Number(mexCfg.width) || 1.5, color: mexColor(src.key) }, itemStyle: { color: mexColor(src.key) }, data };
    if (mode === "bps" && src.key === "chainlink") { line.lineStyle.type = "dashed"; line.lineStyle.width = 1; line.name = "Chainlink(基准0)"; }
    series.push(line);
  }
  if (marks.length) series.push({ name: "突变", type: "scatter", data: marks, z: 10, silent: true });
  const yName = mode === "raw" ? "价格" : mode === "dev" ? "%(相对基准)" : "bps(vs Chainlink)";
  const opt = {
    animation: false, backgroundColor: "transparent",
    tooltip: { trigger: "axis", backgroundColor: "#171d25", borderColor: "#28313d", textStyle: { color: "#d8dee6", fontSize: 12 } },
    legend: { top: 2, left: 8, textStyle: { color: "#8b97a5", fontSize: 11 }, itemWidth: 14, itemHeight: 7 },
    grid: { left: 64, right: 14, top: 30, bottom: 46 },
    xAxis: { type: "time", axisLine: { lineStyle: { color: "#28313d" } }, axisLabel: { color: "#6b7684", formatter: (v) => fmtT(v) }, splitLine: { show: false } },
    yAxis: { type: "value", scale: true, axisLabel: { color: "#6b7684" }, splitLine: { lineStyle: { color: "#1c232c" } }, name: yName, nameTextStyle: { color: "#8b97a5" } },
    dataZoom: [{ type: "inside" }, { type: "slider", height: 16, bottom: 4, borderColor: "#28313d", backgroundColor: "#141920", fillerColor: "rgba(24,95,165,.18)", textStyle: { color: "#6b7684" } }],
    series,
  };
  if (!mexOptSync) {
    mexChart.setOption(opt, true);
    mexOptSync = true;
  } else {
    const prev = chart ? (chart.getOption() || {}) : {};
    const zoom = prev.dataZoom && prev.dataZoom.length ? [prev.dataZoom[0].start, prev.dataZoom[0].end] : null;
    if (zoom) opt.dataZoom[0].start = zoom[0], opt.dataZoom[0].end = zoom[1];
    mexChart.setOption(opt);
  }
  renderMexStat();
}
function renderMexStat() {
  const el = document.getElementById("mexStat");
  if (!el) return;
  let html = '<thead><tr><th>来源</th><th>价格</th><th>更新</th><th>延迟/间隔</th><th>态</th></tr></thead><tbody>';
  let any = false;
  for (const src of MEX_SRCS) {
    const arr = mexBuf[src.key];
    const lastP = arr && arr.length ? arr[arr.length - 1][1] : null;
    const lastT = arr && arr.length ? arr[arr.length - 1][0] : 0;
    const age = lastT ? Math.round((Date.now() - lastT) / 1000) : null;
    const gap = arr && arr.length > 1 ? (arr[arr.length - 1][0] - arr[arr.length - 2][0]) / 1000 : null;
    let lat = "—";
    const rtt = mexRtt[src.key];
    if (rtt != null && rtt >= 0) lat = "RTT " + rtt + "ms";
    else if (rtt === -1) lat = '<span class="off">探测失败</span>';
    else if (gap != null) lat = "间隔 " + gap.toFixed(1) + "s";
    const errWhy = mexDiag.errs[src.key] || (src.key === "chainlink" && mexDiag.pmConn && mexDiag.pmConn !== "open" ? "RTDS " + mexDiag.pmConn : "");
    const st = age == null ? '<b class="off" title="' + errWhy + '">断</b>' : age <= 3 ? '<b class="on">●</b>' : age <= 10 ? "<b>◐</b>" : '<b class="off">○</b>';
    html += "<tr><td><span class='mexsw' style='background:" + mexColor(src.key) + "'></span><b>" + src.label + "</b></td>" +
      "<td class='cl-gap'>" + fmtPx(lastP) + "</td><td>" + (age == null ? "—" : age + "s前") + "</td><td>" + lat + "</td><td>" + st + "</td></tr>";
    any = true;
  }
  if (!any) html += '<tr><td colspan="5" class="muted">等待数据…</td></tr>';
  const diagBits = [];
  for (const k in mexDiag.errs) if (mexDiag.errs[k]) diagBits.push(MEX_SRCS.find((s2) => s2.key === k)?.label + ": " + mexDiag.errs[k]);
  if (mexDiag.pmConn && mexDiag.pmConn !== "open") diagBits.push("Chainlink依赖RTDS: " + mexDiag.pmConn);
  if (diagBits.length) html += '<tr><td colspan="5" class="muted" style="text-align:left">' + diagBits.slice(0, 3).join("；") + "</td></tr>";
  el.innerHTML = html + "</tbody>";
}

// ---------- 钱包共振监控（Polymarket 盈利钱包跟单信号） ----------

// ---------- 钱包共振监控（Polymarket 盈利钱包跟单信号） ----------
const TRACKED = [
  ["0x0cb038487586d1119b165466072e9baf666f3a90", "HF0"],
  ["0x7cd156802a8c9f4c9c8a1fa6270529c8b43e8d05", "MO1"],
  ["0xc58e8d8a2479c5bea8d34b97cb0efbe0fa73c6b6", "TW1"],
  ["0xeda9247a2b3c99a9e0bf46cdac6e1974365cf589", "TW2"],
  ["0x66b1c3fe0239c733b84f810f3d0b6e3a4b536b9d", "MO2"],
  ["0x78f4dc76a15a640cb2b47183fe64eff5874938ee", "MO3"],
  ["0x5160f668b2edbd455b9083b126174f12a592d6cf", "LT1"],
  ["0xe8d68fd271ad704a55f9bb0e4fc6276ee9adc964", "SC1"],
  ["0x3725d52f3c252e8374999cc8617292ea2608ad88", "SC2"],
  ["0xe4f7f7950210228a32c9ebbfb633c004a6d230f6", "MO4"],
  ["0x1c194c5be36cf18b15d56664c56d5b10bf969bcc", "MO5"],
  ["0x3d8e05bae5bd5543aea31bba95f0ab51f4503a59", "HV1"],
];
const TRACKED_MAP = {};
TRACKED.forEach((x) => { TRACKED_MAP[x[0].toLowerCase()] = x[1]; });

const WF_SERIES = [];
for (const c of ["btc", "eth", "sol", "bnb", "xrp", "doge"]) {
  WF_SERIES.push({ key: c + "5m", pat: c + "-updown-5m-", secs: 300, label: COIN_LABEL[c] + " 5m", ev: "https://polymarket.com/event/" + c + "-updown-5m-" });
}
for (const c of ["btc", "eth", "sol", "bnb", "xrp", "doge"]) {
  WF_SERIES.push({ key: c + "15m", pat: c + "-updown-15m-", secs: 900, label: COIN_LABEL[c] + " 15m", ev: "https://polymarket.com/event/" + c + "-updown-15m-" });
}

// 只看勾选的系列（localStorage 持久化；未勾选=不轮询/不出信号/不进流水）
const wfFilter = loadWfFilter();
function loadWfFilter() {
  try { return Object.assign({}, JSON.parse(localStorage.getItem("btcmon_wf_filter") || "{}")); }
  catch (e) { return {}; }
}
function saveWfFilter() { localStorage.setItem("btcmon_wf_filter", JSON.stringify(wfFilter)); }
function wfFilterOn(key) { return wfFilter[key] !== false; }

function renderWfFilters() {
  const el = document.getElementById("wfFilters");
  if (!el) return;
  el.innerHTML = WF_SERIES.map((s) =>
    '<label style="display:inline-flex;align-items:center;gap:3px;cursor:pointer"><input type="checkbox" data-key="' + s.key + '"' + (wfFilterOn(s.key) ? " checked" : "") + ' style="width:13px;height:13px"> ' + s.label + "</label>"
  ).join("") + '<span class="muted">← 只看勾选</span>';
  el.querySelectorAll("input").forEach((cb) => {
    cb.onchange = () => {
      wfFilter[cb.getAttribute("data-key")] = cb.checked;
      saveWfFilter();
      wf.markets = wf.markets.filter((m) => wfFilterOn(m.key));   // 立即停止已取消系列的轮询
      for (const k in wf.signals) {                                // 隐藏已取消系列的信号
        const s = wf.signals[k];
        if (s.key && !wfFilterOn(s.key)) delete wf.signals[k];
      }
      renderWfSignals();
      wfRefreshMarkets();
    };
  });
}

const wf = { running: false, markets: [], mktIdx: 0, lastRefresh: 0, lastTs: {}, feed: [], signals: {}, fired: {}, reqs: 0 };

function wfBeep(times) {
  if (!document.getElementById("wfSound").checked) return;
  try {
    const ctx = wf.audio || (wf.audio = new (window.AudioContext || window.webkitAudioContext)());
    for (let i = 0; i < times; i++) {
      const o = ctx.createOscillator(); const g = ctx.createGain();
      o.frequency.value = 880; o.connect(g); g.connect(ctx.destination);
      const t0 = ctx.currentTime + i * 0.28;
      g.gain.setValueAtTime(0.001, t0); g.gain.exponentialRampToValueAtTime(0.25, t0 + 0.03);
      g.gain.exponentialRampToValueAtTime(0.001, t0 + 0.22);
      o.start(t0); o.stop(t0 + 0.25);
    }
  } catch (e) {}
}

async function wfRefreshMarkets() {
  const nowSec = Math.floor(Date.now() / 1000);
  const slugs = [];
  for (const s of WF_SERIES) {
    if (!wfFilterOn(s.key)) continue;                     // 只看勾选的系列
    const base = nowSec - (nowSec % s.secs);
    slugs.push({ slug: s.pat + base, s, endSec: base + s.secs });
    slugs.push({ slug: s.pat + (base + s.secs), s, endSec: base + 2 * s.secs });
  }
  try {
    const j = await fetchJSON("/api/wf-markets?slugs=" + slugs.map((x) => x.slug).join(","), 15000);
    const found = [];
    for (const m of (j.markets || [])) {
      const s = slugs.find((x) => x.slug === m.slug);
      const end = Math.floor(new Date(m.end).getTime() / 1000);
      if (!s || end <= nowSec) continue;
      found.push({ cid: m.cid, slug: m.slug, label: s.s.label, end, evUrl: s.s.ev + m.slug, secs: s.s.secs, up: m.up, dn: m.dn, key: s.s.key });
    }
    wf.markets = found;
  } catch (e) { /* 保留旧列表 */ }
  wf.lastRefresh = Date.now();
}

// 补充: wf-markets 需要返回 clobTokenIds 供 S2 拉订单簿 —— 解析在 worker 端补齐后此处直接用
function wfMarketBySlug(slug) {
  return wf.markets.find((m) => m.slug === slug) || null;
}

// ---------- 策略信号 S1（反转接针） ----------
// 真实条件概率: P(领先侧获胜 | |gap|bps, 剩余时间档)。来源: 60 天币安 1m + PM 结算验证 (analysis/cond_tables.json)
// 行格式 [gap上限, P]；gapAbs 落在 [上一上限, 本上限) 用本档 P。r40≈剩余40-55%，r20≈剩余≤30%。
const TRUE_P = {
  "5m": {
    btc: { r40: [[2, 0.77], [5, 0.96], [10, 0.99], [20, 1.00], [1e9, 1.00]], r20: [[2, 0.87], [5, 0.99], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    eth: { r40: [[2, 0.74], [5, 0.94], [10, 0.99], [20, 1.00], [1e9, 1.00]], r20: [[2, 0.85], [5, 0.99], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    sol: { r40: [[2, 0.71], [5, 0.93], [10, 0.98], [20, 1.00], [1e9, 1.00]], r20: [[2, 0.82], [5, 0.99], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    bnb: { r40: [[2, 0.76], [5, 0.96], [10, 0.99], [20, 1.00], [1e9, 1.00]], r20: [[2, 0.87], [5, 1.00], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    xrp: { r40: [[2, 0.72], [5, 0.93], [10, 0.97], [20, 0.98], [1e9, 0.99]], r20: [[2, 0.82], [5, 0.98], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    doge: { r40: [[2, 0.72], [5, 0.93], [10, 0.98], [20, 0.99], [1e9, 1.00]], r20: [[2, 0.81], [5, 0.99], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
  },
  "15m": {
    btc: { r40: [[2, 0.69], [5, 0.92], [10, 0.99], [20, 0.99], [1e9, 0.99]], r20: [[2, 0.81], [5, 0.99], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    eth: { r40: [[2, 0.70], [5, 0.89], [10, 0.98], [20, 1.00], [1e9, 1.00]], r20: [[2, 0.78], [5, 0.97], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    sol: { r40: [[2, 0.62], [5, 0.87], [10, 0.94], [20, 0.98], [1e9, 1.00]], r20: [[2, 0.71], [5, 0.95], [10, 0.99], [20, 1.00], [1e9, 1.00]] },
    bnb: { r40: [[2, 0.69], [5, 0.92], [10, 0.98], [20, 0.99], [1e9, 1.00]], r20: [[2, 0.80], [5, 0.98], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    xrp: { r40: [[2, 0.67], [5, 0.89], [10, 0.96], [20, 0.99], [1e9, 0.99]], r20: [[2, 0.78], [5, 0.98], [10, 1.00], [20, 1.00], [1e9, 1.00]] },
    doge: { r40: [[2, 0.64], [5, 0.87], [10, 0.96], [20, 0.99], [1e9, 1.00]], r20: [[2, 0.75], [5, 0.97], [10, 0.99], [20, 1.00], [1e9, 1.00]] },
  },
};
function trueSideP(coin, per, gapAbs, remainFrac) {
  const t = TRUE_P[per] && TRUE_P[per][coin];
  if (!t) return null;
  const row = remainFrac <= 0.30 ? t.r20 : (remainFrac <= 0.55 ? t.r40 : null);
  if (!row) return null;
  for (const [hi, p] of row) { if (gapAbs < hi) return p; }
  return null;
}

// 币安近似 TWAP gap（Chainlink 流缺失时的兜底；与结算有 ~3-9% 偏差，GO 阈值提高）
function binanceTwapGap(coin, winSec) {
  const s = seriesStore.binance;
  if (!s || !s.times || !s.times.length) return null;
  const idx = IDX_COIN.indexOf(coin);
  const nowSec = Math.floor(Date.now() / 1000);
  const wStart = nowSec - (nowSec % winSec);
  let startP = null;
  const inWin = [];
  for (let i = 0; i < s.times.length; i++) {
    const t = s.times[i] / 1000;
    if (t > nowSec) break;
    if (t < wStart - 15) continue;
    const v = s.rows[i] ? s.rows[i][idx] : null;
    if (v == null) continue;
    if (t <= wStart + 12) { if (startP == null) startP = v; continue; }
    inWin.push([t, v]);
  }
  if (startP == null || inWin.length < 4) return null;
  const recent = inWin.filter(([t]) => t >= nowSec - 61);
  const vals = (recent.length >= 4 ? recent : inWin).map(([t, v]) => v);
  const twap = vals.reduce((a, b) => a + b, 0) / vals.length;
  return { gap: (twap - startP) / startP * 1e4 };
}

const s1 = { lastWin: null, signals: {} };  // coin -> {t0, end, dir, prevPct, strength, cid, evUrl, ask}
const s2 = { books: {}, lastFetch: {} };

function windowMovePct(coin, startSec, endSec) {
  const s = seriesStore.binance;
  if (!s || !s.times || !s.times.length) return null;
  const idx = IDX_COIN.indexOf(coin);
  let first = null, last = null;
  for (let i = 0; i < s.times.length; i++) {
    const t = s.times[i];
    if (t < startSec * 1000 || t >= endSec * 1000) continue;
    const v = s.rows[i] ? s.rows[i][idx] : null;
    if (v == null) continue;
    if (first === null) first = v;
    last = v;
  }
  if (first == null || last == null || first <= 0) return null;
  return (last / first - 1) * 100;
}

function s1Evaluate() {
  const now = Date.now();
  const curWin = Math.floor(now / 300000);
  if (s1.lastWin === null) { s1.lastWin = curWin; return; }
  if (curWin <= s1.lastWin) return;
  const endedEnd = curWin * 300;            // 刚结束窗口的结束(秒)
  const endedStart = endedEnd - 300;
  s1.lastWin = curWin;
  wfRefreshMarkets();                        // 边界处立即刷新新窗口市场
  for (const c of ["btc", "eth", "sol", "bnb", "xrp", "doge"]) {
    const pct = windowMovePct(c, endedStart, endedEnd);
    if (pct == null) continue;
    const a = Math.abs(pct);
    if (a < 0.15) { delete s1.signals[c]; continue; }
    const strength = a >= 0.20 ? "s" : "m";
    const dir = pct < 0 ? "Up" : "Down";    // 跌→买Up 涨→买Down
    // 新窗口（当前窗口）市场信息
    const mk = wfMarketBySlug(c + "-updown-5m-" + endedEnd) || wfMarketBySlug(c + "-updown-5m-" + (endedEnd + 300));
    s1.signals[c] = {
      t0: endedEnd * 1000, end: (endedEnd + 300) * 1000, dir, prevPct: pct, strength,
      cid: mk ? mk.cid : null, evUrl: mk ? mk.evUrl : null, up: mk ? mk.up : null, dn: mk ? mk.dn : null,
      label: COIN_LABEL[c] + " 5m", ask: null, askTs: 0,
    };
    wfBeep(strength === "s" ? 2 : 1);
    if (strength === "s") wfNotify("S1 反转信号 " + COIN_LABEL[c], (pct < 0 ? "下跌 " : "上涨 ") + pct.toFixed(2) + "% → 买 " + dir + "，限价≤0.55");
  }
  renderS1();
}

async function s1FetchAsk(sig) {
  if (!sig.up && !sig.dn) return;
  const token = sig.dir === "Up" ? sig.up : sig.dn;
  if (!token) return;
  try {
    const b = await fetchJSON("/api/wf-book?token=" + token, 8000);
    sig.ask = b.ask; sig.bid = b.bid; sig.askTs = Date.now();
  } catch (e) {}
}

function renderS1() {
  const el = document.getElementById("s1Table");
  if (!el) return;
  const now = Date.now();
  let html = '<thead><tr><th>币种</th><th>上一窗口</th><th>信号</th><th>建议限价</th><th>当前卖价</th><th>状态</th></tr></thead><tbody>';
  let any = false;
  for (const c of ["btc", "eth", "sol", "bnb", "xrp", "doge"]) {
    const sig = s1.signals[c];
    const left = Math.floor(now / 300000) * 300;
    const prevEnd = left * 1000;
    const pct = (sig && sig.prevPct != null) ? sig.prevPct : windowMovePct(c, (left - 300) * 1000, prevEnd);
    any = true;
    if (!sig || now - sig.t0 > 90000) {
      html += "<tr><td><b>" + COIN_LABEL[c] + "</b></td><td class='cl-gap'>" + (pct == null ? "—" : (pct >= 0 ? "+" : "") + pct.toFixed(2) + "%") + "</td>" +
        "<td><span class='watch'>无信号</span></td><td>—</td><td>—</td><td><span class='watch'>等待 ≥0.15%</span></td></tr>";
      continue;
    }
    const ageS = Math.round((now - sig.t0) / 1000);
    const remain = Math.max(0, 90 - ageS);
    const okEntry = sig.ask != null && sig.ask <= 0.55;
    html += "<tr><td><b>" + COIN_LABEL[c] + "</b></td><td class='cl-gap'>" + (sig.prevPct >= 0 ? "+" : "") + sig.prevPct.toFixed(2) + "%</td>" +
      '<td><span class="siglv ' + sig.strength + '">' + (sig.strength === "s" ? "强" : "中") + " 买" + sig.dir + "</span></td>" +
      "<td class='cl-gap'>≤0.55</td>" +
      "<td class='cl-gap'>" + (sig.ask == null ? "…" : sig.ask.toFixed(3)) + "</td>" +
      "<td>" + (remain > 0 ? (okEntry ? '<span class="go">可入场</span> 剩' + remain + "s" : '<span class="watch">等低价</span> 剩' + remain + "s") : '<span class="watch">入场窗口已过</span>') + "</td></tr>";
  }
  if (!any) html += '<tr><td colspan="6" class="muted">等待币安数据…</td></tr>';
  el.innerHTML = html + "</tbody>";
  const upd = document.getElementById("s1Updated");
  if (upd) {
    const barAge = window.__lastBarTs ? Math.round((Date.now() - window.__lastBarTs) / 1000) : null;
    upd.textContent = "v" + APP_VER + " · 下一评估 " + (300 - Math.floor((Date.now() % 300000) / 1000)) + "s · 币安流 " + (barAge == null ? "无" : barAge + "s前") + (window.__lastJsErr ? " · ⚠" + window.__lastJsErr : "");
  }
}

// ---------- 策略信号 S2（TWAP 锁定收割） ----------
function FEE_C(p) { return 0.07 * p * (1 - p); }
async function s2FetchBook(coin, per) {
  const nowSec = Math.floor(Date.now() / 1000);
  const base = nowSec - (nowSec % (per === "5m" ? 300 : 900));
  const mk = wfMarketBySlug((per === "5m" ? coin + "-updown-5m-" : coin + "-updown-15m-") + base);
  if (!mk || (!mk.up && !mk.dn)) return null;
  const key = per + coin;
  if (s2.lastFetch[key] && Date.now() - s2.lastFetch[key] < 6000) return s2.books[key];
  s2.lastFetch[key] = Date.now();
  try {
    const [bu, bd] = await Promise.all([
      mk.up ? fetchJSON("/api/wf-book?token=" + mk.up, 8000) : Promise.resolve(null),
      mk.dn ? fetchJSON("/api/wf-book?token=" + mk.dn, 8000) : Promise.resolve(null),
    ]);
    s2.books[key] = { askUp: bu ? bu.ask : null, askDn: bd ? bd.ask : null, ts: Date.now() };
    return s2.books[key];
  } catch (e) { return s2.books[key] || null; }
}

async function renderS2() {
  const el = document.getElementById("s2Table");
  if (!el) return;
  const now = Date.now();
  // 链上流状态
  const clCoins = clStore.coins ? Object.keys(clStore.coins).filter((c) => clStore.coins[c] && clStore.coins[c].p != null) : [];
  const clAge = clStore.lastTs ? Math.round((Date.now() - clStore.lastTs) / 1000) : null;
  const upd = document.getElementById("s2Updated");
  if (upd) upd.textContent = (clCoins.length
    ? "链上流: " + clCoins.map((c) => COIN_LABEL[c]).join("/") + (clAge != null ? " · " + clAge + "s前" : "")
    : "链上流: 无 · 币安近似") + " · v" + APP_VER + (window.__lastJsErr ? " · ⚠" + window.__lastJsErr : "");
  let html = '<thead><tr><th>市场</th><th>剩余</th><th>TWAP领先</th><th>真实P</th><th>卖价</th><th>边际</th><th>信号</th></tr></thead><tbody>';
  for (const per of ["5m", "15m"]) {
    const winSec = per === "5m" ? 300 : 900;
    const endMs = Math.ceil(now / (winSec * 1000)) * winSec * 1000;
    const remain = Math.round((endMs - now) / 1000);
    for (const c of ["btc", "eth", "sol", "bnb", "xrp", "doge"]) {
      const d = clStore.coins && clStore.coins[c];
      let gap = per === "5m" ? (d && d.gap5) : (d && d.gap15);
      let src = "链";
      if (gap == null || !isFinite(gap)) {
        const b = binanceTwapGap(c, winSec);
        if (b) { gap = b.gap; src = "近"; }
      }
      const b = remain <= 130 ? await s2FetchBook(c, per) : null;
      const label = COIN_LABEL[c] + " " + per;
      if (gap == null || !isFinite(gap)) {
        html += "<tr><td><b>" + label + "</b></td><td>" + remain + "s</td><td colspan='5' class='watch'>暂无数据</td></tr>";
        continue;
      }
      const side = gap >= 0 ? "Up" : "Down";
      const gapAbs = Math.abs(gap);
      const remainFrac = remain / winSec;
      const tp = remain <= (per === "5m" ? 190 : 560) ? trueSideP(c, per, gapAbs, remainFrac) : null;
      const ask = b ? (side === "Up" ? b.askUp : b.askDn) : null;
      const minEdge = src === "链" ? 0.02 : 0.03;
      const edge = (tp != null && ask != null && ask > 0 && ask < 0.995) ? (tp - ask - FEE_C(ask)) : null;
      let badge = '<span class="watch">待命（剩≤120s 触发）</span>';
      if (remain <= 120 && tp != null) {
        if (gapAbs < 0.5) badge = '<span class="watch">观察·领先太弱</span>';
        else if (edge != null && edge >= minEdge) badge = '<span class="go">GO 买' + side + "</span>";
        else if (edge != null && edge >= 0) badge = '<span class="edge">边缘 ' + (edge * 100).toFixed(1) + "¢</span>";
        else badge = '<span class="watch">无边际' + (edge == null ? "" : " (" + (edge * 100).toFixed(1) + "¢)") + "</span>";
      } else if (tp != null) {
        badge = '<span class="watch">领先 ' + side + " " + gapAbs.toFixed(1) + "bps</span>";
      }
      html += "<tr><td><b>" + label + "</b>" + (src === "近" ? '<span class="muted">(近)</span>' : "") + "</td><td>" + remain + "s</td>" +
        "<td class='cl-gap'>" + (gap >= 0 ? "+" : "") + gap.toFixed(1) + (src === "近" ? "<span class='muted'>(近)</span>" : "") + "</td>" +
        "<td>" + (tp == null ? "—" : tp.toFixed(2)) + "</td>" +
        "<td class='cl-gap'>" + (ask == null ? "—" : ask.toFixed(3)) + "</td>" +
        "<td class='cl-gap'>" + (edge == null ? "—" : (edge * 100).toFixed(1) + "¢") + "</td>" +
        "<td>" + badge + "</td></tr>";
      if (edge != null && edge >= minEdge && remain <= 120 && gapAbs >= 2) {
        const gk = "s2go|" + c + "|" + per + "|" + endMs;
        if (!s2.books[gk]) {
          s2.books[gk] = 1;
          wfBeep(3);
          wfNotify("S2 GO " + label + " 买" + side, (src === "近" ? "[币安近似] " : "") + "gap " + gap.toFixed(1) + "bps · 真实P " + tp.toFixed(2) + " · 卖价 " + (ask == null ? "?" : ask.toFixed(3)) + " · 边际 " + (edge * 100).toFixed(1) + "¢");
        }
      }
      // 供扩展桥推送：GO 与“强边际临近”都记录
      if (remain <= 120 && gapAbs >= 2 && edge != null && edge >= 0.005) {
        s2.active = s2.active || {};
        const pkS2 = c + "|" + per, pvS2 = s2.active[pkS2];
        s2.active[pkS2] = { type: "S2", coin: c, per, side, gap: +gap.toFixed(1), tp, ask, edge: +(edge * 100).toFixed(1), ts: (pvS2 && pvS2.end === endMs ? pvS2.ts : Date.now()), end: endMs, label, slug: (per === "5m" ? c + "-updown-5m-" : c + "-updown-15m-") + (Math.floor(endMs / 1000) - (per === "5m" ? 300 : 900)) };
      }
    }
  }
  el.innerHTML = html + "</tbody>";
}

async function wfPollOnce() {
  if (!wf.markets.length) return;
  const m = wf.markets[wf.mktIdx % wf.markets.length];
  wf.mktIdx++;
  if (m.key && !wfFilterOn(m.key)) return;                // 已取消勾选：跳过轮询
  let trs = null;
  try {
    trs = await fetchJSON("/api/wf-trades?market=" + m.cid, 10000);
    wf.reqs++;
  } catch (e) { return; }
  if (!Array.isArray(trs)) return;
  let maxTs = wf.lastTs[m.cid] || 0;
  const fresh = [];
  for (const t of trs) {
    const ts = Math.floor(Number(t.timestamp) || 0);
    if (ts > maxTs) maxTs = ts;
    const w = String(t.proxyWallet || "").toLowerCase();
    if (!TRACKED_MAP[w] || t.side !== "BUY") continue;
    if (ts <= Date.now() / 1000 - 600) continue;
    fresh.push({ ts, wallet: w, tag: TRACKED_MAP[w], price: Number(t.price), outcome: String(t.outcome), size: Number(t.size), key: m.key });
  }
  wf.lastTs[m.cid] = maxTs;
  for (const e of fresh) {
    e.label = m.label; e.end = m.end; e.cid = m.cid; e.slug = m.slug; e.evUrl = m.evUrl;
    wf.feed.unshift(e);
    wfFeedEntry(e);
    wfUpdateSignal(e);
  }
  if (fresh.length) renderWfSignals();
  const st = document.getElementById("wfStatus");
  if (st) st.textContent = "监控 " + wf.markets.length + " 个市场 · " + wf.reqs + " 请求 · 更新 " + fmtT(Date.now());
}

function wfUpdateSignal(e) {
  const key = e.cid + "|" + e.outcome;
  const now = Date.now() / 1000;
  const st = wf.signals[key] || (wf.signals[key] = { entries: [], label: e.label, outcome: e.outcome, end: e.end, evUrl: e.evUrl, slug: e.slug, key: e.key });
  st.entries.push({ ts: e.ts, wallet: e.wallet, price: e.price, tag: e.tag });
  st.entries = st.entries.filter((x) => now - x.ts <= 120);
  const wallets = {};
  st.entries.forEach((x) => { wallets[x.wallet] = x.price; });
  const n = Object.keys(wallets).length;
  const avgP = st.entries.reduce((s, x) => s + x.price, 0) / st.entries.length;
  st.n = n; st.avgP = avgP; st.lastT = e.ts;
  const firedKey = key + "|" + n;
  if (n >= 2 && !wf.fired[firedKey]) {
    wf.fired[firedKey] = true;
    if (n >= 3) { wfBeep(3); wfNotify("≥3 钱包共振 " + st.label + " " + st.outcome, "均价 " + avgP.toFixed(3) + " · 建议限价 ≤" + (avgP + 0.02).toFixed(3)); }
    else { wfBeep(1); }
  }
}

function wfFeedEntry(e) {
  const el = document.getElementById("wfFeed");
  if (!el) return;
  const empty = el.querySelector(".empty");
  if (empty) empty.remove();
  const div = document.createElement("div");
  div.className = "fi";
  const left = e.end - Date.now() / 1000;
  div.innerHTML = "<span><b>" + e.tag + "</b> 买 " + e.outcome + " @<b>" + e.price.toFixed(3) + "</b> ×" + Math.round(e.size) + " · " + e.label + "</span>" +
    "<span class='muted'>剩 " + Math.max(0, Math.round(left)) + "s · " + fmtT(e.ts * 1000) + "</span>";
  el.prepend(div);
  while (el.children.length > 30) el.removeChild(el.lastChild);
}

function renderWfSignals() {
  const el = document.getElementById("wfSignals");
  if (!el) return;
  const now = Date.now() / 1000;
  const live = Object.entries(wf.signals).filter(([k, s]) => s.n >= 2 && (!s.key || wfFilterOn(s.key)) && now - s.lastT <= 180);
  wf.activeConf = live.map(([k, s]) => ({
    type: s.n >= 3 ? "CONF3" : "CONF2", coin: (s.label || "").split(" ")[0].toUpperCase(),
    per: (s.label || "").includes("15m") ? "15m" : "5m", side: s.outcome,
    avgP: s.avgP, wallets: s.n, ts: Math.round(s.lastT * 1000), end: Math.round(s.end * 1000),
    label: s.label, evUrl: s.evUrl,
  }));
  live.sort((a, b) => (b[1].n - a[1].n) || (b[1].lastT - a[1].lastT));
  if (!live.length) {
    el.innerHTML = '<div class="empty" style="padding:12px">暂无活跃共振（需 ≥2 个跟踪钱包 120 秒内同市场同方向买入）</div>';
    return;
  }
  el.innerHTML = live.map(([k, s]) => {
    const lv = s.n >= 3 ? "lv3" : "";
    const left = Math.max(0, Math.round(s.end - now));
    const lim = (s.avgP + 0.02).toFixed(3);
    const wnames = [...new Set(s.entries.map((x) => x.tag))].join(" ");
    return '<div class="sig ' + lv + '"><div class="hd"><b>' + (s.n >= 3 ? "★≥3 钱包共振 " : "双钱包共振 ") + s.label + " 买 " + s.outcome + "</b>" +
      '<span>剩 ' + left + "s</span></div>" +
      '<div class="row"><span>钱包: ' + wnames + "</span><span>均价 " + s.avgP.toFixed(3) + "</span></div>" +
      '<div class="row"><span>建议限价 ≤' + lim + "（+5~30s 内）</span>" + '<a href="' + s.evUrl + '" target="_blank">打开市场 ↗</a></div></div>';
  }).join("");
}

function wfNotify(title, body) {
  try {
    if (window.Notification && Notification.permission === "granted") new Notification(title, { body });
  } catch (e) {}
}

function wfStart() {
  if (wf.running) return;
  wf.running = true;
  wfRefreshMarkets();
  setInterval(() => { if (Date.now() - wf.lastRefresh > 45000) wfRefreshMarkets(); }, 5000);
  setInterval(() => { renderWfSignals(); }, 2000);
  const loop = () => {
    if (!wf.running) return;
    wfPollOnce().finally(() => setTimeout(loop, 1200));
  };
  loop();
}

document.getElementById("wfToggle").onclick = () => {
  const btn = document.getElementById("wfToggle");
  if (wf.running) { wf.running = false; btn.textContent = "启动"; }
  else { wf.running = false; btn.textContent = "暂停"; wfStart(); }
};
document.getElementById("wfNotify").onclick = () => {
  try { if (window.Notification && Notification.permission !== "granted") Notification.requestPermission(); } catch (e) {}
};

// ---------- 扩展桥：聚合信号推送（供 Gate 网页扩展/本机桥读取） ----------
function collectSignals() {
  const list = [];
  for (const c in s1.signals) {
    const s = s1.signals[c];
    if (Date.now() - s.t0 <= 90000) {
      list.push({ type: "S1", coin: c.toUpperCase(), per: "5m", side: s.dir, strength: s.strength, limit: 0.55, ts: s.t0, end: s.end, label: s.label, evUrl: s.evUrl || ("https://polymarket.com/event/" + c + "-updown-5m-" + Math.floor(s.end / 1000)) });
    }
  }
  const act = s2.active || {};
  const nowMs = Date.now();
  for (const k in act) if (!act[k].end || act[k].end > nowMs) list.push(act[k]);   // 过期窗口条目不再推送

  for (const x of (wf.activeConf || [])) list.push(x);
  return list;
}
let lastPushKey = "__init__", lastPushAt = 0;
setInterval(async () => {
  try {
    const list = collectSignals();
    const ident = JSON.stringify(list.map((s) => [s.type, s.coin, s.per, s.side, s.end]));
    if (ident === lastPushKey && Date.now() - lastPushAt < 30000) return;  // 信号集合未变：30秒才刷新数值（省D1写配额）
    lastPushKey = ident; lastPushAt = Date.now();
    await fetch("/api/signals", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token: PUT_TOKEN, list }) });
  } catch (e) {}
}, 3000);

// ---------- 交互 ----------
function initMexControls() {
  const b = (id) => document.getElementById(id);
  b("mexMode").value = mexCfg.mode;
  b("mexWidth").value = mexCfg.width;
  b("mexThr").value = mexCfg.thr;
  const cc = b("mexColors");
  cc.innerHTML = MEX_SRCS.map((s2) => '<input type="color" data-key="' + s2.key + '" value="' + mexColor(s2.key) + '" title="' + s2.label + '">').join("");
  cc.querySelectorAll("input").forEach((inp) => {
    inp.oninput = () => { mexCfg.colors[inp.getAttribute("data-key")] = inp.value; saveMexCfg(); renderMex(); };
  });
  b("mexMode").onchange = (e) => { mexCfg.mode = e.target.value; saveMexCfg(); mexOptSync = false; renderMex(); };
  b("mexWidth").oninput = (e) => { mexCfg.width = Number(e.target.value) || 1.5; saveMexCfg(); renderMex(); };
  b("mexThr").onchange = (e) => { mexCfg.thr = Number(e.target.value) || 5; saveMexCfg(); };
  const bp = b("btnPred");
  const applyPred = () => {
    const show = localStorage.getItem("predPanel") === "1";
    document.getElementById("cards").style.display = show ? "" : "none";
    bp.textContent = show ? "隐藏预测" : "预测面板";
    if (chart) chart.resize();
  };
  bp.onclick = () => { localStorage.setItem("predPanel", localStorage.getItem("predPanel") === "1" ? "0" : "1"); applyPred(); };
  applyPred();
  window.addEventListener("resize", () => { if (mexChart) mexChart.resize(); });
}
function initTabs() {
  const el = document.getElementById("tabs");
  el.innerHTML = SOURCES.map((s) => '<button data-src="' + s + '"' + (s === activeTab ? ' class="active"' : "") + ">" + SRC_LABEL[s] + "</button>").join("");
  el.querySelectorAll("button").forEach((b) => {
    b.onclick = () => {
      activeTab = b.getAttribute("data-src");
      el.querySelectorAll("button").forEach((x) => x.classList.remove("active"));
      b.classList.add("active");
      renderAll();
    };
  });
}
document.getElementById("dateSel").onchange = async (e) => {
  viewDate = e.target.value;
  if (viewDate === "yesterday" && !yesterdayLoaded) {
    yesterdayLoaded = true;
    await loadDay("yesterday");
  }
  worker.postMessage({ type: "date", mode: viewDate });
  renderAll();
};
["fType", "fLeader", "fFollower"].forEach((id) => { document.getElementById(id).onchange = renderMatches; });
document.getElementById("histSel").onchange = renderHist;
document.getElementById("btnParams").onclick = () => {
  document.getElementById("pThreshold").value = params.threshold;
  document.getElementById("pBefore").value = params.before;
  document.getElementById("pAfter").value = params.after;
  document.getElementById("pMinGap").value = params.minGap;
  document.getElementById("pMode2").checked = !!params.mode2;
  document.getElementById("modalMask").classList.add("show");
};
document.getElementById("pCancel").onclick = () => document.getElementById("modalMask").classList.remove("show");
document.getElementById("btnBackfill").onclick = runBackfill;
document.getElementById("pApply").onclick = () => {
  params = {
    threshold: Math.max(0.05, parseFloat(document.getElementById("pThreshold").value) || 0.15),
    before: parseFloat(document.getElementById("pBefore").value) || -60,
    after: parseFloat(document.getElementById("pAfter").value) || 300,
    minGap: Math.max(0, parseFloat(document.getElementById("pMinGap").value) || 30),
    mode2: document.getElementById("pMode2").checked,
  };
  saveParams();
  worker.postMessage({ type: "params", params });
  document.getElementById("modalMask").classList.remove("show");
};
setInterval(() => {
  const sec = Math.floor((Date.now() - pageStart) / 1000);
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s2 = sec % 60;
  document.querySelector("#chipRun b").textContent = (h ? h + "时" : "") + m + "分" + s2 + "秒";
}, 1000);
window.addEventListener("resize", () => { if (chart) chart.resize(); if (histChart) histChart.resize(); });

// ---------- 启动 ----------
(async function init() {
  initTabs();
  worker.postMessage({ type: "init", params });
  await pollStatus();
  await loadDay("today");
  renderAll();
  renderWfFilters();
  initMexControls();
  setInterval(pollStatus, 60000);
  wfStart(); // 钱包共振监控（Polymarket 盈利钱包跟踪）
  // S1/S2 策略信号引擎
  setInterval(() => { s1Evaluate(); }, 1000);
  setInterval(() => {
    renderS1();
    for (const c in s1.signals) {
      const sig = s1.signals[c];
      if (Date.now() - sig.t0 <= 90000 && Date.now() - sig.askTs > 5000) s1FetchAsk(sig);
    }
  }, 4000);
  setInterval(() => { if (!s2.busy) { s2.busy = true; renderS2().finally(() => { s2.busy = false; }); } }, 2000);
})();
