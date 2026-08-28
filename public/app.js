// 主线程：数据加载/缓存 + 渲染（ECharts 图表、预测卡片、统计表、lag直方图）
const SOURCES = ["binance", "okx", "polymarket"];
const SRC_LABEL = { binance: "币安", okx: "OKX", polymarket: "Polymarket" };
const COIN_LABEL = { btc: "BTC", eth: "ETH", sol: "SOL", bnb: "BNB", doge: "DOGE", xrp: "XRP" };
const COIN_COLOR = { btc: "#f7931a", eth: "#627eea", sol: "#9945ff", bnb: "#f0b90b", doge: "#c9a633", xrp: "#25a768" };
const IDX_COIN = ["btc", "eth", "sol", "bnb", "doge", "xrp"];
const TYPE_LABEL = { bottom: "谷底", top: "谷顶" };
const TZ = 8 * 3600 * 1000;

const DEFAULT_PARAMS = { threshold: 0.15, before: -60, after: 300, minGap: 30, mode2: false };
const PUT_TOKEN = "pmtk_9f3ac41e7d2b8056"; // 浏览器实时数据回传令牌（页面打开期间把三源10s数据写回D1）
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
const worker = new Worker("engine.js");
worker.onmessage = (e) => {
  const m = e.data;
  if (m.type === "series") {
    seriesStore[m.source] = { times: m.times, rows: m.rows };
    if (m.source === activeTab) scheduleRender();
  } else if (m.type === "bar") {
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

function renderChart() {
  const src = activeTab;
  const s = seriesStore[src];
  const an = analysisStore[src];
  if (!chart) chart = echarts.init(document.getElementById("mainChart"), null, { renderer: "canvas" });
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
  // 拐点散点（谷底红▲ 谷顶绿▼）
  const bottoms = [], tops = [];
  if (an && an.pivotsByCoin) {
    for (const coin in an.pivotsByCoin) {
      if (!coins.includes(coin) || !base[coin]) continue;
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

  const prevZoom = chart.getOption && chart.getOption().dataZoom;
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
    legend: { top: 4, right: 8, textStyle: { color: "#8b97a5", fontSize: 12 }, itemWidth: 16, itemHeight: 8 },
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

// ---------- 交互 ----------
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
  setInterval(pollStatus, 60000);
})();
