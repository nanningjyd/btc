# btc-monitor · 五币联动实时监测系统

> 目标：7×24 采集 BTC / ETH / SOL / BNB / DOGE 的 10 秒级价格，同图绘制五币归一化曲线；
> 识别 BTC/ETH 谷底/谷顶转折点，量化 SOL/BNB/DOGE 的跟随时间差与概率；
> 币安、OKX、Polymarket 三源独立出图与统计；关页不断采，随时可查当天与昨天。
> 技术栈：Cloudflare Worker（后端 + 静态托管）+ D1（SQLite 数据库）+ 浏览器 ECharts（前端分析）。

---

## 一、整体架构（数据流）

```
┌─────────────────────────────────────────────────────────────────┐
│  云端 7×24 采集（Cloudflare Cron，每分钟触发）                  │
│    Worker.scheduled → sampleRound() → D1.bars                    │
│    OKX：WS（云端出口稳定可达）                                   │
│    Polymarket/币安：云端出口被地域封锁 → 由下方本地/VPS采集器补   │
└─────────────────────────────────────────────────────────────────┘
        ▲                                  ▲
        │ 回传 /api/put                     │ 回传 /api/put
┌──────────────┐                  ┌──────────────────────┐
│ 本地采集器    │                  │ VPS 采集器(可迁移)   │
│ collector.js │                  │ vps/collector_pm.py  │
│ (走10808代理) │                  │ (任意VPS, systemd)   │
│ 币安+OKX+PM  │                  │ 仅 Polymarket       │
└──────────────┘                  └──────────────────────┘

浏览器前端（任何人打开即用，无需代理）：
  Worker 托管 index.html/app.js/engine.js/echarts.min.js
    → GET /api/bars 拉取当天+昨天完整数据（关页时段也被云端/采集器补齐）
    → 前端直连三源 WebSocket 续采实时数据（低延迟，浏览器直连不受墙影响）
    → Web Worker(engine.js) 内做 ZigZag 转折识别 + 滞后匹配 + 概率统计 + 预测
    → ECharts 渲染五币同图、滞后统计表、时间差记录、lag 分布、预测卡片
```

## 二、目录结构

```
btc-monitor/
├── wrangler.jsonc          # Worker 部署配置（账号/路由/Cron/D1绑定/环境变量）
├── schema.sql              # D1 建表 SQL（bars / health / meta）
├── package.json            # 依赖 wrangler（开发部署用）
├── .gitignore              # 已忽略 node_modules / .wrangler / vps_config.json / *.log
├── src/
│   └── worker.js           # 【后端核心】API + Cron 采集 + 静态托管（420 行）
├── public/                 # 【前端】Worker 以静态资源托管
│   ├── index.html          # 页面结构 + 样式（深色主题，红涨绿跌）
│   ├── app.js              # 主线程：拉历史、收 Worker 消息、ECharts 渲染、UI 交互、回传
│   ├── engine.js           # Web Worker：实时WS采集 + 10s聚合 + 分析引擎（核心算法）
│   └── echarts.min.js      # 本地打包的 ECharts（不走 CDN，国内加载稳）
├── collector/              # 【本地常驻采集器】Node，走 10808 代理
│   ├── collector.js        # 连三源 WS，10s 聚合，POST 回 /api/put
│   ├── package.json        # 依赖 ws + https-proxy-agent
│   ├── 启动采集器.bat       # 双击启动（Windows）
│   └── README.md           # 本地采集器使用说明
└── vps/                    # 【VPS 可迁移采集器】Python，部署任意 VPS
    ├── collector_pm.py     # 跑在 VPS 上：连 Polymarket RTDS WS，10s聚合回传
    ├── config.json         # 由部署脚本生成（put_url / put_token）—— 已在 VPS 上
    ├── deploy_vps.py       # 一键部署/迁移脚本（SSH + systemd）
    ├── vps_config.json     # ⚠️ 含 VPS 密码，已 gitignore，不入库
    └── README.md           # VPS 采集器迁移说明
```

## 三、各模块功能

### 1. 后端 `src/worker.js`
- **三源采集逻辑**（云端 Cron 调用 `sampleRound`）：
  - 币安：`startBinanceStream()` 走 WebSocket（`stream.binance.com:9443`，REST 被出口封锁）；含 3 次自动重连 + 诊断。
  - OKX：`fetchOKX()` 走 REST 全量（`/api/v5/market/tickers`，单次请求拿全部价格；WS tickers 推送过密会打爆免费版 CPU，故不用）。
  - Polymarket：`startPolymarketStream()` 走 RTDS WebSocket（`ws-live-data.polymarket.com`），订阅 `crypto_prices_chainlink`（主源）+ `crypto_prices`（币安源补缺），带 filters。
- **采样机制**：每轮 6 个连续 10s 桶（参数 `SAMPLES_PER_RUN`），桶末前 0.8s 取快照；`bucketStart` 把时间戳对齐到 10s 边界。
- **入库**：`insertBars()` 用 `INSERT OR REPLACE`（主键 source+ts），只存收盘价 6 列（btc/eth/sol/bnb/doge/xrp），不存 OHLC。
- **API 路由**（`handleApi`）：
  - `GET /api/healthz` 健康检查
  - `GET /api/status` 各源条数/最近时间/健康/诊断（前端顶部"云采集"徽标用）
  - `GET /api/bars?source=&date=` 按源+日期(东八区)拉数据（前端主数据源）
  - `POST /api/put` 浏览器/采集器回传实时数据（校验 `PUT_TOKEN` + 时间戳 ±10min）
  - `GET /api/collect?secret=` 手动触发一轮采集（需 `COLLECT_SECRET`）
  - `GET /api/probe?secret=` 三源连通性探测（币安各主机/WS、OKX REST/WS、Polymarket）
- **Cron**：`* * * * *` 每分钟采样；`5 16 * * *`（=东八区 00:05）执行 `cleanup()` 删 48h 前数据。

### 2. 数据库 `schema.sql`
- `bars(ts, source, btc,eth,sol,bnb,doge,xrp)` 主键 (source,ts)，WITHOUT ROWID，`idx_bars_ts`。
- `health(source, last_ok, last_err, updated)` 各源健康状态。
- `meta(k, v)` 存 `pm_symbols`（Polymarket 实测到的符号）和 `last_diag`（诊断）。
- 数据保留：2 天，由 Cron 每日清理。

### 3. 前端 `public/`
- **`engine.js`（Web Worker，核心分析）**：
  - 实时 WS 采集三源（浏览器直连，断线 8s 重连）+ 10s 聚合（`doTick`/`scheduleTick`）+ 回传主线程。
  - `zigzag()`：**ZigZag 转折识别**，按阈值(默认 0.15%)和最小间隔识别谷底(bottom)/谷顶(top)，`confirmedT` 为确认时刻。
  - `matchLags()`：**滞后匹配**，在窗口 [before, after]（默认 -60s~+300s）内为每条主流币(BTC/ETH)拐点找同向的跟随币(SOL/BNB/DOGE，无 BNB 则 XRP)拐点，记录时间差 lag = 跟随−主流（秒）。
  - `analyze()`：按 "源×主流币×跟随币×谷底/谷顶" 统计 次数n、跟随率rate、均值/中位/最小/最大、lag 分桶直方图；生成预测事件（新确认的主流币拐点 → 根据历史中位 lag 给出跟随币预计见底/见顶时间与概率）。
  - 模式2（15min ±1% 备忘策略）：默认关，参数开关。
  - 消息协议：`init/history/params/date` 入，`series/analysis/predict/conn/bar` 出。
- **`app.js`（主线程）**：
  - 拉 `/api/bars` 历史 → 发给 Worker 的 `history`；监听 Worker 消息渲染 ECharts 主图（五币归一化同图，谷底▲红/谷顶▼绿）、滞后统计表、时间差记录表（可筛选类型/主流币/跟随币）、lag 分布图、实时预测卡片。
  - 顶部状态徽标（云采集条数/实时连接/运行秒）、日期选择器（今天/昨天）、参数弹窗（阈值/窗口/间隔/模式2，存 localStorage）。
  - 把前端实时聚合的 bar 通过 `POST /api/put` 回传（弥补云端出口封锁的源）。
- **`index.html`**：页面骨架 + 深色主题样式（红涨绿跌：红 `#e04b4b` / 绿 `#22a06b`）。

### 4. 本地采集器 `collector/collector.js`
- Node 程序，走 HTTP 代理（默认 `http://127.0.0.1:10808`，可用环境变量 `PROXY` 覆盖）连三源 WS，10s 聚合，`POST /api/put` 回传 D1。
- 用途：云端 Cron 对币安/Polymarket 出口被地域封锁，本机代理出口可达（币安已验证通），由它补币安数据；Polymarket 是否通取决于代理出口地区（美国节点也被 Polymarket 封）。
- 常驻：双击 `启动采集器.bat`，或 `node collector.js`。

### 5. VPS 采集器 `vps/`
- `collector_pm.py`：纯 Python（依赖 websocket-client，自动 pip 安装），连 Polymarket RTDS WS，10s 聚合回传。配置从同目录 `config.json` 读 `put_url`/`put_token`，**代码本身不含敏感值，迁移不改本文件**。
- `deploy_vps.py`：**一键部署/迁移**。读 `vps_config.json` → SSH 连接 VPS → 装 python3/pip/websocket-client → 上传采集器+config → 写 systemd 服务 `btc-pm` → 启动并开机自启。
- `vps_config.json`（⚠️ 已 gitignore）：仅含 4 个迁移字段 `vps_host / vps_user / vps_password / vps_port`（外加可选 `put_url / put_token`）。**换 VPS 只改这 4 个字段 + 跑 `python deploy_vps.py` 即可。**

## 四、配置项与迁移清单（换电脑/换账号要改什么）

| 场景 | 要改的文件/凭据 | 说明 |
|---|---|---|
| 换电脑继续开发 | GitHub 仓库 `nanningjyd/btc`（已推送）；`npm install` + `pip install paramiko` | 代码全在 Git，克隆即可 |
| 部署到自己 Cloudflare 账号 | `wrangler.jsonc` 的 `account_id`、`database_id`；`wrangler secret put COLLECT_SECRET`；`CF_API_TOKEN` 环境变量 | 需重建 D1（`wrangler d1 create` + 执行 `schema.sql`） |
| 换 VPS | `vps/vps_config.json` 的 4 个字段，再跑 `python deploy_vps.py` | 唯一需改的迁移点 |
| 换回传地址/令牌 | `wrangler.jsonc` 的 `vars.PUT_TOKEN`；同步改 `collector/collector.js` 的 `PUT_TOKEN` 和 `vps/collector_pm.py` 的 `config.json` | 三者必须一致 |
| 自定义域名 | Cloudflare 控制台绑定，或用 `X-Auth-Key` 调 workers/domains 接口 | 当前绑定 `btc.hhxx.eu.org` |

### 敏感配置位置（迁移时注意，勿入库）
- `vps_config.json`：VPS 密码 —— 已在 `.gitignore`，本地保管。
- `wrangler.jsonc`：`account_id` / `database_id` / `vars.PUT_TOKEN` —— 项目配置，迁移换账号需改。
- `collector/collector.js`：`PUT_TOKEN`、`PROXY` —— 明文（PROXY 可环境变量覆盖）。
- 本地 `备忘btc.txt`（不在仓库）：`CF_API_TOKEN`、`GH_TOKEN`、`COLLECT_SECRET`、VPS 地址/密码 —— 换电脑时随身拷贝，**切勿 commit**。

## 五、已知限制（重要）
- **Polymarket 地域封锁**：其 Cloudflare WAF 对"数据中心 IP"整段返回 451（error 1026）。实测 Cloudflare Workers（美）、本机代理出口（美）、Oracle VPS（数据中心）均被挡。Polymarket 只对"非受限国 + 住宅 IP"放行。采集器代码已就绪，**出口一通即自动采**。
- 当前三源实况：币安=本地采集器经代理；OKX=云端 Cron（稳定）；Polymarket=待住宅 IP。
- 若想要"立刻可用的第三独立源"，可在 `vps/collector_pm.py` 或 `collector/collector.js` 里把数据源换成 Coinbase/Kraken（非美区可直达），改动很小。

## 六、本地开发/调试命令
```bash
# 安装依赖
npm install                      # 在 btc-monitor 根目录
pip install paramiko            # 用于 VPS 部署脚本（可选）

# 部署后端
wrangler deploy                 # 需 CF_API_TOKEN 环境变量

# 手动触发采集 / 探测（需 COLLECT_SECRET）
curl "https://<域名>/api/collect?secret=<COLLECT_SECRET>"
curl "https://<域名>/api/probe?secret=<COLLECT_SECRET>"

# 本地采集器（需本机代理 10808）
cd collector && npm install && node collector.js

# VPS 采集器迁移
cd vps && 改 vps_config.json && python deploy_vps.py
```
