# 本地采集器（7×24 三源数据回传 D1）

因为 Cloudflare 定时任务（Cron）的出口 IP 落在美国，币安(REST+WS)和 Polymarket(RTDS) 都会返回 `451/403`（法律地域封锁），云端只能可靠采集 OKX。本采集器跑在你本机，经 10808 代理（v2rayN）连三源 WebSocket，把 10s 数据推回 D1，实现币安/OKX 7×24 采集（Polymarket 需代理出口为非美区）。

## 运行方式

双击 `启动采集器.bat`，或命令行：

```
node collector.js
```

- 依赖代理 10808（默认 `http://127.0.0.1:10808`），可用环境变量覆盖：`set PROXY=http://127.0.0.1:10809`。
- 依赖已装：`ws`、`https-proxy-agent`（重装：`npm install --registry=https://registry.npmmirror.com`）。
- 每 10 秒聚合一次、每 10 秒推送一次到 `https://btc.hhxx.eu.org/api/put`。

## 开机自启（可选）

以管理员运行 PowerShell：

```powershell
schtasks /create /tn "BTC采集器" /tr "\"C:\Program Files\nodejs\node.exe\" \"E:\AI专用文件夹\btc\btc-monitor\collector\collector.js\"" /sc onlogon /rl highest /f
```

删除自启：`schtasks /delete /tn "BTC采集器" /f`

## 数据源状态说明

| 源 | 本地采集器(经10808) | 云端 Cron | 最终覆盖 |
|---|---|---|---|
| 币安 | ✅ 200/WS 通 | ❌ 451/403 | 本地 7×24 |
| OKX | ✅ 200/WS 通 | ✅ 通 | 双通道冗余 |
| Polymarket | ⚠️ 依赖代理出口节点（当前出口=美国→451） | ❌ 451 | 需把 v2rayN 切到非美区节点 |

Polymarket 要通：v2rayN 把出口节点换成香港/新加坡/日本等非美区即可，采集器会自动连上。
