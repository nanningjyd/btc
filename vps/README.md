# Polymarket VPS 采集器（一键迁移）

把 Polymarket 实时价格（RTDS / Chainlink 聚合价）采集器跑在任意 VPS 上，10s 聚合后推回 Cloudflare D1（`/api/put`）。

## 为什么放 VPS

Polymarket 对数据中心 IP 有 Cloudflare WAF 封禁（`451 + error code 1026`）。实测以下出口全被挡：

| 出口 | 结果 |
|---|---|
| Cloudflare Workers（美国数据中心） | 451 |
| 本机 10808 代理（美国节点） | 451 |
| Oracle 韩国 VPS（数据中心 IP） | 451 + error 1026 |
| 本机直连（中国） | 451 |

**结论**：Polymarket 只对「非受限国家 + 住宅 IP」放行。当前 VPS 是数据中心 IP 所以仍 451。要让 VPS 采集器真正采到数据，二选一：
1. 给 VPS 挂一个非美区**住宅代理**，让采集器走它（`collector_pm.py` 里 `wss://ws-live-data.polymarket.com` 前加代理）；
2. 换一台非美区、非数据中心 IP 的 VPS（如某些住宅 IP 服务器）。

采集器代码本身没问题，端点一通即自动开始推数据，无需改动。

## 迁移（换 VPS 只需改一个文件）

编辑同目录 `vps_config.json`：

```json
{
  "vps_host": "新VPS地址",
  "vps_user": "root",
  "vps_password": "新密码",
  "vps_port": 22,
  "put_url": "https://btc.hhxx.eu.org/api/put",
  "put_token": "pmtk_9f3ac41e7d2b8056"
}
```

然后本机执行一条命令即可完成部署+开机自启：

```
python deploy_vps.py
```

（本机需装 paramiko：`pip install paramiko`；脚本会自动在 VPS 装 python3/pip/websocket-client 并注册 systemd 服务 `btc-pm`）

## 文件

- `collector_pm.py` — VPS 上的采集器（连 RTDS → 聚合 → 推 D1），迁移无需改它
- `deploy_vps.py` — 本机一键部署脚本
- `vps_config.json` — 唯一需要改的配置文件（含 VPS 密码，**已 gitignore，勿提交**）

## 运维

```
# 查看日志
ssh root@<vps> 'journalctl -u btc-pm -f'
# 重启
ssh root@<vps> 'systemctl restart btc-pm'
# 卸载
ssh root@<vps> 'systemctl disable --now btc-pm'
```
