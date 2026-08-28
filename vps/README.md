# VPNGate VPN 方案部署指南

## 概述

本方案通过 VPNGate 免费 VPN 服务器（OpenVPN 协议）将 Polymarket API 请求路由到住宅 IP，绕过 Cloudflare WAF 对数据中心 IP 的封锁（HTTP 451 / 1026）。

## 文件说明

| 文件 | 说明 |
|------|------|
| `vpngate_manager.py` | VPNGate 自动选服与 OpenVPN 连接管理器 |
| `collector_pm.py` | Polymarket 价格采集器（集成 VPN 连通性检查） |
| `config.json` | 运行时配置（put_url、put_token、VPN 参数） |
| `deploy_vps.py` | 一键部署脚本（SSH 上传 + 注册 systemd 服务） |
| `vps_config.json` | 部署脚本的 VPS 连接配置（不上传到服务器） |

## 快速部署

### 1. 配置 VPS 连接信息

编辑 `vps_config.json`：

```json
{
  "vps_host": "你的VPS地址",
  "vps_user": "root",
  "vps_password": "你的密码",
  "vps_port": 22,
  "put_url": "https://btc.hhxx.eu.org/api/put",
  "put_token": "你的PUT_TOKEN"
}
```

### 2. 安装依赖

```bash
pip install paramiko
```

### 3. 执行部署

```bash
python deploy_vps.py
```

部署脚本会自动完成：
- 安装 OpenVPN（如果未安装）
- 上传采集器和 VPN 管理器到 `/opt/btc-pm/`
- 写入 `config.json`
- 安装 Python 依赖（websocket-client）
- 注册两个 systemd 服务：`btc-vpngate` 和 `btc-pm`
- 启动服务并验证连通性

### 4. 查看日志

```bash
# 查看 VPN 管理器日志
ssh root@你的VPS 'journalctl -u btc-vpngate -f'

# 查看采集器日志
ssh root@你的VPS 'journalctl -u btc-pm -f'
```

## VPNGate 管理器工作原理

1. **获取服务器列表**：从 `https://www.vpngate.net/api/iphone/` 拉取 CSV 格式的服务器数据
2. **智能选服**：根据连通性评分（综合评分、延迟、速度、在线时长、活跃用户数）筛选最优节点
3. **OpenVPN 连接**：解码 Base64 编码的 .ovpn 配置，调用系统 OpenVPN 客户端连接
4. **连通性验证**：TCP 连接测试 `ws-live-data.polymarket.com:443`
5. **守护监控**：每 30 秒检查一次连通性，连续 3 次失败则自动换服务器重连

## 手动测试

### 列出可用服务器

```bash
python vpngate_manager.py --list
```

### 只连接一次（测试用）

```bash
python vpngate_manager.py --once
```

### 按国家筛选（如日本）

```bash
python vpngate_manager.py --country JP --once
```

### 守护模式（持续运行）

```bash
python vpngate_manager.py
```

## 故障排查

### VPN 连接失败

```bash
# 查看 OpenVPN 日志
journalctl -u btc-vpngate -n 50

# 手动测试 VPNGate API
curl -H "User-Agent: Mozilla/5.0" https://www.vpngate.net/api/iphone/ | head -5
```

### Polymarket 不可达

```bash
# 检查出口 IP 是否被封锁
curl https://ipinfo.io/ip
curl https://www.polymarket.com

# 检查 VPN 连接状态
systemctl status btc-vpngate
```

### 采集器无数据

```bash
# 查看采集器日志
journalctl -u btc-pm -n 50

# 检查 config.json 配置
cat /opt/btc-pm/config.json
```

## 注意事项

- VPNGate 是免费公共 VPN 服务，服务器稳定性无法保证，本方案已实现自动换服
- 建议仅将 VPNGate 用于 Polymarket 数据采集，不建议传输敏感数据
- 如 VPNGate 服务器普遍不可用，可考虑切换到方案四（住宅代理服务商）
- 本方案默认使用 TCP 协议连接 OpenVPN（端口 443），穿透能力更强
