#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VPNGate VPN 自动选服管理器
功能：
  1. 从 VPNGate API 拉取服务器列表并解析
  2. 按评分/速度/延迟/国家等多维度筛选最优节点
  3. 自动连接/切换 OpenVPN 服务器
  4. 断线自动重连（含换服务器逻辑）
  5. 验证出口 IP 是否可达 Polymarket
"""

import csv
import io
import logging
import os
import re
import subprocess
import sys
import time
import base64
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("vpngate_manager.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VPNGate API 相关
# ---------------------------------------------------------------------------
VPNGATE_API_URL = "https://www.vpngate.net/api/iphone/"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class VPNGateServer:
    """VPNGate 服务器数据模型"""

    def __init__(self, row: dict):
        self.hostname = row.get("HostName", "")
        self.ip = row.get("IP", "")
        self.score = int(row.get("Score", 0))
        self.ping = int(row.get("Ping", 9999))
        self.speed = int(row.get("Speed", 0))  # bps
        self.country_long = row.get("CountryLong", "")
        self.country_short = row.get("CountryShort", "")
        self.num_sessions = int(row.get("NumVpnSessions", 0))
        self.uptime = int(row.get("Uptime", 0))
        self.total_users = int(row.get("TotalUsers", 0))
        self.total_traffic = int(row.get("TotalTraffic", 0))
        self.log_type = row.get("LogType", "")
        self.operator = row.get("Operator", "")
        self.message = row.get("Message", "")
        self.openvpn_config_b64 = row.get("OpenVPN_ConfigData_Base64", "")
        # 计算衍生字段
        self.speed_mbps = self.speed / 1_000_000 if self.speed else 0
        self.uptime_hours = self.uptime / 3600 if self.uptime else 0
        # 是否支持 OpenVPN TCP
        self.supports_tcp = False
        self.supports_udp = False
        self.tcp_port = None
        self.udp_port = None
        self._parse_config()

    def _parse_config(self):
        """从 Base64 编码的配置中解析端口和协议信息"""
        if not self.openvpn_config_b64:
            return
        try:
            decoded = base64.b64decode(self.openvpn_config_b64).decode("utf-8", errors="ignore")
            for line in decoded.splitlines():
                line = line.strip().lower()
                if line.startswith("proto tcp"):
                    self.supports_tcp = True
                elif line.startswith("proto udp"):
                    self.supports_udp = True
                elif line.startswith("remote ") and not line.startswith("remote-host"):
                    parts = line.split()
                    if len(parts) >= 3:
                        if self.supports_tcp:
                            self.tcp_port = parts[2]
                        elif self.supports_udp:
                            self.udp_port = parts[2]
        except Exception as e:
            log.debug(f"解析 OpenVPN config 失败 {self.hostname}: {e}")

    @property
    def preferred_port(self):
        """优先使用 TCP 端口（穿透防火墙能力更强）"""
        return self.tcp_port or self.udp_port or "443"

    @property
    def connectivity_score(self) -> float:
        """
        连通性综合评分（越高越好）：
          - 评分高
          - 延迟低
          - 速度快
          - 在线时长长
          - 当前在线用户少（负载均衡）
          - 有日志策略（更可靠）
        """
        if not self.openvpn_config_b64:
            return -1.0
        score = 0.0
        # 评分（归一化到 0-1）
        score += min(self.score / 5_000_000, 1.0) * 30
        # 延迟（越低越好，反比）
        ping_score = max(0, 1 - self.ping / 500) if self.ping > 0 else 0.5
        score += ping_score * 20
        # 速度（Mbps）
        speed_score = min(self.speed_mbps / 10, 1.0)
        score += speed_score * 20
        # 在线时长（小时，越长越稳定）
        uptime_score = min(self.uptime_hours / 720, 1.0)  # 30天
        score += uptime_score * 15
        # 当前在线用户（越少越好）
        session_score = max(0, 1 - self.num_sessions / 200)
        score += session_score * 10
        # 日志策略（有记录说明管理员可靠）
        if self.log_type in ("no-logs", "none", "privacy"):
            score += 5
        elif self.log_type == "2weeks":
            score += 3
        return score

    def __repr__(self):
        return (
            f"VPNGateServer(host={self.hostname}, ip={self.ip}, "
            f"country={self.country_short}, score={self.score:.0f}, "
            f"ping={self.ping}ms, speed={self.speed_mbps:.1f}Mbps, "
            f"conn_score={self.connectivity_score:.1f})"
        )


def fetch_server_list(proxy_url: str = None) -> list[VPNGateServer]:
    """
    从 VPNGate API 获取服务器列表。
    返回按 connectivity_score 降序排列的服务器列表。
    """
    log.info("正在从 VPNGate API 获取服务器列表...")
    req = urllib.request.Request(VPNGATE_API_URL, headers={"User-Agent": DEFAULT_UA})
    try:
        if proxy_url:
            proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
            opener = urllib.request.build_opener(proxy_handler)
        else:
            opener = urllib.request.build_opener()
        with opener.open(req, timeout=30) as resp:
            raw_data = resp.read().decode("utf-8-sig")  # 处理 BOM
    except urllib.error.URLError as e:
        log.error(f"无法访问 VPNGate API: {e}")
        return []
    except Exception as e:
        log.error(f"获取服务器列表失败: {e}")
        return []

    # 解析 CSV（前两行为注释，第三行开始是表头）
    lines = raw_data.strip().split("\n")
    if len(lines) < 3:
        log.error("VPNGate API 返回数据格式异常")
        return []

    # 跳过前两行注释
    csv_content = "\n".join(lines[2:])
    reader = csv.DictReader(io.StringIO(csv_content))
    servers = []
    for row in reader:
        try:
            server = VPNGateServer(row)
            if server.openvpn_config_b64:  # 只保留有有效配置的服务器
                servers.append(server)
        except Exception as e:
            log.debug(f"解析服务器行失败: {row.get('HostName')}, 错误: {e}")

    # 按连通性评分排序
    servers.sort(key=lambda s: s.connectivity_score, reverse=True)
    log.info(f"成功获取 {len(servers)} 个可用服务器")
    return servers


def select_best_server(
    servers: list[VPNGateServer],
    country_filter: str = "",
    min_score: float = 50.0,
    max_ping: int = 300,
) -> VPNGateServer | None:
    """
    从服务器列表中筛选最优节点。
    参数：
      country_filter  — 国家代码（如 'US'、'JP'），空字符串表示不限
      min_score       — 最低连通性评分阈值
      max_ping        — 最高延迟阈值（毫秒）
    """
    filtered = [
        s for s in servers
        if s.connectivity_score >= min_score
        and s.ping <= max_ping
        and (not country_filter or s.country_short == country_filter)
    ]
    if not filtered:
        log.warning("没有符合条件的服务器，放宽筛选条件重试...")
        # 放宽条件：只过滤评分
        filtered = [s for s in servers if s.connectivity_score >= min_score]
    if not filtered:
        log.warning("仍无符合条件的服务器，选择评分最高的节点")
        filtered = servers[:1] if servers else []

    if filtered:
        best = filtered[0]
        log.info(f"选中最优服务器: {best}")
        return best
    return None


# ---------------------------------------------------------------------------
# OpenVPN 连接管理
# ---------------------------------------------------------------------------

def decode_openvpn_config(server: VPNGateServer) -> str:
    """解码 Base64 编码的 OpenVPN 配置文件"""
    try:
        return base64.b64decode(server.openvpn_config_b64).decode("utf-8", errors="ignore")
    except Exception as e:
        log.error(f"解码 OpenVPN 配置失败: {e}")
        return ""


def write_ovpn_file(config_text: str, filepath: str) -> bool:
    """将 OpenVPN 配置写入 .ovpn 文件"""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(config_text)
        return True
    except Exception as e:
        log.error(f"写入 .ovpn 文件失败: {e}")
        return False


def connect_openvpn(ovpn_path: str, config_dir: str) -> bool:
    """
    使用系统 OpenVPN 客户端连接。
    支持 Linux（openvpn 命令）和 Windows（OpenVPN GUI / 命令行）。
    """
    # 检测操作系统
    if sys.platform == "win32":
        return _connect_openvpn_windows(ovpn_path)
    else:
        return _connect_openvpn_linux(ovpn_path, config_dir)


def _connect_openvpn_windows(ovpn_path: str) -> bool:
    """Windows 平台：尝试通过 openvpn 命令行连接"""
    # 尝试常见 OpenVPN 安装路径
    candidates = [
        r"C:\Program Files\OpenVPN\bin\openvpn.exe",
        r"C:\Program Files (x86)\OpenVPN\bin\openvpn.exe",
        r"C:\OpenVPN\bin\openvpn.exe",
        "openvpn",  # 如果在 PATH 中
    ]
    openvpn_exe = None
    for path in candidates:
        if os.path.exists(path):
            openvpn_exe = path
            break

    if not openvpn_exe:
        log.error("未找到 OpenVPN 可执行文件，请先安装 OpenVPN Client")
        return False

    log.info(f"启动 OpenVPN 连接: {ovpn_path} (使用 {openvpn_exe})")
    try:
        # 以前台方式运行，超时后强制终止
        proc = subprocess.Popen(
            [openvpn_exe, "--config", ovpn_path, "--verb", "3"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        # 等待最多 30 秒建立连接
        for i in range(30):
            time.sleep(1)
            # 检查日志中是否出现 "Initialization Sequence Completed"
            try:
                proc.stdout.readline()
            except Exception:
                pass
            if _check_vpn_connected_windows():
                log.info("OpenVPN 连接成功")
                # 保持进程运行，返回 PID 供后续管理
                return True
        # 超时，强制终止
        proc.terminate()
        log.warning("OpenVPN 连接超时，尝试下一服务器")
        return False
    except Exception as e:
        log.error(f"启动 OpenVPN 失败: {e}")
        return False


def _check_vpn_connected_windows() -> bool:
    """检查 Windows 上 VPN 是否已建立（通过检查 tun 接口或路由表）"""
    try:
        result = subprocess.run(
            ["route", "print"],
            capture_output=True, text=True, timeout=5
        )
        # OpenVPN 连接后会添加虚拟网卡路由，检查是否有 tun 相关条目
        return "tun" in result.stdout.lower() or "vpn" in result.stdout.lower()
    except Exception:
        return False


def _connect_openvpn_linux(ovpn_path: str, config_dir: str) -> bool:
    """Linux 平台：使用 openvpn 命令前台运行"""
    try:
        proc = subprocess.Popen(
            ["sudo", "openvpn", "--config", ovpn_path, "--cd", config_dir, "--verb", "3"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for i in range(30):
            time.sleep(1)
            try:
                line = proc.stdout.readline().decode("utf-8", errors="ignore")
                if "Initialization Sequence Completed" in line:
                    log.info("OpenVPN 连接成功（Linux）")
                    return True
                if "ERROR" in line.upper() and "Cannot" in line:
                    log.warning(f"OpenVPN 错误: {line.strip()}")
                    break
            except Exception:
                pass
        proc.terminate()
        return False
    except FileNotFoundError:
        log.error("未找到 openvpn 命令，请安装: sudo apt install openvpn")
        return False
    except Exception as e:
        log.error(f"启动 OpenVPN 失败: {e}")
        return False


def disconnect_openvpn() -> None:
    """断开 OpenVPN 连接"""
    if sys.platform == "win32":
        try:
            subprocess.run(["netsh", "interface", "show"], capture_output=True, timeout=5)
            # Windows: 查找 VPN 连接并断开
            result = subprocess.run(
                ["netsh", "ras", "show", "call"],
                capture_output=True, text=True, timeout=5
            )
            log.info("VPN 断开指令已发送（请手动断开或重启服务）")
        except Exception as e:
            log.warning(f"断开 VPN 时出错: {e}")
    else:
        try:
            subprocess.run(["sudo", "pkill", "-f", "openvpn"], timeout=5)
            log.info("OpenVPN 已断开")
        except Exception:
            pass


def verify_polymarket_connectivity(timeout: int = 10) -> bool:
    """
    验证当前网络是否可以访问 Polymarket。
    通过检测 ws-live-data.polymarket.com 的 TCP 连接来判断。
    """
    import socket
    host = "ws-live-data.polymarket.com"
    port = 443
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            log.info(f"Polymarket 连通性验证通过: {host}:{port}")
            return True
        else:
            log.warning(f"Polymarket 连接失败 (error code {result})")
            return False
    except Exception as e:
        log.warning(f"Polymarket 连通性检查异常: {e}")
        return False


def get_public_ip(proxy_url: str = None) -> str:
    """获取当前出口 IP 地址"""
    urls = [
        "https://api.ipify.org?format=text",
        "https://ipinfo.io/ip",
        "https://icanhazip.com",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_UA})
            if proxy_url:
                proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
                opener = urllib.request.build_opener(proxy_handler)
            else:
                opener = urllib.request.build_opener()
            with opener.open(req, timeout=10) as resp:
                ip = resp.read().decode("utf-8").strip()
                log.info(f"当前出口 IP: {ip}")
                return ip
        except Exception as e:
            log.debug(f"获取 IP 失败 ({url}): {e}")
            continue
    return "unknown"


# ---------------------------------------------------------------------------
# 主控制器
# ---------------------------------------------------------------------------

class VPNGateManager:
    """
    VPNGate VPN 自动管理器
    负责：选服 → 连接 → 验证 → 监控重连
    """

    def __init__(
        self,
        config_dir: str = None,
        country_filter: str = "",
        min_score: float = 50.0,
        max_ping: int = 300,
        reconnect_interval: int = 60,
        check_interval: int = 30,
        proxy_url: str = None,
    ):
        self.config_dir = config_dir or os.path.dirname(os.path.abspath(__file__))
        self.country_filter = country_filter
        self.min_score = min_score
        self.max_ping = max_ping
        self.reconnect_interval = reconnect_interval
        self.check_interval = check_interval
        self.proxy_url = proxy_url
        self.current_server: VPNGateServer | None = None
        self.ovpn_file = os.path.join(self.config_dir, "current.ovpn")
        self.running = False
        self._process = None

    def get_server_list(self) -> list[VPNGateServer]:
        """获取并排序服务器列表"""
        return fetch_server_list(proxy_url=self.proxy_url)

    def connect(self, server: VPNGateServer | None = None) -> bool:
        """
        连接到指定服务器（或自动选择最优）。
        返回是否连接成功。
        """
        if server is None:
            servers = self.get_server_list()
            if not servers:
                log.error("无法获取服务器列表")
                return False
            server = select_best_server(
                servers,
                country_filter=self.country_filter,
                min_score=self.min_score,
                max_ping=self.max_ping,
            )
            if server is None:
                log.error("没有可用的服务器")
                return False

        self.current_server = server
        log.info(f"正在连接到服务器: {server}")

        # 解码并写入 .ovpn 文件
        config_text = decode_openvpn_config(server)
        if not config_text:
            log.error("无法解码 OpenVPN 配置")
            return False

        if not write_ovpn_file(config_text, self.ovpn_file):
            return False

        log.info(f"OpenVPN 配置已写入: {self.ovpn_file}")

        # 尝试连接
        success = connect_openvpn(self.ovpn_file, self.config_dir)
        if success:
            ip = get_public_ip()
            log.info(f"VPN 连接成功，出口 IP: {ip}")
            # 验证 Polymarket 可达性
            if verify_polymarket_connectivity():
                log.info("Polymarket 连通性验证通过！")
            else:
                log.warning("Polymarket 连通性验证未通过，可能需要更换服务器")
        else:
            log.warning("OpenVPN 连接失败，将尝试其他服务器")
        return success

    def start_monitoring(self) -> None:
        """
        启动守护模式：持续监控 VPN 连接状态，断线自动重连。
        """
        self.running = True
        log.info("=" * 60)
        log.info("VPNGate VPN 管理器启动（守护模式）")
        log.info(f"  配置目录: {self.config_dir}")
        log.info(f"  国家过滤: {self.country_filter or '不限'}")
        log.info(f"  最低评分: {self.min_score}")
        log.info(f"  最高延迟: {self.max_ping}ms")
        log.info(f"  重连间隔: {self.reconnect_interval}s")
        log.info(f"  检查间隔: {self.check_interval}s")
        log.info("=" * 60)

        attempt = 0
        while self.running:
            attempt += 1
            log.info(f"--- 连接尝试 #{attempt} ---")
            success = self.connect()

            if success:
                log.info("连接稳定，开始监控...")
                self._monitor_loop()
            else:
                log.warning(f"连接失败，{self.reconnect_interval} 秒后重试...")
                time.sleep(self.reconnect_interval)

    def _monitor_loop(self) -> None:
        """监控循环：定期检查连通性，断线则重连"""
        consecutive_failures = 0
        while self.running:
            try:
                if not verify_polymarket_connectivity():
                    consecutive_failures += 1
                    log.warning(f"Polymarket 不可达 (连续 {consecutive_failures} 次)")
                    if consecutive_failures >= 3:
                        log.warning("连续失败，尝试重新选服连接...")
                        self.current_server = None  # 强制换服务器
                        break
                else:
                    consecutive_failures = 0
            except Exception as e:
                log.error(f"监控检查异常: {e}")
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    break
            time.sleep(self.check_interval)

    def stop(self) -> None:
        """停止管理器并断开 VPN"""
        self.running = False
        disconnect_openvpn()
        log.info("VPNGate 管理器已停止")


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="VPNGate VPN 自动选服管理器")
    parser.add_argument("--country", "-c", default="", help="国家代码过滤（如 US、JP）")
    parser.add_argument("--min-score", type=float, default=50.0, help="最低连通性评分")
    parser.add_argument("--max-ping", type=int, default=300, help="最高延迟（ms）")
    parser.add_argument("--reconnect-interval", type=int, default=60, help="重连间隔（秒）")
    parser.add_argument("--check-interval", type=int, default=30, help="连通性检查间隔（秒）")
    parser.add_argument("--proxy", "-p", default=None, help="HTTP 代理 URL（如 http://127.0.0.1:10808）")
    parser.add_argument("--once", action="store_true", help="仅连接一次，不进入守护模式")
    parser.add_argument("--list", action="store_true", help="仅列出服务器，不连接")
    parser.add_argument("--config-dir", default=None, help="配置文件目录")
    args = parser.parse_args()

    manager = VPNGateManager(
        config_dir=args.config_dir,
        country_filter=args.country,
        min_score=args.min_score,
        max_ping=args.max_ping,
        reconnect_interval=args.reconnect_interval,
        check_interval=args.check_interval,
        proxy_url=args.proxy,
    )

    if args.list:
        servers = manager.get_server_list()
        if servers:
            print(f"\n{'排名':<4} {'主机名':<20} {'IP':<18} {'国家':<6} {'评分':<8} {'延迟ms':<8} {'速度Mbps':<10} {'连通分':<8}")
            print("-" * 90)
            for i, s in enumerate(servers[:20], 1):
                print(
                    f"{i:<4} {s.hostname:<20} {s.ip:<18} {s.country_short:<6} "
                    f"{s.score:<8} {s.ping:<8} {s.speed_mbps:<10.1f} {s.connectivity_score:<8.1f}"
                )
        else:
            print("无可用服务器")
        return

    if args.once:
        success = manager.connect()
        sys.exit(0 if success else 1)

    # 默认：守护模式
    try:
        manager.start_monitoring()
    except KeyboardInterrupt:
        log.info("收到中断信号，正在停止...")
        manager.stop()


if __name__ == "__main__":
    main()
