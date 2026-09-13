#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 一键部署/迁移脚本：把 Polymarket 采集器 + VPNGate VPN 管理器部署到任意 VPS。
# 迁移只需改同目录 vps_config.json 的 vps_host/vps_user/vps_password/vps_port，再运行本脚本。
import json, os, sys, time
BASE = os.path.dirname(os.path.abspath(__file__))
cfg = json.load(open(os.path.join(BASE, "vps_config.json"), encoding="utf-8"))
HOST = cfg["vps_host"]
USER = cfg["vps_user"]
PWD = cfg["vps_password"]
PORT = int(cfg.get("vps_port", 22))
PUT_URL = cfg.get("put_url", "https://btc.hhxx.eu.org/api/put")
PUT_TOKEN = cfg.get("put_token", "")
REMOTE_DIR = "/opt/btc-pm"
COLLECTOR_SERVICE = "btc-pm"
VPNGATE_SERVICE = "btc-vpngate"
SERVICE_COLLECTOR = """[Unit]
Description=Polymarket price collector with VPN
After=network-online.target btc-vpngate.service
Wants=network-online.target btc-vpngate.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 {dir}/collector_pm.py
Restart=always
RestartSec=10
WorkingDirectory={dir}
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
""".format(dir=REMOTE_DIR)
SERVICE_VPNGATE = """[Unit]
Description=VPNGate VPN auto-manager for Polymarket
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 {dir}/vpngate_manager.py --once
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=30
WorkingDirectory={dir}
StandardOutput=journal
StandardError=journal
# 开机不自动启动 VPN（先等管理员确认网络正常）
# StartLimitBurst=5
# StartLimitIntervalSec=60

[Install]
WantedBy=multi-user.target
""".format(dir=REMOTE_DIR)


def ensure_paramiko():
    try:
        import paramiko
    except ImportError:
        print("本地缺少 paramiko，正在安装...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "paramiko"], check=False)
        import paramiko
    return paramiko


def main():
    paramiko = ensure_paramiko()
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("连接 %s@%s:%s ..." % (USER, HOST, PORT))
    client.connect(HOST, port=PORT, username=USER, password=PWD,
                   timeout=20, allow_agent=False, look_for_keys=False)

    def sh(cmd):
        _, out, err = client.exec_command(cmd)
        o = out.read().decode(errors="replace")
        e = err.read().decode(errors="replace")
        code = out.channel.recv_exit_status()
        if code != 0 and "No such file" not in e and "not found" not in e.lower():
            print("  [warn] %s -> %s" % (cmd.split()[0], (e or o).strip()[:120]))
        return code, o, e

    # ---- 1/6 检查依赖 ----
    print("1/6 检查 Python3 与 pip ...")
    sh("command -v python3 || (apt-get update -y && apt-get install -y python3)")
    sh("python3 -m pip --version 2>/dev/null || (apt-get update -y && apt-get install -y python3-pip)")

    # ---- 2/6 安装 OpenVPN ----
    print("2/6 安装 OpenVPN ...")
    code, o, e = sh("dpkg -l openvpn 2>/dev/null | grep ^ii || echo 'NOT_INSTALLED'")
    if "NOT_INSTALLED" in o:
        sh("apt-get update -y && apt-get install -y openvpn")
        print("  OpenVPN 已安装")
    else:
        print("  OpenVPN 已存在，跳过")

    # ---- 3/6 上传文件 ----
    print("3/6 上传采集器与 VPN 管理器 ...")
    sh("mkdir -p %s" % REMOTE_DIR)
    sftp = client.open_sftp()
    sftp.put(os.path.join(BASE, "collector_pm.py"), REMOTE_DIR + "/collector_pm.py")
    sftp.put(os.path.join(BASE, "vpngate_manager.py"), REMOTE_DIR + "/vpngate_manager.py")

    # 上传 config.json（合并 put_url/put_token 与 VPN 配置）
    remote_cfg = {
        "put_url": PUT_URL,
        "put_token": PUT_TOKEN,
        "vpn_enabled": True,
        "vpn_check_interval": 30,
        "vpn_reconnect_wait": 10,
        "vpn_country_filter": "",
        "vpn_min_score": 50.0,
        "vpn_max_ping": 300,
    }
    with sftp.file(REMOTE_DIR + "/config.json", "w") as f:
        f.write(json.dumps(remote_cfg, indent=2, ensure_ascii=False))
    sftp.close()

    # ---- 4/6 安装 Python 依赖 ----
    print("4/6 安装 Python 依赖 ...")
    sh("python3 -m pip install -q websocket-client")

    # ---- 5/6 注册 systemd 服务 ----
    print("5/6 注册 systemd 服务 (%s, %s) ..." % (COLLECTOR_SERVICE, VPNGATE_SERVICE))
    sftp = client.open_sftp()
    with sftp.file("/etc/systemd/system/%s.service" % VPNGATE_SERVICE, "w") as f:
        f.write(SERVICE_VPNGATE)
    with sftp.file("/etc/systemd/system/%s.service" % COLLECTOR_SERVICE, "w") as f:
        f.write(SERVICE_COLLECTOR)
    sftp.close()

    # ---- 6/6 启动服务 ----
    print("6/6 启动服务 ...")
    sh("systemctl daemon-reload")
    # 先启动 VPN 管理器（一次性连接）
    sh("systemctl enable --now %s" % VPNGATE_SERVICE)
    time.sleep(8)  # 等待 VPN 连接建立
    # 再启动采集器
    sh("systemctl enable --now %s" % COLLECTOR_SERVICE)
    time.sleep(3)

    _, out, _ = client.exec_command("systemctl is-active %s" % COLLECTOR_SERVICE)
    state = out.read().decode().strip()
    print("采集器状态:", state)

    _, out, _ = client.exec_command("systemctl is-active %s" % VPNGATE_SERVICE)
    state2 = out.read().decode().strip()
    print("VPN 管理器状态:", state2)

    # 检查 VPN 连通性
    _, out, _ = client.exec_command(
        "python3 -c \"import socket; s=socket.socket(); s.settimeout(8); "
        "r=s.connect_ex(('ws-live-data.polymarket.com',443)); s.close(); print('OK' if r==0 else 'FAIL')\"")
    vpn_result = out.read().decode().strip()
    print("Polymarket 连通性:", vpn_result)

    client.close()
    print("\n完成。查看日志:")
    print("  采集器: ssh %s@%s 'journalctl -u %s -f'" % (USER, HOST, COLLECTOR_SERVICE))
    print("  VPN:    ssh %s@%s 'journalctl -u %s -f'" % (USER, HOST, VPNGATE_SERVICE))


if __name__ == "__main__":
    main()
