#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 一键部署/迁移脚本：把 Polymarket 采集器部署到任意 VPS。
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
SERVICE_NAME = "btc-pm"

SERVICE = """[Unit]
Description=Polymarket price collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 {dir}/collector_pm.py
Restart=always
RestartSec=5
WorkingDirectory={dir}

[Install]
WantedBy=multi-user.target
""".format(dir=REMOTE_DIR)


def ensure_paramiko():
    try:
        import paramiko  # noqa
    except ImportError:
        print("本地缺少 paramiko，正在安装...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "paramiko"], check=False)
        import paramiko  # noqa
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
        if code != 0:
            print("  [warn] %s -> %s" % (cmd.split()[0], (e or o).strip()[:120]))
        return code

    print("1/5 检查 python3 与 pip ...")
    sh("command -v python3 || (apt-get update -y && apt-get install -y python3)")
    sh("python3 -m pip --version 2>/dev/null || (apt-get update -y && apt-get install -y python3-pip)")

    print("2/5 上传采集器与配置 ...")
    sh("mkdir -p %s" % REMOTE_DIR)
    sftp = client.open_sftp()
    sftp.put(os.path.join(BASE, "collector_pm.py"), REMOTE_DIR + "/collector_pm.py")
    with sftp.file(REMOTE_DIR + "/config.json", "w") as f:
        f.write(json.dumps({"put_url": PUT_URL, "put_token": PUT_TOKEN}))
    sftp.close()

    print("3/5 安装 websocket-client ...")
    sh("python3 -m pip install -q websocket-client")

    print("4/5 注册 systemd 服务 %s ..." % SERVICE_NAME)
    sftp = client.open_sftp()
    with sftp.file("/etc/systemd/system/%s.service" % SERVICE_NAME, "w") as f:
        f.write(SERVICE)
    sftp.close()

    print("5/5 启动并设为开机自启 ...")
    sh("systemctl daemon-reload")
    sh("systemctl enable --now %s" % SERVICE_NAME)
    sh("systemctl restart %s" % SERVICE_NAME)
    time.sleep(3)
    _, out, _ = client.exec_command("systemctl is-active %s" % SERVICE_NAME)
    state = out.read().decode().strip()
    print("服务状态:", state)
    client.close()
    print("完成。查看日志: ssh %s@%s 'journalctl -u %s -f'" % (USER, HOST, SERVICE_NAME))


if __name__ == "__main__":
    main()
