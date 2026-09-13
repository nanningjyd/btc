#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Polymarket 采集器（跑在 VPS 上）：连 RTDS WebSocket，10s 聚合推回 Cloudflare D1
# 配置从同目录 config.json 读 put_url / put_token，VPS 迁移无需改本文件
# 新增：VPN 连通性检查 — 若 VPN 断线则等待重连，不丢弃已有数据
import json, os, time, threading, sys, urllib.request, socket
BASE = os.path.dirname(os.path.abspath(__file__))
_cfg = {}
try:
    with open(os.path.join(BASE, "config.json"), encoding="utf-8") as f:
        _cfg = json.load(f)
except Exception:
    pass
PUT_URL = _cfg.get("put_url", "https://btc.hhxx.eu.org/api/put")
PUT_TOKEN = _cfg.get("put_token", "")
# VPNGate 配置（来自 config.json，可选）
VPN_ENABLED = _cfg.get("vpn_enabled", True)
VPN_CHECK_INTERVAL = _cfg.get("vpn_check_interval", 30)  # 秒
VPN_RECONNECT_WAIT = _cfg.get("vpn_reconnect_wait", 10)  # 秒

# 依赖：websocket-client（纯 Python，pip 装一次）
try:
    import websocket
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "websocket-client"], check=False)
    import websocket

PM_MAP = {"btc/usd": "btc", "eth/usd": "eth", "sol/usd": "sol", "xrp/usd": "xrp", "bnb/usd": "bnb", "doge/usd": "doge"}
PM_BINANCE_MAP = {"btcusdt": "btc", "ethusdt": "eth", "solusdt": "sol", "xrpusdt": "xrp", "bnbusdt": "bnb", "dogeusdt": "doge"}
IDX = {"btc": 0, "eth": 1, "sol": 2, "bnb": 3, "doge": 4, "xrp": 5}
latest = {}
lock = threading.Lock()

# VPN 状态跟踪
vpn_connected = False
vpn_check_thread = None
vpn_stop_event = threading.Event()


def check_vpn_connectivity() -> bool:
    """检查 VPN 是否可用（TCP 连接测试）"""
    if not VPN_ENABLED:
        return True  # 未启用 VPN，直接返回可用
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(8)
        result = sock.connect_ex(("ws-live-data.polymarket.com", 443))
        sock.close()
        if result == 0:
            return True
        return False
    except Exception:
        return False


def vpn_monitor_loop():
    """后台监控线程：定期检查 VPN 连通性"""
    global vpn_connected
    while not vpn_stop_event.is_set():
        try:
            prev_state = vpn_connected
            vpn_connected = check_vpn_connectivity()
            if prev_state != vpn_connected:
                if vpn_connected:
                    print(f"[VPN] 连通性恢复，Polymarket 可达")
                else:
                    print(f"[VPN] 连通性丢失，等待重连...")
        except Exception as e:
            print(f"[VPN] 检查异常: {e}")
        vpn_stop_event.wait(VPN_CHECK_INTERVAL)


def start_vpn_monitor():
    """启动 VPN 监控线程"""
    global vpn_check_thread, vpn_stop_event
    if VPN_ENABLED:
        vpn_stop_event.clear()
        vpn_check_thread = threading.Thread(target=vpn_monitor_loop, daemon=True)
        vpn_check_thread.start()
        print(f"[VPN] 连通性监控已启动，间隔 {VPN_CHECK_INTERVAL}s")
    else:
        vpn_connected = True  # 未启用 VPN，默认认为可用
        print("[VPN] 未启用 VPN 模式")


def stop_vpn_monitor():
    """停止 VPN 监控线程"""
    global vpn_check_thread
    if vpn_check_thread and VPN_ENABLED:
        vpn_stop_event.set()
        vpn_check_thread.join(timeout=5)
        vpn_check_thread = None


def wait_for_vpn(timeout: int = 300):
    """等待 VPN 连通性恢复，最多等待 timeout 秒"""
    if not VPN_ENABLED:
        return True
    print(f"[VPN] 等待 Polymarket 可达...（最多 {timeout}s）")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check_vpn_connectivity():
            print("[VPN] Polymarket 已可达，继续采集")
            return True
        time.sleep(5)
        print(".", end="", flush=True)
    print("\n[VPN] 等待超时，Polymarket 仍不可达")
    return False


def on_open(ws):
    print("Polymarket RTDS connected")
    ws.send(json.dumps({
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices_chainlink", "type": "update", "filters": "btc/usd,eth/usd,sol/usd,xrp/usd,bnb/usd,doge/usd"},
            {"topic": "crypto_prices", "type": "update", "filters": "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt"},
        ],
    }))


def on_message(ws, message):
    if not message or message == "PONG":
        return
    try:
        msg = json.loads(message)
    except Exception:
        return
    topic = msg.get("topic", "")
    items = []
    if msg.get("payload"):
        p = msg["payload"]
        items = p if isinstance(p, list) else [p]
    elif msg.get("symbol"):
        items = [msg]
    with lock:
        for it in items:
            sym = str(it.get("symbol", "")).lower()
            v = it.get("value", it.get("price"))
            if v is None:
                continue
            try:
                v = float(v)
            except Exception:
                continue
            c = PM_BINANCE_MAP.get(sym) if topic == "crypto_prices" else PM_MAP.get(sym)
            if c:
                ts = it.get("timestamp")
                if ts is None:
                    ts = time.time() * 1000
                latest[c] = (v, float(ts))


def on_error(ws, error):
    print("ws error:", error)


def heartbeat(ws):
    while True:
        time.sleep(5)
        try:
            ws.send("PING")
        except Exception:
            break


def run_ws():
    """WebSocket 主循环：每次连接前检查 VPN 连通性"""
    while True:
        # 检查 VPN 是否可用
        if not check_vpn_connectivity():
            print("[VPN] Polymarket 不可达，等待重连...")
            if not wait_for_vpn(timeout=300):
                print("[VPN] 重连超时，5 秒后重试")
                time.sleep(5)
                continue
        try:
            ws = websocket.WebSocketApp(
                "wss://ws-live-data.polymarket.com",
                on_open=on_open, on_message=on_message, on_error=on_error)
            threading.Thread(target=heartbeat, args=(ws,), daemon=True).start()
            ws.run_forever()
        except Exception as e:
            print("ws crash:", e)
        print("reconnect in 5s")
        time.sleep(5)


def bucket_start(ts):
    return int(ts // 10000) * 10000


def push(bars):
    data = json.dumps({"token": PUT_TOKEN, "bars": bars}).encode("utf-8")
    req = urllib.request.Request(PUT_URL, data=data, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=15)
        return True
    except Exception as e:
        print("push fail:", e)
        return False


def loop():
    buf = []
    while True:
        time.sleep(10)
        ts = bucket_start(time.time() * 1000) - 10000
        row = [None] * 6
        has = False
        now = time.time() * 1000
        with lock:
            for coin, (v, t) in list(latest.items()):
                if now - t < 20000:
                    row[IDX[coin]] = v
                    has = True
        if has:
            buf.append({"source": "polymarket", "ts": ts, "row": row})
        if buf:
            bars = buf[:60]
            if push(bars):
                buf = buf[len(bars):]
                print("pushed", len(bars), "bars")
            elif len(buf) > 500:
                buf = buf[-300:]


if __name__ == "__main__":
    print("Polymarket collector start ->", PUT_URL)
    print("VPN enabled:", VPN_ENABLED)
    # 启动 VPN 监控
    start_vpn_monitor()
    try:
        threading.Thread(target=loop, daemon=True).start()
        run_ws()
    finally:
        stop_vpn_monitor()
