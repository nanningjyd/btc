#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Polymarket 采集器（跑在 VPS 上）：连 RTDS WebSocket，10s 聚合推回 Cloudflare D1
# 配置从同目录 config.json 读 put_url / put_token，VPS 迁移无需改本文件
import json, os, time, threading, sys, urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
_cfg = {}
try:
    with open(os.path.join(BASE, "config.json"), encoding="utf-8") as f:
        _cfg = json.load(f)
except Exception:
    pass
PUT_URL = _cfg.get("put_url", "https://btc.hhxx.eu.org/api/put")
PUT_TOKEN = _cfg.get("put_token", "")

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
    while True:
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
    threading.Thread(target=loop, daemon=True).start()
    run_ws()
