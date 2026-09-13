# -*- coding: utf-8 -*-
# 构建 dist/worker.js：把 public/ 下的前端资产内联进 src/worker.js 的 __INLINE_ASSETS__ 占位符。
# 部署：deploy.sh（Cloudflare API 直接上传模块 Worker）
import json, os, io

ROOT = os.path.dirname(os.path.abspath(__file__))
PUB = os.path.join(ROOT, "public")
ASSETS = ["index.html", "app.js", "engine.js", "echarts.min.js", "vpngate.html"]

def build():
    import time as _t
    ver = _t.strftime("%Y%m%d%H%M")
    parts = []
    for name in ASSETS:
        p = os.path.join(PUB, name)
        with io.open(p, "r", encoding="utf-8") as f:
            val = f.read()
        # JSON 字符串字面量是合法的 JS 字符串字面量（ensure_ascii 转 \uXXXX，规避编码问题）
        parts.append('  "%s": %s' % (name, json.dumps(val, ensure_ascii=True)))
    obj = "var INLINE_ASSETS = {\n" + ",\n".join(parts) + "\n};"
    with io.open(os.path.join(ROOT, "src", "worker.js"), "r", encoding="utf-8") as f:
        src = f.read()
    stmt = "const INLINE_ASSETS = __INLINE_ASSETS__;"
    if stmt not in src:
        raise SystemExit("placeholder statement not found in src/worker.js")
    src = src.replace(stmt, "const INLINE_ASSETS = {\n" + ",\n".join(parts) + "\n};", 1)
    src = src.replace("__VER__", ver)  # 版本号注入（资源缓存穿透）
    outdir = os.path.join(ROOT, "dist")
    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    out = os.path.join(outdir, "worker.js")
    with io.open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write(src)
    meta = {
        "main_module": "worker.js",
        "compatibility_date": "2026-08-01",
        "keep_bindings": ["secret_text"],
        "bindings": [
            {"type": "d1", "name": "DB", "id": "8e146097-6fce-46fc-9345-3daa9907c7ff"},
            {"type": "plain_text", "name": "SAMPLES_PER_RUN", "text": "6"},
            {"type": "plain_text", "name": "PUT_TOKEN", "text": "pmtk_9f3ac41e7d2b8056"},
        ],
    }
    with io.open(os.path.join(outdir, "metadata.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    print("built", out, os.path.getsize(out), "bytes")

if __name__ == "__main__":
    build()
