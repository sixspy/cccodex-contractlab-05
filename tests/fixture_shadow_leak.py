"""串数复现夹具：影子试跑结果混进正式对账通过数。

旧管道没有影子批次表，影子试跑结果只能直落正式批次表 ingress_batches，
正式对账 /api/reconcile 的通过数因此吃进影子试跑数 —— 本夹具复现这一刻。

用法：python tests/fixture_shadow_leak.py
- 修复前（无影子试跑管道）：用 SQL 模拟旧管道把影子试跑结果直写正式批次表，
  打印串数证据（正式对账通过数吃进影子数）。
- 修复后（影子管道已隔离）：改走 API 试跑，核对正式表零写入、对账零串数；
  正式通过对账吃到影子数即以退出码 1 失败。
"""
import sys
import tempfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import app as app_module
from fastapi.testclient import TestClient


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="shadow-leak-"))
    app_module.DB_PATH = tmp / "leak.db"
    app_module.init_db()
    c = TestClient(app_module.app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}

    for _ in range(2):
        r = c.post("/api/ingress/validate", headers=h,
                   json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
        assert r.json()["result"] == "通过"
    before = c.get("/api/reconcile", headers=h).json()
    print(f"[夹具] 正式通过 2 笔落账，对账通过数 = {before['passed']}")

    probe = c.post("/api/shadow/validate", headers=h,
                   json={"subject": "订单入站", "payload_fields": "order_id,amount,currency,channel"})
    if probe.status_code == 404:
        print("[夹具] 无影子试跑管道（修复前）：模拟旧管道把影子试跑结果直写正式批次表")
        now = datetime.now().isoformat(timespec="seconds")
        with app_module.connect() as conn:
            for _ in range(3):
                conn.execute(
                    "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    ("订单入站", "order_id,amount,currency,channel", "v4-shadow", "通过",
                     "影子试跑直落正式表（未隔离）", now))
        after = c.get("/api/reconcile", headers=h).json()
        leaked = after["passed"] - before["passed"]
        print(f"[夹具] 影子试跑 3 笔后，对账通过数 = {after['passed']}（吃进影子 {leaked} 笔）")
        assert leaked == 3, "未复现串数"
        print("[夹具] 串数复现成功：正式对账通过数混进了影子试跑结果")
        return 0

    print("[夹具] 检测到影子试跑管道（修复后）：改走 API 试跑，核对正式对账零串数")
    assert probe.status_code == 200, probe.text
    for _ in range(2):
        r = c.post("/api/shadow/validate", headers=h,
                   json={"subject": "订单入站", "payload_fields": "order_id,amount,currency,channel"})
        assert r.status_code == 200, r.text
    after = c.get("/api/reconcile", headers=h).json()
    leaked = after["passed"] - before["passed"]
    shadow_n = len(c.get("/api/shadow", headers=h).json()["items"])
    print(f"[夹具] 影子队列 {shadow_n} 笔，对账通过数 = {after['passed']}（串入影子 {leaked} 笔）")
    if leaked != 0 or after["ingress_total"] != before["ingress_total"]:
        print("[夹具] 串数仍存在：正式对账吃到影子数")
        return 1
    print("[夹具] 隔离确认：影子试跑零写入正式表，正式对账未吃到影子数")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
