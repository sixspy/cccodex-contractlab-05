"""影子/正式隔离测试。

口径：隔离册登记甲路径 —— 影子试跑只写影子批次表 shadow_batches，
正式表（ingress_batches / buffers）零写入；正式对账只认正式批次表。
偏离隔离册一律拒绝并记「影子偏了」；影子不得计入正式通过，不得改正式启用。
"""
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app as app_module
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _client(username="admin", password="123456"):
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": username, "password": password}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _formal_pass(c, h, fields="order_id,amount,currency"):
    return c.post("/api/ingress/validate", headers=h,
                  json={"subject": "订单入站", "payload_fields": fields})


def _shadow_run(c, h, fields="order_id,amount,currency,channel"):
    return c.post("/api/shadow/validate", headers=h,
                  json={"subject": "订单入站", "payload_fields": fields})


def test_shadow_run_zero_formal_writes():
    """甲路径：影子试跑只写影子批次表，正式表零写入。"""
    c, h = _client()
    r = _shadow_run(c, h)
    assert r.status_code == 200 and r.json()["result"] == "通过"
    assert r.json()["track"] == "影子"
    sh = c.get("/api/shadow", headers=h).json()
    assert len(sh["items"]) == 1
    assert sh["items"][0]["contract_version"] == "v4-shadow"
    # 正式表零写入
    assert c.get("/api/ingress", headers=h).json()["items"] == []
    with app_module.connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM shadow_batches").fetchone()["c"] == 1


def test_formal_reconcile_excludes_shadow_counts():
    """隔离失败测：正式对账吃到影子数即失败。"""
    c, h = _client()
    _formal_pass(c, h)
    _formal_pass(c, h)
    for _ in range(3):
        assert _shadow_run(c, h).json()["result"] == "通过"
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["ingress_total"] == 2
    assert recon["passed"] == 2  # 吃进影子数则此处失败
    assert len(c.get("/api/shadow", headers=h).json()["items"]) == 3


def test_shadow_missing_fields_stays_out_of_buffer():
    """影子缺字段只记影子队列，不进双写缓冲、不动正式账。"""
    c, h = _client()
    r = _shadow_run(c, h, fields="order_id,amount")
    assert r.json()["result"] == "缺字段"
    assert set(r.json()["missing"]) == {"currency", "channel"}
    assert c.get("/api/buffers", headers=h).json()["items"] == []
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["ingress_total"] == 0 and recon["buffer_rows"] == 0


def test_shadow_contract_cannot_enable():
    """禁止影子改正式启用：拒绝并记影子偏了，正式启用保持不动。"""
    c, h = _client()
    contracts = c.get("/api/contracts", headers=h).json()["items"]
    shadow = next(x for x in contracts if x["track"] == "影子")
    r = c.post(f"/api/contracts/{shadow['id']}/enable", headers=h)
    assert r.status_code == 409
    after = c.get("/api/contracts", headers=h).json()["items"]
    assert next(x for x in after if x["version"] == "v3")["status"] == "启用"
    assert next(x for x in after if x["track"] == "影子")["status"] == "试跑"
    devs = c.get("/api/isolation", headers=h).json()["deviations"]
    assert any(d["kind"] == "影子偏了" for d in devs)


def test_formal_contract_enable_still_works():
    """对照：正式契约可整版切换启用，影子轨不受牵连。"""
    c, h = _client()
    contracts = c.get("/api/contracts", headers=h).json()["items"]
    v2 = next(x for x in contracts if x["version"] == "v2")
    r = c.post(f"/api/contracts/{v2['id']}/enable", headers=h)
    assert r.status_code == 200
    after = c.get("/api/contracts", headers=h).json()["items"]
    assert next(x for x in after if x["version"] == "v2")["status"] == "启用"
    assert next(x for x in after if x["track"] == "影子")["status"] == "试跑"


def test_shadow_track_rejected_at_formal_ingress():
    """偏离隔离册：影子轨混走正式入站 → 拒绝并记影子偏了。"""
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency,channel",
                     "track": "影子"})
    assert r.status_code == 409
    assert "影子偏了" in r.json()["detail"]
    assert c.get("/api/ingress", headers=h).json()["items"] == []
    devs = c.get("/api/isolation", headers=h).json()["deviations"]
    assert any(d["kind"] == "影子偏了" for d in devs)


def test_isolation_register_locked_same_day():
    """隔离册选定甲路径后当日禁止再换。"""
    c, h = _client()
    reg = c.get("/api/isolation", headers=h).json()["register"]
    assert reg["path"] == "甲"
    r = c.post("/api/isolation/path", headers=h, json={"path": "乙"})
    assert r.status_code == 409
    reg2 = c.get("/api/isolation", headers=h).json()["register"]
    assert reg2["path"] == "甲"
    devs = c.get("/api/isolation", headers=h).json()["deviations"]
    assert any(d["kind"] == "影子偏了" for d in devs)


def test_views_do_not_cross_contaminate():
    """影子队列、正式列表、对账账册、策略附页、目录页各认各的，禁止串数。"""
    c, h = _client()
    _formal_pass(c, h)
    _shadow_run(c, h)
    _shadow_run(c, h)
    formal = c.get("/api/ingress", headers=h).json()["items"]
    shadow = c.get("/api/shadow", headers=h).json()["items"]
    assert len(formal) == 1 and all(x["contract_version"] == "v3" for x in formal)
    assert len(shadow) == 2 and all(x["contract_version"] == "v4-shadow" for x in shadow)
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["ingress_total"] == 1 and recon["passed"] == 1
    iso = c.get("/api/isolation", headers=h).json()
    assert iso["register"]["path"] == "甲"
    catalog = c.get("/api/contracts", headers=h).json()["items"]
    assert {x["track"] for x in catalog} == {"正式", "影子"}


def test_concurrent_dual_submit_separate_tracks(monkeypatch):
    """影子轨与正式轨双人同时抢提交：分轨排队，后到只记影子争用。"""
    c1, h1 = _client("admin", "123456")    # 值班甲：正式轨
    c2, h2 = _client("viewer", "view123")  # 值班乙：影子轨
    hold = threading.Event()
    entered = threading.Event()
    monkeypatch.setattr(app_module, "SUBMIT_HOLD", hold)
    monkeypatch.setattr(app_module, "SUBMIT_ENTERED", entered)
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            formal_fut = ex.submit(_formal_pass, c1, h1)
            assert entered.wait(timeout=5), "正式提交未进入临界区"
            shadow_fut = ex.submit(_shadow_run, c2, h2)
            rs = shadow_fut.result(timeout=5)  # 影子轨不等正式轨：分轨排队
            assert rs.status_code == 200 and rs.json()["result"] == "通过"
            assert rs.json()["contention"] is True   # 后到者（影子）记争用
            hold.set()                               # 放行正式轨
            rf = formal_fut.result(timeout=5)
            assert rf.status_code == 200 and rf.json()["result"] == "通过"
            assert rf.json()["contention"] is False  # 先到者不记争用
    finally:
        hold.set()
        with app_module._inflight_lock:
            app_module._inflight.clear()
    # 只记一条影子争用，后到为影子轨
    sh = c1.get("/api/shadow", headers=h1).json()
    assert len(sh["contentions"]) == 1
    assert sh["contentions"][0]["detail"] == "影子争用"
    assert sh["contentions"][0]["late_track"] == "影子"
    # 各轨各账，互不串数
    assert len(sh["items"]) == 1 and sh["items"][0]["contract_version"] == "v4-shadow"
    formal_items = c1.get("/api/ingress", headers=h1).json()["items"]
    assert len(formal_items) == 1 and formal_items[0]["contract_version"] == "v3"
    recon = c1.get("/api/reconcile", headers=h1).json()
    assert recon["passed"] == 1 and recon["ingress_total"] == 1


def test_shadow_page_is_fifth_tab():
    """影子页序戊：前端导航第 5 位为影子页。"""
    html = (Path(__file__).resolve().parents[1] / "frontend" / "index.html").read_text(encoding="utf-8")
    m = re.search(r"const nav=\[([^\]]+)\]", html)
    assert m, "未找到前端导航目录"
    tabs = [x.strip().strip("'\"") for x in m.group(1).split(",")]
    assert tabs[4] == "影子试跑", f"第 5 页（戊）应为影子试跑，实为 {tabs[4]!r}"
