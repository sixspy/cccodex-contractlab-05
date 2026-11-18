
import hashlib, secrets, sqlite3, threading
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import date, datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ---- 影子隔离（隔离册登记甲路径：影子只写影子批次表，正式表零写入） ----
ISOLATION_PATH = "甲"
ISOLATION_NOTE = "甲路径：影子试跑只写影子批次表 shadow_batches，正式表零写入；正式对账只认正式批次表，不计影子。"

_inflight_lock = threading.Lock()
_inflight: dict[str, set[str]] = {}
_track_locks = {"正式": threading.Lock(), "影子": threading.Lock()}
SUBMIT_HOLD = None     # 测试钩子：首到提交线程进入临界区后等待放行
SUBMIT_ENTERED = None  # 测试钩子：提交线程已进入临界区

def _gate_enter(subject: str, track: str) -> bool:
    """分轨排队：登记本轨在途；他轨已在途则本提交为后到，须记影子争用。"""
    with _inflight_lock:
        others = _inflight.get(subject, set()) - {track}
        contention = bool(others)
        first = not _inflight.get(subject)
        _inflight.setdefault(subject, set()).add(track)
    if SUBMIT_ENTERED is not None:
        SUBMIT_ENTERED.set()
    if first and SUBMIT_HOLD is not None:
        SUBMIT_HOLD.wait(timeout=5)
    return contention

def _gate_exit(subject: str, track: str) -> None:
    with _inflight_lock:
        tracks = _inflight.get(subject)
        if tracks is not None:
            tracks.discard(track)
            if not tracks:
                del _inflight[subject]

def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _record_deviation(conn, detail: str, now: str) -> None:
    conn.execute("INSERT INTO deviations(kind,detail,created_at) VALUES (?,?,?)",
                 ("影子偏了", detail, now))

def _record_contention(conn, subject: str, late_track: str, now: str) -> None:
    conn.execute("INSERT INTO contentions(subject,late_track,detail,created_at) VALUES (?,?,?,?)",
                 (subject, late_track, "影子争用", now))

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shadow_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS isolation_register(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          path TEXT NOT NULL,
          decided_on TEXT NOT NULL,
          note TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contentions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          late_track TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))
        # 隔离路径选定即写进影子隔离册，当日禁止再换
        if conn.execute("SELECT COUNT(*) c FROM isolation_register").fetchone()["c"] == 0:
            conn.execute("INSERT INTO isolation_register(path,decided_on,note) VALUES (?,?,?)",
                         (ISOLATION_PATH, date.today().isoformat(), ISOLATION_NOTE))

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present
    track: str = "正式"  # 隔离册甲路径：正式入站只受理正式轨

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    if body.track != "正式":
        # 偏离影子隔离册：拒绝并记影子偏了
        with connect() as c:
            _record_deviation(c, f"正式入站拒收{body.track}轨提交：主题={body.subject}（隔离册{ISOLATION_PATH}路径，影子不得落正式表）", _now())
        raise HTTPException(409, "偏离影子隔离册：影子试跑不得走正式入站，已记影子偏了")
    contention = _gate_enter(body.subject, "正式")
    try:
        with _track_locks["正式"]:
            fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
            with connect() as c:
                ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
                if not ctr:
                    raise HTTPException(404, "无启用正式契约")
                if contention:
                    _record_contention(c, body.subject, "正式", _now())
                need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
                missing = sorted(need - fields)
                now = _now()
                if missing:
                    if ctr["mode"] == "兼容":
                        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                        (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                        return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid, "track": "正式", "contention": contention}
                    cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                    (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
                    return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid, "track": "正式", "contention": contention}
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
                return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid, "track": "正式", "contention": contention}
    finally:
        _gate_exit(body.subject, "正式")

class ShadowIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/shadow/validate")
def shadow_validate(body: ShadowIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    contention = _gate_enter(body.subject, "影子")
    try:
        with _track_locks["影子"]:
            fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
            with connect() as c:
                ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='影子' AND status='试跑' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
                if not ctr:
                    raise HTTPException(404, "无试跑影子契约")
                need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
                missing = sorted(need - fields)
                result = "通过" if not missing else "缺字段"
                detail = "字段齐全" if not missing else "缺字段:" + ",".join(missing)
                # 甲路径：只写影子批次表；正式表零写入，不碰 contracts/ingress_batches/buffers
                cur = c.execute("INSERT INTO shadow_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], result, detail, _now()))
                if contention:
                    _record_contention(c, body.subject, "影子", _now())
                return {"result": result, "version": ctr["version"], "track": "影子", "missing": missing, "shadow_id": cur.lastrowid, "contention": contention}
    finally:
        _gate_exit(body.subject, "影子")

@app.get("/api/shadow")
def shadow_queue(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        items = [dict(r) for r in c.execute("SELECT * FROM shadow_batches ORDER BY id DESC").fetchall()]
        fights = [dict(r) for r in c.execute("SELECT * FROM contentions ORDER BY id DESC").fetchall()]
    return {"items": items, "contentions": fights}

@app.get("/api/isolation")
def isolation(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        reg = c.execute("SELECT * FROM isolation_register ORDER BY id DESC LIMIT 1").fetchone()
        devs = [dict(r) for r in c.execute("SELECT * FROM deviations ORDER BY id DESC").fetchall()]
    return {"register": dict(reg) if reg else None, "deviations": devs}

class PathSwitch(BaseModel):
    path: str

@app.post("/api/isolation/path")
def switch_path(body: PathSwitch, authorization: str | None = Header(default=None)):
    require_user(authorization)
    today = date.today().isoformat()
    with connect() as c:
        reg = c.execute("SELECT * FROM isolation_register ORDER BY id DESC LIMIT 1").fetchone()
        if reg and reg["decided_on"] == today:
            _record_deviation(c, f"当日禁止再换隔离路径：已登记{reg['path']}路径，拒绝改走{body.path}路径", _now())
            c.commit()
            raise HTTPException(409, "影子隔离册当日禁止再换，已记影子偏了")
        c.execute("INSERT INTO isolation_register(path,decided_on,note) VALUES (?,?,?)",
                  (body.path, today, f"改走{body.path}路径"))
    return {"ok": True, "path": body.path}

@app.post("/api/contracts/{contract_id}/enable")
def enable_contract(contract_id: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE id=?", (contract_id,)).fetchone()
        if not ctr:
            raise HTTPException(404, "契约不存在")
        if ctr["track"] != "正式":
            # 禁止影子改正式启用：拒绝并记影子偏了
            _record_deviation(c, f"禁止影子改正式启用：契约{ctr['version']}为{ctr['track']}轨", _now())
            c.commit()
            raise HTTPException(409, "禁止影子改正式启用，已记影子偏了")
        c.execute("UPDATE contracts SET status='停用' WHERE subject=? AND track='正式' AND status='启用'", (ctr["subject"],))
        c.execute("UPDATE contracts SET status='启用' WHERE id=?", (contract_id,))
    return {"ok": True, "version": ctr["version"], "status": "启用"}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    # 甲路径写隔离：ingress_batches 只落正式轨（影子零写入），对账天然只含正式，不靠过滤凑数
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    return {"ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected, "buffer_rows": buf_rows, "aligned": buffered == buf_rows}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
