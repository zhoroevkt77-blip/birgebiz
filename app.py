"""БИЗ БИРГЕБИЗ! — Россиядагы мекендештер үчүн жарыя тактасы.

FastAPI + PostgreSQL. Аккаунт жок: ар бир браузер өзүнүн жашыруун
токенин түзөт, жарыянын ээси ошол токен менен аныкталат (базада анын
хэши гана сакталат). Админ ADMIN_TOKEN аркылуу баарын өчүрө алат.
"""
import asyncio
import base64
import hashlib
import re
import time
import urllib.request
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field, field_validator

import pricing
from pricing import (FREE_LIMIT, MAX_PHOTO_BYTES, MAX_PHOTOS, MAX_THUMB_BYTES, contact_key,
                     fee_for, parse_image, parse_receipt)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
SALT = os.environ.get("HASH_SALT", "birgebiz")
# Төлөм реквизиттери. Бош болсо — акы алуу өчүк (бардык жарыялар бекер, эсеп жүрө берет).
PAY_INFO = os.environ.get("PAY_INFO", "").strip()
STATIC = Path(__file__).parent / "static"

REPORTS_TO_HIDE = 3        # ушунча «шектүү» кабардан кийин жарыя жашырылат
MAX_ACTIVE_PER_OWNER = 30  # бир адамга активдүү жарыя
MAX_POSTS_PER_IP_HOUR = 6  # бир IPден саатына
AD_LIFETIME_DAYS = 60      # жарыя ушунча күндөн кийин тизмеден чыгат

CATS = {
    "job", "jobseek", "carsale", "house", "law", "cargo", "ride", "ticket", "med",
    "beauty", "food", "edu", "auto", "help", "market", "repair",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS ads (
    id          TEXT PRIMARY KEY,
    owner_hash  TEXT NOT NULL,
    ip_hash     TEXT NOT NULL,
    cat         TEXT NOT NULL,
    sub         TEXT NOT NULL,
    title       TEXT NOT NULL,
    body        TEXT NOT NULL,
    price       BIGINT,
    cur         TEXT NOT NULL DEFAULT 'RUB',
    per         TEXT NOT NULL DEFAULT '',
    city        TEXT NOT NULL,
    area        TEXT NOT NULL DEFAULT '',
    name        TEXT NOT NULL DEFAULT '',
    phone       TEXT NOT NULL DEFAULT '',
    wa          TEXT NOT NULL DEFAULT '',
    tg          TEXT NOT NULL DEFAULT '',
    for_kg      BOOLEAN NOT NULL DEFAULT FALSE,
    reports     INT NOT NULL DEFAULT 0,
    hidden      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ads_created_idx ON ads (created_at DESC);
CREATE INDEX IF NOT EXISTS ads_owner_idx ON ads (owner_hash);
CREATE TABLE IF NOT EXISTS fx_daily (
    day  TEXT NOT NULL,
    cur  TEXT NOT NULL,
    val  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (day, cur)
);
ALTER TABLE ads ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE ads ADD COLUMN IF NOT EXISTS fee INT NOT NULL DEFAULT 0;
ALTER TABLE ads ADD COLUMN IF NOT EXISTS contact_key TEXT NOT NULL DEFAULT '';
ALTER TABLE ads ADD COLUMN IF NOT EXISTS receipt BYTEA;
ALTER TABLE ads ADD COLUMN IF NOT EXISTS receipt_type TEXT;
ALTER TABLE ads ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS ads_status_idx ON ads (status);
ALTER TABLE ads ADD COLUMN IF NOT EXISTS thumb BYTEA;
CREATE TABLE IF NOT EXISTS ad_photos (
    ad_id  TEXT NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    idx    INT NOT NULL,
    data   BYTEA NOT NULL,
    mime   TEXT NOT NULL,
    PRIMARY KEY (ad_id, idx)
);
CREATE TABLE IF NOT EXISTS fx_services (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    rate        DOUBLE PRECISION NOT NULL,
    fee         TEXT NOT NULL DEFAULT '',
    note        TEXT NOT NULL DEFAULT '',
    contact     TEXT NOT NULL DEFAULT '',
    sort        INT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS free_usage (
    contact_key TEXT PRIMARY KEY,
    used        INT NOT NULL DEFAULT 0
);
ALTER TABLE ads ADD COLUMN IF NOT EXISTS views INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS ads_ckey_idx ON ads (contact_key);
CREATE TABLE IF NOT EXISTS blocked (
    contact_key TEXT PRIMARY KEY,
    reason      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS settings (
    k           TEXT PRIMARY KEY,
    v           TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reports (
    ad_id    TEXT NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    ip_hash  TEXT NOT NULL,
    PRIMARY KEY (ad_id, ip_hash)
);
"""

pool: AsyncConnectionPool | None = None

# Тизмеге керектүү мамычалар (сүрөт жана чек сыяктуу оор маалыматсыз)
AD_COLS = ("id, owner_hash, cat, sub, title, body, price, cur, per, city, area, name, phone, wa, tg, "
           "for_kg, reports, hidden, created_at, status, fee")


def h(value: str) -> str:
    return hashlib.sha256((SALT + ":" + value).encode()).hexdigest()


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    return fwd.split(",")[0].strip() or (request.client.host if request.client else "?")


def is_admin(token: str | None) -> bool:
    return bool(ADMIN_TOKEN) and bool(token) and secrets.compare_digest(token, ADMIN_TOKEN)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool
    if not DATABASE_URL:
        print("ЭСКЕРТҮҮ: DATABASE_URL коюлган эмес — сайт ачылат, бирок жарыялар иштебейт.", flush=True)
    else:
        try:
            pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=False,
                                       kwargs={"row_factory": dict_row})
            await pool.open(wait=True, timeout=15)
            async with db().connection() as conn:
                await conn.execute(SCHEMA)
            print("База туташты, таблицалар даяр.", flush=True)
        except Exception as e:  # база жок болсо да сайт ачылсын
            print(f"ЭСКЕРТҮҮ: базага туташкан жок: {e}", flush=True)
            if pool:
                await pool.close()
            pool = None
    yield
    if pool:
        await pool.close()


def db():
    if pool is None:
        raise HTTPException(503, "База туташкан жок. Railway'де DATABASE_URL өзгөрмөсүн текшериңиз.")
    return pool


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)


class AdIn(BaseModel):
    cat: str
    sub: str = Field(min_length=1, max_length=400)
    title: str = Field(min_length=5, max_length=80)
    text: str = Field(min_length=10, max_length=1500)
    price: int | None = Field(default=None, ge=0, le=100_000_000)
    cur: str = "RUB"
    per: str = Field(default="", max_length=30)
    city: str = Field(min_length=2, max_length=40)
    area: str = Field(default="", max_length=60)
    name: str = Field(default="", max_length=40)
    phone: str = Field(default="", max_length=24)
    wa: str = Field(default="", max_length=24)
    tg: str = Field(default="", max_length=40)
    forKg: bool = False
    receipt: str | None = Field(default=None, max_length=1_700_000)
    photos: list[str] = Field(default_factory=list, max_length=MAX_PHOTOS)
    thumb: str | None = Field(default=None, max_length=170_000)

    @field_validator("cat")
    @classmethod
    def _cat(cls, v):
        if v not in CATS:
            raise ValueError("белгисиз категория")
        return v

    @field_validator("cur")
    @classmethod
    def _cur(cls, v):
        return v if v in ("RUB", "KGS") else "RUB"

    @field_validator("*", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v


def to_json(row: dict, owner_hash: str | None, admin: bool) -> dict:
    out = {
        "id": row["id"], "cat": row["cat"], "sub": row["sub"], "title": row["title"],
        "text": row["body"], "price": row["price"], "cur": row["cur"], "per": row["per"],
        "city": row["city"], "area": row["area"], "name": row["name"],
        "phone": row["phone"], "wa": row["wa"], "tg": row["tg"], "forKg": row["for_kg"],
        "created": int(row["created_at"].timestamp() * 1000),
        "mine": owner_hash is not None and row["owner_hash"] == owner_hash,
        "status": row.get("status") or "active",
        "photos": int(row.get("nphotos") or 0),
        "thumb": bool(row.get("has_thumb")),
    }
    if out["mine"] or admin:
        out["fee"] = row.get("fee") or 0
    if admin:
        out["reports"] = row["reports"]
        out["hidden"] = row["hidden"]
    return out


# ── Валюта курсу: КР Улуттук банкы (nbkr.kg), 6 саатта бир жаңырат ──
FX_URL = "https://www.nbkr.kg/XML/daily.xml"
FX_CUR = ("USD", "EUR", "RUB", "KZT", "CNY")
_fx = {"rates": {}, "day": "", "t": 0.0, "try": 0.0}
_fx_lock = asyncio.Lock()


def _fx_download():
    req = urllib.request.Request(FX_URL, headers={"User-Agent": "Mozilla/5.0 (BIZBIRGEBIZ)"})
    x = urllib.request.urlopen(req, timeout=8).read().decode("utf-8", "ignore")
    dm = re.search(r'Date="(\d\d)\.(\d\d)\.(\d{4})"', x)
    day = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}" if dm else time.strftime("%Y-%m-%d")
    got = {}
    for c in FX_CUR:
        m = re.search(r'ISOCode="%s"\s*>\s*<Nominal>\s*(\d+)\s*</Nominal>\s*<Value>\s*([\d.,]+)\s*</Value>' % c, x)
        if m:
            got[c] = float(m.group(2).replace(",", ".")) / max(1, int(m.group(1)))
    return day, got


async def fx_rates():
    now = time.time()
    if _fx["rates"] and now - _fx["t"] < 6 * 3600:
        return _fx
    async with _fx_lock:
        if (_fx["rates"] and time.time() - _fx["t"] < 6 * 3600) or time.time() - _fx["try"] < 600:
            return _fx
        _fx["try"] = time.time()
        try:
            day, got = await asyncio.to_thread(_fx_download)
            if got.get("USD"):
                prev = {}
                if pool is not None:
                    async with pool.connection() as conn:
                        for k, v in got.items():
                            await conn.execute("INSERT INTO fx_daily (day, cur, val) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (day, k, v))
                        cur = await conn.execute(
                            "SELECT DISTINCT ON (cur) cur, val FROM fx_daily WHERE day < %s ORDER BY cur, day DESC", (day,))
                        prev = {row["cur"]: row["val"] for row in await cur.fetchall()}
                _fx.update(day=day, t=time.time(), rates={
                    k: {"v": round(v, 4), "d": round(v - prev[k], 4) if k in prev else None} for k, v in got.items()})
        except Exception as e:
            print("fx:", e, flush=True)
    return _fx


@app.get("/api/rates")
async def rates():
    fx = await fx_rates()
    if not fx["rates"]:
        raise HTTPException(503, "Курс азыр жеткиликсиз.")
    return {"day": fx["day"], "rates": fx["rates"], "source": "КР Улуттук банкы"}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "db": pool is not None}


async def free_used(conn, key: str) -> int:
    if not key:
        return 0
    cur = await conn.execute("SELECT used FROM free_usage WHERE contact_key=%s", (key,))
    row = await cur.fetchone()
    return row["used"] if row else 0


@app.get("/api/quote")
async def quote(cat: str, sub: str = "", phone: str = "", wa: str = "", tg: str = ""):
    """Жарыя берүүнүн акыркы кадамында: бекерби, канча калды, төлөм керекпи."""
    fee = fee_for(cat, sub)
    key = contact_key(phone, wa, tg)
    async with db().connection() as conn:
        used = await free_used(conn, key)
    left = max(0, FREE_LIMIT - used)
    needs = fee > 0 and left == 0 and bool(PAY_INFO)
    return {"fee": fee, "free_left": left, "free_limit": FREE_LIMIT, "pay_enabled": bool(PAY_INFO),
            "needs_payment": needs, "pay_info": PAY_INFO if needs else ""}


@app.get("/api/ads")
async def list_ads(x_owner_token: str | None = Header(default=None),
                   x_admin_token: str | None = Header(default=None)):
    admin = is_admin(x_admin_token)
    owner_hash = h(x_owner_token) if x_owner_token else None
    async with db().connection() as conn:
        cur = await conn.execute(
            f"""SELECT {AD_COLS}, (thumb IS NOT NULL) AS has_thumb,
                       (SELECT count(*) FROM ad_photos p WHERE p.ad_id = ads.id) AS nphotos
                FROM ads
                WHERE created_at > now() - interval '{AD_LIFETIME_DAYS} days'
                  AND ((status = 'active' AND NOT hidden
                        AND contact_key NOT IN (SELECT contact_key FROM blocked))
                       OR owner_hash = %s OR (%s AND status = 'active'))
                ORDER BY created_at DESC LIMIT 2000""",
            (owner_hash or "", admin),
        )
        rows = await cur.fetchall()
    return {"ads": [to_json(r, owner_hash, admin) for r in rows], "admin": admin}


@app.post("/api/ads", status_code=201)
async def create_ad(ad: AdIn, request: Request,
                    x_owner_token: str | None = Header(default=None)):
    if not x_owner_token or len(x_owner_token) < 20:
        raise HTTPException(400, "Барактын жаңыртып, кайра аракет кылыңыз.")
    if not (ad.phone or ad.wa or ad.tg):
        raise HTTPException(422, "Жок дегенде бир байланыш жазыңыз: телефон, WhatsApp же Telegram.")
    owner_hash = h(x_owner_token)
    ip_hash = h(client_ip(request))
    async with db().connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM ads WHERE ip_hash=%s AND created_at > now() - interval '1 hour'",
            (ip_hash,))
        if (await cur.fetchone())["n"] >= MAX_POSTS_PER_IP_HOUR:
            raise HTTPException(429, "Бир саатта өтө көп жарыя бердиңиз. Бир аздан кийин кайра аракет кылыңыз.")
        cur = await conn.execute(
            f"SELECT count(*) AS n FROM ads WHERE owner_hash=%s AND created_at > now() - interval '{AD_LIFETIME_DAYS} days'",
            (owner_hash,))
        if (await cur.fetchone())["n"] >= MAX_ACTIVE_PER_OWNER:
            raise HTTPException(429, f"Бир адамга {MAX_ACTIVE_PER_OWNER} жарыяга чейин. Эскилерин өчүрүңүз.")
        key0 = contact_key(ad.phone, ad.wa, ad.tg)
        cur = await conn.execute(
            """SELECT 1 FROM blocked b WHERE b.contact_key = %s
               OR b.contact_key IN (SELECT contact_key FROM ads WHERE owner_hash = %s) LIMIT 1""",
            (key0, owner_hash))
        if await cur.fetchone():
            raise HTTPException(403, "Сиздин жарыя берүү мүмкүнчүлүгүңүз модератор тарабынан чектелген.")
        try:
            photos = [parse_image(x, MAX_PHOTO_BYTES, "Сүрөт") for x in ad.photos[:MAX_PHOTOS]]
            thumb = parse_image(ad.thumb, MAX_THUMB_BYTES, "Кичине сүрөт")[0] if (ad.thumb and photos) else None
        except ValueError as e:
            raise HTTPException(422, str(e))
        fee = fee_for(ad.cat, ad.sub)
        key = contact_key(ad.phone, ad.wa, ad.tg)
        status, charged, receipt, rtype = "active", 0, None, None
        async with conn.transaction():
            if fee > 0 and key:
                await conn.execute("INSERT INTO free_usage (contact_key, used) VALUES (%s, 0) ON CONFLICT DO NOTHING", (key,))
                cur = await conn.execute("SELECT used FROM free_usage WHERE contact_key=%s FOR UPDATE", (key,))
                used = (await cur.fetchone())["used"]
                if used >= FREE_LIMIT and PAY_INFO:
                    if not ad.receipt:
                        raise HTTPException(402, f"Бекер {FREE_LIMIT} жарыяңыз бүттү. Бул жарыянын баасы {fee} ₽ — төлөп, чекти жүктөңүз.")
                    try:
                        receipt, rtype = parse_receipt(ad.receipt)
                    except ValueError as e:
                        raise HTTPException(422, str(e))
                    status, charged = "pending", fee
                else:
                    await conn.execute("UPDATE free_usage SET used = used + 1 WHERE contact_key=%s", (key,))
            ad_id = "a" + secrets.token_hex(6)
            cur = await conn.execute(
                """INSERT INTO ads (id, owner_hash, ip_hash, cat, sub, title, body, price, cur, per,
                                    city, area, name, phone, wa, tg, for_kg, status, fee, contact_key,
                                    receipt, receipt_type, thumb)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   RETURNING """ + AD_COLS + ", (thumb IS NOT NULL) AS has_thumb",
                (ad_id, owner_hash, ip_hash, ad.cat, ad.sub, ad.title, ad.text, ad.price, ad.cur,
                 ad.per, ad.city, ad.area, ad.name, ad.phone, ad.wa, ad.tg, ad.forKg, status, charged, key,
                 receipt, rtype, thumb))
            row = dict(await cur.fetchone())
            for i, (data, mime) in enumerate(photos):
                await conn.execute("INSERT INTO ad_photos (ad_id, idx, data, mime) VALUES (%s,%s,%s,%s)",
                                   (ad_id, i, data, mime))
            row["nphotos"] = len(photos)
        left = max(0, FREE_LIMIT - await free_used(conn, key)) if key else FREE_LIMIT
    out = to_json(row, owner_hash, False)
    out["free_left"] = left
    return out


@app.delete("/api/ads/{ad_id}")
async def delete_ad(ad_id: str,
                    x_owner_token: str | None = Header(default=None),
                    x_admin_token: str | None = Header(default=None)):
    async with db().connection() as conn:
        if is_admin(x_admin_token):
            cur = await conn.execute("DELETE FROM ads WHERE id=%s", (ad_id,))
        elif x_owner_token:
            cur = await conn.execute("DELETE FROM ads WHERE id=%s AND owner_hash=%s",
                                     (ad_id, h(x_owner_token)))
        else:
            raise HTTPException(403, "Бул жарыяны өчүрүүгө укугуңуз жок.")
        if cur.rowcount == 0:
            raise HTTPException(404, "Жарыя табылган жок же ал сиздики эмес.")
    return {"ok": True}


@app.post("/api/ads/{ad_id}/report")
async def report_ad(ad_id: str, request: Request):
    ip_hash = h(client_ip(request))
    async with db().connection() as conn:
        cur = await conn.execute(
            "INSERT INTO reports (ad_id, ip_hash) SELECT %s, %s WHERE EXISTS (SELECT 1 FROM ads WHERE id=%s) "
            "ON CONFLICT DO NOTHING", (ad_id, ip_hash, ad_id))
        if cur.rowcount:
            await conn.execute(
                "UPDATE ads SET reports = reports + 1, hidden = (reports + 1 >= %s) WHERE id=%s",
                (REPORTS_TO_HIDE, ad_id))
    return {"ok": True}


@app.post("/api/ads/{ad_id}/view")
async def view_ad(ad_id: str):
    async with db().connection() as conn:
        await conn.execute("UPDATE ads SET views = views + 1 WHERE id=%s", (ad_id,))
    return {"ok": True}


@app.get("/api/notice")
async def notice_get():
    async with db().connection() as conn:
        cur = await conn.execute("SELECT v, updated_at FROM settings WHERE k='notice'")
        r = await cur.fetchone()
    if not r:
        return {"text": "", "kind": "info", "id": ""}
    kind, _, text = r["v"].partition("|")
    return {"text": text, "kind": kind or "info", "id": str(int(r["updated_at"].timestamp()))}


@app.post("/api/ads/{ad_id}/unhide")
async def unhide_ad(ad_id: str, x_admin_token: str | None = Header(default=None)):
    if not is_admin(x_admin_token):
        raise HTTPException(403, "Админ гана.")
    async with db().connection() as conn:
        await conn.execute("UPDATE ads SET hidden=false, reports=0 WHERE id=%s", (ad_id,))
        await conn.execute("DELETE FROM reports WHERE ad_id=%s", (ad_id,))
    return {"ok": True}


# ── Жарыянын сүрөттөрү ──
IMG_CACHE = {"Cache-Control": "public, max-age=2592000, immutable"}


@app.get("/api/ads/{ad_id}/thumb")
async def ad_thumb(ad_id: str):
    async with db().connection() as conn:
        cur = await conn.execute("SELECT thumb FROM ads WHERE id=%s AND thumb IS NOT NULL", (ad_id,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Сүрөт жок.")
    return Response(bytes(row["thumb"]), media_type="image/jpeg", headers=IMG_CACHE)


@app.get("/api/ads/{ad_id}/photo/{idx}")
async def ad_photo(ad_id: str, idx: int):
    async with db().connection() as conn:
        cur = await conn.execute("SELECT data, mime FROM ad_photos WHERE ad_id=%s AND idx=%s", (ad_id, idx))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Сүрөт жок.")
    return Response(bytes(row["data"]), media_type=row["mime"], headers=IMG_CACHE)


# ── Которуу сервистеринин курсу (админ толтурат) ──
class FxSvcIn(BaseModel):
    id: str | None = Field(default=None, max_length=20)
    name: str = Field(min_length=2, max_length=40)
    rate: float = Field(gt=0.2, lt=5)          # 1 ₽ = канча сом
    fee: str = Field(default="", max_length=60)
    note: str = Field(default="", max_length=120)
    contact: str = Field(default="", max_length=80)
    sort: int = Field(default=0, ge=-1000, le=1000)

    @field_validator("*", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v


def svc_json(r):
    return {"id": r["id"], "name": r["name"], "rate": r["rate"], "fee": r["fee"], "note": r["note"],
            "contact": r["contact"], "sort": r["sort"], "updated": int(r["updated_at"].timestamp() * 1000)}


@app.get("/api/fxsvc")
async def fxsvc_list():
    async with db().connection() as conn:
        cur = await conn.execute("SELECT * FROM fx_services ORDER BY sort DESC, rate DESC")
        rows = await cur.fetchall()
    return {"items": [svc_json(r) for r in rows]}


@app.post("/api/fxsvc")
async def fxsvc_save(svc: FxSvcIn, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    sid = svc.id or ("s" + secrets.token_hex(4))
    async with db().connection() as conn:
        cur = await conn.execute(
            """INSERT INTO fx_services (id, name, rate, fee, note, contact, sort, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s, now())
               ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, rate=EXCLUDED.rate, fee=EXCLUDED.fee,
                   note=EXCLUDED.note, contact=EXCLUDED.contact, sort=EXCLUDED.sort, updated_at=now()
               RETURNING *""", (sid, svc.name, svc.rate, svc.fee, svc.note, svc.contact, svc.sort))
        row = await cur.fetchone()
    return svc_json(row)


@app.delete("/api/fxsvc/{sid}")
async def fxsvc_delete(sid: str, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        await conn.execute("DELETE FROM fx_services WHERE id=%s", (sid,))
    return {"ok": True}


# ── Админ: төлөмдөрдү текшерүү жана статистика ──
def need_admin(tok):
    if not is_admin(tok):
        raise HTTPException(403, "Админ гана.")


@app.get("/api/admin/pending")
async def admin_pending(x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        cur = await conn.execute(f"""SELECT {AD_COLS}, receipt, receipt_type, (thumb IS NOT NULL) AS has_thumb,
            (SELECT count(*) FROM ad_photos p WHERE p.ad_id = ads.id) AS nphotos
            FROM ads WHERE status='pending' ORDER BY created_at ASC LIMIT 100""")
        rows = await cur.fetchall()
    out = []
    for r in rows:
        j = to_json(r, None, True)
        if r.get("receipt"):
            j["receipt"] = "data:%s;base64,%s" % (r["receipt_type"] or "image/jpeg",
                                                 base64.b64encode(bytes(r["receipt"])).decode())
        out.append(j)
    return {"items": out}


@app.post("/api/ads/{ad_id}/approve")
async def admin_approve(ad_id: str, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        # жарыянын 60 күнү тастыкталган күндөн башталат
        cur = await conn.execute("UPDATE ads SET status='active', reviewed_at=now(), created_at=now() "
                                 "WHERE id=%s AND status='pending'", (ad_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, "Жарыя табылган жок же мурда эле каралган.")
    return {"ok": True}


@app.post("/api/ads/{ad_id}/reject")
async def admin_reject(ad_id: str, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        cur = await conn.execute("UPDATE ads SET status='rejected', reviewed_at=now() "
                                 "WHERE id=%s AND status='pending'", (ad_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, "Жарыя табылган жок же мурда эле каралган.")
    return {"ok": True}


LIVE = f"created_at > now() - interval '{AD_LIFETIME_DAYS} days'"


@app.get("/api/admin/stats")
async def admin_stats(x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        cur = await conn.execute(f"""SELECT
            count(*) AS total,
            count(*) FILTER (WHERE status='active' AND NOT hidden AND {LIVE}) AS active,
            count(*) FILTER (WHERE created_at > now() - interval '1 day') AS today,
            count(*) FILTER (WHERE NOT ({LIVE})) AS expired,
            count(*) FILTER (WHERE status='pending') AS pending,
            count(*) FILTER (WHERE hidden) AS hidden,
            count(*) FILTER (WHERE thumb IS NOT NULL) AS photos,
            coalesce(sum(views), 0) AS views,
            count(DISTINCT contact_key) FILTER (WHERE contact_key <> '') AS users,
            coalesce(sum(fee) FILTER (WHERE status='active' AND fee > 0), 0) AS earned
            FROM ads""")
        st = dict(await cur.fetchone())
        cur = await conn.execute("""SELECT count(*) AS n FROM (SELECT contact_key FROM ads WHERE contact_key <> ''
            GROUP BY contact_key HAVING min(created_at) > now() - interval '1 day') t""")
        st["users_today"] = (await cur.fetchone())["n"]
        cur = await conn.execute("SELECT count(*) AS n FROM blocked")
        st["blocked"] = (await cur.fetchone())["n"]
        cur = await conn.execute(f"""SELECT cat, count(*) AS n FROM ads WHERE status='active' AND NOT hidden
            AND {LIVE} GROUP BY cat ORDER BY n DESC""")
        st["by_cat"] = await cur.fetchall()
        cur = await conn.execute(f"""SELECT city, count(*) AS n FROM ads WHERE status='active' AND NOT hidden
            AND {LIVE} GROUP BY city ORDER BY n DESC LIMIT 15""")
        st["by_city"] = await cur.fetchall()
    return {**st, "pay_enabled": bool(PAY_INFO), "free_limit": FREE_LIMIT}


ADMIN_FILTERS = {
    "all": "TRUE",
    "active": f"status='active' AND NOT hidden AND {LIVE}",
    "today": "created_at > now() - interval '1 day'",
    "expired": f"NOT ({LIVE})",
    "hidden": "hidden",
    "pending": "status='pending'",
    "rejected": "status='rejected'",
    "photos": "thumb IS NOT NULL",
    "blocked": "contact_key IN (SELECT contact_key FROM blocked)",
}


@app.get("/api/admin/ads")
async def admin_ads(f: str = "all", q: str = "", cat: str = "", key: str = "",
                    x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    where, args = [ADMIN_FILTERS.get(f, "TRUE")], []
    if cat:
        where.append("cat = %s"); args.append(cat)
    if key:
        where.append("contact_key = %s"); args.append(key)
    q = q.strip()[:60]
    if q:
        where.append("(title ILIKE %s OR body ILIKE %s OR name ILIKE %s OR phone ILIKE %s OR wa ILIKE %s "
                     "OR tg ILIKE %s OR city ILIKE %s OR area ILIKE %s OR sub ILIKE %s OR id = %s)")
        like = "%" + q.replace("%", "").replace("_", "") + "%"
        args += [like] * 9 + [q]
    async with db().connection() as conn:
        cur = await conn.execute(f"""SELECT {AD_COLS}, contact_key, views, (thumb IS NOT NULL) AS has_thumb,
            (SELECT count(*) FROM ad_photos p WHERE p.ad_id = ads.id) AS nphotos,
            NOT ({LIVE}) AS expired,
            contact_key IN (SELECT contact_key FROM blocked) AS blocked
            FROM ads WHERE {" AND ".join(where)} ORDER BY created_at DESC LIMIT 300""", args)
        rows = await cur.fetchall()
    out = []
    for r in rows:
        j = to_json(r, None, True)
        j.update(key=r["contact_key"], views=r["views"], expired=r["expired"], blocked=r["blocked"])
        out.append(j)
    return {"items": out}


@app.get("/api/admin/users")
async def admin_users(q: str = "", blocked: bool = False,
                      x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    where, args = ["a.contact_key <> ''"], []
    q = q.strip()[:60]
    if q:
        like = "%" + q.replace("%", "").replace("_", "") + "%"
        where.append("(a.name ILIKE %s OR a.phone ILIKE %s OR a.wa ILIKE %s OR a.tg ILIKE %s OR a.contact_key ILIKE %s)")
        args += [like] * 5
    having = "HAVING bool_or(b.contact_key IS NOT NULL)" if blocked else ""
    async with db().connection() as conn:
        cur = await conn.execute(f"""SELECT a.contact_key AS key, count(*) AS n,
            count(*) FILTER (WHERE a.status='active' AND NOT a.hidden AND a.{LIVE}) AS active,
            max(a.created_at) AS last, min(a.created_at) AS first,
            (array_agg(a.name ORDER BY a.created_at DESC))[1] AS name,
            (array_agg(a.phone ORDER BY a.created_at DESC))[1] AS phone,
            (array_agg(a.wa ORDER BY a.created_at DESC))[1] AS wa,
            (array_agg(a.tg ORDER BY a.created_at DESC))[1] AS tg,
            (array_agg(a.city ORDER BY a.created_at DESC))[1] AS city,
            coalesce(sum(a.views), 0) AS views, coalesce(sum(a.reports), 0) AS reports,
            max(coalesce(fu.used, 0)) AS used,
            bool_or(b.contact_key IS NOT NULL) AS blocked, max(b.reason) AS reason
            FROM ads a LEFT JOIN free_usage fu ON fu.contact_key = a.contact_key
                       LEFT JOIN blocked b ON b.contact_key = a.contact_key
            WHERE {" AND ".join(where)} GROUP BY a.contact_key {having}
            ORDER BY max(a.created_at) DESC LIMIT 500""", args)
        rows = await cur.fetchall()
    for r in rows:
        r["last"] = int(r["last"].timestamp() * 1000)
        r["first"] = int(r["first"].timestamp() * 1000)
    return {"items": rows, "free_limit": FREE_LIMIT}


class BlockIn(BaseModel):
    key: str = Field(min_length=3, max_length=80)
    reason: str = Field(default="", max_length=200)


@app.post("/api/admin/block")
async def admin_block(b: BlockIn, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        await conn.execute("""INSERT INTO blocked (contact_key, reason) VALUES (%s, %s)
            ON CONFLICT (contact_key) DO UPDATE SET reason = EXCLUDED.reason""", (b.key.strip(), b.reason.strip()))
    return {"ok": True}


@app.delete("/api/admin/block/{key}")
async def admin_unblock(key: str, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    async with db().connection() as conn:
        await conn.execute("DELETE FROM blocked WHERE contact_key=%s", (key,))
    return {"ok": True}


class NoticeIn(BaseModel):
    text: str = Field(default="", max_length=400)
    kind: str = "info"


@app.post("/api/admin/notice")
async def admin_notice(n: NoticeIn, x_admin_token: str | None = Header(default=None)):
    need_admin(x_admin_token)
    kind = n.kind if n.kind in ("info", "warn", "good") else "info"
    async with db().connection() as conn:
        if n.text.strip():
            await conn.execute("""INSERT INTO settings (k, v, updated_at) VALUES ('notice', %s, now())
                ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = now()""", (kind + "|" + n.text.strip(),))
        else:
            await conn.execute("DELETE FROM settings WHERE k='notice'")
    return {"ok": True}


@app.exception_handler(HTTPException)
async def http_err(_req, exc: HTTPException):
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_err(_req, exc):
    msg = "Форманы текшериңиз."
    try:
        first = exc.errors()[0]
        field = first.get("loc", ["", ""])[-1]
        names = {"title": "Аталышы (5–80 тамга)", "text": "Толук маалымат (10–1500 тамга)",
                 "price": "Баасы", "city": "Шаар", "cat": "Категория", "sub": "Түрү"}
        if field in names:
            msg = f"{names[field]} туура эмес толтурулган."
    except Exception:
        pass
    return JSONResponse({"error": msg}, status_code=422)


app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/manifest.webmanifest")
async def manifest():
    return FileResponse(STATIC / "manifest.webmanifest", media_type="application/manifest+json")


@app.get("/sw.js")
async def sw():
    return FileResponse(STATIC / "sw.js", media_type="text/javascript",
                        headers={"Cache-Control": "no-cache"})


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/admin")
async def admin_page():
    """Админ панель: өзүнчө барак, жалпы колдонуучуларга баскыч көрүнбөйт."""
    return FileResponse(STATIC / "admin.html", headers={"Cache-Control": "no-cache", "X-Robots-Tag": "noindex"})
