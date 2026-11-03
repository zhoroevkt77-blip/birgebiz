"""БИЗ БИРГЕБИЗ! — Россиядагы мекендештер үчүн жарыя тактасы.

FastAPI + PostgreSQL. Аккаунт жок: ар бир браузер өзүнүн жашыруун
токенин түзөт, жарыянын ээси ошол токен менен аныкталат (базада анын
хэши гана сакталат). Админ ADMIN_TOKEN аркылуу баарын өчүрө алат.
"""
import hashlib
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field, field_validator

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
SALT = os.environ.get("HASH_SALT", "birgebiz")
STATIC = Path(__file__).parent / "static"

REPORTS_TO_HIDE = 3        # ушунча шикаяттан кийин жарыя жашырылат
MAX_ACTIVE_PER_OWNER = 30  # бир адамга активдүү жарыя
MAX_POSTS_PER_IP_HOUR = 6  # бир IPден саатына
AD_LIFETIME_DAYS = 60      # жарыя ушунча күндөн кийин тизмеден чыгат

CATS = {
    "job", "house", "law", "fin", "cargo", "ride", "ticket", "med",
    "beauty", "food", "edu", "auto", "help", "market",
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
CREATE TABLE IF NOT EXISTS reports (
    ad_id    TEXT NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    ip_hash  TEXT NOT NULL,
    PRIMARY KEY (ad_id, ip_hash)
);
"""

pool: AsyncConnectionPool | None = None


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
    sub: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=5, max_length=80)
    body: str = Field(min_length=10, max_length=1500)
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
    }
    if admin:
        out["reports"] = row["reports"]
        out["hidden"] = row["hidden"]
    return out


@app.get("/healthz")
async def healthz():
    return {"ok": True, "db": pool is not None}


@app.get("/api/ads")
async def list_ads(x_owner_token: str | None = Header(default=None),
                   x_admin_token: str | None = Header(default=None)):
    admin = is_admin(x_admin_token)
    owner_hash = h(x_owner_token) if x_owner_token else None
    async with db().connection() as conn:
        cur = await conn.execute(
            f"""SELECT * FROM ads
                WHERE created_at > now() - interval '{AD_LIFETIME_DAYS} days'
                  AND (NOT hidden OR %s OR owner_hash = %s)
                ORDER BY created_at DESC LIMIT 2000""",
            (admin, owner_hash or ""),
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
        ad_id = "a" + secrets.token_hex(6)
        cur = await conn.execute(
            """INSERT INTO ads (id, owner_hash, ip_hash, cat, sub, title, body, price, cur, per,
                                city, area, name, phone, wa, tg, for_kg)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (ad_id, owner_hash, ip_hash, ad.cat, ad.sub, ad.title, ad.body, ad.price, ad.cur,
             ad.per, ad.city, ad.area, ad.name, ad.phone, ad.wa, ad.tg, ad.forKg))
        row = await cur.fetchone()
    return to_json(row, owner_hash, False)


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


@app.post("/api/ads/{ad_id}/unhide")
async def unhide_ad(ad_id: str, x_admin_token: str | None = Header(default=None)):
    if not is_admin(x_admin_token):
        raise HTTPException(403, "Админ гана.")
    async with db().connection() as conn:
        await conn.execute("UPDATE ads SET hidden=false, reports=0 WHERE id=%s", (ad_id,))
        await conn.execute("DELETE FROM reports WHERE ad_id=%s", (ad_id,))
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
        names = {"title": "Аталышы (5–80 тамга)", "body": "Толук маалымат (10–1500 тамга)",
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
