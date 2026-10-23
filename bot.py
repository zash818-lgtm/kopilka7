"""Голосовой бот учёта трат + Telegram Mini App с календарём.

Голосовое → Groq Whisper (текст) → Groq LLM (название, сумма, категория) → SQLite.
Календарь открывается кнопкой в боте (Mini App), его отдаёт этот же сервер.
"""
import asyncio
import hashlib
import hmac
import html
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Message,
    WebAppInfo,
)
from dotenv import load_dotenv

load_dotenv()

# ───────────── Настройки ─────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
WEBAPP_URL = (os.getenv("WEBAPP_URL") or (f"https://{_domain}" if _domain else "")).rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "data/expenses.db")
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Tashkent"))
STT_MODEL = os.getenv("STT_MODEL", "whisper-large-v3-turbo")
STT_LANGUAGE = os.getenv("STT_LANGUAGE", "ru")  # пусто = автоопределение (если говоришь по-узбекски)
PARSE_MODEL = os.getenv("PARSE_MODEL", "openai/gpt-oss-20b")
ALLOWED_USERS = {int(x) for x in os.getenv("ALLOWED_USERS", "").replace(" ", "").split(",") if x}
GROQ_URL = "https://api.groq.com/openai/v1"
INDEX_HTML = Path(__file__).parent / "webapp" / "index.html"

CATEGORIES = {
    "Продукты": "🛒",
    "Кафе": "🍽",
    "Транспорт": "🚕",
    "Красота": "💇",
    "Здоровье": "💊",
    "Одежда": "👕",
    "Дом": "🏠",
    "Связь": "📱",
    "Развлечения": "🎮",
    "Подарки": "🎁",
    "Образование": "📚",
    "Другое": "📦",
}

log = logging.getLogger("expenses")
http: aiohttp.ClientSession | None = None
pending_edits: dict[int, int] = {}  # user_id → id траты, которую сейчас меняем


def today() -> date:
    return datetime.now(TZ).date()


def fmt(n: int) -> str:
    return f"{int(n):,}".replace(",", " ")


def allowed(uid: int) -> bool:
    return not ALLOWED_USERS or uid in ALLOWED_USERS


# ───────────── База ─────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS expenses(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    amount INTEGER NOT NULL,
    category TEXT NOT NULL,
    spent_on TEXT NOT NULL,
    created_at TEXT NOT NULL,
    raw_text TEXT
);
CREATE INDEX IF NOT EXISTS idx_user_day ON expenses(user_id, spent_on);
"""
COLS = "id, title, amount, category, spent_on"


def _row(r) -> dict:
    return {"id": r[0], "title": r[1], "amount": r[2], "category": r[3], "date": r[4]}


async def db_init():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def db_add(uid, title, amount, category, day: str, raw=None) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO expenses(user_id,title,amount,category,spent_on,created_at,raw_text) VALUES(?,?,?,?,?,?,?)",
            (uid, title, int(amount), category, day, datetime.now(TZ).isoformat(), raw),
        )
        await db.commit()
        return cur.lastrowid


async def db_get(uid, eid) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"SELECT {COLS} FROM expenses WHERE id=? AND user_id=?", (eid, uid))
        r = await cur.fetchone()
        return _row(r) if r else None


async def db_update(uid, eid, **fields) -> bool:
    mapping = {"title": "title", "amount": "amount", "category": "category", "date": "spent_on"}
    sets = [(mapping[k], v) for k, v in fields.items() if k in mapping and v is not None]
    if not sets:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            f"UPDATE expenses SET {', '.join(f'{c}=?' for c, _ in sets)} WHERE id=? AND user_id=?",
            (*[v for _, v in sets], eid, uid),
        )
        await db.commit()
        return cur.rowcount > 0


async def db_delete(uid, eid) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM expenses WHERE id=? AND user_id=?", (eid, uid))
        await db.commit()
        return cur.rowcount > 0


async def db_range(uid, start: date, end: date) -> list[dict]:
    """Траты с start включительно по end не включая."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            f"SELECT {COLS} FROM expenses WHERE user_id=? AND spent_on>=? AND spent_on<? ORDER BY spent_on, id",
            (uid, start.isoformat(), end.isoformat()),
        )
        return [_row(r) for r in await cur.fetchall()]


async def db_last(uid) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"SELECT {COLS} FROM expenses WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,))
        r = await cur.fetchone()
        return _row(r) if r else None


# ───────────── Разбор суммы (запасной, без ИИ) ─────────────
NUM = r"\d+(?:[ \u00a0.,]\d+)*"
SUFFIX = r"(?:тыс|тыщ|тысяч)\w*|к(?![а-яё])|k(?![a-z])|минг|ming|(?:млн|миллион)\w*|mln"
AMOUNT_RE = re.compile(rf"({NUM})\s*({SUFFIX})?", re.I)
PURE_AMOUNT_RE = re.compile(rf"\s*({NUM})\s*({SUFFIX})?\s*(?:сум\w*|сўм|sum|so'?m)?\s*[.!]?\s*", re.I)
SUM_WORDS = re.compile(r"(?<![а-яёa-z])(?:сум\w*|сўм|sum|so'?m)(?![а-яёa-z])", re.I)


def _to_number(raw: str) -> float:
    raw = raw.replace("\u00a0", " ").strip()
    if re.fullmatch(r"\d{1,3}(?:[ .,]\d{3})+", raw):  # 200.000 / 200 000 / 1,500,000
        return float(re.sub(r"[ .,]", "", raw))
    raw = raw.replace(" ", "").replace(",", ".")
    if raw.count(".") > 1:
        raw = raw.replace(".", "")
    return float(raw)


def _mult(suffix: str | None) -> int:
    s = (suffix or "").lower()
    if s.startswith(("млн", "миллион", "mln")):
        return 1_000_000
    if s:
        return 1000
    return 1


def pure_amount(text: str) -> int | None:
    """Если сообщение — только сумма («150000», «150 тысяч»), вернуть её."""
    m = PURE_AMOUNT_RE.fullmatch(text)
    if not m:
        return None
    try:
        v = int(round(_to_number(m.group(1)) * _mult(m.group(2))))
    except ValueError:
        return None
    return v if v > 0 else None


def parse_fallback(text: str) -> list[dict]:
    items = []
    for part in re.split(r"[;\n]|,(?!\d)|\s+и\s+|\s+плюс\s+", text):
        best = None
        for m in AMOUNT_RE.finditer(part):
            try:
                v = int(round(_to_number(m.group(1)) * _mult(m.group(2))))
            except ValueError:
                continue
            if v > 0 and (best is None or v > best[0]):
                best = (v, m.span())
        if not best:
            continue
        amount, (s, e) = best
        title = part[:s] + " " + part[e:]
        offset = 0
        if re.search(r"позавчера", title, re.I):
            offset, title = -2, re.sub(r"позавчера", "", title, flags=re.I)
        elif re.search(r"вчера|kecha", title, re.I):
            offset, title = -1, re.sub(r"вчера|kecha", "", title, flags=re.I)
        title = SUM_WORDS.sub("", title)
        title = re.sub(r"\s+", " ", title).strip(" -—:.,!?")
        items.append({"title": title[:1].upper() + title[1:], "amount": amount, "category": "Другое", "day_offset": offset})
    return items


# ───────────── Groq: голос → текст, текст → траты ─────────────
PARSE_PROMPT = f"""Ты парсер расходов. Пользователь живёт в Ташкенте, валюта — узбекский сум.
Он диктует траты по-русски или по-узбекски. Верни ТОЛЬКО JSON:
{{"items":[{{"title":"Стрижка","amount":200000,"category":"Красота","day_offset":0}}]}}
Правила:
- title: короткое название покупки с большой буквы (1–3 слова), без суммы и без слова «сум».
- amount: целое число в сумах. «тысяч/тыщ/к/минг» = ×1000, «млн/миллион» = ×1000000,
  «200.000» и «200 000» = 200000, «полтора миллиона» = 1500000, «пятьдесят тысяч» = 50000.
- category: строго одна из: {", ".join(CATEGORIES)}.
  Еда/вода/магазин → Продукты; ресторан/кофе/доставка еды → Кафе; такси/бензин/метро → Транспорт;
  стрижка/маникюр/косметика → Красота; аптека/врач → Здоровье; телефон/интернет → Связь.
- day_offset: 0 — сегодня, -1 — «вчера», -2 — «позавчера».
- Несколько покупок в одной фразе → несколько элементов.
- Сумма не названа или это не трата → {{"items":[]}}."""


def _auth():
    return {"Authorization": f"Bearer {GROQ_API_KEY}"}


async def transcribe(data: bytes, filename: str) -> str:
    form = aiohttp.FormData()
    form.add_field("file", data, filename=filename)
    form.add_field("model", STT_MODEL)
    form.add_field("response_format", "json")
    form.add_field("temperature", "0")
    form.add_field("prompt", "Траты: стрижка 200 000 сум, вода 7 тысяч сум, такси 25 тысяч.")
    if STT_LANGUAGE:
        form.add_field("language", STT_LANGUAGE)
    async with http.post(f"{GROQ_URL}/audio/transcriptions", data=form, headers=_auth()) as r:
        if r.status != 200:
            raise RuntimeError(f"STT {r.status}: {await r.text()}")
        return (await r.json()).get("text", "").strip()


def _normalize(raw_items) -> list[dict]:
    out = []
    for it in raw_items if isinstance(raw_items, list) else []:
        try:
            amount = int(round(float(str(it.get("amount", 0)).replace(" ", ""))))
        except (ValueError, TypeError, AttributeError):
            continue
        if amount <= 0:
            continue
        title = str(it.get("title") or "").strip()[:60]
        cat = it.get("category") if it.get("category") in CATEGORIES else "Другое"
        try:
            off = max(-30, min(0, int(it.get("day_offset") or 0)))
        except (ValueError, TypeError):
            off = 0
        out.append({"title": title[:1].upper() + title[1:], "amount": amount, "category": cat, "day_offset": off})
    return out


async def parse_llm(text: str) -> list[dict]:
    payload = {
        "model": PARSE_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": PARSE_PROMPT}, {"role": "user", "content": text}],
    }
    async with http.post(f"{GROQ_URL}/chat/completions", json=payload, headers=_auth()) as r:
        if r.status != 200:
            raise RuntimeError(f"LLM {r.status}: {await r.text()}")
        data = await r.json()
    content = data["choices"][0]["message"]["content"]
    return _normalize(json.loads(content).get("items", []))


async def parse_text(text: str) -> list[dict]:
    try:
        items = await parse_llm(text)
    except Exception as e:  # сеть/лимиты/кривой JSON → разбираем сами
        log.warning("LLM parse failed, fallback: %s", e)
        items = parse_fallback(text)
    for it in items:
        if not it["title"]:
            it["title"] = "Без названия"
    return items


# ───────────── Бот ─────────────
dp = Dispatcher()


def webapp_button(text="📅 Открыть календарь") -> list[InlineKeyboardButton]:
    return [InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL))] if WEBAPP_URL else []


def expense_kb(saved: list[tuple[int, dict]]) -> InlineKeyboardMarkup:
    rows = []
    if len(saved) == 1:
        eid = saved[0][0]
        rows.append([
            InlineKeyboardButton(text="✏️ Изменить", callback_data=f"edit:{eid}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del:{eid}"),
        ])
    else:
        for eid, it in saved:
            rows.append([
                InlineKeyboardButton(text=f"✏️ {it['title'][:22]}", callback_data=f"edit:{eid}"),
                InlineKeyboardButton(text="🗑", callback_data=f"del:{eid}"),
            ])
    if btn := webapp_button():
        rows.append(btn)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def line(it: dict, d: date | None = None) -> str:
    t = today()
    when = ""
    if d and d != t:
        when = " · вчера" if d == t - timedelta(days=1) else f" · {d.strftime('%d.%m')}"
    return f"{CATEGORIES.get(it['category'], '📦')} <b>{html.escape(it['title'])}</b> — {fmt(it['amount'])} сум{when}"


async def day_total(uid) -> int:
    t = today()
    return sum(x["amount"] for x in await db_range(uid, t, t + timedelta(days=1)))


async def reply(msg: Message, status: Message | None, text: str, kb=None):
    if status:
        await status.edit_text(text, reply_markup=kb)
    else:
        await msg.answer(text, reply_markup=kb)


async def process(msg: Message, text: str, status: Message | None = None, heard: str | None = None):
    uid = msg.from_user.id
    heard_line = f"🎙 <i>«{html.escape(heard)}»</i>\n\n" if heard else ""

    if uid in pending_edits:
        return await apply_edit(msg, text, status, heard_line)

    items = await parse_text(text)
    if not items:
        return await reply(msg, status, heard_line + "🤔 Не нашёл сумму. Скажи, например: <b>«стрижка 200 тысяч»</b>")

    t = today()
    saved = []
    for it in items:
        d = t + timedelta(days=it["day_offset"])
        eid = await db_add(uid, it["title"], it["amount"], it["category"], d.isoformat(), text)
        saved.append((eid, it, d))

    body = "\n".join("✅ " + line(it, d) for _, it, d in saved)
    total = await day_total(uid)
    await reply(msg, status, f"{heard_line}{body}\n\n📊 За сегодня: <b>{fmt(total)} сум</b>",
                expense_kb([(eid, it) for eid, it, _ in saved]))


async def apply_edit(msg: Message, text: str, status: Message | None, heard_line: str):
    uid = msg.from_user.id
    eid = pending_edits.pop(uid)
    exp = await db_get(uid, eid)
    if not exp:
        return await reply(msg, status, "Эта трата уже удалена.")

    amount = pure_amount(text)
    if amount:
        await db_update(uid, eid, amount=amount)
    else:
        items = await parse_text(text)
        if not items:
            pending_edits[uid] = eid
            return await reply(msg, status, heard_line + "🤔 Не понял. Пришли сумму (<code>150000</code>) "
                                                         "или «название сумма». /cancel — отмена")
        new = items[0]
        title = exp["title"] if new["title"] == "Без названия" else new["title"]
        await db_update(uid, eid, title=title, amount=new["amount"], category=new["category"])

    exp = await db_get(uid, eid)
    await reply(msg, status, f"{heard_line}✏️ Обновил: {line(exp)}", expense_kb([(eid, exp)]))


@dp.message(CommandStart())
async def cmd_start(msg: Message):
    if not allowed(msg.from_user.id):
        return await msg.answer("Это приватный бот.")
    kb = InlineKeyboardMarkup(inline_keyboard=[webapp_button()]) if WEBAPP_URL else None
    await msg.answer(
        "👋 Я записываю твои траты.\n\n"
        "🎙 Просто отправь голосовое: <b>«стрижка 200 тысяч»</b>\n"
        "✍️ Или напиши текстом: <code>вода 7000</code>\n"
        "🧾 Можно несколько сразу: <i>«такси 25 тысяч и кофе 30 тысяч»</i>\n"
        "⏪ И за прошлые дни: <i>«вчера бензин 150 тысяч»</i>\n\n"
        "/today — траты за сегодня\n/month — итоги месяца\n/undo — удалить последнюю",
        reply_markup=kb,
    )


@dp.message(Command("cancel"))
async def cmd_cancel(msg: Message):
    pending_edits.pop(msg.from_user.id, None)
    await msg.answer("Ок, отменил.")


@dp.message(Command("today"))
async def cmd_today(msg: Message):
    uid = msg.from_user.id
    t = today()
    items = await db_range(uid, t, t + timedelta(days=1))
    if not items:
        return await msg.answer("Сегодня трат ещё нет 🙌")
    total = sum(x["amount"] for x in items)
    body = "\n".join(line(x) for x in items)
    await msg.answer(f"📅 <b>Сегодня</b>\n\n{body}\n\nИтого: <b>{fmt(total)} сум</b>",
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[webapp_button()]) if WEBAPP_URL else None)


@dp.message(Command("month"))
async def cmd_month(msg: Message):
    uid = msg.from_user.id
    t = today()
    start = t.replace(day=1)
    items = await db_range(uid, start, t + timedelta(days=1))
    if not items:
        return await msg.answer("В этом месяце трат ещё нет.")
    total = sum(x["amount"] for x in items)
    by_cat: dict[str, int] = {}
    for x in items:
        by_cat[x["category"]] = by_cat.get(x["category"], 0) + x["amount"]
    cats = "\n".join(
        f"{CATEGORIES.get(c, '📦')} {c} — {fmt(s)} ({round(s / total * 100)}%)"
        for c, s in sorted(by_cat.items(), key=lambda kv: -kv[1])
    )
    avg = total // t.day
    await msg.answer(f"📊 <b>{t.strftime('%m.%Y')}</b>\n\nВсего: <b>{fmt(total)} сум</b>\n"
                     f"В среднем в день: {fmt(avg)} сум\n\n{cats}",
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[webapp_button()]) if WEBAPP_URL else None)


@dp.message(Command("undo"))
async def cmd_undo(msg: Message):
    uid = msg.from_user.id
    last = await db_last(uid)
    if not last:
        return await msg.answer("Удалять нечего.")
    await db_delete(uid, last["id"])
    await msg.answer(f"🗑 Удалил: {line(last)}")


@dp.message(F.voice | F.audio | F.video_note)
async def on_voice(msg: Message):
    if not allowed(msg.from_user.id):
        return
    media = msg.voice or msg.audio or msg.video_note
    if (media.duration or 0) > 120:
        return await msg.answer("Слишком длинное голосовое — до 2 минут, пожалуйста.")
    if msg.voice:
        filename = "voice.ogg"
    elif msg.video_note:
        filename = "video.mp4"
    else:
        filename = msg.audio.file_name or "audio.mp3"

    status = await msg.answer("🎧 Распознаю…")
    try:
        buf = await msg.bot.download(media)
        text = await transcribe(buf.read(), filename)
    except Exception as e:
        log.exception("STT failed: %s", e)
        return await status.edit_text("⚠️ Не получилось распознать. Попробуй ещё раз или напиши текстом.")
    if not text:
        return await status.edit_text("🤷 Ничего не услышал. Попробуй ещё раз.")
    await process(msg, text, status=status, heard=text)


@dp.message(F.text & ~F.text.startswith("/"))
async def on_text(msg: Message):
    if not allowed(msg.from_user.id):
        return
    await process(msg, msg.text)


@dp.callback_query(F.data.startswith("del:"))
async def cb_delete(cb: CallbackQuery):
    uid, eid = cb.from_user.id, int(cb.data.split(":")[1])
    exp = await db_get(uid, eid)
    if not exp or not await db_delete(uid, eid):
        return await cb.answer("Уже удалено")
    pending_edits.pop(uid, None)
    rows = [r for r in (cb.message.reply_markup.inline_keyboard if cb.message.reply_markup else [])
            if not any(b.callback_data and b.callback_data.endswith(f":{eid}") for b in r)]
    await cb.message.edit_text(f"{cb.message.html_text}\n\n🗑 Удалено: {html.escape(exp['title'])}",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows) if rows else None)
    await cb.answer("Удалено")


@dp.callback_query(F.data.startswith("edit:"))
async def cb_edit(cb: CallbackQuery):
    uid, eid = cb.from_user.id, int(cb.data.split(":")[1])
    exp = await db_get(uid, eid)
    if not exp:
        return await cb.answer("Эта трата удалена")
    pending_edits[uid] = eid
    await cb.message.answer(
        f"✏️ Меняем: {line(exp)}\n\nОтправь голосом или текстом:\n"
        "• только сумму — <code>150000</code>\n• или заново — <code>стрижка 150 тысяч</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="cancel")]]),
    )
    await cb.answer()


@dp.callback_query(F.data == "cancel")
async def cb_cancel(cb: CallbackQuery):
    pending_edits.pop(cb.from_user.id, None)
    await cb.message.edit_text("Ок, без изменений.")
    await cb.answer()


# ───────────── Mini App: сервер и API ─────────────
def check_init_data(init_data: str) -> int | None:
    """Проверяем подпись Telegram, чтобы чужой не увидел твои траты."""
    if not init_data:
        return None
    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    received = pairs.pop("hash", None)
    if not received:
        return None
    check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, received):
        return None
    if time.time() - int(pairs.get("auth_date", "0")) > 7 * 86400:
        return None
    try:
        return int(json.loads(pairs.get("user", "{}"))["id"])
    except (KeyError, ValueError, TypeError):
        return None


@web.middleware
async def auth_mw(request: web.Request, handler):
    if request.path.startswith("/api/"):
        uid = check_init_data(request.headers.get("X-Init-Data", ""))
        if not uid or not allowed(uid):
            return web.json_response({"error": "unauthorized"}, status=401)
        request["uid"] = uid
    return await handler(request)


def _validate(body: dict, partial: bool) -> dict:
    out = {}
    if "title" in body or not partial:
        title = str(body.get("title", "")).strip()[:60]
        if not title:
            raise web.HTTPBadRequest(text="title")
        out["title"] = title
    if "amount" in body or not partial:
        try:
            amount = int(body.get("amount"))
        except (TypeError, ValueError):
            raise web.HTTPBadRequest(text="amount")
        if not 0 < amount < 10**12:
            raise web.HTTPBadRequest(text="amount")
        out["amount"] = amount
    if "category" in body or not partial:
        cat = body.get("category", "Другое")
        out["category"] = cat if cat in CATEGORIES else "Другое"
    if "date" in body or not partial:
        try:
            out["date"] = date.fromisoformat(str(body.get("date") or today().isoformat())).isoformat()
        except ValueError:
            raise web.HTTPBadRequest(text="date")
    return out


async def h_index(_):
    return web.FileResponse(INDEX_HTML, headers={"Cache-Control": "no-cache"})


async def h_list(request: web.Request):
    month = request.query.get("month") or today().strftime("%Y-%m")
    try:
        y, m = map(int, month.split("-"))
        start = date(y, m, 1)
    except ValueError:
        raise web.HTTPBadRequest(text="month")
    end = date(y + (m == 12), m % 12 + 1, 1)
    items = await db_range(request["uid"], start, end)
    return web.json_response({"items": items, "categories": CATEGORIES, "today": today().isoformat()})


async def h_create(request: web.Request):
    data = _validate(await request.json(), partial=False)
    eid = await db_add(request["uid"], data["title"], data["amount"], data["category"], data["date"], "mini app")
    return web.json_response(await db_get(request["uid"], eid))


async def h_update(request: web.Request):
    eid = int(request.match_info["id"])
    data = _validate(await request.json(), partial=True)
    if not await db_update(request["uid"], eid, **data):
        raise web.HTTPNotFound()
    return web.json_response(await db_get(request["uid"], eid))


async def h_delete(request: web.Request):
    if not await db_delete(request["uid"], int(request.match_info["id"])):
        raise web.HTTPNotFound()
    return web.json_response({"ok": True})


def build_app() -> web.Application:
    app = web.Application(middlewares=[auth_mw])
    app.router.add_get("/", h_index)
    app.router.add_get("/health", lambda _: web.json_response({"ok": True}))
    app.router.add_get("/api/expenses", h_list)
    app.router.add_post("/api/expenses", h_create)
    app.router.add_patch("/api/expenses/{id:\\d+}", h_update)
    app.router.add_delete("/api/expenses/{id:\\d+}", h_delete)
    return app


# ───────────── Запуск ─────────────
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not BOT_TOKEN or not GROQ_API_KEY:
        raise SystemExit("Заполни BOT_TOKEN и GROQ_API_KEY в переменных окружения")
    await db_init()

    global http
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    await bot.set_my_commands([
        BotCommand(command="today", description="Траты за сегодня"),
        BotCommand(command="month", description="Итоги месяца"),
        BotCommand(command="undo", description="Удалить последнюю трату"),
        BotCommand(command="start", description="Как пользоваться"),
    ])
    if WEBAPP_URL:
        await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Календарь", web_app=WebAppInfo(url=WEBAPP_URL)))
        log.info("Mini App: %s", WEBAPP_URL)
    else:
        log.warning("WEBAPP_URL не задан — календарь не будет открываться из бота")

    runner = web.AppRunner(build_app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Web server on :%s", PORT)

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot)
    finally:
        await http.close()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
