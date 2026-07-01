"""
FunsDiia Bot
────────────────────────────────────────────
Збирає дані користувача → оновлює index в папці 2 →
пушить папку 2 на GitHub → отримує URL → генерує QR →
кладе QR у 1/assets/q.png → пушить папку 1.

Env vars (обов'язкові):
  TELEGRAM_BOT_TOKEN  — токен бота
  ADMIN_IDS           — через кому (напр. 123,456)
  SQLITECLOUD_URL     — рядок підключення SQLite Cloud

Env vars (опційні):
  GROUP_CHAT_ID       — ID групи для сповіщень
  PAGES_GH_TOKEN      — токен основного GitHub акаунта (папка 1)
  GH_TOKEN_2          — токен іншого GitHub акаунта (папка 2)
  GH_USERNAME         — логін основного акаунта
  GH_USERNAME_2       — логін іншого акаунта
  PAGES_REPO_1        — назва репо для папки 1 (default: diia-main-pages)
  PAGES_REPO_2        — назва репо для папки 2 (default: site2-pages)
  DEEPSEEK_API_KEY    — ключ DeepSeek AI
  REFERRAL_REWARD     — бонус за реферала (default: 19)
  MIN_WITHDRAW        — мінімум для виводу (default: 50)
  BOT_USERNAME        — username бота без @
  LOG_LEVEL           — рівень логування (default: INFO)
"""

import asyncio
import base64
import hashlib
import html as _html_escape
import io
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import pytz
import requests as _req
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

import chain_deploy
import db as _db

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────

def _env_int(key: str, default: int) -> int:
    v = os.getenv(key, "").strip()
    return int(v) if v.lstrip("-").isdigit() else default


def _env_int_list(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip().lstrip("-").isdigit()]


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не знайдено!")

ADMIN_IDS: list[int] = _env_int_list(os.getenv("ADMIN_IDS", os.getenv("ADMIN_CHAT_ID", "")))
if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS не задано!")

_raw_group = os.getenv("GROUP_CHAT_ID", "").strip()
GROUP_CHAT_ID: Optional[int] = int(_raw_group) if _raw_group.lstrip("-").isdigit() else None

PAGES_GH_TOKEN: str = os.getenv("PAGES_GH_TOKEN", "")
DEEPSEEK_API_KEY: str = os.getenv("DEEPSEEK_API_KEY", "")
AI_ENABLED = bool(DEEPSEEK_API_KEY)

TIMEZONE = pytz.timezone("Europe/Kyiv")
BOT_USERNAME = os.getenv("BOT_USERNAME", "FunsDiia_bot")
REFERRAL_REWARD = _env_int("REFERRAL_REWARD", 19)
MIN_WITHDRAW = _env_int("MIN_WITHDRAW", 50)

ORDER_PHOTOS_DIR = "order_photos"
SITE_TEMPLATE_DIR = "1"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
)
logger = logging.getLogger(__name__)

# ── FSM states ─────────────────────────────────────────────────────────────────

(
    AWAIT_FIO, AWAIT_DOB, AWAIT_SEX, AWAIT_ADDRESS,
    AWAIT_RIGHTS_CHOICE, AWAIT_ZAGRAN_CHOICE, AWAIT_DIPLOMA_CHOICE, AWAIT_PHOTO,
    AWAIT_FEEDBACK,
    AWAIT_TARIFF_NAME, AWAIT_TARIFF_PRICE, AWAIT_TARIFF_DAYS, AWAIT_TARIFF_EMOJI,
    AWAIT_BROADCAST,
    AWAIT_PROMO_CODE, AWAIT_PROMO_DISCOUNT, AWAIT_PROMO_USES,
    AWAIT_USER_SEARCH, AWAIT_BALANCE_UID, AWAIT_BALANCE_AMOUNT,
    AWAIT_ORDER_COMPLETE_FILE, AWAIT_REPLY_TO_USER, AWAIT_CUSTOM_PAYMENT_TEXT,
    AWAIT_REJECT_REASON,
    AWAIT_TARIFF_EDIT_PRICE, AWAIT_TARIFF_EDIT_NAME, AWAIT_TARIFF_EDIT_EMOJI,
    AWAIT_WELCOME_TEXT,
) = range(28)

# ── Defaults ───────────────────────────────────────────────────────────────────

DEFAULT_TARIFFS = {
    "1_day":   {"name": "1 день",   "price": 20,  "days": 1,    "emoji": "🌙", "active": True},
    "30_days": {"name": "30 днів",  "price": 70,  "days": 30,   "emoji": "📅", "active": True},
    "90_days": {"name": "90 днів",  "price": 150, "days": 90,   "emoji": "🌿", "active": True},
    "180_days":{"name": "180 днів", "price": 190, "days": 180,  "emoji": "🌟", "active": True},
    "forever": {"name": "Назавжди", "price": 250, "days": None, "emoji": "💎", "active": True},
}

DEFAULT_SETTINGS = {
    "bot_enabled":        True,
    "payment_card":       "5355 5732 5047 6310",
    "payment_holder":     "SenseBank",
    "payment_link":       "https://send.monobank.ua/jar/6R3gd9Ew8w",
    "welcome_text":       "",
    "maintenance_mode":   False,
    "new_orders_enabled": True,
    "ai_check_receipts":  True,
    "ai_auto_deploy":     True,
    "ai_support":         True,
}

# ── DB dispatch ────────────────────────────────────────────────────────────────
# Заповнюється у main() після init_db()
_DB: dict = {}

USERS_KEY    = "users"
ORDERS_KEY   = "orders"
FEEDBACK_KEY = "feedback"
TARIFFS_KEY  = "tariffs"
PROMOS_KEY   = "promos"
SETTINGS_KEY = "settings"


def _load(key: str, default=None):
    if default is None:
        default = {}
    loader = _DB.get(key, {}).get("load")
    if loader:
        return loader() or default
    return default


def _save(key: str, data) -> bool:
    saver = _DB.get(key, {}).get("save")
    return saver(data) if saver else False


# ── Helpers ────────────────────────────────────────────────────────────────────

def now_str() -> str:
    return datetime.now(TIMEZONE).isoformat()


def now_fmt(fmt="%d.%m.%Y %H:%M") -> str:
    return datetime.now(TIMEZONE).strftime(fmt)


def gen_id(prefix="") -> str:
    return prefix + hashlib.sha256(f"{time.time()}{random.random()}".encode()).hexdigest()[:8]


def is_admin(uid) -> bool:
    return int(uid) in ADMIN_IDS


def esc(text) -> str:
    return _html_escape.escape(str(text))


def log_action(action: str, uid=None, details: dict = None):
    _db.log_action_db(now_str(), action, str(uid) if uid else None, details or {})


# ── Tariffs / Settings ─────────────────────────────────────────────────────────

def load_tariffs() -> dict:
    raw = _load(TARIFFS_KEY, DEFAULT_TARIFFS)
    for v in raw.values():
        if "text" in v and "name" not in v:
            v["name"] = v.pop("text")
        v.setdefault("emoji", "📦")
    return raw


def save_tariffs(t): _save(TARIFFS_KEY, t)
def active_tariffs() -> dict: return {k: v for k, v in load_tariffs().items() if v.get("active", True)}


def load_settings() -> dict:
    return {**DEFAULT_SETTINGS, **_load(SETTINGS_KEY, {})}


def save_settings(s): _save(SETTINGS_KEY, s)
def get_setting(key): return load_settings().get(key, DEFAULT_SETTINGS.get(key))


# ── Promos ─────────────────────────────────────────────────────────────────────

def load_promos() -> dict: return _load(PROMOS_KEY, {})
def save_promos(p): _save(PROMOS_KEY, p)


def check_promo(code: str, uid: str) -> dict:
    promos = load_promos()
    code = code.upper().strip()
    if code not in promos:
        return {"ok": False, "discount": 0, "msg": "❌ Промо-код не знайдено"}
    p = promos[code]
    if not p.get("active", True):
        return {"ok": False, "discount": 0, "msg": "❌ Промо-код вже не активний"}
    if p.get("max_uses", 0) and p.get("uses", 0) >= p["max_uses"]:
        return {"ok": False, "discount": 0, "msg": "❌ Промо-код вичерпано"}
    if uid in p.get("used_by", []):
        return {"ok": False, "discount": 0, "msg": "❌ Ви вже використали цей код"}
    expires = p.get("expires")
    if expires and datetime.fromisoformat(expires) < datetime.now(TIMEZONE):
        return {"ok": False, "discount": 0, "msg": "❌ Промо-код протерміновано"}
    return {"ok": True, "discount": p.get("discount", 0), "msg": f"✅ Знижка {p.get('discount', 0)}%"}


def apply_promo(code: str, uid: str):
    promos = load_promos()
    code = code.upper().strip()
    if code in promos:
        promos[code].setdefault("used_by", []).append(uid)
        promos[code]["uses"] = promos[code].get("uses", 0) + 1
        save_promos(promos)


# ── DeepSeek AI ────────────────────────────────────────────────────────────────

_DEEPSEEK_URL   = "https://api.deepseek.com/chat/completions"
_DEEPSEEK_MODEL = "deepseek-chat"

_SYS_SUPPORT = (
    "Ти — чат-підтримка сервісу FunsDiia. Відповідай коротко, дружньо, українською. "
    "Якщо питання про оплату — скажи, що реквізити надає адміністратор після підтвердження замовлення."
)
_SYS_RECEIPT = (
    'Ти — верифікатор чеків. Відповідай ТІЛЬКИ JSON без зайвого тексту: '
    '{"ok": true/false, "confidence": 0-100, "amount": число|null, "reason": "опис"}. '
    "ok=true якщо це реальний банківський чек."
)


def _deepseek(messages: list, system: str = "", max_tokens: int = 500) -> str:
    if not DEEPSEEK_API_KEY:
        return ""
    payload = {
        "model": _DEEPSEEK_MODEL,
        "max_tokens": max_tokens,
        "temperature": 0.3,
        "messages": ([{"role": "system", "content": system}] if system else []) + messages,
    }
    try:
        resp = _req.post(
            _DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json=payload, timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error("DeepSeek error: %s", e)
        return ""


async def ai_check_receipt(photo_bytes: bytes, expected_amount: int) -> dict:
    result = {"ok": None, "confidence": 0, "amount": None, "reason": "", "auto_approved": False}
    if not AI_ENABLED or not photo_bytes:
        return result
    b64 = base64.b64encode(photo_bytes).decode()
    messages = [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
        {"type": "text", "text": f"Очікувана сума: {expected_amount}₴. Проаналізуй цей чек."},
    ]}]
    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: _deepseek(messages, _SYS_RECEIPT, 300)
    )
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            result.update(parsed)
            result["auto_approved"] = bool(parsed.get("ok")) and int(parsed.get("confidence", 0)) >= 80
    except Exception as e:
        logger.warning("Receipt JSON parse: %s | raw=%s", e, raw[:100])
    return result


async def ai_support_reply(text: str, history: list = None) -> str:
    if not AI_ENABLED:
        return ""
    messages = (history or []) + [{"role": "user", "content": text}]
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _deepseek(messages, _SYS_SUPPORT, 400)
    )


async def ai_transliterate(fio_ua: str) -> str:
    if not AI_ENABLED or not fio_ua:
        return fio_ua
    messages = [{"role": "user", "content":
        f"Транслітеруй ПІБ латиницею (стандарт КМУ 2010): '{fio_ua}'. "
        "Відповідь — ТІЛЬКИ транслітерація."}]
    result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: _deepseek(messages, "", 50)
    )
    return result.strip("\"' ") or fio_ua


# ── Document generators ────────────────────────────────────────────────────────

def _rnd_digits(n): return "".join(str(random.randint(0, 9)) for _ in range(n))


def gen_address() -> str:
    districts = ["Харківський", "Чугуївський", "Ізюмський", "Лозівський", "Богодухівський"]
    cities    = ["м. Харків", "м. Чугуїв", "м. Мерефа", "м. Люботин", "смт Пісочин"]
    streets   = ["Гарібальді", "Сумська", "Пушкінська", "Полтавський Шлях", "пр. Науки"]
    return (
        f"Харківська область, {random.choice(districts)} район "
        f"{random.choice(cities)}, вул. {random.choice(streets)}, "
        f"буд. {random.randint(1, 150)}, кв. {random.randint(1, 250)}"
    )


def gen_values_dict(data: dict) -> dict:
    now = datetime.now(TIMEZONE)
    date_now = now.strftime("%d.%m.%Y")
    date_out = (now + timedelta(days=3650)).strftime("%d.%m.%Y")
    date_give_z = (now - timedelta(days=random.randint(1000, 2000))).strftime("%d.%m.%Y")
    date_out_z  = (now + timedelta(days=random.randint(3000, 4000))).strftime("%d.%m.%Y")

    universities = ["ХНУ імені Каразіна", "НТУ ХПІ", "ХНЕУ", "ХНМУ", "ХНУРЕ"]
    faculties    = ["Фізико-технічний", "Комп'ютерних наук", "Економічний", "Медичний"]

    sex = data.get("sex", "M")
    sex_ua = "Ч" if sex == "M" else "Ж"
    sex_en = "M" if sex == "M" else "W"

    univ = random.choice(universities)
    return {
        "fio":               data.get("fio", ""),
        "fio_en":            data.get("fio_en") or data.get("fio", ""),
        "birth":             data.get("dob", ""),
        "date_give":         date_now,
        "date_out":          date_out,
        "organ":             "0512",
        "rnokpp":            _rnd_digits(10),
        "uznr":              f"{random.randint(1990,2010)}0128-{random.randint(10000,99999)}",
        "pass_number":       _rnd_digits(9),
        "registeredOn":      date_now,
        "legalAdress":       "Харківська область",
        "live":              "Харківська область",
        "bank_adress":       data.get("address") or gen_address(),
        "sex":               sex_ua,
        "sex_en":            sex_en,
        "rights_categories": "A, B",
        "prava_number":      f"AUX{random.randint(100000,999999)}",
        "prava_date_give":   date_now,
        "prava_date_out":    date_out,
        "pravaOrgan":        "0512",
        "university":        univ,
        "fakultet":          random.choice(faculties),
        "stepen_dip":        "Магістра",
        "univer_dip":        univ,
        "dayout_dip":        date_out,
        "special_dip":       "Прикладна математика",
        "number_dip":        f"MT-{random.randint(100000,999999)}",
        "form":              "Очна",
        "zagran_number":     f"FX{random.randint(100000,999999)}",
        "dateGiveZ":         date_give_z,
        "dateOutZ":          date_out_z,
        "student_number":    f"{random.randint(2020,2024)}{random.randint(100000,999999)}",
        "student_date_give": date_now,
        "student_date_out":  date_out,
        "isRightsEnabled":   data.get("is_rights", True),
        "isZagranEnabled":   data.get("is_zagran", True),
        "isDiplomaEnabled":  data.get("is_diploma", False),
        "isStudyEnabled":    data.get("is_study", False),
        "isRojdenie":        False,
        "photo_passport":    "1.png",
        "photo_rights":      "1.png",
        "photo_students":    "1.png",
        "photo_zagran":      "1.png",
        "signPng":           "sign.png",
        "order_id":          data.get("order_id", ""),
        "generated_at":      now_str(),
        # Підписка — заповнюється після деплою через окремий апдейт
        "subscription_end":  data.get("subscription_end", ""),
        "is_expired":        data.get("is_expired", False),
    }


def values_to_js(d: dict) -> str:
    lines = [f"// Автоматично згенеровано: {d.get('generated_at', '')}", ""]
    for key, val in d.items():
        if isinstance(val, bool):
            lines.append(f"var {key} = {'true' if val else 'false'};")
        elif isinstance(val, (int, float)):
            lines.append(f"var {key} = {val};")
        else:
            escaped = str(val).replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'var {key} = "{escaped}";')
    # Завжди включаємо змінні для watermark/expiry навіть якщо не в dict
    if "subscription_end" not in d:
        lines.append('var subscription_end = "";')
    if "is_expired" not in d:
        lines.append("var is_expired = false;")
    return "\n".join(lines) + "\n"


# ── Subscription helpers ───────────────────────────────────────────────────────

def calc_subscription_end(tariff_key: str, tariffs: dict) -> Optional[str]:
    """Повертає ISO-рядок дати закінчення підписки або None якщо безстроково."""
    t = tariffs.get(tariff_key, {})
    days = t.get("days")
    if not days:
        return None  # forever
    end_dt = datetime.now(TIMEZONE) + timedelta(days=days)
    return end_dt.isoformat()


def days_until_expiry(subscription_end: str) -> int:
    """Кількість повних днів до закінчення (може бути від'ємним)."""
    end_dt = datetime.fromisoformat(subscription_end)
    if end_dt.tzinfo is None:
        end_dt = TIMEZONE.localize(end_dt)
    delta = end_dt.replace(hour=0, minute=0, second=0, microsecond=0) - \
            datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    return delta.days


async def subscription_check_job(context: ContextTypes.DEFAULT_TYPE):
    """Job що запускається щогодини і перевіряє підписки."""
    orders = _load(ORDERS_KEY)
    users  = _load(USERS_KEY)
    now    = datetime.now(TIMEZONE)

    for oid, order in orders.items():
        sub_end = order.get("subscription_end")
        status  = order.get("status", "")
        uid2    = order.get("user_id", "")

        if not sub_end or status in ("expired", "rejected", "pending"):
            continue
        if users.get(uid2, {}).get("banned"):
            continue

        days_left = days_until_expiry(sub_end)

        # Сповіщати тільки один раз на кожен "milestone"
        notified = order.get("notified_days", [])

        # ── Нагадування: 3 / 2 / 1 день до закінчення
        for remind_day in (3, 2, 1):
            if days_left == remind_day and remind_day not in notified:
                tariff_name = order.get("tariff_name", "")
                pages_url   = order.get("pages_url", "")
                try:
                    await context.bot.send_message(
                        uid2,
                        f"⏰ <b>Підписка закінчується через {remind_day} {'день' if remind_day == 1 else 'дні'}!</b>\n\n"
                        f"📦 Тариф: {esc(tariff_name)}\n"
                        f"📅 Дійсна до: {datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}\n\n"
                        f"{'🔗 Ваш кабінет: ' + pages_url + chr(10) + chr(10) if pages_url else ''}"
                        f"Щоб продовжити доступ — оберіть тариф нижче 👇",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔄 Продовжити підписку", callback_data="catalog")],
                        ]),
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                    orders[oid].setdefault("notified_days", []).append(remind_day)
                    log_action("sub_reminder", uid2, {"oid": oid, "days_left": remind_day})
                except Exception as e:
                    logger.error("sub_reminder send [%s]: %s", uid2, e)

        # ── День закінчення (0 днів)
        if days_left == 0 and 0 not in notified:
            tariff_name = order.get("tariff_name", "")
            try:
                await context.bot.send_message(
                    uid2,
                    f"🔴 <b>Сьогодні закінчується підписка!</b>\n\n"
                    f"📦 Тариф: {esc(tariff_name)}\n\n"
                    f"Без продовження доступ до кабінету буде заблоковано. "
                    f"Продовжте зараз, щоб уникнути водяного знаку. 💧",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Продовжити зараз", callback_data="catalog")],
                    ]),
                    parse_mode="HTML",
                )
                orders[oid].setdefault("notified_days", []).append(0)
                log_action("sub_expiring_today", uid2, {"oid": oid})
            except Exception as e:
                logger.error("sub_expiring_today [%s]: %s", uid2, e)

        # ── Підписка прострочена (days_left < 0)
        if days_left < 0 and status == "deployed":
            orders[oid]["status"] = "expired"
            orders[oid]["expired_at"] = now.isoformat()
            log_action("sub_expired", uid2, {"oid": oid})
            try:
                await context.bot.send_message(
                    uid2,
                    f"❌ <b>Підписка прострочена!</b>\n\n"
                    f"Водяний знак активовано на вашому кабінеті.\n"
                    f"Для відновлення доступу — оформіть нове замовлення 👇",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Відновити доступ", callback_data="catalog")],
                    ]),
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error("sub_expired notify [%s]: %s", uid2, e)

            # Сповістити адмінів
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        f"⚠️ <b>Підписка закінчилась</b>\n"
                        f"👤 {uid2}  📦 <code>{oid}</code>\n"
                        f"📅 {datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass

    _save(ORDERS_KEY, orders)


# ── UI helpers ─────────────────────────────────────────────────────────────────

def mkb(*rows): return InlineKeyboardMarkup(list(rows))
def back_btn(cb): return [InlineKeyboardButton("🔙 Назад", callback_data=cb)]


async def safe_edit(query, text: str, kb=None, **kw):
    kw.setdefault("parse_mode", "HTML")
    if kb:
        kw["reply_markup"] = kb
    try:
        await query.edit_message_text(text, **kw)
    except BadRequest:
        await query.message.reply_text(text, **kw)


def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_admin(uid):
            if update.callback_query:
                await update.callback_query.answer("❌ Немає доступу", show_alert=True)
            else:
                await update.message.reply_text("❌ Немає доступу.")
            return
        return await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper


async def notify_group(bot, text: str, kb=None):
    if not GROUP_CHAT_ID:
        return None
    kw = {"parse_mode": "HTML"}
    if kb:
        kw["reply_markup"] = kb
    try:
        return await bot.send_message(GROUP_CHAT_ID, text, **kw)
    except Exception as e:
        logger.error("notify_group: %s", e)


async def notify_group_photo(bot, photo_bytes: bytes, caption: str, kb=None):
    if not GROUP_CHAT_ID:
        return None
    try:
        buf = io.BytesIO(photo_bytes); buf.name = "photo.png"
        kw = {"caption": caption, "parse_mode": "HTML"}
        if kb:
            kw["reply_markup"] = kb
        return await bot.send_photo(GROUP_CHAT_ID, buf, **kw)
    except Exception as e:
        logger.error("notify_group_photo: %s", e)


# ── /start ─────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    users = _load(USERS_KEY)
    settings = load_settings()

    ref_by = None
    if context.args:
        pot = context.args[0]
        if pot != uid and pot in users:
            ref_by = pot

    if uid not in users:
        users[uid] = {
            "username":    update.effective_user.username,
            "first_name":  update.effective_user.first_name,
            "balance":     0,
            "referred_by": ref_by,
            "ref_count":   0,
            "has_bought":  False,
            "joined_date": now_str(),
            "total_spent": 0,
            "total_orders":0,
            "banned":      False,
            "vip":         False,
        }
        _save(USERS_KEY, users)
        log_action("new_user", uid, {"ref_by": ref_by})
        if ref_by:
            try:
                await context.bot.send_message(
                    ref_by,
                    f"👋 <b>Новий реферал!</b>\n{esc(update.effective_user.first_name)} "
                    f"приєднався за вашим посиланням!\nВи отримаєте {REFERRAL_REWARD}₴ після першого замовлення.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

    u = users.get(uid, {})
    if u.get("banned"):
        await update.effective_message.reply_text("🚫 <b>Ваш акаунт заблоковано.</b>", parse_mode="HTML")
        return
    if settings.get("maintenance_mode") and not is_admin(uid):
        await update.effective_message.reply_text("🛠 <b>Технічне обслуговування.</b> Спробуйте пізніше.", parse_mode="HTML")
        return

    vip = " 👑" if u.get("vip") else ""
    bal = u.get("balance", 0)
    bal_line = f"\n💰 Баланс: <b>{bal}₴</b>" if bal > 0 else ""
    welcome = settings.get("welcome_text") or (
        f"👋 <b>{esc(update.effective_user.first_name)}{vip}</b>{bal_line}\n\n"
        "🪪 <b>FunsDiia</b> — генерація документів швидко.\n⚡️ Готово за 10 хвилин."
    )

    kb_rows = [
        [InlineKeyboardButton("🛒 Замовити", callback_data="catalog")],
        [InlineKeyboardButton("📂 Мої замовлення", callback_data="my_orders"),
         InlineKeyboardButton("👤 Профіль", callback_data="profile")],
        [InlineKeyboardButton("🎟 Промо-код", callback_data="promo_enter"),
         InlineKeyboardButton("👥 Реферали", callback_data="ref_menu")],
        [InlineKeyboardButton("💬 Підтримка", callback_data="feedback"),
         InlineKeyboardButton("ℹ️ Про нас", callback_data="about")],
    ]
    if is_admin(uid):
        kb_rows.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])

    await update.effective_message.reply_text(
        welcome, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML",
    )
    context.user_data.clear()


# ── Profile / Orders / Catalog ─────────────────────────────────────────────────

async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    u = _load(USERS_KEY).get(uid, {})
    orders = _load(ORDERS_KEY)
    my = [o for o in orders.values() if o.get("user_id") == uid]
    done = sum(1 for o in my if o.get("status") == "completed")
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    vip_label = "👑 VIP" if u.get("vip") else "👤 Стандарт"
    text = (
        f"👤 <b>{esc(u.get('first_name', 'Профіль'))}</b>  {vip_label}\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"💰 Баланс: <b>{u.get('balance', 0)}₴</b>  ·  👥 Рефералів: <b>{u.get('ref_count', 0)}</b>\n"
        f"📦 Замовлень: <b>{len(my)}</b>  ·  ✅ Виконано: <b>{done}</b>\n\n"
        f"🔗 Реф. посилання:\n<code>{ref_link}</code>"
    )
    await safe_edit(q, text, mkb(
        [InlineKeyboardButton("💸 Вивести", callback_data="withdraw")],
        back_btn("home"),
    ), disable_web_page_preview=True)


async def my_orders_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    orders = _load(ORDERS_KEY)
    my = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("user_id") == uid],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    if not my:
        await safe_edit(q, "📭 <b>Замовлень ще немає.</b>\n\nОберіть тариф у каталозі!",
                        mkb(back_btn("home")))
        return
    status_map = {"pending":"⏳","approved":"✅","completed":"🎉","rejected":"❌","deployed":"🌐"}
    text = "📂 <b>Ваші замовлення</b>\n\n"
    for oid, o in my[:10]:
        st = status_map.get(o.get("status", ""), "·")
        t_name = load_tariffs().get(o.get("tariff", ""), {}).get("name", o.get("tariff", "?"))
        text += f"{st} <b>#{esc(oid)}</b>  {esc(t_name)}  <i>{o.get('created_at','')[:10]}</i>\n"
        if o.get("pages_url"):
            text += f"   🔗 <a href='{esc(o['pages_url'])}'>Відкрити</a>\n"
    await safe_edit(q, text, mkb(back_btn("home")), disable_web_page_preview=True)


async def show_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = active_tariffs()
    text = "🛒 <b>Оберіть тариф</b>\n\n"
    for k, t in tariffs.items():
        d = "∞ безстроково" if not t.get("days") else f"{t['days']} дн."
        text += f"{t.get('emoji','📦')} <b>{esc(t.get('name'))}</b> — <b>{t.get('price')}₴</b>  <i>({d})</i>\n"
    kb_rows = [
        [InlineKeyboardButton(f"{t.get('emoji','📦')} {t.get('name')} — {t.get('price')}₴",
                              callback_data=f"tar:{k}")]
        for k, t in tariffs.items()
    ]
    kb_rows.append(back_btn("home"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


async def select_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    if not get_setting("new_orders_enabled") and not is_admin(uid):
        await q.answer("❌ Прийом замовлень призупинено", show_alert=True)
        return
    key = q.data.split(":")[1]
    tariffs = active_tariffs()
    if key not in tariffs:
        await q.answer("❌ Тариф недоступний", show_alert=True)
        return
    t = tariffs[key]
    context.user_data.update({"tariff": key, "tariff_name": t.get("name"),
                               "tariff_price": t.get("price"), "state": AWAIT_FIO})
    await safe_edit(q,
        f"{t.get('emoji','📦')} <b>{esc(t.get('name'))}</b> — {t.get('price')}₴\n\n"
        "📝 <b>Крок 1/7</b> — Введіть ПІБ українською:\n<i>Приклад: Іванов Іван Іванович</i>",
    )


# ── Questionnaire steps ────────────────────────────────────────────────────────

async def select_sex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["sex"] = q.data.split(":")[1]
    context.user_data["state"] = AWAIT_ADDRESS
    sex_text = "Чоловік ♂️" if context.user_data["sex"] == "M" else "Жінка ♀️"
    await safe_edit(q,
        f"✅ Стать: <b>{sex_text}</b>\n\n"
        "🏠 <b>Крок 4/7</b> — Адреса прописки\n"
        "<i>Приклад: м. Харків, вул. Сумська, 5, кв. 12</i>\n"
        "<i>Або /skip для автогенерації</i>",
    )


async def _ask_rights(update, context):
    kb = mkb([InlineKeyboardButton("✅ Так", callback_data="rights:yes"),
               InlineKeyboardButton("❌ Ні",  callback_data="rights:no")])
    text = "🚗 <b>Крок 5/7</b> — Є водійські права?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def _ask_zagran(update, context):
    kb = mkb([InlineKeyboardButton("✅ Так", callback_data="zagran:yes"),
               InlineKeyboardButton("❌ Ні",  callback_data="zagran:no")])
    text = "🌍 <b>Крок 6/7</b> — Є закордонний паспорт?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def _ask_diploma(update, context):
    kb = mkb([InlineKeyboardButton("✅ Так", callback_data="diploma:yes"),
               InlineKeyboardButton("❌ Ні",  callback_data="diploma:no")])
    text = "🎓 <b>Крок 7/7</b> — Є диплом або студентський квиток?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def select_rights(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["is_rights"] = (update.callback_query.data.split(":")[1] == "yes")
    context.user_data["state"] = AWAIT_ZAGRAN_CHOICE
    await _ask_zagran(update, context)


async def select_zagran(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["is_zagran"] = (update.callback_query.data.split(":")[1] == "yes")
    context.user_data["state"] = AWAIT_DIPLOMA_CHOICE
    await _ask_diploma(update, context)


async def select_diploma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = update.callback_query.data.split(":")[1] == "yes"
    context.user_data["is_diploma"] = val
    context.user_data["is_study"]   = val
    context.user_data["state"] = AWAIT_PHOTO
    text = "📸 <b>Останній крок</b> — Надішліть фото 3×4\n<i>Обличчя на світлому фоні</i>"
    await safe_edit(update.callback_query, text)


# ── Main message handler ───────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if _load(USERS_KEY).get(uid, {}).get("banned"):
        return

    state = context.user_data.get("state")
    text  = (update.message.text or "").strip()

    # Адмін reply → відповідь клієнту
    if is_admin(uid) and update.message.reply_to_message:
        await _handle_admin_reply_msg(update, context)
        return

    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context)
        return

    # Промо (публічний)
    if state == AWAIT_PROMO_CODE and not is_admin(uid):
        result = check_promo(text, uid)
        if result["ok"]:
            apply_promo(text, uid)
            context.user_data["promo_discount"] = result["discount"]
            context.user_data["promo_code"]     = text.upper().strip()
            log_action("promo_used", uid, {"code": text})
        context.user_data["state"] = None
        await update.message.reply_text(result["msg"], parse_mode="HTML")
        return

    # Зворотний зв'язок
    if state == AWAIT_FEEDBACK:
        fid = gen_id("fb_")
        feedbacks = _load(FEEDBACK_KEY)
        feedbacks[fid] = {
            "user_id":    uid,
            "username":   update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "feedback":   text,
            "created_at": now_str(),
            "status":     "new",
        }
        _save(FEEDBACK_KEY, feedbacks)
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"💬 <b>Відгук #{esc(fid)}</b>\n"
                    f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
                    f"🆔 {uid}\n📝 {esc(text)}",
                    reply_markup=mkb([InlineKeyboardButton("✍️ Відповісти", callback_data=f"reply_fb:{fid}")]),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        context.user_data["state"] = None
        await update.message.reply_text("✅ <b>Дякуємо за відгук!</b> Відповімо найближчим часом. 🌸", parse_mode="HTML")
        return

    # ── Анкета ──
    if state == AWAIT_FIO:
        if len(text.split()) < 2:
            await update.message.reply_text("❌ Мінімум 2 слова (Прізвище Ім'я).")
            return
        context.user_data["fio"] = text
        context.user_data["state"] = AWAIT_DOB
        await update.message.reply_text(
            "📅 <b>Крок 2/7</b> — Дата народження\nФормат: <b>ДД.ММ.РРРР</b>", parse_mode="HTML")
        return

    if state == AWAIT_DOB:
        if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", text):
            await update.message.reply_text("❌ Формат: ДД.ММ.РРРР")
            return
        context.user_data["dob"] = text
        context.user_data["state"] = AWAIT_SEX
        await update.message.reply_text(
            "👤 <b>Крок 3/7</b> — Стать",
            reply_markup=mkb([InlineKeyboardButton("♂️ Чоловік", callback_data="sex:M"),
                               InlineKeyboardButton("♀️ Жінка",   callback_data="sex:W")]),
            parse_mode="HTML",
        )
        return

    if state == AWAIT_ADDRESS:
        context.user_data["address"] = "" if text.lower() in ("/skip", "skip") else text
        context.user_data["state"] = AWAIT_RIGHTS_CHOICE
        await _ask_rights(update, context)
        return

    # Адмін стани
    if is_admin(uid):
        await _handle_admin_state(update, context, state, text, uid)
        return

    # AI підтримка
    if AI_ENABLED and get_setting("ai_support") and text and not state:
        history = context.user_data.get("ai_history", [])
        reply = await ai_support_reply(text, history[-8:])
        if reply:
            history.append({"role": "user", "content": text})
            history.append({"role": "assistant", "content": reply})
            context.user_data["ai_history"] = history[-16:]
            await update.message.reply_text(
                f"🤖 {reply}\n\n<i>Для замовлення натисніть /start</i>", parse_mode="HTML")
            return

    # Переслати адміну
    try:
        fwd = await update.message.forward(ADMIN_IDS[0])
        await context.bot.send_message(
            ADMIN_IDS[0],
            f"📩 <b>Повідомлення</b>\n"
            f"👤 {esc(update.effective_user.first_name)} | 🆔 {uid}\n📅 {now_fmt()}",
            reply_to_message_id=fwd.message_id, parse_mode="HTML",
        )
        await update.message.reply_text("✉️ Повідомлення передано адміністратору.", parse_mode="HTML")
    except Exception as e:
        logger.error("Forward error: %s", e)


# ── Media handler ──────────────────────────────────────────────────────────────

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    state = context.user_data.get("state")

    if state == AWAIT_PHOTO and update.message.photo:
        await _process_order(update, context, uid)
    elif is_admin(uid) and state == AWAIT_ORDER_COMPLETE_FILE:
        await _process_complete_order_files(update, context)
    else:
        await _forward_receipt(update, context, uid)


async def _process_order(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    photo_file  = await update.message.photo[-1].get_file()
    photo_bytes = bytes(await photo_file.download_as_bytearray())

    oid = gen_id("ord_")
    context.user_data["order_id"] = oid

    # Зберегти фото
    os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)
    photo_path = os.path.join(ORDER_PHOTOS_DIR, f"{oid}.png")
    with open(photo_path, "wb") as f:
        f.write(photo_bytes)

    # Транслітерація ПІБ
    fio_ua = context.user_data.get("fio", "")
    if AI_ENABLED and fio_ua:
        context.user_data["fio_en"] = await ai_transliterate(fio_ua)

    values_data = gen_values_dict({**context.user_data, "order_id": oid})
    js_content  = values_to_js(values_data)

    discount    = context.user_data.get("promo_discount", 0)
    base_price  = context.user_data.get("tariff_price", 0)
    final_price = int(base_price * (100 - discount) / 100)

    orders = _load(ORDERS_KEY)
    orders[oid] = {
        "user_id":     uid,
        "tariff":      context.user_data.get("tariff"),
        "tariff_name": context.user_data.get("tariff_name"),
        "fio":         context.user_data.get("fio"),
        "dob":         context.user_data.get("dob"),
        "sex":         context.user_data.get("sex"),
        "address":     context.user_data.get("address", ""),
        "is_rights":   context.user_data.get("is_rights", True),
        "is_zagran":   context.user_data.get("is_zagran", True),
        "is_diploma":  context.user_data.get("is_diploma", False),
        "is_study":    context.user_data.get("is_study", False),
        "promo":       context.user_data.get("promo_code"),
        "discount":    discount,
        "price":       base_price,
        "final_price": final_price,
        "created_at":  now_str(),
        "status":      "pending",
        "photo_path":  photo_path,
        "values_data": values_data,
        "js_content":  js_content,
    }
    _save(ORDERS_KEY, orders)

    users = _load(USERS_KEY)
    if uid in users:
        users[uid]["total_orders"] = users[uid].get("total_orders", 0) + 1
        _save(USERS_KEY, users)

    await _handle_referral_bonus(context, uid)
    log_action("new_order", uid, {"oid": oid, "tariff": context.user_data.get("tariff")})

    price_text = f"{final_price}₴" + (f" (знижка {discount}%)" if discount else "")
    caption = (
        f"📦 <b>НОВЕ ЗАМОВЛЕННЯ #{esc(oid)}</b>\n\n"
        f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
        f"🆔 {uid}\n"
        f"💎 {esc(context.user_data.get('tariff_name',''))} — {price_text}\n"
        f"📝 ПІБ: {esc(context.user_data.get('fio',''))}\n"
        f"📅 ДН: {esc(context.user_data.get('dob',''))}\n"
        f"👤 {'Чоловік' if context.user_data.get('sex')=='M' else 'Жінка'}\n"
        f"🚗 Права: {'Так' if context.user_data.get('is_rights') else 'Ні'}\n"
        f"🌍 Загран: {'Так' if context.user_data.get('is_zagran') else 'Ні'}\n"
        f"🎓 Диплом: {'Так' if context.user_data.get('is_diploma') else 'Ні'}\n"
        f"⏰ {now_fmt()}"
    )

    has_gh = bool(PAGES_GH_TOKEN) and bool(os.getenv("GH_TOKEN_2"))
    admin_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Підтвердити + деплой", callback_data=f"adm_approve_deploy:{uid}:{oid}")],
        [InlineKeyboardButton("✅ Підтвердити (без деплою)", callback_data=f"adm_approve:{uid}:{oid}")],
        [InlineKeyboardButton("❌ Відхилити", callback_data=f"adm_reject:{uid}:{oid}")],
        *([[InlineKeyboardButton("🚀 Деплой вручну", callback_data=f"adm_push_pages:{uid}:{oid}")]] if has_gh else []),
    ])

    for admin_id in ADMIN_IDS:
        try:
            buf = io.BytesIO(photo_bytes); buf.name = f"photo_{oid}.png"
            await context.bot.send_photo(admin_id, buf, caption=caption,
                                         reply_markup=admin_kb, parse_mode="HTML")
            js_buf = io.BytesIO(js_content.encode()); js_buf.name = f"values_{oid}.js"
            await context.bot.send_document(admin_id, js_buf, caption=f"📄 values.js для #{oid}")
        except Exception as e:
            logger.error("Admin notify error: %s", e)

    await notify_group_photo(context.bot, photo_bytes, caption, admin_kb)
    await update.message.reply_text(
        f"✅ <b>Замовлення #{esc(oid)} прийнято!</b>\n\n"
        f"💳 До сплати: <b>{price_text}</b>\n\n"
        "📌 Далі:\n1️⃣ Отримаєте реквізити\n2️⃣ Надішліть фото чека\n3️⃣ Отримаєте посилання ⚡️",
        parse_mode="HTML",
    )
    context.user_data.clear()


async def _forward_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    orders = _load(ORDERS_KEY)
    user_orders = sorted(
        [(oid, o) for oid, o in orders.items()
         if o.get("user_id") == uid and o.get("status") in ("pending", "approved")],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    if not user_orders:
        await update.message.reply_text(
            "⚠️ <b>Активне замовлення не знайдено.</b>\n\nОформіть замовлення через /start",
            parse_mode="HTML")
        return

    last_oid, last_order = user_orders[0]
    expected_price = last_order.get("final_price", 0)

    await update.message.reply_text("✅ <b>Чек отримано!</b> Перевіряємо...", parse_mode="HTML")

    # Завантажити байти чека
    receipt_bytes = b""
    if update.message.photo:
        f = await update.message.photo[-1].get_file()
        receipt_bytes = bytes(await f.download_as_bytearray())
    elif update.message.document and (update.message.document.mime_type or "").startswith("image/"):
        f = await update.message.document.get_file()
        receipt_bytes = bytes(await f.download_as_bytearray())

    settings = load_settings()
    ai_result = {"ok": None, "confidence": 0, "amount": None, "reason": "", "auto_approved": False}
    if AI_ENABLED and settings.get("ai_check_receipts", True) and receipt_bytes:
        ai_result = await ai_check_receipt(receipt_bytes, expected_price)

    auto_ok = (
        ai_result.get("auto_approved", False)
        and settings.get("ai_auto_deploy", True)
        and bool(PAGES_GH_TOKEN)
        and bool(os.getenv("GH_TOKEN_2"))
    )
    confidence = ai_result.get("confidence", 0)

    if auto_ok:
        orders[last_oid]["status"] = "approved"
        orders[last_oid]["receipt_ai"] = ai_result
        _save(ORDERS_KEY, orders)
        log_action("ai_receipt_approved", uid, {"oid": last_oid, "confidence": confidence})

        await update.message.reply_text(
            f"🎉 <b>Оплату підтверджено автоматично!</b>\n🤖 AI ({confidence}% впевненість)\n⏳ Готуємо кабінет...",
            parse_mode="HTML")

        try:
            folder1_url = await _run_chain_deploy(last_oid, last_order)
            orders = _load(ORDERS_KEY)
            sub_end = calc_subscription_end(orders[last_oid].get("tariff", ""), load_tariffs())
            orders[last_oid].update({
                "pages_url": folder1_url, "status": "deployed",
                "deployed_at": now_str(), "subscription_end": sub_end,
                "notified_days": [],
            })
            _save(ORDERS_KEY, orders)

            sub_line = f"\n📅 Підписка до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>" if sub_end else "\n♾ Підписка: безстрокова"
            await update.message.reply_text(
                f"✅ <b>Кабінет готовий!</b>\n\n🔗 {folder1_url}{sub_line}\n\n📋 Замовлення: <code>{esc(last_oid)}</code>",
                parse_mode="HTML")

            await notify_group(context.bot,
                f"🤖 <b>АВТО-ДЕПЛОЙ</b>\n"
                f"👤 {esc(update.effective_user.first_name)} | <code>{uid}</code>\n"
                f"📦 <code>{esc(last_oid)}</code> | AI: {confidence}%\n🔗 {folder1_url}")
        except Exception as e:
            logger.error("Auto-deploy error: %s", e, exc_info=True)
            await update.message.reply_text("⚠️ Оплату підтверджено, але деплой не вдався. Адміністратор виправить.", parse_mode="HTML")
        return

    # Надіслати чек адміну
    first_name = esc(update.effective_user.first_name)
    username   = esc(update.effective_user.username or "")
    ai_line = ""
    if AI_ENABLED and ai_result.get("ok") is not None:
        ai_line = (
            f"\n🤖 AI: {'✅' if ai_result.get('ok') else '⚠️'} {confidence}%"
            + (f" | {ai_result.get('amount')}₴" if ai_result.get("amount") else "")
        )

    info = (
        f"📑 <b>Чек від клієнта</b>\n"
        f"👤 {first_name} (@{username})\n🆔 {uid}\n"
        f"📦 <code>{esc(last_oid)}</code>\n💰 Очікується: {expected_price}₴\n"
        f"📅 {now_fmt()}{ai_line}"
    )
    receipt_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Підтвердити + деплой", callback_data=f"adm_approve_deploy:{uid}:{last_oid}")],
        [InlineKeyboardButton("✅ Підтвердити (без деплою)", callback_data=f"adm_approve:{uid}:{last_oid}")],
        [InlineKeyboardButton("❌ Відхилити", callback_data=f"adm_reject:{uid}:{last_oid}")],
    ])

    for admin_id in ADMIN_IDS:
        try:
            if receipt_bytes:
                buf = io.BytesIO(receipt_bytes); buf.name = "receipt.jpg"
                await context.bot.send_photo(admin_id, buf, caption=info,
                                             reply_markup=receipt_kb, parse_mode="HTML")
            else:
                fwd = await update.message.forward(admin_id)
                await context.bot.send_message(admin_id, info,
                    reply_to_message_id=fwd.message_id, reply_markup=receipt_kb, parse_mode="HTML")
        except Exception as e:
            logger.error("Receipt fwd (admin %s): %s", admin_id, e)

    if GROUP_CHAT_ID:
        try:
            if receipt_bytes:
                buf = io.BytesIO(receipt_bytes); buf.name = "receipt.jpg"
                await context.bot.send_photo(GROUP_CHAT_ID, buf, caption=info,
                                             reply_markup=receipt_kb, parse_mode="HTML")
            else:
                await context.bot.send_message(GROUP_CHAT_ID, info,
                    reply_markup=receipt_kb, parse_mode="HTML")
        except Exception as e:
            logger.error("Receipt fwd (group): %s", e)

    await update.message.reply_text(
        "✅ <b>Чек отримано!</b>\nАдміністратор перевірить найближчим часом. 🌸", parse_mode="HTML")


# ── Chain deploy helper ────────────────────────────────────────────────────────

async def _run_chain_deploy(oid: str, order: dict) -> str:
    """
    Повний ланцюжок:
      1. Оновлює 2/index.html значеннями замовлення
      2. Пушить папку 2 → отримує URL
      3. Генерує QR → 1/assets/q.png
      4. Пушить папку 1 → повертає URL папки 1
    """
    values_data = order.get("values_data")
    if not values_data:
        values_data = gen_values_dict({
            "fio": order.get("fio", ""), "dob": order.get("dob", ""),
            "sex": order.get("sex", "M"), "is_rights": order.get("is_rights", True),
            "is_zagran": order.get("is_zagran", True), "is_diploma": order.get("is_diploma", False),
            "is_study": order.get("is_study", False), "address": order.get("address", ""),
            "order_id": oid,
        })

    result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: chain_deploy.run_full_chain(values_data, order_id=oid)
    )
    return result["folder1_url"]


async def _handle_referral_bonus(context, uid: str):
    users = _load(USERS_KEY)
    u = users.get(uid, {})
    if u.get("has_bought"):
        return
    ref_by = u.get("referred_by")
    if ref_by and ref_by in users:
        users[ref_by]["balance"]   = users[ref_by].get("balance", 0) + REFERRAL_REWARD
        users[ref_by]["ref_count"] = users[ref_by].get("ref_count", 0) + 1
        try:
            await context.bot.send_message(
                ref_by,
                f"💰 <b>Реферальний бонус +{REFERRAL_REWARD}₴!</b>\nБаланс: {users[ref_by]['balance']}₴",
                parse_mode="HTML")
        except Exception:
            pass
    users[uid]["has_bought"] = True
    _save(USERS_KEY, users)


# ── Admin: reply helpers ───────────────────────────────────────────────────────

async def _handle_admin_reply_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    m = re.search(r"🆔\s*(\d+)", reply_text)
    if not m:
        await update.message.reply_text("⚠️ Не знайдено ID клієнта в цьому повідомленні.")
        return
    client_id = m.group(1)
    try:
        await context.bot.send_message(
            client_id,
            f"💬 <b>Відповідь адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸",
            parse_mode="HTML")
        await update.message.reply_text(f"✅ Відповідь надіслано → {client_id}")
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: {e}")


async def _do_reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = context.user_data.get("reply_to_uid")
    fid    = context.user_data.get("reply_fb_id")
    if target:
        try:
            await context.bot.send_message(
                target,
                f"💬 <b>Відповідь адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸",
                parse_mode="HTML")
            if fid:
                feedbacks = _load(FEEDBACK_KEY)
                if fid in feedbacks:
                    feedbacks[fid]["status"]      = "replied"
                    feedbacks[fid]["admin_reply"] = update.message.text
                    _save(FEEDBACK_KEY, feedbacks)
            await update.message.reply_text(f"✅ Відповідь надіслана → {target}")
        except Exception as e:
            await update.message.reply_text(f"❌ Помилка: {e}")
    context.user_data["state"] = None
    context.user_data.pop("reply_to_uid", None)
    context.user_data.pop("reply_fb_id",  None)


# ── Admin state machine ────────────────────────────────────────────────────────

async def _handle_admin_state(update, context, state, text, uid):
    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context); return

    if state == AWAIT_WELCOME_TEXT:
        s = load_settings(); s["welcome_text"] = text; save_settings(s)
        context.user_data["state"] = None
        await update.message.reply_text("✅ Текст привітання оновлено!"); return

    if state == AWAIT_BROADCAST:
        context.user_data["broadcast_text"] = text
        context.user_data["state"] = None
        users = _load(USERS_KEY)
        await update.message.reply_text(
            f"📢 <b>Попередній перегляд:</b>\n\n{text}\n\n👥 Отримають: {len(users)}",
            reply_markup=mkb(
                [InlineKeyboardButton("✅ Надіслати", callback_data="broadcast_go"),
                 InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")],
            ), parse_mode="HTML"); return

    if state == AWAIT_PROMO_CODE:
        context.user_data["new_promo_code"] = text.upper().strip()
        context.user_data["state"] = AWAIT_PROMO_DISCOUNT
        await update.message.reply_text(f"✅ Код: <b>{esc(context.user_data['new_promo_code'])}</b>\n\nВведіть знижку у %:", parse_mode="HTML"); return

    if state == AWAIT_PROMO_DISCOUNT:
        try:
            context.user_data["new_promo_discount"] = int(text)
            context.user_data["state"] = AWAIT_PROMO_USES
            await update.message.reply_text(f"✅ Знижка: {text}%\n\nМакс. використань (0 = ∞):")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_PROMO_USES:
        try:
            uses = int(text)
            code = context.user_data["new_promo_code"]
            promos = load_promos()
            promos[code] = {"discount": context.user_data["new_promo_discount"],
                            "max_uses": uses, "uses": 0, "active": True,
                            "used_by": [], "created_at": now_str(), "created_by": uid}
            save_promos(promos)
            context.user_data["state"] = None
            log_action("promo_created", uid, {"code": code})
            await update.message.reply_text(
                f"✅ <b>Промо-код</b> <code>{esc(code)}</code>\n💰 {context.user_data['new_promo_discount']}%  |  👥 {'∞' if not uses else uses}",
                parse_mode="HTML")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_USER_SEARCH:
        context.user_data["state"] = None
        users = _load(USERS_KEY)
        q = text.strip().lstrip("@")
        found = [(uid2, u) for uid2, u in users.items()
                 if q in (u.get("username") or "") or q in str(uid2) or q in (u.get("first_name") or "")]
        if not found:
            await update.message.reply_text("🔍 Не знайдено."); return
        await _send_user_card(update, context, found[0][0], found[0][1]); return

    if state == AWAIT_BALANCE_UID:
        context.user_data["balance_target_uid"] = text.strip()
        context.user_data["state"] = AWAIT_BALANCE_AMOUNT
        await update.message.reply_text("💰 Введіть суму (+ або -):")
        return

    if state == AWAIT_BALANCE_AMOUNT:
        try:
            amount = int(text)
            target_uid = context.user_data.get("balance_target_uid")
            users = _load(USERS_KEY)
            if target_uid not in users:
                await update.message.reply_text("❌ Не знайдено.")
            else:
                users[target_uid]["balance"] = max(0, users[target_uid].get("balance", 0) + amount)
                _save(USERS_KEY, users)
                context.user_data["state"] = None
                await update.message.reply_text(
                    f"✅ Баланс {target_uid}: {'+' if amount>=0 else ''}{amount}₴\nНовий: {users[target_uid]['balance']}₴")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_CUSTOM_PAYMENT_TEXT:
        parts = text.split("\n")
        if len(parts) >= 2:
            s = load_settings()
            s["payment_card"]   = parts[0].strip()
            s["payment_holder"] = parts[1].strip()
            if len(parts) >= 3:
                s["payment_link"] = parts[2].strip()
            save_settings(s)
            context.user_data["state"] = None
            await update.message.reply_text("✅ Реквізити оновлено!")
        else:
            await update.message.reply_text("❌ Мінімум 2 рядки: картка + отримувач")
        return

    if state == AWAIT_REJECT_REASON:
        oid = context.user_data.get("reject_oid")
        client_uid = context.user_data.get("reject_uid")
        if oid and client_uid:
            orders = _load(ORDERS_KEY)
            if oid in orders:
                orders[oid]["status"] = "rejected"
                orders[oid]["reject_reason"] = text
                _save(ORDERS_KEY, orders)
            try:
                await context.bot.send_message(client_uid,
                    f"❌ <b>Замовлення #{esc(oid)} відхилено</b>\n\nПричина: {esc(text)}",
                    parse_mode="HTML")
            except Exception:
                pass
            context.user_data["state"] = None
            await update.message.reply_text(f"✅ Замовлення #{oid} відхилено.")
        return

    if state in (AWAIT_TARIFF_EDIT_PRICE, AWAIT_TARIFF_EDIT_NAME, AWAIT_TARIFF_EDIT_EMOJI):
        key = context.user_data.get("edit_tariff_key")
        tariffs = load_tariffs()
        if key in tariffs:
            if state == AWAIT_TARIFF_EDIT_PRICE:
                try:
                    tariffs[key]["price"] = int(text)
                    save_tariffs(tariffs)
                    context.user_data["state"] = None
                    await update.message.reply_text(f"✅ Ціна → {text}₴")
                except ValueError:
                    await update.message.reply_text("❌ Введіть число!")
            elif state == AWAIT_TARIFF_EDIT_NAME:
                tariffs[key]["name"] = text
                save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Назва → {esc(text)}")
            else:
                tariffs[key]["emoji"] = text.strip()
                save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Емоджі → {text}")
        return

    if state == AWAIT_TARIFF_NAME:
        context.user_data["new_t_name"] = text
        context.user_data["state"] = AWAIT_TARIFF_PRICE
        await update.message.reply_text("💰 Ціна (₴):"); return

    if state == AWAIT_TARIFF_PRICE:
        try:
            context.user_data["new_t_price"] = int(text)
            context.user_data["state"] = AWAIT_TARIFF_DAYS
            await update.message.reply_text("📅 Кількість днів (0 = безстроково):")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_TARIFF_DAYS:
        try:
            context.user_data["new_t_days"] = int(text) or None
            context.user_data["state"] = AWAIT_TARIFF_EMOJI
            await update.message.reply_text("😊 Емоджі (наприклад: 🌟):")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_TARIFF_EMOJI:
        name  = context.user_data["new_t_name"]
        price = context.user_data["new_t_price"]
        days  = context.user_data.get("new_t_days")
        emj   = text.strip() or "📦"
        key   = re.sub(r"[^a-z0-9]", "_", name.lower())[:20]
        tariffs = load_tariffs()
        base, c = key, 1
        while key in tariffs:
            key = f"{base}_{c}"; c += 1
        tariffs[key] = {"name": name, "price": price, "days": days, "emoji": emj, "active": True}
        save_tariffs(tariffs)
        context.user_data["state"] = None
        log_action("tariff_created", uid, {"key": key})
        await update.message.reply_text(
            f"✅ Тариф: {emj} {esc(name)} — {price}₴  ({'∞' if not days else f'{days} дн.'})"); return


# ── Admin: complete order (manual files) ──────────────────────────────────────

async def _process_complete_order_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    oid = context.user_data.get("complete_oid")
    client_uid = context.user_data.get("complete_uid")
    if not oid or not client_uid:
        return
    try:
        caption = f"📁 Ваші файли за замовленням #{esc(oid)} 🌸"
        if update.message.document:
            await context.bot.send_document(client_uid, update.message.document.file_id, caption=caption)
        elif update.message.photo:
            await context.bot.send_photo(client_uid, update.message.photo[-1].file_id, caption=caption)
        elif update.message.video:
            await context.bot.send_video(client_uid, update.message.video.file_id, caption=caption)

        orders = _load(ORDERS_KEY)
        if oid in orders:
            orders[oid]["status"] = "completed"
            orders[oid]["completed_at"] = now_str()
            _save(ORDERS_KEY, orders)
            users = _load(USERS_KEY)
            if client_uid in users:
                users[client_uid]["total_spent"] = (
                    users[client_uid].get("total_spent", 0) + orders[oid].get("final_price", 0))
                _save(USERS_KEY, users)

        log_action("order_completed", None, {"oid": oid})
        await update.message.reply_text(f"✅ Файли надіслані клієнту {client_uid} (#{oid})")
        context.user_data["state"] = None
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: {e}")


# ── Admin panel ────────────────────────────────────────────────────────────────

@admin_only
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users  = _load(USERS_KEY)
    orders = _load(ORDERS_KEY)
    pending = sum(1 for o in orders.values() if o.get("status") == "pending")
    await safe_edit(q,
        f"⚙️ <b>Адмін-панель</b>\n\n"
        f"👥 Користувачів: <b>{len(users)}</b>  ·  ⏳ В черзі: <b>{pending}</b>\n"
        f"🔑 GH основний: {'✅' if PAGES_GH_TOKEN else '❌'}  ·  "
        f"🔑 GH інший: {'✅' if os.getenv('GH_TOKEN_2') else '❌'}\n"
        f"🕐 {now_fmt()}",
        mkb(
            [InlineKeyboardButton("📊 Статистика",   callback_data="adm:stats"),
             InlineKeyboardButton("📋 Замовлення",   callback_data="adm:orders")],
            [InlineKeyboardButton("👥 Користувачі",  callback_data="adm:users"),
             InlineKeyboardButton("🔍 Пошук",        callback_data="adm:search")],
            [InlineKeyboardButton("💰 Тарифи",       callback_data="adm:tariffs"),
             InlineKeyboardButton("🎟 Промо-коди",   callback_data="adm:promos")],
            [InlineKeyboardButton("📢 Розсилка",     callback_data="adm:broadcast"),
             InlineKeyboardButton("💬 Відгуки",      callback_data="adm:feedbacks")],
            [InlineKeyboardButton("⚙️ Налаштування", callback_data="adm:settings"),
             InlineKeyboardButton("📜 Логи",         callback_data="adm:logs")],
            [InlineKeyboardButton("🚀 Деплой (ланцюжок)", callback_data="adm:chain_deploy")],
            [InlineKeyboardButton("📥 Вивантажити БД", callback_data="adm:export_db")],
            back_btn("home"),
        ),
    )


@admin_only
async def adm_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users  = _load(USERS_KEY)
    orders = _load(ORDERS_KEY)
    total_o    = len(orders)
    done_o     = sum(1 for o in orders.values() if o.get("status") == "completed")
    pending_o  = sum(1 for o in orders.values() if o.get("status") == "pending")
    deployed_o = sum(1 for o in orders.values() if o.get("status") == "deployed")
    rejected_o = sum(1 for o in orders.values() if o.get("status") == "rejected")
    revenue    = sum(o.get("final_price", 0) for o in orders.values()
                     if o.get("status") in ("completed", "deployed"))
    yesterday  = (datetime.now(TIMEZONE) - timedelta(hours=24)).isoformat()
    new_u_24h  = sum(1 for u in users.values() if u.get("joined_date", "") > yesterday)
    new_o_24h  = sum(1 for o in orders.values() if o.get("created_at", "") > yesterday)

    await safe_edit(q,
        f"📊 <b>Статистика</b>  {now_fmt()}\n\n"
        f"👥 Юзерів: <b>{len(users)}</b>  (+{new_u_24h} за 24г)\n"
        f"📦 Замовлення: <b>{total_o}</b>  |  ✅ {done_o}  🌐 {deployed_o}  ⏳ {pending_o}  ❌ {rejected_o}\n"
        f"📈 За 24г: +{new_o_24h} замовлень\n\n"
        f"💰 Дохід: <b>{revenue}₴</b>",
        mkb(back_btn("admin_panel")),
    )


@admin_only
async def adm_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    orders = _load(ORDERS_KEY)
    sf = context.user_data.get("orders_filter", "pending")
    filtered = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("status") == sf],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    status_map = {"pending":"⏳","approved":"✅","completed":"🎉","rejected":"❌","deployed":"🌐"}
    st = status_map.get(sf, "📋")
    text = f"📋 <b>Замовлення {st}</b>  ({len(filtered)})\n\n"
    kb_rows = []
    for oid, o in filtered[:15]:
        text += f"#{esc(oid)}  {esc(o.get('fio','?')[:20])}  {o.get('created_at','')[:10]}\n"
        kb_rows.append([InlineKeyboardButton(
            f"{st} #{oid} · {o.get('fio','?')[:18]}", callback_data=f"adm_order_view:{oid}")])
    kb_rows.append([
        InlineKeyboardButton("⏳", callback_data="adm_order_filter:pending"),
        InlineKeyboardButton("✅", callback_data="adm_order_filter:approved"),
        InlineKeyboardButton("🎉", callback_data="adm_order_filter:completed"),
        InlineKeyboardButton("🌐", callback_data="adm_order_filter:deployed"),
        InlineKeyboardButton("❌", callback_data="adm_order_filter:rejected"),
    ])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text or "📭 Замовлень немає.", InlineKeyboardMarkup(kb_rows))


@admin_only
async def adm_order_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    oid = q.data.split(":")[1]
    o   = _load(ORDERS_KEY).get(oid, {})
    uid2 = o.get("user_id", "?")
    status_map = {"pending":"⏳","approved":"✅","completed":"🎉","rejected":"❌","deployed":"🌐"}
    st = status_map.get(o.get("status",""), o.get("status","?"))
    url = esc(o.get("pages_url", ""))
    pages_line = f"\n🔗 <a href='{url}'>Відкрити кабінет</a>" if url else ""
    text = (
        f"📋 <b>#{esc(oid)}</b>  {st}\n"
        f"👤 {esc(o.get('fio','?'))}  ·  ДН: {esc(o.get('dob','?'))}\n"
        f"🆔 {uid2}\n"
        f"💎 {esc(o.get('tariff_name','?'))}  ·  💰 {o.get('final_price','?')}₴\n"
        f"📅 {o.get('created_at','')[:16]}{pages_line}"
    )
    has_gh = bool(PAGES_GH_TOKEN) and bool(os.getenv("GH_TOKEN_2"))
    kb_rows = [
        [InlineKeyboardButton("✅ Підтвердити + деплой", callback_data=f"adm_approve_deploy:{uid2}:{oid}")],
        [InlineKeyboardButton("✅ Підтвердити (без деплою)", callback_data=f"adm_approve:{uid2}:{oid}"),
         InlineKeyboardButton("❌ Відхилити", callback_data=f"adm_reject:{uid2}:{oid}")],
        [InlineKeyboardButton("📨 Надіслати файли", callback_data=f"adm_complete:{uid2}:{oid}"),
         InlineKeyboardButton("💬 Написати", callback_data=f"adm_msg:{uid2}")],
    ]
    if has_gh:
        kb_rows.append([InlineKeyboardButton("🚀 Деплой вручну", callback_data=f"adm_push_pages:{uid2}:{oid}")])
    if o.get("pages_url"):
        kb_rows.append([InlineKeyboardButton("🔗 Надіслати посилання", callback_data=f"adm_send_link:{uid2}:{oid}")])
    kb_rows.append(back_btn("adm:orders"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows), disable_web_page_preview=True)


@admin_only
async def adm_order_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["orders_filter"] = update.callback_query.data.split(":")[1]
    await adm_orders(update, context)


@admin_only
async def adm_approve_deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Підтвердити оплату + одразу деплой повним ланцюжком."""
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    await safe_edit(q, "⏳ <b>Підтверджуємо і деплоємо...</b>")

    orders = _load(ORDERS_KEY)
    order  = orders.get(oid)
    if not order:
        await safe_edit(q, "❌ Замовлення не знайдено.")
        return

    orders[oid]["status"] = "approved"
    _save(ORDERS_KEY, orders)
    log_action("receipt_approved", q.from_user.id, {"oid": oid})

    try:
        await context.bot.send_message(client_uid,
            f"✅ <b>Оплату підтверджено!</b>\n⏳ Готуємо кабінет...\n📋 <code>{esc(oid)}</code>",
            parse_mode="HTML")
    except Exception as e:
        logger.error("Client notify: %s", e)

    try:
        folder1_url = await _run_chain_deploy(oid, order)

        orders = _load(ORDERS_KEY)
        sub_end = calc_subscription_end(orders[oid].get("tariff", ""), load_tariffs())
        orders[oid].update({
            "pages_url": folder1_url, "status": "deployed",
            "deployed_at": now_str(), "subscription_end": sub_end,
            "notified_days": [],
        })
        _save(ORDERS_KEY, orders)
        log_action("pages_deployed", q.from_user.id, {"oid": oid, "url": folder1_url})

        sub_line = f"\n📅 Підписка до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>" if sub_end else "\n♾ Підписка: безстрокова"
        try:
            await context.bot.send_message(client_uid,
                f"✅ <b>Кабінет готовий!</b>\n\n🔗 {folder1_url}{sub_line}\n\n"
                f"⏱ Якщо не відкривається — зачекайте 1-2 хвилини.\n📋 <code>{esc(oid)}</code>",
                parse_mode="HTML")
        except Exception as e:
            logger.error("Client deploy notify: %s", e)

        await notify_group(context.bot,
            f"🚀 <b>Деплой завершено</b>\n📦 <code>{esc(oid)}</code> | 👤 <code>{client_uid}</code>\n🔗 {folder1_url}",
            mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")]))

        await safe_edit(q,
            f"✅ <b>Підтверджено і задеплоєно!</b>\n📦 <code>{esc(oid)}</code>\n🔗 {esc(folder1_url)}",
            mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")],
                back_btn("admin_panel")))

    except Exception as e:
        logger.error("adm_approve_deploy: %s", e, exc_info=True)
        await safe_edit(q,
            f"⚠️ Оплату підтверджено, але деплой не вдався:\n<code>{esc(str(e)[:300])}</code>",
            mkb([InlineKeyboardButton("🔄 Спробувати знову", callback_data=f"adm_push_pages:{client_uid}:{oid}")],
                back_btn("admin_panel")))


async def adm_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    orders = _load(ORDERS_KEY)
    if oid in orders:
        orders[oid]["status"] = "approved"
        _save(ORDERS_KEY, orders)
    s = load_settings()
    price = orders.get(oid, {}).get("final_price", "?")
    payment_text = (
        f"✅ <b>Замовлення #{esc(oid)} підтверджено!</b>\n\n"
        f"💳 Картка: <code>{esc(s.get('payment_card','—'))}</code>\n"
        f"👤 {esc(s.get('payment_holder','—'))}\n"
        f"🔗 {s.get('payment_link','')}\n\n"
        f"💰 Сума: <b>{price}₴</b>\n\n📤 Надішліть скріншот оплати сюди!"
    )
    try:
        await context.bot.send_message(client_uid, payment_text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        await q.answer(f"Помилка: {e}", show_alert=True)
        return
    log_action("order_approved", q.from_user.id, {"oid": oid})
    kb_rows = [
        [InlineKeyboardButton("🚀 Деплой (ланцюжок)", callback_data=f"adm_push_pages:{client_uid}:{oid}")],
        [InlineKeyboardButton("📨 Файли вручну", callback_data=f"adm_complete:{client_uid}:{oid}")],
        back_btn("adm:orders"),
    ]
    await safe_edit(q, f"✅ Реквізити надіслані {client_uid}. Оберіть спосіб деплою:",
                    InlineKeyboardMarkup(kb_rows))


@admin_only
async def adm_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["reject_uid"] = parts[1]
    context.user_data["reject_oid"] = parts[2]
    context.user_data["state"]      = AWAIT_REJECT_REASON
    await safe_edit(q, f"❌ <b>Відхилення #{esc(parts[2])}</b>\n\nВведіть причину:")


@admin_only
async def adm_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["complete_uid"] = parts[1]
    context.user_data["complete_oid"] = parts[2]
    context.user_data["state"]        = AWAIT_ORDER_COMPLETE_FILE
    await safe_edit(q, f"📨 <b>Файли для #{esc(parts[2])}</b>\n\nНадішліть файли для клієнта {parts[1]}.")


@admin_only
async def adm_push_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Підтвердження деплою ланцюжком."""
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]

    if not PAGES_GH_TOKEN or not os.getenv("GH_TOKEN_2"):
        await safe_edit(q,
            "❌ <b>Токени GitHub не встановлено!</b>\n\n"
            "Потрібні: <code>PAGES_GH_TOKEN</code> і <code>GH_TOKEN_2</code> в GitHub Secrets.",
            mkb(back_btn("admin_panel")))
        return

    order = _load(ORDERS_KEY).get(oid, {})
    await safe_edit(q,
        f"🚀 <b>Підтвердіть деплой</b>\n\n"
        f"📦 <code>{esc(oid)}</code>\n👤 <code>{client_uid}</code>\n📝 {esc(order.get('fio','?'))}\n\n"
        "Буде виконано:\n1️⃣ Оновлення 2/index.html\n2️⃣ Пуш папки 2 → URL\n"
        "3️⃣ QR → 1/assets/q.png\n4️⃣ Пуш папки 1 → посилання клієнту",
        mkb([InlineKeyboardButton("✅ Деплоїти", callback_data=f"adm_push_go:{client_uid}:{oid}")],
            [InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")]))


@admin_only
async def adm_push_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    await safe_edit(q, "⏳ <b>Деплоємо ланцюжком...</b>\nЦе може зайняти 30–60 секунд.")

    orders = _load(ORDERS_KEY)
    order  = orders.get(oid)
    if not order:
        await safe_edit(q, "❌ Замовлення не знайдено.")
        return

    try:
        folder1_url = await _run_chain_deploy(oid, order)

        orders = _load(ORDERS_KEY)
        sub_end = calc_subscription_end(orders[oid].get("tariff", ""), load_tariffs())
        orders[oid].update({
            "pages_url": folder1_url, "status": "deployed",
            "deployed_at": now_str(), "subscription_end": sub_end,
            "notified_days": [],
        })
        _save(ORDERS_KEY, orders)
        log_action("pages_deployed", q.from_user.id, {"oid": oid, "url": folder1_url})

        sub_line = f"\n📅 Підписка до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>" if sub_end else "\n♾ Підписка: безстрокова"
        try:
            await context.bot.send_message(client_uid,
                f"✅ <b>Кабінет готовий!</b>\n\n🔗 {folder1_url}{sub_line}\n\n"
                f"⏱ Якщо не відкривається — зачекайте 1-2 хв.\n📋 <code>{esc(oid)}</code>",
                parse_mode="HTML")
        except Exception as e:
            logger.error("Client notify: %s", e)

        await notify_group(context.bot,
            f"🚀 <b>Деплой завершено</b>\n"
            f"📦 <code>{esc(oid)}</code> | 👤 <code>{client_uid}</code>\n"
            f"📝 {esc(order.get('fio',''))}\n🔗 {folder1_url}",
            mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")]))

        await safe_edit(q,
            f"✅ <b>Деплой успішний!</b>\n📦 <code>{esc(oid)}</code>\n🔗 <code>{esc(folder1_url)}</code>\n\n"
            f"✅ Посилання надіслано клієнту.\n⏱ Сайт активний через ~1-2 хв.",
            mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")],
                back_btn("admin_panel")))

    except Exception as e:
        logger.error("adm_push_go: %s", e, exc_info=True)
        await safe_edit(q,
            f"❌ <b>Помилка деплою</b>\n\n<code>{esc(str(e)[:500])}</code>",
            mkb(back_btn("admin_panel")))


@admin_only
async def adm_chain_deploy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Деплой ланцюжком без конкретного замовлення (просто оновити сайт)."""
    q = update.callback_query
    has_gh = bool(PAGES_GH_TOKEN) and bool(os.getenv("GH_TOKEN_2"))
    status = "✅ Готовий" if has_gh else "❌ Потрібні PAGES_GH_TOKEN і GH_TOKEN_2"
    await safe_edit(q,
        f"🚀 <b>Деплой ланцюжком</b>\n\n{status}\n\n"
        "Ця дія:\n1️⃣ Пушить папку 2 → отримує URL\n"
        "2️⃣ Генерує QR з URL → 1/assets/q.png\n"
        "3️⃣ Пушить папку 1 (з новим QR)\n\n"
        "Без прив'язки до замовлення — просто оновити сайт.",
        mkb([InlineKeyboardButton("🚀 Запустити", callback_data="chain_deploy_run")] if has_gh else [],
            back_btn("admin_panel")))


@admin_only
async def adm_chain_deploy_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_edit(q, "⏳ <b>Запускаємо ланцюжок деплою...</b>")
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: chain_deploy.run_full_chain()
        )
        folder2_url = result["folder2_url"]
        folder1_url = result["folder1_url"]
        log_action("chain_deploy", q.from_user.id, result)
        await safe_edit(q,
            f"🚀 <b>Ланцюжок деплою завершено!</b>\n\n"
            f"📁 Папка 2:\n🔗 {esc(folder2_url)}\n\n"
            f"📁 Папка 1 (з QR):\n🔗 <b>{esc(folder1_url)}</b>\n\n"
            f"📷 QR у <code>1/assets/q.png</code> веде на папку 2.\n"
            f"⏱ Зачекайте 1-2 хвилини, якщо сайти ще не відкрились.",
            mkb(back_btn("admin_panel")))
    except chain_deploy.DeployError as e:
        await safe_edit(q, f"❌ <b>Помилка деплою</b>\n\n<code>{esc(str(e))}</code>",
                        mkb(back_btn("admin_panel")))
    except Exception as e:
        logger.error("chain_deploy_run: %s", e, exc_info=True)
        await safe_edit(q, f"❌ <b>Непередбачена помилка</b>\n\n<code>{esc(str(e)[:500])}</code>",
                        mkb(back_btn("admin_panel")))


@admin_only
async def adm_send_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    orders = _load(ORDERS_KEY)
    order  = orders.get(oid, {})
    url    = order.get("pages_url", "")
    if not url:
        await q.answer("❌ URL не знайдено — спочатку зробіть деплой", show_alert=True)
        return
    try:
        await context.bot.send_message(client_uid,
            f"✅ <b>Кабінет готовий!</b>\n\n🔗 {url}\n\n📋 <code>{esc(oid)}</code>",
            parse_mode="HTML")
        await notify_group(context.bot,
            f"📤 Посилання надіслано\n👤 <code>{client_uid}</code> | 📦 <code>{esc(oid)}</code>\n🔗 {url}")
        await safe_edit(q, f"✅ Посилання надіслано → {client_uid}\n🔗 {esc(url)}",
                        mkb(back_btn("admin_panel")))
    except Exception as e:
        await q.answer(f"Помилка: {e}", show_alert=True)


# ── Admin: users ───────────────────────────────────────────────────────────────

@admin_only
async def adm_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users = _load(USERS_KEY)
    sorted_u = sorted(users.items(), key=lambda x: x[1].get("joined_date",""), reverse=True)[:15]
    text = f"👥 <b>Користувачі ({len(users)})</b>\n\n"
    for uid2, u in sorted_u:
        badges = ("👑" if u.get("vip") else "") + ("🚫" if u.get("banned") else "") + ("💰" if u.get("has_bought") else "🆕")
        text += f"{badges} <b>{esc(u.get('first_name','?'))}</b> (@{esc(u.get('username','?'))})\n"
        text += f"   🆔 {uid2} | 💳 {u.get('balance',0)}₴ | 👥 {u.get('ref_count',0)}\n\n"
    await safe_edit(q, text, mkb(
        [InlineKeyboardButton("🔍 Пошук", callback_data="adm:search"),
         InlineKeyboardButton("💰 Баланс", callback_data="adm:balance")],
        back_btn("admin_panel"),
    ))


@admin_only
async def adm_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_USER_SEARCH
    await safe_edit(update.callback_query, "🔍 Введіть @username, ID або ім'я:", mkb(back_btn("admin_panel")))


@admin_only
async def adm_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_BALANCE_UID
    await safe_edit(update.callback_query, "💰 Введіть ID користувача:", mkb(back_btn("admin_panel")))


async def _send_user_card(update, context, uid2: str, u: dict):
    orders = _load(ORDERS_KEY)
    count  = sum(1 for o in orders.values() if o.get("user_id") == uid2)
    text = (
        f"👤 <b>{esc(u.get('first_name','?'))}</b>\n🆔 <code>{uid2}</code>\n"
        f"📱 @{esc(u.get('username','?'))}\n💰 {u.get('balance',0)}₴  |  📦 {count} замовлень\n"
        f"👥 Рефералів: {u.get('ref_count',0)}  |  VIP: {'👑' if u.get('vip') else '—'}  |  Бан: {'🚫' if u.get('banned') else '—'}"
    )
    kb = mkb(
        [InlineKeyboardButton("🔓" if u.get("banned") else "🚫", callback_data=f"adm_ban:{uid2}"),
         InlineKeyboardButton("👤" if u.get("vip") else "👑",    callback_data=f"adm_vip:{uid2}")],
        [InlineKeyboardButton("💰 Баланс", callback_data="adm:balance"),
         InlineKeyboardButton("💬 Написати", callback_data=f"adm_msg:{uid2}")],
        back_btn("adm:users"),
    )
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


@admin_only
async def adm_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    users = _load(USERS_KEY)
    if uid2 in users:
        users[uid2]["banned"] = not users[uid2].get("banned", False)
        _save(USERS_KEY, users)
        action = "заблоковано" if users[uid2]["banned"] else "розблоковано"
        await q.answer(f"Користувача {action}!", show_alert=True)
        await _send_user_card(update, context, uid2, users[uid2])


@admin_only
async def adm_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    users = _load(USERS_KEY)
    if uid2 in users:
        users[uid2]["vip"] = not users[uid2].get("vip", False)
        _save(USERS_KEY, users)
        try:
            await context.bot.send_message(uid2,
                f"👑 <b>{'VIP-статус надано!' if users[uid2]['vip'] else 'VIP знято.'}</b>",
                parse_mode="HTML")
        except Exception:
            pass
        await q.answer("VIP змінено!", show_alert=True)
        await _send_user_card(update, context, uid2, users[uid2])


@admin_only
async def adm_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    context.user_data["state"]        = AWAIT_REPLY_TO_USER
    context.user_data["reply_to_uid"] = uid2
    context.user_data.pop("reply_fb_id", None)
    await safe_edit(q, f"💬 Повідомлення клієнту {uid2}\n\nВведіть текст:", mkb(back_btn("admin_panel")))


@admin_only
async def adm_confirm_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    uid2, amount = parts[1], int(parts[2])
    users = _load(USERS_KEY)
    if uid2 in users:
        users[uid2]["balance"] = max(0, users[uid2].get("balance", 0) - amount)
        _save(USERS_KEY, users)
        try:
            await context.bot.send_message(uid2,
                f"💰 <b>Вивід підтверджено!</b>\n{amount}₴ буде відправлено найближчим часом. 🌸",
                parse_mode="HTML")
        except Exception:
            pass
        log_action("withdraw_confirmed", q.from_user.id, {"uid": uid2, "amount": amount})
    await safe_edit(q, f"✅ Вивід {amount}₴ для {uid2} підтверджено.")


# ── Admin: tariffs ─────────────────────────────────────────────────────────────

@admin_only
async def adm_tariffs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = load_tariffs()
    text = "💰 <b>Тарифи</b>\n\n"
    kb_rows = []
    for k, t in tariffs.items():
        st = "✅" if t.get("active", True) else "❌"
        d  = "∞" if not t.get("days") else f"{t['days']}д"
        text += f"{st} {t.get('emoji','📦')} <b>{esc(t.get('name',''))}</b> — {t.get('price')}₴ ({d})\n"
        kb_rows.append([
            InlineKeyboardButton(f"{st} {t.get('name')}", callback_data=f"tariff_toggle:{k}"),
            InlineKeyboardButton("✏️", callback_data=f"tariff_edit:{k}"),
            InlineKeyboardButton("🗑️", callback_data=f"tariff_del:{k}"),
        ])
    kb_rows.append([InlineKeyboardButton("➕ Додати", callback_data="tariff_add")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def tariff_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = update.callback_query.data.split(":")[1]
    tariffs = load_tariffs()
    if key in tariffs:
        tariffs[key]["active"] = not tariffs[key].get("active", True)
        save_tariffs(tariffs)
    await adm_tariffs(update, context)


@admin_only
async def tariff_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = update.callback_query.data.split(":")[1]
    tariffs = load_tariffs()
    if key in tariffs:
        del tariffs[key]
        save_tariffs(tariffs)
    await adm_tariffs(update, context)


@admin_only
async def tariff_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    key = q.data.split(":")[1]
    context.user_data["edit_tariff_key"] = key
    t = load_tariffs().get(key, {})
    await safe_edit(q, f"✏️ <b>{t.get('emoji','📦')} {esc(t.get('name',''))}</b> — {t.get('price')}₴\n\nЩо змінити?",
        mkb([InlineKeyboardButton("📝 Назва", callback_data=f"tedit_name:{key}"),
              InlineKeyboardButton("💰 Ціна",  callback_data=f"tedit_price:{key}")],
            [InlineKeyboardButton("😊 Емоджі", callback_data=f"tedit_emoji:{key}")],
            back_btn("adm:tariffs")))


@admin_only
async def tedit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_tariff_key"] = update.callback_query.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_NAME
    await safe_edit(update.callback_query, "📝 Введіть нову назву:")


@admin_only
async def tedit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_tariff_key"] = update.callback_query.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_PRICE
    await safe_edit(update.callback_query, "💰 Введіть нову ціну (₴):")


@admin_only
async def tedit_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_tariff_key"] = update.callback_query.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_EMOJI
    await safe_edit(update.callback_query, "😊 Введіть нове емоджі:")


@admin_only
async def tariff_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_TARIFF_NAME
    await safe_edit(update.callback_query, "➕ <b>Новий тариф</b>\n\nКрок 1/4: Назва:")


# ── Admin: promos ──────────────────────────────────────────────────────────────

@admin_only
async def adm_promos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    promos = load_promos()
    text = f"🎟️ <b>Промо-коди ({len(promos)})</b>\n\n"
    kb_rows = []
    for code, p in promos.items():
        st = "✅" if p.get("active", True) else "❌"
        text += f"{st} <code>{esc(code)}</code> — {p.get('discount',0)}% ({p.get('uses',0)}/{p.get('max_uses',0) or '∞'})\n"
        kb_rows.append([InlineKeyboardButton(f"{st} {code}", callback_data=f"promo_toggle:{code}"),
                         InlineKeyboardButton("🗑️", callback_data=f"promo_del:{code}")])
    kb_rows.append([InlineKeyboardButton("➕ Створити", callback_data="adm_create_promo")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text or "📭 Немає промо-кодів.", InlineKeyboardMarkup(kb_rows))


@admin_only
async def promo_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.callback_query.data.split(":")[1]
    promos = load_promos()
    if code in promos:
        promos[code]["active"] = not promos[code].get("active", True)
        save_promos(promos)
    await adm_promos(update, context)


@admin_only
async def promo_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.callback_query.data.split(":")[1]
    promos = load_promos()
    if code in promos:
        del promos[code]
        save_promos(promos)
    await adm_promos(update, context)


@admin_only
async def adm_create_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(update.callback_query, "🎟️ Введіть назву коду (наприклад: SALE20):",
                    mkb(back_btn("adm:promos")))


# ── Admin: broadcast ───────────────────────────────────────────────────────────

@admin_only
async def adm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = _load(USERS_KEY)
    active = sum(1 for u in users.values() if not u.get("banned"))
    context.user_data["state"] = AWAIT_BROADCAST
    await safe_edit(update.callback_query,
        f"📢 <b>Розсилка</b>\nОтримають: <b>{active}</b> активних\n\nВведіть текст (HTML підтримується):",
        mkb(back_btn("admin_panel")))


@admin_only
async def broadcast_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    text  = context.user_data.pop("broadcast_text", "")
    users = _load(USERS_KEY)
    success = failed = blocked = 0
    await safe_edit(q, "📢 <b>Розсилка...</b>")
    for uid2, u in users.items():
        if u.get("banned"):
            blocked += 1; continue
        try:
            await context.bot.send_message(uid2, text, parse_mode="HTML")
            success += 1
            if success % 25 == 0:
                await asyncio.sleep(1)
        except (Forbidden, Exception):
            failed += 1
    log_action("broadcast", q.from_user.id, {"success": success, "failed": failed})
    await context.bot.send_message(q.from_user.id,
        f"📢 <b>Розсилка завершена!</b>\n✅ {success}  ❌ {failed}  🔇 {blocked}", parse_mode="HTML")


# ── Admin: feedbacks ───────────────────────────────────────────────────────────

@admin_only
async def adm_feedbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    feedbacks = _load(FEEDBACK_KEY)
    sorted_fb = sorted(feedbacks.items(), key=lambda x: x[1].get("created_at",""), reverse=True)[:10]
    text = f"💬 <b>Відгуки ({len(feedbacks)})</b>\n\n"
    kb_rows = []
    for fid, f in sorted_fb:
        st   = {"new":"🟢","read":"🔵","replied":"🟣"}.get(f.get("status","new"),"⚪")
        text += f"{st} <b>#{esc(fid)}</b> — {esc(f.get('first_name','?'))}\n{esc(f.get('feedback','')[:40])}\n\n"
        kb_rows.append([InlineKeyboardButton(f"✍️ #{fid}", callback_data=f"reply_fb:{fid}")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text or "📭 Немає відгуків.", InlineKeyboardMarkup(kb_rows))


@admin_only
async def reply_fb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    fid = q.data.split(":")[1]
    feedbacks = _load(FEEDBACK_KEY)
    fb = feedbacks.get(fid, {})
    if fb:
        feedbacks[fid]["status"] = "read"
        _save(FEEDBACK_KEY, feedbacks)
    context.user_data["reply_to_uid"] = fb.get("user_id")
    context.user_data["reply_fb_id"]  = fid
    context.user_data["state"]        = AWAIT_REPLY_TO_USER
    await safe_edit(q,
        f"✍️ <b>Відповідь #{esc(fid)}</b>\n\n"
        f"Від: {esc(fb.get('first_name','?'))}\n{esc(fb.get('feedback','?'))}\n\nВведіть відповідь:")


# ── Admin: settings ────────────────────────────────────────────────────────────

@admin_only
async def adm_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings()
    text = (
        f"⚙️ <b>Налаштування</b>\n\n"
        f"🛠 Тех. обслуговування: {'🔴 Увімк.' if s.get('maintenance_mode') else '🟢 Вимк.'}\n"
        f"📦 Нові замовлення: {'✅' if s.get('new_orders_enabled') else '⛔️'}\n\n"
        f"🤖 AI: {'✅' if AI_ENABLED else '❌ DEEPSEEK_API_KEY не задано'}\n"
        f"  · Перевірка чеків: {'✅' if s.get('ai_check_receipts',True) else '❌'}\n"
        f"  · Авто-деплой: {'✅' if s.get('ai_auto_deploy',True) else '❌'}\n"
        f"  · Підтримка: {'✅' if s.get('ai_support',True) else '❌'}\n\n"
        f"💳 {esc(s.get('payment_card','—'))}  ({esc(s.get('payment_holder','—'))})"
    )
    await safe_edit(q, text, mkb(
        [InlineKeyboardButton("🛠 Тех. обслуговування", callback_data="toggle_maintenance"),
         InlineKeyboardButton("📦 Замовлення", callback_data="toggle_orders")],
        [InlineKeyboardButton("🤖 AI чеки", callback_data="toggle_ai_receipts"),
         InlineKeyboardButton("🚀 Авто-деплой", callback_data="toggle_ai_deploy")],
        [InlineKeyboardButton("💬 AI підтримка", callback_data="toggle_ai_support")],
        [InlineKeyboardButton("💳 Реквізити", callback_data="edit_payment"),
         InlineKeyboardButton("📝 Привітання", callback_data="edit_welcome")],
        back_btn("admin_panel"),
    ))


@admin_only
async def toggle_maintenance(update, context):
    s = load_settings(); s["maintenance_mode"] = not s.get("maintenance_mode"); save_settings(s)
    await adm_settings(update, context)


@admin_only
async def toggle_orders(update, context):
    s = load_settings(); s["new_orders_enabled"] = not s.get("new_orders_enabled", True); save_settings(s)
    await adm_settings(update, context)


@admin_only
async def toggle_ai_receipts(update, context):
    s = load_settings(); s["ai_check_receipts"] = not s.get("ai_check_receipts", True); save_settings(s)
    await adm_settings(update, context)


@admin_only
async def toggle_ai_deploy(update, context):
    s = load_settings(); s["ai_auto_deploy"] = not s.get("ai_auto_deploy", True); save_settings(s)
    await adm_settings(update, context)


@admin_only
async def toggle_ai_support(update, context):
    s = load_settings(); s["ai_support"] = not s.get("ai_support", True); save_settings(s)
    await adm_settings(update, context)


@admin_only
async def edit_payment(update, context):
    context.user_data["state"] = AWAIT_CUSTOM_PAYMENT_TEXT
    await safe_edit(update.callback_query,
        "💳 <b>Реквізити</b>\n\nВведіть 3 рядки:\n1) Картка\n2) Отримувач\n3) Посилання Mono (необов'язково)")


@admin_only
async def edit_welcome(update, context):
    context.user_data["state"] = AWAIT_WELCOME_TEXT
    await safe_edit(update.callback_query, "📝 Введіть новий текст привітання (HTML):")


# ── Admin: logs & export ───────────────────────────────────────────────────────

@admin_only
async def adm_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    logs = _db.load_logs_db()
    if not isinstance(logs, list):
        logs = []
    text = f"📜 <b>Останні дії ({len(logs)})</b>\n\n"
    for entry in logs[:20]:
        text += f"🕐 {entry.get('ts','')[:16]} | <code>{esc(entry.get('action','?'))}</code> | {entry.get('uid','?')}\n"
    await safe_edit(q, text, mkb(back_btn("admin_panel")))


@admin_only
async def adm_export_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_edit(q, "📥 <b>Вивантаження БД</b>\n\nОберіть що вивантажити:",
        mkb([InlineKeyboardButton("👥 Користувачі", callback_data="export:users")],
            [InlineKeyboardButton("📦 Замовлення",  callback_data="export:orders")],
            [InlineKeyboardButton("🎟 Промо-коди",  callback_data="export:promos")],
            [InlineKeyboardButton("💬 Відгуки",     callback_data="export:feedback")],
            [InlineKeyboardButton("📜 Логи",        callback_data="export:logs")],
            [InlineKeyboardButton("📊 Вся БД (ZIP)", callback_data="export:all")],
            back_btn("admin_panel")))


@admin_only
async def adm_export_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    export_type = q.data.split(":")[1]
    await q.answer("⏳ Готуємо...")
    admin_id = q.from_user.id

    try:
        if export_type == "users":
            data = _load(USERS_KEY, {})
            filename = f"users_{now_fmt('%Y%m%d_%H%M')}.json"
            content  = json.dumps(data, ensure_ascii=False, indent=2).encode()
            caption  = f"👥 Користувачі: {len(data)}"

        elif export_type == "orders":
            data = _load(ORDERS_KEY, {})
            export_data = {oid: {k: v for k, v in o.items() if k not in ("js_content", "values_data")}
                           for oid, o in data.items()}
            filename = f"orders_{now_fmt('%Y%m%d_%H%M')}.json"
            content  = json.dumps(export_data, ensure_ascii=False, indent=2).encode()
            caption  = f"📦 Замовлення: {len(data)}"

        elif export_type == "promos":
            data = load_promos()
            filename = f"promos_{now_fmt('%Y%m%d_%H%M')}.json"
            content  = json.dumps(data, ensure_ascii=False, indent=2).encode()
            caption  = f"🎟 Промо-коди: {len(data)}"

        elif export_type == "feedback":
            data = _load(FEEDBACK_KEY, {})
            filename = f"feedback_{now_fmt('%Y%m%d_%H%M')}.json"
            content  = json.dumps(data, ensure_ascii=False, indent=2).encode()
            caption  = f"💬 Відгуки: {len(data)}"

        elif export_type == "logs":
            logs = _db.load_logs_db()
            filename = f"logs_{now_fmt('%Y%m%d_%H%M')}.json"
            content  = json.dumps(logs, ensure_ascii=False, indent=2).encode()
            caption  = f"📜 Логи: {len(logs)}"

        elif export_type == "all":
            import zipfile
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                orders_data = _load(ORDERS_KEY, {})
                orders_export = {oid: {k: v for k, v in o.items() if k not in ("js_content","values_data")}
                                 for oid, o in orders_data.items()}
                zf.writestr("users.json",    json.dumps(_load(USERS_KEY, {}),    ensure_ascii=False, indent=2))
                zf.writestr("orders.json",   json.dumps(orders_export,            ensure_ascii=False, indent=2))
                zf.writestr("feedback.json", json.dumps(_load(FEEDBACK_KEY, {}),  ensure_ascii=False, indent=2))
                zf.writestr("promos.json",   json.dumps(load_promos(),            ensure_ascii=False, indent=2))
                zf.writestr("settings.json", json.dumps(load_settings(),          ensure_ascii=False, indent=2))
                zf.writestr("tariffs.json",  json.dumps(load_tariffs(),           ensure_ascii=False, indent=2))
                zf.writestr("logs.json",     json.dumps(_db.load_logs_db(),       ensure_ascii=False, indent=2))
            zip_buf.seek(0)
            filename = f"db_{now_fmt('%Y%m%d_%H%M')}.zip"
            content  = zip_buf.read()
            caption  = f"📊 Повна БД | {now_fmt()}"
        else:
            await q.answer("❌ Невідомий тип", show_alert=True)
            return

        buf = io.BytesIO(content); buf.name = filename
        await context.bot.send_document(admin_id, buf, caption=caption, parse_mode="HTML")
        log_action("db_export", admin_id, {"type": export_type})

    except Exception as e:
        logger.error("Export error: %s", e, exc_info=True)
        await context.bot.send_message(admin_id,
            f"❌ <b>Помилка вивантаження</b>\n\n<code>{esc(str(e)[:300])}</code>",
            parse_mode="HTML")


# ── Other callbacks ────────────────────────────────────────────────────────────

async def promo_enter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(update.callback_query,
        "🎟️ <b>Введіть промо-код:</b>", mkb(back_btn("home")))


async def ref_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    u   = _load(USERS_KEY).get(uid, {})
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    await safe_edit(q,
        f"👥 <b>Реферальна програма</b>\n\nЗа кожного друга — <b>{REFERRAL_REWARD}₴</b>\n\n"
        f"💰 Баланс: {u.get('balance',0)}₴  ·  Запрошено: {u.get('ref_count',0)}\n"
        f"Мінімум виводу: {MIN_WITHDRAW}₴\n\n🔗 <code>{ref_link}</code>",
        mkb([InlineKeyboardButton("💸 Вивести", callback_data="withdraw")], back_btn("home")),
        disable_web_page_preview=True)


async def withdraw_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    bal = _load(USERS_KEY).get(uid, {}).get("balance", 0)
    if bal < MIN_WITHDRAW:
        await safe_edit(q, f"❌ Мінімум: {MIN_WITHDRAW}₴\nВаш баланс: {bal}₴", mkb(back_btn("ref_menu")))
        return
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id,
                f"💰 <b>Запит на вивід</b>\n"
                f"👤 {esc(update.effective_user.first_name)} | 🆔 {uid} | 💳 {bal}₴\n📅 {now_fmt()}",
                reply_markup=mkb([InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_withdraw:{uid}:{bal}")]),
                parse_mode="HTML")
        except Exception:
            pass
    await safe_edit(q, "✅ <b>Запит відправлено!</b>\nАдміністратор обробить протягом 24 годин. 🌸",
                    mkb(back_btn("ref_menu")))


async def feedback_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_FEEDBACK
    await safe_edit(update.callback_query,
        "💬 <b>Написати нам</b>\n\nВведіть повідомлення:", mkb(back_btn("home")))


async def about_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = load_settings()
    await safe_edit(update.callback_query,
        f"🪪 <b>FunsDiia</b> — генерація документів\n\n"
        "1️⃣ Обрати тариф\n2️⃣ Ввести дані\n3️⃣ Надіслати фото 3×4\n"
        "4️⃣ Оплатити → отримати посилання\n\n"
        f"💳 <code>{esc(s.get('payment_card','—'))}</code>  ({esc(s.get('payment_holder','—'))})\n"
        "⚡️ До 10 хвилин",
        mkb(back_btn("home")))


# ── Callback router ────────────────────────────────────────────────────────────

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    d   = q.data
    uid = str(q.from_user.id)

    if _load(USERS_KEY).get(uid, {}).get("banned") and not is_admin(uid):
        await q.answer("🚫 Акаунт заблоковано", show_alert=True)
        return

    await q.answer()

    try:
        routes = {
            "home":              cmd_start,
            "catalog":           show_catalog,
            "profile":           show_profile,
            "my_orders":         my_orders_handler,
            "ref_menu":          ref_menu,
            "withdraw":          withdraw_handler,
            "feedback":          feedback_menu,
            "about":             about_handler,
            "promo_enter":       promo_enter,
            "admin_panel":       admin_panel,
            "adm:stats":         adm_stats,
            "adm:orders":        adm_orders,
            "adm:users":         adm_users,
            "adm:search":        adm_search,
            "adm:balance":       adm_balance,
            "adm:tariffs":       adm_tariffs,
            "adm:promos":        adm_promos,
            "adm:broadcast":     adm_broadcast,
            "adm:feedbacks":     adm_feedbacks,
            "adm:settings":      adm_settings,
            "adm:logs":          adm_logs,
            "adm:export_db":     adm_export_db,
            "adm:chain_deploy":  adm_chain_deploy_menu,
            "chain_deploy_run":  adm_chain_deploy_run,
            "broadcast_go":      broadcast_go,
            "tariff_add":        tariff_add,
            "adm_create_promo":  adm_create_promo,
            "toggle_maintenance":toggle_maintenance,
            "toggle_orders":     toggle_orders,
            "toggle_ai_receipts":toggle_ai_receipts,
            "toggle_ai_deploy":  toggle_ai_deploy,
            "toggle_ai_support": toggle_ai_support,
            "edit_payment":      edit_payment,
            "edit_welcome":      edit_welcome,
        }
        if d in routes:
            return await routes[d](update, context)

        if d.startswith("tar:"):               return await select_tariff(update, context)
        if d.startswith("sex:"):               return await select_sex(update, context)
        if d.startswith("rights:"):            return await select_rights(update, context)
        if d.startswith("zagran:"):            return await select_zagran(update, context)
        if d.startswith("diploma:"):           return await select_diploma(update, context)
        if d.startswith("adm_approve_deploy:"): return await adm_approve_deploy(update, context)
        if d.startswith("adm_approve:"):       return await adm_approve(update, context)
        if d.startswith("adm_reject:"):        return await adm_reject(update, context)
        if d.startswith("adm_complete:"):      return await adm_complete(update, context)
        if d.startswith("adm_push_pages:"):    return await adm_push_pages(update, context)
        if d.startswith("adm_push_go:"):       return await adm_push_go(update, context)
        if d.startswith("adm_send_link:"):     return await adm_send_link(update, context)
        if d.startswith("confirm_withdraw:"):  return await adm_confirm_withdraw(update, context)
        if d.startswith("adm_order_view:"):    return await adm_order_view(update, context)
        if d.startswith("adm_order_filter:"):  return await adm_order_filter(update, context)
        if d.startswith("tariff_toggle:"):     return await tariff_toggle(update, context)
        if d.startswith("tariff_edit:"):       return await tariff_edit(update, context)
        if d.startswith("tariff_del:"):        return await tariff_del(update, context)
        if d.startswith("tedit_name:"):        return await tedit_name(update, context)
        if d.startswith("tedit_price:"):       return await tedit_price(update, context)
        if d.startswith("tedit_emoji:"):       return await tedit_emoji(update, context)
        if d.startswith("promo_toggle:"):      return await promo_toggle(update, context)
        if d.startswith("promo_del:"):         return await promo_del(update, context)
        if d.startswith("reply_fb:"):          return await reply_fb(update, context)
        if d.startswith("adm_ban:"):           return await adm_ban(update, context)
        if d.startswith("adm_vip:"):           return await adm_vip(update, context)
        if d.startswith("adm_msg:"):           return await adm_msg(update, context)
        if d.startswith("export:"):            return await adm_export_do(update, context)

        logger.warning("Unhandled callback: %s", d)

    except Exception as e:
        logger.error("button_handler [%s]: %s", d, e, exc_info=True)
        try:
            await q.message.reply_text("😔 Сталася помилка. Спробуйте ще раз або натисніть /start.")
        except Exception:
            pass


# ── Commands ───────────────────────────────────────────────────────────────────

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Немає доступу.")
        return
    users  = _load(USERS_KEY)
    orders = _load(ORDERS_KEY)
    pending = sum(1 for o in orders.values() if o.get("status") == "pending")
    await update.message.reply_text(
        f"👑 <b>Адмін-панель</b>\n👥 {len(users)} | ⏳ {pending} в черзі | 🕐 {now_fmt()}",
        reply_markup=mkb(
            [InlineKeyboardButton("📊 Статистика",   callback_data="adm:stats"),
             InlineKeyboardButton("📋 Замовлення",   callback_data="adm:orders")],
            [InlineKeyboardButton("👥 Користувачі",  callback_data="adm:users"),
             InlineKeyboardButton("💰 Тарифи",       callback_data="adm:tariffs")],
            [InlineKeyboardButton("⚙️ Налаштування", callback_data="adm:settings"),
             InlineKeyboardButton("🚀 Деплой",       callback_data="adm:chain_deploy")],
            [InlineKeyboardButton("📥 Вивантажити БД", callback_data="adm:export_db")],
        ), parse_mode="HTML")


async def cmd_deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск повного ланцюжку деплою з команди."""
    if not is_admin(update.effective_user.id):
        return
    msg = await update.message.reply_text("⏳ <b>Запускаємо деплой ланцюжком...</b>", parse_mode="HTML")
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: chain_deploy.run_full_chain()
        )
        log_action("chain_deploy_cmd", update.effective_user.id, result)
        await msg.edit_text(
            f"🚀 <b>Деплой завершено!</b>\n\n"
            f"📁 Папка 2:\n🔗 {esc(result['folder2_url'])}\n\n"
            f"📁 Папка 1 (з QR):\n🔗 <b>{esc(result['folder1_url'])}</b>\n\n"
            f"📷 QR у <code>1/assets/q.png</code>\n⏱ Зачекайте 1-2 хвилини.",
            parse_mode="HTML")
    except chain_deploy.DeployError as e:
        await msg.edit_text(f"❌ <b>Помилка деплою</b>\n\n<code>{esc(str(e))}</code>", parse_mode="HTML")
    except Exception as e:
        logger.error("cmd_deploy: %s", e, exc_info=True)
        await msg.edit_text(f"❌ <b>Непередбачена помилка</b>\n\n<code>{esc(str(e)[:400])}</code>", parse_mode="HTML")


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("📥 Оберіть:", reply_markup=mkb(
        [InlineKeyboardButton("👥 Користувачі", callback_data="export:users")],
        [InlineKeyboardButton("📦 Замовлення",  callback_data="export:orders")],
        [InlineKeyboardButton("📊 Вся БД (ZIP)", callback_data="export:all")],
    ), parse_mode="HTML")


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Використання: /ban <user_id>"); return
    uid2 = context.args[0]
    users = _load(USERS_KEY)
    if uid2 not in users:
        await update.message.reply_text("❌ Не знайдено"); return
    users[uid2]["banned"] = True
    _save(USERS_KEY, users)
    await update.message.reply_text(f"🚫 {uid2} заблоковано.")


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Використання: /unban <user_id>"); return
    uid2 = context.args[0]
    users = _load(USERS_KEY)
    if uid2 in users:
        users[uid2]["banned"] = False
        _save(USERS_KEY, users)
    await update.message.reply_text(f"✅ {uid2} розблоковано.")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Використання: /balance <user_id> <amount>"); return
    uid2, amount = context.args[0], int(context.args[1])
    users = _load(USERS_KEY)
    if uid2 not in users:
        await update.message.reply_text("❌ Не знайдено"); return
    users[uid2]["balance"] = max(0, users[uid2].get("balance", 0) + amount)
    _save(USERS_KEY, users)
    await update.message.reply_text(f"✅ {uid2} → {users[uid2]['balance']}₴")


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Error: %s", context.error, exc_info=True)
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id,
                f"❌ <b>Помилка бота</b>\n\n{esc(str(context.error)[:300])}", parse_mode="HTML")
        except Exception:
            pass


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)

    _db.init_db()
    logger.info("✅ SQLite Cloud підключено.")

    _DB.update({
        USERS_KEY:    {"load": _db.load_users,       "save": _db.save_users},
        ORDERS_KEY:   {"load": _db.load_orders,      "save": _db.save_orders},
        FEEDBACK_KEY: {"load": _db.load_feedback,    "save": _db.save_feedback},
        TARIFFS_KEY:  {"load": _db.load_tariffs_db,  "save": _db.save_tariffs_db},
        PROMOS_KEY:   {"load": _db.load_promos_db,   "save": _db.save_promos_db},
        SETTINGS_KEY: {"load": _db.load_settings_db, "save": _db.save_settings_db},
    })

    if not PAGES_GH_TOKEN:
        logger.warning("PAGES_GH_TOKEN не встановлено — деплой папки 1 буде недоступний")
    if not os.getenv("GH_TOKEN_2"):
        logger.warning("GH_TOKEN_2 не встановлено — деплой папки 2 буде недоступний")
    if not GROUP_CHAT_ID:
        logger.warning("GROUP_CHAT_ID не встановлено")

    app = Application.builder().token(TOKEN).build()

    # Перевірка підписок — кожну годину
    app.job_queue.run_repeating(
        subscription_check_job,
        interval=3600,   # 1 година
        first=60,        # перший запуск через 60 сек після старту
        name="subscription_check",
    )

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("help",    cmd_start))
    app.add_handler(CommandHandler("admin",   cmd_admin))
    app.add_handler(CommandHandler("deploy",  cmd_deploy))
    app.add_handler(CommandHandler("export",  cmd_export))
    app.add_handler(CommandHandler("ban",     cmd_ban))
    app.add_handler(CommandHandler("unban",   cmd_unban))
    app.add_handler(CommandHandler("balance", cmd_balance))

    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VIDEO, handle_media))
    app.add_error_handler(error_handler)

    logger.info("🌸 FunsDiia Bot запущено! Admins: %s | Group: %s", ADMIN_IDS, GROUP_CHAT_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
