"""
FunsDiia Bot (Ultra-Enhanced Edition)
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
from typing import Optional, Union

import pytz
import qrcode
import requests as _req
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Update,
)
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
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
    AWAIT_WITHDRAW_CARD,
) = range(29)

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
    return _html_escape.escape(str(text if text is not None else ""))


def log_action(action: str, uid=None, details: dict = None):
    try:
        _db.log_action_db(now_str(), action, str(uid) if uid else None, details or {})
    except Exception as e:
        logger.error("Error writing action log: %s", e)


def progress_bar(step: int, total: int = 7) -> str:
    """Генерує візуальний індикатор прогресу створення документа."""
    filled = "▓" * step
    unfilled = "░" * (total - step)
    return f"[{filled}{unfilled}] {step}/{total}"


def generate_qr_bytes(text: str) -> bytes:
    """Генерує QR код та повертає його у байтах."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=2,
    )
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


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
        return {"ok": False, "discount": 0, "msg": "❌ Промокод не знайдено."}
    p = promos[code]
    if not p.get("active", True):
        return {"ok": False, "discount": 0, "msg": "❌ Промокод уже неактивний."}
    if p.get("max_uses", 0) and p.get("uses", 0) >= p["max_uses"]:
        return {"ok": False, "discount": 0, "msg": "❌ Ліміт використання промокоду вичерпано."}
    if uid in p.get("used_by", []):
        return {"ok": False, "discount": 0, "msg": "❌ Ви вже активували цей промокод."}
    expires = p.get("expires")
    if expires and datetime.fromisoformat(expires) < datetime.now(TIMEZONE):
        return {"ok": False, "discount": 0, "msg": "❌ Термін дії промокоду закінчився."}
    return {"ok": True, "discount": p.get("discount", 0), "msg": f"🎉 <b>Промокод активовано!</b>\nЗнижка: <b>{p.get('discount', 0)}%</b>"}


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
    "Ти — ввічливий та швидкий онлайн-консультант сервісу FunsDiia. Відповідай коротко, життєрадісно, українською мовою. "
    "Якщо запитують про оплату або реквізити — поясни, що реквізити видаються автоматично при оформленні замовлення в боті."
)
_SYS_RECEIPT = (
    'Ти — експертна система верифікації квитанцій та банківських чеків. Відповідай ТІЛЬКИ у форматі JSON без маркдауну: '
    '{"ok": true/false, "confidence": 0-100, "amount": число|null, "reason": "короткий опис"}. '
    "ok=true якщо це справжній фінансовий чек/квитанція про переказ."
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
        logger.error("DeepSeek API Exception: %s", e)
        return ""


async def ai_check_receipt(photo_bytes: bytes, expected_amount: int) -> dict:
    result = {"ok": None, "confidence": 0, "amount": None, "reason": "", "auto_approved": False}
    if not AI_ENABLED or not photo_bytes:
        return result
    b64 = base64.b64encode(photo_bytes).decode()
    messages = [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
        {"type": "text", "text": f"Очікувана сума платежу: {expected_amount} UAH. Проаналізуй квитанцію."},
    ]}]
    raw = await asyncio.to_thread(_deepseek, messages, _SYS_RECEIPT, 300)
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            result.update(parsed)
            result["auto_approved"] = bool(parsed.get("ok")) and int(parsed.get("confidence", 0)) >= 80
    except Exception as e:
        logger.warning("Receipt JSON parse error: %s | raw response: %s", e, raw[:100])
    return result


async def ai_support_reply(text: str, history: list = None) -> str:
    if not AI_ENABLED:
        return ""
    messages = (history or []) + [{"role": "user", "content": text}]
    return await asyncio.to_thread(_deepseek, messages, _SYS_SUPPORT, 400)


async def ai_transliterate(fio_ua: str) -> str:
    if not AI_ENABLED or not fio_ua:
        return fio_ua
    messages = [{"role": "user", "content":
        f"Транслітеруй ПІБ латиницею за офіційним стандартом КМУ 2010: '{fio_ua}'. "
        "Поверни ТІЛЬКИ результат без додаткових слів чи лапок."}]
    result = await asyncio.to_thread(_deepseek, messages, "", 50)
    return result.strip("\"' ") or fio_ua


# ── Document generators ────────────────────────────────────────────────────────

def _rnd_digits(n): return "".join(str(random.randint(0, 9)) for _ in range(n))


def gen_address() -> str:
    districts = ["Харківський", "Чугуївський", "Ізюмський", "Лозівський", "Богодухівський"]
    cities    = ["м. Харків", "м. Чугуїв", "м. Мерефа", "м. Люботин", "смт Пісочин"]
    streets   = ["Сумська", "Пушкінська", "Полтавський Шлях", "пр. Науки", "вул. Свободи"]
    return (
        f"Харківська область, {random.choice(districts)} район, "
        f"{random.choice(cities)}, вул. {random.choice(streets)}, "
        f"буд. {random.randint(1, 150)}, кв. {random.randint(1, 250)}"
    )


def gen_values_dict(data: dict) -> dict:
    now = datetime.now(TIMEZONE)
    date_now = now.strftime("%d.%m.%Y")
    date_out = (now + timedelta(days=3650)).strftime("%d.%m.%Y")
    date_give_z = (now - timedelta(days=random.randint(1000, 2000))).strftime("%d.%m.%Y")
    date_out_z  = (now + timedelta(days=random.randint(3000, 4000))).strftime("%d.%m.%Y")

    universities = ["ХНУ імені В. Н. Каразіна", "НТУ «ХПІ»", "ХНЕУ ім. С. Кузнеця", "ХНМУ", "ХНУРЕ"]
    faculties    = ["Комп'ютерних наук", "Фізико-технічний", "Економічний", "Медичний", "Інформаційних технологій"]

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
    if "subscription_end" not in d:
        lines.append('var subscription_end = "";')
    if "is_expired" not in d:
        lines.append("var is_expired = false;")
    return "\n".join(lines) + "\n"


# ── Subscription helpers ───────────────────────────────────────────────────────

def calc_subscription_end(tariff_key: str, tariffs: dict) -> Optional[str]:
    t = tariffs.get(tariff_key, {})
    days = t.get("days")
    if not days:
        return None
    end_dt = datetime.now(TIMEZONE) + timedelta(days=days)
    return end_dt.isoformat()


def days_until_expiry(subscription_end: str) -> int:
    end_dt = datetime.fromisoformat(subscription_end)
    if end_dt.tzinfo is None:
        end_dt = TIMEZONE.localize(end_dt)
    delta = end_dt.replace(hour=0, minute=0, second=0, microsecond=0) - \
            datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    return delta.days


async def subscription_check_job(context: ContextTypes.DEFAULT_TYPE):
    orders = _load(ORDERS_KEY)
    users  = _load(USERS_KEY)

    for oid, order in orders.items():
        sub_end = order.get("subscription_end")
        status  = order.get("status", "")
        uid2    = order.get("user_id", "")

        if not sub_end or status in ("expired", "rejected", "pending"):
            continue
        if users.get(uid2, {}).get("banned"):
            continue

        days_left = days_until_expiry(sub_end)
        notified = order.get("notified_days", [])

        for remind_day in (3, 2, 1):
            if days_left == remind_day and remind_day not in notified:
                tariff_name = order.get("tariff_name", "")
                pages_url   = order.get("pages_url", "")
                try:
                    await context.bot.send_message(
                        uid2,
                        f"⏰ <b>Увага! Підписка закінчується через {remind_day} {'день' if remind_day == 1 else 'дні'}!</b>\n\n"
                        f"📦 Тариф: <b>{esc(tariff_name)}</b>\n"
                        f"📅 Дійсна до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>\n\n"
                        f"{'🔗 Ваш кабінет: ' + pages_url + chr(10) + chr(10) if pages_url else ''}"
                        f"Щоб зберегти доступ без обмежень — продовжіть підписку заздалегідь 👇",
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

        if days_left == 0 and 0 not in notified:
            tariff_name = order.get("tariff_name", "")
            try:
                await context.bot.send_message(
                    uid2,
                    f"🔴 <b>Сьогодні останній день дії підписки!</b>\n\n"
                    f"📦 Тариф: <b>{esc(tariff_name)}</b>\n\n"
                    f"Завтра доступ буде обмежено, та з'явиться водяний знак. 💧",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Продовжити зараз", callback_data="catalog")],
                    ]),
                    parse_mode="HTML",
                )
                orders[oid].setdefault("notified_days", []).append(0)
                log_action("sub_expiring_today", uid2, {"oid": oid})
            except Exception as e:
                logger.error("sub_expiring_today [%s]: %s", uid2, e)

        if days_left < 0 and status == "deployed":
            orders[oid]["status"] = "expired"
            orders[oid]["expired_at"] = now_str()
            log_action("sub_expired", uid2, {"oid": oid})
            try:
                await context.bot.send_message(
                    uid2,
                    f"❌ <b>Термін дії вашої підписки закінчився.</b>\n\n"
                    f"На вашому документі активовано обмеження.\n"
                    f"Оформіть новий тариф для швидкого відновлення повного доступу 👇",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⚡️ Відновити доступ", callback_data="catalog")],
                    ]),
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error("sub_expired notify [%s]: %s", uid2, e)

            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        f"⚠️ <b>Підписка закінчилась</b>\n"
                        f"👤 <code>{uid2}</code> | Замовлення: <code>{oid}</code>\n"
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
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            await query.message.reply_text(text, **kw)


def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_admin(uid):
            if update.callback_query:
                await update.callback_query.answer("❌ Доступ обмежено (Тільки для адмінів)", show_alert=True)
            else:
                await update.message.reply_text("❌ Доступ обмежено.")
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
        logger.error("notify_group error: %s", e)


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
        logger.error("notify_group_photo error: %s", e)


# ── /start & Global Actions ────────────────────────────────────────────────────

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
                    f"🎉 <b>Новий реферал!</b>\nКористувач {esc(update.effective_user.first_name)} приєднався за вашим посиланням!\n"
                    f"Ви отримаєте <b>{REFERRAL_REWARD}₴</b> після його першого замовлення.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

    u = users.get(uid, {})
    if u.get("banned"):
        await update.effective_message.reply_text("🚫 <b>Ваш акаунт заблоковано.</b>\nЗверніться до підтримки.", parse_mode="HTML")
        return
    if settings.get("maintenance_mode") and not is_admin(uid):
        await update.effective_message.reply_text("🛠 <b>Ведуться технічні роботи.</b>\nСпробуйте трохи пізніше.", parse_mode="HTML")
        return

    vip = " 👑" if u.get("vip") else ""
    bal = u.get("balance", 0)
    bal_line = f"\n💰 Баланс: <b>{bal}₴</b>" if bal > 0 else ""
    welcome = settings.get("welcome_text") or (
        f"👋 <b>Вітаємо, {esc(update.effective_user.first_name)}{vip}!</b>{bal_line}\n\n"
        "✨ <b>FunsDiia</b> — швидка генерація онлайн-документів.\n"
        "⚡️ Автоматична обробка, висока якість та готовність за 5-10 хвилин!"
    )

    kb_rows = [
        [InlineKeyboardButton("🛒 Замовити документ", callback_data="catalog")],
        [InlineKeyboardButton("📂 Мої замовлення", callback_data="my_orders"),
         InlineKeyboardButton("👤 Профіль", callback_data="profile")],
        [InlineKeyboardButton("🎟 Промокод", callback_data="promo_enter"),
         InlineKeyboardButton("👥 Партнерка", callback_data="ref_menu")],
        [InlineKeyboardButton("💬 Підтримка / AI", callback_data="feedback"),
         InlineKeyboardButton("ℹ️ Про сервіс", callback_data="about")],
    ]
    if is_admin(uid):
        kb_rows.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])

    await update.effective_message.reply_text(
        welcome, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML",
    )
    context.user_data.clear()


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.effective_message.reply_text(
        "❌ <b>Операцію скасовано.</b> Повертаємось у головне меню.",
        reply_markup=mkb([InlineKeyboardButton("🏠 Головне меню", callback_data="home")]),
        parse_mode="HTML"
    )


# ── Profile / Orders / Catalog ─────────────────────────────────────────────────

async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    u = _load(USERS_KEY).get(uid, {})
    orders = _load(ORDERS_KEY)
    my = [o for o in orders.values() if o.get("user_id") == uid]
    done = sum(1 for o in my if o.get("status") in ("completed", "deployed"))
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    vip_label = "👑 VIP Клієнт" if u.get("vip") else "👤 Стандарт"
    text = (
        f"👤 <b>Профіль: {esc(u.get('first_name', 'Користувач'))}</b> ({vip_label})\n"
        f"🆔 ID: <code>{uid}</code>\n\n"
        f"💰 Баланс: <b>{u.get('balance', 0)}₴</b>\n"
        f"👥 Запрошено друзів: <b>{u.get('ref_count', 0)}</b>\n"
        f"📦 Всього замовлень: <b>{len(my)}</b> (Виконано: <b>{done}</b>)\n\n"
        f"🔗 Ваше реферальне посилання:\n<code>{ref_link}</code>"
    )
    btns = []
    if u.get("balance", 0) >= MIN_WITHDRAW:
        btns.append([InlineKeyboardButton("💸 Вивести кошти", callback_data="withdraw")])
    btns.append([InlineKeyboardButton("📂 Переглянути свої замовлення", callback_data="my_orders")])
    btns.append(back_btn("home"))
    await safe_edit(q, text, InlineKeyboardMarkup(btns), disable_web_page_preview=True)


async def my_orders_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    orders = _load(ORDERS_KEY)
    my = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("user_id") == uid],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    if not my:
        await safe_edit(q, "📭 <b>У вас ще немає замовлень.</b>\n\nОберіть відповідний тариф у каталозі!",
                        mkb([InlineKeyboardButton("🛒 Перейти до каталогу", callback_data="catalog")], back_btn("home")))
        return

    status_map = {
        "pending": "⏳ В обробці",
        "approved": "💳 Оплачено",
        "completed": "🎉 Виконано",
        "rejected": "❌ Відхилено",
        "deployed": "🌐 Готово (Активно)",
        "expired": "💧 Термін закінчився",
    }
    
    text = "📂 <b>Ваші останні замовлення:</b>\n\n"
    kb_rows = []
    for oid, o in my[:8]:
        st = status_map.get(o.get("status", ""), "·")
        t_name = load_tariffs().get(o.get("tariff", ""), {}).get("name", o.get("tariff", "?"))
        text += f"🔹 <b>#{esc(oid)}</b> | {esc(t_name)} | {st}\n"
        if o.get("pages_url"):
            text += f"   🔗 <a href='{esc(o['pages_url'])}'>Відкрити документ</a>\n"
        kb_rows.append([InlineKeyboardButton(f"📄 Подробніше #{oid}", callback_data=f"user_order_view:{oid}")])

    kb_rows.append(back_btn("home"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows), disable_web_page_preview=True)


async def user_order_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    oid = q.data.split(":")[1]
    orders = _load(ORDERS_KEY)
    order = orders.get(oid)
    if not order:
        await q.answer("❌ Замовлення не знайдено", show_alert=True)
        return

    sub_end = order.get("subscription_end")
    sub_str = "♾ Безстроково"
    if sub_end:
        sub_str = datetime.fromisoformat(sub_end).strftime("%d.%m.%Y %H:%M")

    text = (
        f"📋 <b>Деталі замовлення #{esc(oid)}</b>\n\n"
        f"📦 Тариф: <b>{esc(order.get('tariff_name',''))}</b>\n"
        f"📝 ПІБ: <b>{esc(order.get('fio',''))}</b>\n"
        f"📅 ДН: <b>{esc(order.get('dob',''))}</b>\n"
        f"💰 Вартість: <b>{order.get('final_price')}₴</b>\n"
        f"🕒 Створено: <b>{order.get('created_at','')[:16]}</b>\n"
        f"⏳ Підписка до: <b>{sub_str}</b>\n"
    )

    kb = []
    if order.get("pages_url"):
        kb.append([InlineKeyboardButton("🌐 Відкрити документ", url=order["pages_url"])])
        kb.append([InlineKeyboardButton("🎴 Мой QR / Кабинет", callback_data=f"show_qr:{oid}")])
    
    if order.get("status") in ("expired", "deployed"):
        kb.append([InlineKeyboardButton("🔄 Продовжити / Оновити підписку", callback_data="catalog")])

    kb.append(back_btn("my_orders"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb))


async def show_qr_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    oid = q.data.split(":")[1]
    orders = _load(ORDERS_KEY)
    order = orders.get(oid)
    if not order or not order.get("pages_url"):
        await q.answer("❌ URL документа недоступний", show_alert=True)
        return

    await q.answer("🔄 Генеруємо QR-код...")
    qr_bytes = await asyncio.to_thread(generate_qr_bytes, order["pages_url"])
    
    buf = io.BytesIO(qr_bytes)
    buf.name = f"qr_{oid}.png"
    
    caption = (
        f"🎴 <b>Ваш QR-код для замовлення #{esc(oid)}</b>\n\n"
        f"🔗 Ссылка: {esc(order['pages_url'])}\n"
        f"📱 Скануйте камеру смартфона для швидкого переходу."
    )
    await context.bot.send_photo(
        chat_id=q.message.chat_id,
        photo=buf,
        caption=caption,
        parse_mode="HTML",
        reply_markup=mkb([InlineKeyboardButton("🌐 Відкрити сайт", url=order["pages_url"])])
    )


async def show_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = active_tariffs()
    text = "🛒 <b>Оберіть необхідний тариф підписки:</b>\n\n"
    for k, t in tariffs.items():
        d = "♾ Безстроково" if not t.get("days") else f"{t['days']} днів"
        text += f"{t.get('emoji','📦')} <b>{esc(t.get('name'))}</b> — <b>{t.get('price')}₴</b> <i>({d})</i>\n"
    
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
        await q.answer("❌ Прийом нових замовлень тимчасово призупинено", show_alert=True)
        return
    key = q.data.split(":")[1]
    tariffs = active_tariffs()
    if key not in tariffs:
        await q.answer("❌ Тариф недоступний", show_alert=True)
        return
    t = tariffs[key]
    context.user_data.update({
        "tariff": key,
        "tariff_name": t.get("name"),
        "tariff_price": t.get("price"),
        "state": AWAIT_FIO
    })
    
    cancel_kb = mkb([InlineKeyboardButton("❌ Скасувати оформлення", callback_data="home")])
    await safe_edit(q,
        f"{t.get('emoji','📦')} Тариф: <b>{esc(t.get('name'))}</b> ({t.get('price')}₴)\n"
        f"Прогрес: {progress_bar(1, 7)}\n\n"
        "📝 <b>Крок 1/7</b> — Введіть ПІБ українською мовою:\n"
        "<i>Приклад: Шевченко Тарас Григорович</i>",
        kb=cancel_kb
    )


# ── Questionnaire steps ────────────────────────────────────────────────────────

async def select_sex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["sex"] = q.data.split(":")[1]
    context.user_data["state"] = AWAIT_ADDRESS
    sex_text = "Чоловік ♂️" if context.user_data["sex"] == "M" else "Жінка ♀️"
    
    cancel_kb = mkb([InlineKeyboardButton("❌ Скасувати", callback_data="home")])
    await safe_edit(q,
        f"✅ Стать: <b>{sex_text}</b>\n"
        f"Прогрес: {progress_bar(4, 7)}\n\n"
        "🏠 <b>Крок 4/7</b> — Адреса прописки:\n"
        "<i>Приклад: м. Харків, вул. Сумська, 10, кв. 25</i>\n\n"
        "💡 <i>Надішліть /skip для автоматичної генерації випадкової адреси</i>",
        kb=cancel_kb
    )


async def _ask_rights(update, context):
    kb = mkb([
        [InlineKeyboardButton("✅ Так", callback_data="rights:yes"),
         InlineKeyboardButton("❌ Ні",  callback_data="rights:no")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="home")]
    ])
    text = f"Прогрес: {progress_bar(5, 7)}\n\n🚗 <b>Крок 5/7</b> — Додати посвідчення водія (права)?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def _ask_zagran(update, context):
    kb = mkb([
        [InlineKeyboardButton("✅ Так", callback_data="zagran:yes"),
         InlineKeyboardButton("❌ Ні",  callback_data="zagran:no")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="home")]
    ])
    text = f"Прогрес: {progress_bar(6, 7)}\n\n🌍 <b>Крок 6/7</b> — Додати закордонний паспорт?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def _ask_diploma(update, context):
    kb = mkb([
        [InlineKeyboardButton("✅ Так", callback_data="diploma:yes"),
         InlineKeyboardButton("❌ Ні",  callback_data="diploma:no")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="home")]
    ])
    text = f"Прогрес: {progress_bar(7, 7)}\n\n🎓 <b>Крок 7/7</b> — Додати диплом / студентський квиток?"
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
    text = (
        "📸 <b>Фінальний штрих!</b>\n\n"
        "Надішліть фотографію користувача (3×4 або портретне фото обличчя на світлому фоні).\n"
        "<i>Фото буде використано для документів.</i>"
    )
    await safe_edit(update.callback_query, text, mkb([InlineKeyboardButton("❌ Скасувати", callback_data="home")]))


# ── Referral & Withdraw Menu ──────────────────────────────────────────────────

async def ref_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    u = _load(USERS_KEY).get(uid, {})
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    text = (
        f"👥 <b>Партнерська програма</b>\n\n"
        f"Запрошуйте друзів та отримуйте <b>{REFERRAL_REWARD}₴</b> за кожне їхнє перше замовлення!\n\n"
        f"📊 Ваша статистика:\n"
        f"• Запрошено друзів: <b>{u.get('ref_count', 0)}</b>\n"
        f"• Доступний баланс: <b>{u.get('balance', 0)}₴</b>\n"
        f"• Мінімальна сума виводу: <b>{MIN_WITHDRAW}₴</b>\n\n"
        f"🔗 Ваше посилання для запрошення:\n<code>{ref_link}</code>"
    )
    btns = []
    if u.get("balance", 0) >= MIN_WITHDRAW:
        btns.append([InlineKeyboardButton("💸 Запросити вивід коштів", callback_data="withdraw")])
    btns.append(back_btn("home"))
    await safe_edit(q, text, InlineKeyboardMarkup(btns), disable_web_page_preview=True)


async def withdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    u = _load(USERS_KEY).get(uid, {})
    if u.get("balance", 0) < MIN_WITHDRAW:
        await q.answer(f"❌ Мінімальна сума для виводу: {MIN_WITHDRAW}₴", show_alert=True)
        return
    context.user_data["state"] = AWAIT_WITHDRAW_CARD
    await safe_edit(q, f"💸 <b>Вивід коштів ({u.get('balance')}₴)</b>\n\nВведіть номер банківської картки для виплати:",
                    mkb([InlineKeyboardButton("❌ Скасувати", callback_data="profile")]))


# ── Main message handler ───────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if _load(USERS_KEY).get(uid, {}).get("banned"):
        return

    state = context.user_data.get("state")
    text  = (update.message.text or "").strip()

    if is_admin(uid) and update.message.reply_to_message:
        await _handle_admin_reply_msg(update, context)
        return

    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context)
        return

    if state == AWAIT_WITHDRAW_CARD:
        card = text
        u = _load(USERS_KEY).get(uid, {})
        bal = u.get("balance", 0)
        context.user_data["state"] = None
        
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"💸 <b>Запит на вивід коштів!</b>\n\n"
                    f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
                    f"🆔 <code>{uid}</code>\n"
                    f"💰 Сума: <b>{bal}₴</b>\n"
                    f"💳 Картка: <code>{esc(card)}</code>",
                    reply_markup=mkb([InlineKeyboardButton("✅ Підтвердити виплату", callback_data=f"adm_withdraw_ok:{uid}:{bal}")]),
                    parse_mode="HTML"
                )
            except Exception:
                pass
        
        await update.message.reply_text("✅ <b>Запит на вивід надіслано!</b> Адміністратор обробить його найближчим часом.", parse_mode="HTML")
        return

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
                    f"💬 <b>Новий відгук/запитання #{esc(fid)}</b>\n"
                    f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
                    f"🆔 {uid}\n📝 {esc(text)}",
                    reply_markup=mkb([InlineKeyboardButton("✍️ Відповісти", callback_data=f"reply_fb:{fid}")]),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        context.user_data["state"] = None
        await update.message.reply_text("✅ <b>Дякуємо! Повідомлення передано оператору.</b> 🌸", parse_mode="HTML")
        return

    # ── Анкета ──
    if state == AWAIT_FIO:
        if len(text.split()) < 2:
            await update.message.reply_text("❌ Будь ласка, введіть як мінімум Прізвище та Ім'я.")
            return
        context.user_data["fio"] = text
        context.user_data["state"] = AWAIT_DOB
        cancel_kb = mkb([InlineKeyboardButton("❌ Скасувати", callback_data="home")])
        await update.message.reply_text(
            f"Прогрес: {progress_bar(2, 7)}\n\n📅 <b>Крок 2/7</b> — Дата народження (Формат: <b>ДД.ММ.РРРР</b>):\n<i>Приклад: 15.08.1998</i>",
            reply_markup=cancel_kb,
            parse_mode="HTML"
        )
        return

    if state == AWAIT_DOB:
        if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", text):
            await update.message.reply_text("❌ Некоректний формат! Введіть дату у форматі ДД.ММ.РРРР (наприклад: 24.08.1991)")
            return
        context.user_data["dob"] = text
        context.user_data["state"] = AWAIT_SEX
        cancel_kb = mkb([
            [InlineKeyboardButton("♂️ Чоловік", callback_data="sex:M"),
             InlineKeyboardButton("♀️ Жінка",   callback_data="sex:W")],
            [InlineKeyboardButton("❌ Скасувати", callback_data="home")]
        ])
        await update.message.reply_text(
            f"Прогрес: {progress_bar(3, 7)}\n\n👤 <b>Крок 3/7</b> — Оберіть стать:",
            reply_markup=cancel_kb,
            parse_mode="HTML",
        )
        return

    if state == AWAIT_ADDRESS:
        context.user_data["address"] = "" if text.lower() in ("/skip", "skip") else text
        context.user_data["state"] = AWAIT_RIGHTS_CHOICE
        await _ask_rights(update, context)
        return

    if is_admin(uid):
        await _handle_admin_state(update, context, state, text, uid)
        return

    if AI_ENABLED and get_setting("ai_support") and text and not state:
        history = context.user_data.get("ai_history", [])
        reply = await ai_support_reply(text, history[-8:])
        if reply:
            history.append({"role": "user", "content": text})
            history.append({"role": "assistant", "content": reply})
            context.user_data["ai_history"] = history[-16:]
            await update.message.reply_text(
                f"🤖 {reply}\n\n<i>💡 Щоб зробити замовлення, натисніть /start</i>", parse_mode="HTML")
            return

    try:
        fwd = await update.message.forward(ADMIN_IDS[0])
        await context.bot.send_message(
            ADMIN_IDS[0],
            f"📩 <b>Вхідне повідомлення</b>\n"
            f"👤 {esc(update.effective_user.first_name)} | 🆔 <code>{uid}</code>\n📅 {now_fmt()}",
            reply_to_message_id=fwd.message_id, parse_mode="HTML",
        )
        await update.message.reply_text("✉️ Повідомлення переслано підтримці.", parse_mode="HTML")
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

    os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)
    photo_path = os.path.join(ORDER_PHOTOS_DIR, f"{oid}.png")
    with open(photo_path, "wb") as f:
        f.write(photo_bytes)

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
        f"🆔 <code>{uid}</code>\n"
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
    
    s = load_settings()
    await update.message.reply_text(
        f"✅ <b>Замовлення #{esc(oid)} успішно сформовано!</b>\n\n"
        f"💳 Сума до сплати: <b>{price_text}</b>\n\n"
        f"📌 <b>Реквізити для оплати:</b>\n"
        f"Картка: <code>{esc(s.get('payment_card'))}</code>\n"
        f"Отримувач: <b>{esc(s.get('payment_holder'))}</b>\n"
        f"{'🔗 Банка: ' + s.get('payment_link') + chr(10) if s.get('payment_link') else ''}\n"
        f"📤 <i>Після оплати надішліть квитанцію/скріншот у цей чат!</i>",
        parse_mode="HTML",
        disable_web_page_preview=True
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
            "⚠️ <b>Активних замовлень не знайдено.</b>\n\nСтворіть нове замовлення через /start",
            parse_mode="HTML")
        return

    last_oid, last_order = user_orders[0]
    expected_price = last_order.get("final_price", 0)

    await update.message.reply_text("🔎 <b>Чек отримано!</b> Запускаємо верифікацію...", parse_mode="HTML")

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
            f"🎉 <b>Оплату підтверджено автоматично!</b> (Точність AI: {confidence}%)\n⚡️ Розпочинаємо автоматичне розгортання вашого кабінету...",
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

            sub_line = f"\n📅 Підписка діє до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>" if sub_end else "\n♾ Підписка: Безстрокова"
            await update.message.reply_text(
                f"🌐 <b>Ваш кабінет успішно створено!</b>\n\n🔗 <b>Посилання:</b> {folder1_url}{sub_line}\n\n📋 Номер замовлення: <code>{esc(last_oid)}</code>",
                parse_mode="HTML")

            await notify_group(context.bot,
                f"🤖 <b>АВТО-ДЕПЛОЙ УСПІШНИЙ</b>\n"
                f"👤 {esc(update.effective_user.first_name)} | <code>{uid}</code>\n"
                f"📦 <code>{esc(last_oid)}</code> | AI: {confidence}%\n🔗 {folder1_url}")
        except Exception as e:
            logger.error("Auto-deploy error: %s", e, exc_info=True)
            await update.message.reply_text("⚠️ Чек перевірено, але під час деплою виникла затримка. Оператор закінчить налаштування вручну.", parse_mode="HTML")
        return

    first_name = esc(update.effective_user.first_name)
    username   = esc(update.effective_user.username or "")
    ai_line = ""
    if AI_ENABLED and ai_result.get("ok") is not None:
        ai_line = (
            f"\n🤖 AI: {'✅ Схожий' if ai_result.get('ok') else '⚠️ Сумнівний'} ({confidence}%)"
            + (f" | Сума: {ai_result.get('amount')}₴" if ai_result.get("amount") else "")
        )

    info = (
        f"📑 <b>Новий чек про оплату</b>\n"
        f"👤 {first_name} (@{username})\n🆔 <code>{uid}</code>\n"
        f"📦 Замовлення: <code>{esc(last_oid)}</code>\n💰 Очікувана сума: <b>{expected_price}₴</b>\n"
        f"📅 {now_fmt()}{ai_line}"
    )
    receipt_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Підтвердити + деплой", callback_data=f"adm_approve_deploy:{uid}:{last_oid}")],
        [InlineKeyboardButton("✅ Підтвердити (без деплою)", callback_data=f"adm_approve:{uid}:{last_oid}")],
        [InlineKeyboardButton("❌ Відхилити чек", callback_data=f"adm_reject:{uid}:{last_oid}")],
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
        "✅ <b>Квитанцію передано модератору!</b>\nПеревірка зазвичай займає від 2 до 10 хвилин. 🌸", parse_mode="HTML")


# ── Chain deploy helper ────────────────────────────────────────────────────────

async def _run_chain_deploy(oid: str, order: dict) -> str:
    values_data = order.get("values_data")
    if not values_data:
        values_data = gen_values_dict({
            "fio": order.get("fio", ""), "dob": order.get("dob", ""),
            "sex": order.get("sex", "M"), "is_rights": order.get("is_rights", True),
            "is_zagran": order.get("is_zagran", True), "is_diploma": order.get("is_diploma", False),
            "is_study": order.get("is_study", False), "address": order.get("address", ""),
            "order_id": oid,
        })

    result = await asyncio.to_thread(chain_deploy.run_full_chain, values_data, oid)
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
                f"💰 <b>Нараховано реферальний бонус +{REFERRAL_REWARD}₴!</b>\nПоточний баланс: <b>{users[ref_by]['balance']}₴</b>",
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
        await update.message.reply_text("⚠️ Не вдалося розпізнати ID клієнта в повідомленні.")
        return
    client_id = m.group(1)
    try:
        await context.bot.send_message(
            client_id,
            f"💬 <b>Відповідь від адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸",
            parse_mode="HTML")
        await update.message.reply_text(f"✅ Відповідь надіслано користувачу <code>{client_id}</code>", parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка надсилання: {e}")


async def _do_reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = context.user_data.get("reply_to_uid")
    fid    = context.user_data.get("reply_fb_id")
    if target:
        try:
            await context.bot.send_message(
                target,
                f"💬 <b>Відповідь від адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸",
                parse_mode="HTML")
            if fid:
                feedbacks = _load(FEEDBACK_KEY)
                if fid in feedbacks:
                    feedbacks[fid]["status"]      = "replied"
                    feedbacks[fid]["admin_reply"] = update.message.text
                    _save(FEEDBACK_KEY, feedbacks)
            await update.message.reply_text(f"✅ Відповідь надіслана → <code>{target}</code>", parse_mode="HTML")
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
        await update.message.reply_text("✅ Вітальне повідомлення успішно оновлено!"); return

    if state == AWAIT_BROADCAST:
        context.user_data["broadcast_text"] = text
        context.user_data["state"] = None
        users = _load(USERS_KEY)
        await update.message.reply_text(
            f"📢 <b>Попередній перегляд розсилки:</b>\n\n{text}\n\n👥 Отримувачів: <b>{len(users)}</b>",
            reply_markup=mkb(
                [InlineKeyboardButton("✅ Підтвердити та надіслати", callback_data="broadcast_go"),
                 InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")],
            ), parse_mode="HTML"); return

    if state == AWAIT_PROMO_CODE:
        context.user_data["new_promo_code"] = text.upper().strip()
        context.user_data["state"] = AWAIT_PROMO_DISCOUNT
        await update.message.reply_text(f"✅ Код: <b>{esc(context.user_data['new_promo_code'])}</b>\n\nВведіть розмір знижки у % (1-100):", parse_mode="HTML"); return

    if state == AWAIT_PROMO_DISCOUNT:
        try:
            context.user_data["new_promo_discount"] = int(text)
            context.user_data["state"] = AWAIT_PROMO_USES
            await update.message.reply_text(f"✅ Знижка: {text}%\n\nВведіть макс. кількість активацій (0 = без ліміту):")
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
                f"✅ <b>Промокод створено!</b>\nКод: <code>{esc(code)}</code>\nЗнижка: {context.user_data['new_promo_discount']}%  |  Ліміт: {'∞' if not uses else uses}",
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
            await update.message.reply_text("🔍 Користувача не знайдено."); return
        await _send_user_card(update, context, found[0][0], found[0][1]); return

    if state == AWAIT_BALANCE_UID:
        context.user_data["balance_target_uid"] = text.strip()
        context.user_data["state"] = AWAIT_BALANCE_AMOUNT
        await update.message.reply_text("💰 Введіть суму зміни балансу (+50 або -20):")
        return

    if state == AWAIT_BALANCE_AMOUNT:
        try:
            amount = int(text)
            target_uid = context.user_data.get("balance_target_uid")
            users = _load(USERS_KEY)
            if target_uid not in users:
                await update.message.reply_text("❌ Користувача не знайдено.")
            else:
                users[target_uid]["balance"] = max(0, users[target_uid].get("balance", 0) + amount)
                _save(USERS_KEY, users)
                context.user_data["state"] = None
                await update.message.reply_text(
                    f"✅ Баланс користувача <code>{target_uid}</code> змінено на {'+' if amount>=0 else ''}{amount}₴\nНовий баланс: <b>{users[target_uid]['balance']}₴</b>", parse_mode="HTML")
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
            await update.message.reply_text("✅ Реквізити успішно збережено!")
        else:
            await update.message.reply_text("❌ Введіть як мінімум 2 рядки: 1) Номер картки 2) ПІБ отримувача")
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
                    f"❌ <b>Замовлення #{esc(oid)} відхилено!</b>\n\nПричина: {esc(text)}",
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
                    await update.message.reply_text(f"✅ Нова ціна: {text}₴")
                except ValueError:
                    await update.message.reply_text("❌ Введіть ціле число!")
            elif state == AWAIT_TARIFF_EDIT_NAME:
                tariffs[key]["name"] = text
                save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Назву змінено на: {esc(text)}")
            else:
                tariffs[key]["emoji"] = text.strip()
                save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Емодзi оновлено: {text}")
        return

    if state == AWAIT_TARIFF_NAME:
        context.user_data["new_t_name"] = text
        context.user_data["state"] = AWAIT_TARIFF_PRICE
        await update.message.reply_text("💰 Введіть ціну в гривнях (₴):"); return

    if state == AWAIT_TARIFF_PRICE:
        try:
            context.user_data["new_t_price"] = int(text)
            context.user_data["state"] = AWAIT_TARIFF_DAYS
            await update.message.reply_text("📅 Кількість днів дії (0 = безстроково):")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_TARIFF_DAYS:
        try:
            context.user_data["new_t_days"] = int(text) or None
            context.user_data["state"] = AWAIT_TARIFF_EMOJI
            await update.message.reply_text("😊 Введіть емодзi тарифу (наприклад: 🌟):")
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
            f"✅ <b>Тариф створено:</b> {emj} {esc(name)} — {price}₴ ({'∞' if not days else f'{days} дн.'})", parse_mode="HTML"); return


# ── Admin: complete order (manual files) ──────────────────────────────────────

async def _process_complete_order_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    oid = context.user_data.get("complete_oid")
    client_uid = context.user_data.get("complete_uid")
    if not oid or not client_uid:
        return
    try:
        caption = f"📁 <b>Готові файли за замовленням #{esc(oid)}</b> 🌸"
        if update.message.document:
            await context.bot.send_document(client_uid, update.message.document.file_id, caption=caption, parse_mode="HTML")
        elif update.message.photo:
            await context.bot.send_photo(client_uid, update.message.photo[-1].file_id, caption=caption, parse_mode="HTML")
        elif update.message.video:
            await context.bot.send_video(client_uid, update.message.video.file_id, caption=caption, parse_mode="HTML")

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
        await update.message.reply_text(f"✅ Файли успішно відправлені клієнту <code>{client_uid}</code> (#{oid})", parse_mode="HTML")
        context.user_data["state"] = None
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка надсилання файлу: {e}")


# ── Admin panel ────────────────────────────────────────────────────────────────

@admin_only
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users  = _load(USERS_KEY)
    orders = _load(ORDERS_KEY)
    pending = sum(1 for o in orders.values() if o.get("status") == "pending")
    await safe_edit(q,
        f"⚙️ <b>Центр Управління (Адмін-Панель)</b>\n\n"
        f"👥 Всього користувачів: <b>{len(users)}</b> | ⏳ Очікують перевірки: <b>{pending}</b>\n"
        f"🔑 GH Token 1: {'✅' if PAGES_GH_TOKEN else '❌'} | GH Token 2: {'✅' if os.getenv('GH_TOKEN_2') else '❌'}\n"
        f"🕒 Серверний час: {now_fmt()}",
        mkb(
            [InlineKeyboardButton("📊 Статистика",   callback_data="adm:stats"),
             InlineKeyboardButton("📋 Замовлення",   callback_data="adm:orders")],
            [InlineKeyboardButton("👥 Користувачі",  callback_data="adm:users"),
             InlineKeyboardButton("🔍 Пошук",        callback_data="adm:search")],
            [InlineKeyboardButton("💰 Тарифи",       callback_data="adm:tariffs"),
             InlineKeyboardButton("🎟 Промокоди",   callback_data="adm:promos")],
            [InlineKeyboardButton("📢 Розсилка",     callback_data="adm:broadcast"),
             InlineKeyboardButton("💬 Відгуки",      callback_data="adm:feedbacks")],
            [InlineKeyboardButton("⚙️ Налаштування", callback_data="adm:settings"),
             InlineKeyboardButton("📜 Логи",         callback_data="adm:logs")],
            [InlineKeyboardButton("🚀 Деплой (ланцюжок)", callback_data="adm:chain_deploy")],
            [InlineKeyboardButton("📥 Завантажити БД", callback_data="adm:export_db")],
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
        f"📊 <b>Детальна статистика</b> ({now_fmt()})\n\n"
        f"👥 Користувачі: <b>{len(users)}</b> (+{new_u_24h} за 24г)\n"
        f"📦 Всього замовлень: <b>{total_o}</b>\n"
        f"├ ✅ Завершено вручну: {done_o}\n"
        f"├ 🌐 Задеплоєно: {deployed_o}\n"
        f"├ ⏳ Очікують: {pending_o}\n"
        f"└ ❌ Відхилено: {rejected_o}\n\n"
        f"📈 Нових замовлень за 24г: <b>+{new_o_24h}</b>\n"
        f"💰 Загальний прибуток: <b>{revenue} UAH</b>",
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
    text = f"📋 <b>Список замовлень [{st}]</b> (Всього: {len(filtered)})\n\n"
    kb_rows = []
    for oid, o in filtered[:12]:
        text += f"• <code>#{esc(oid)}</code> — {esc(o.get('fio','?')[:20])} ({o.get('created_at','')[:10]})\n"
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
    await safe_edit(q, text or "📭 Замовлень з таким статусом не знайдено.", InlineKeyboardMarkup(kb_rows))


@admin_only
async def adm_order_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    oid = q.data.split(":")[1]
    o   = _load(ORDERS_KEY).get(oid, {})
    uid2 = o.get("user_id", "?")
    status_map = {"pending":"⏳ Очікує","approved":"✅ Сплачено","completed":"🎉 Виконано","rejected":"❌ Відхилено","deployed":"🌐 Задеплоєно"}
    st = status_map.get(o.get("status",""), o.get("status","?"))
    url = esc(o.get("pages_url", ""))
    pages_line = f"\n🔗 <b>URL:</b> <a href='{url}'>Відкрити сайт</a>" if url else ""
    text = (
        f"📋 <b>Замовлення #{esc(oid)}</b> ({st})\n\n"
        f"👤 ПІБ: <b>{esc(o.get('fio','?'))}</b>\n"
        f"📅 ДН: {esc(o.get('dob','?'))}\n"
        f"🆔 User ID: <code>{uid2}</code>\n"
        f"💎 Тариф: {esc(o.get('tariff_name','?'))} (<b>{o.get('final_price','?')}₴</b>)\n"
        f"📅 Створено: {o.get('created_at','')[:16]}{pages_line}"
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
        kb_rows.append([InlineKeyboardButton("🚀 Запустити деплой вручну", callback_data=f"adm_push_pages:{uid2}:{oid}")])
    if o.get("pages_url"):
        kb_rows.append([InlineKeyboardButton("🔗 Надіслати посилання клієнту", callback_data=f"adm_send_link:{uid2}:{oid}")])
    kb_rows.append(back_btn("adm:orders"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows), disable_web_page_preview=True)


@admin_only
async def adm_order_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["orders_filter"] = update.callback_query.data.split(":")[1]
    await adm_orders(update, context)


@admin_only
async def adm_approve_deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    await safe_edit(q, "⏳ <b>Підтверджуємо оплату та запускаємо деплой...</b>")

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
            f"✅ <b>Оплату підтверджено!</b>\n⏳ Генерація документів та створення сайту зазвичай займає 1-2 хвилини...\n📋 Замовлення: <code>{esc(oid)}</code>",
            parse_mode="HTML")
    except Exception as e:
        logger.error("Client notify error: %s", e)

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

        sub_line = f"\n📅 Підписка діє до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>" if sub_end else "\n♾ Підписка: Безстрокова"
        try:
            await context.bot.send_message(client_uid,
                f"🎉 <b>Ваш персональний кабінет готовий!</b>\n\n🔗 <b>Ссылка:</b> {folder1_url}{sub_line}\n\n"
                f"⏱ Якщо сторінка ще не завантажується — зачекайте 1 хвилину.\n📋 <code>{esc(oid)}</code>",
                parse_mode="HTML")
        except Exception as e:
            logger.error("Client deploy notify error: %s", e)

        await notify_group(context.bot,
            f"🚀 <b>УСПІШНИЙ ДЕПЛОЙ</b>\n📦 Замовлення: <code>{esc(oid)}</code>\n👤 Клієнт: <code>{client_uid}</code>\n🔗 {folder1_url}",
            mkb([InlineKeyboardButton("🔗 Перевидати посилання", callback_data=f"adm_send_link:{client_uid}:{oid}")]))

        await safe_edit(q,
            f"✅ <b>Замовлення підтверджено та розгорнуто!</b>\n📦 <code>{esc(oid)}</code>\n🔗 {esc(folder1_url)}",
            mkb([InlineKeyboardButton("🔗 Надіслати клієнту ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")],
                back_btn("admin_panel")))

    except Exception as e:
        logger.error("adm_approve_deploy: %s", e, exc_info=True)
        await safe_edit(q,
            f"⚠️ Оплату підтверджено, але виникла помилка під час деплою:\n<code>{esc(str(e)[:300])}</code>",
            mkb([InlineKeyboardButton("🔄 Спробувати деплой знову", callback_data=f"adm_push_pages:{client_uid}:{oid}")],
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
        f"👤 Отримувач: <b>{esc(s.get('payment_holder','—'))}</b>\n"
        f"{'🔗 Банка: ' + s.get('payment_link') + chr(10) if s.get('payment_link') else ''}\n"
        f"💰 Сума до сплати: <b>{price}₴</b>\n\n📤 Надішліть квитанцію про оплату сюди!"
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
    await safe_edit(q, f"✅ Реквізити надіслані користувачу <code>{client_uid}</code>. Оберіть подальшу дію:",
                    InlineKeyboardMarkup(kb_rows))


@admin_only
async def adm_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["reject_uid"] = parts[1]
    context.user_data["reject_oid"] = parts[2]
    context.user_data["state"]      = AWAIT_REJECT_REASON
    await safe_edit(q, f"❌ <b>Відхилення замовлення #{esc(parts[2])}</b>\n\nВведіть причину відхилення для клієнта:")


@admin_only
async def adm_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["complete_uid"] = parts[1]
    context.user_data["complete_oid"] = parts[2]
    context.user_data["state"]        = AWAIT_ORDER_COMPLETE_FILE
    await safe_edit(q, f"📨 <b>Передача файлів для #{esc(parts[2])}</b>\n\nНадішліть документи/файли у чат.")


@admin_only
async def adm_push_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]

    if not PAGES_GH_TOKEN or not os.getenv("GH_TOKEN_2"):
        await safe_edit(q,
            "❌ <b> GitHub Токени не налаштовано!</b>\n\n"
            "Перевірте змінні <code>PAGES_GH_TOKEN</code> і <code>GH_TOKEN_2</code>.",
            mkb(back_btn("admin_panel")))
        return

    order = _load(ORDERS_KEY).get(oid, {})
    await safe_edit(q,
        f"🚀 <b>Підтвердження публікації (Деплою)</b>\n\n"
        f"📦 Замовлення: <code>{esc(oid)}</code>\n"
        f"👤 Клієнт: <code>{client_uid}</code>\n"
        f"📝 ПІБ: <b>{esc(order.get('fio','?'))}</b>\n\n"
        "Буде виконано процес:\n1️⃣ Оновлення конфігурації\n2️⃣ Пуш папки 2\n"
        "3️⃣ Генерація QR\n4️⃣ Пуш папки 1",
        mkb([InlineKeyboardButton("🚀 Запустити процес", callback_data=f"adm_push_go:{client_uid}:{oid}")],
            [InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")]))


@admin_only
async def adm_push_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    await safe_edit(q, "⏳ <b>Триває процес автоматичного розгортання...</b>\nЗазвичай це займає 30–60 секунд.")

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

        sub_line = f"\n📅 Підписка діє до: <b>{datetime.fromisoformat(sub_end).strftime('%d.%m.%Y')}</b>" if sub_end else "\n♾ Підписка: Безстрокова"
        try:
            await context.bot.send_message(client_uid,
                f"✅ <b>Кабінет створено!</b>\n\n🔗 {folder1_url}{sub_line}\n\n📋 Замовлення: <code>{esc(oid)}</code>",
                parse_mode="HTML")
        except Exception as e:
            logger.error("Client notify error: %s", e)

        await notify_group(context.bot,
            f"🚀 <b>Деплой завершено</b>\n"
            f"📦 <code>{esc(oid)}</code> | 👤 <code>{client_uid}</code>\n"
            f"📝 {esc(order.get('fio',''))}\n🔗 {folder1_url}",
            mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")]))

        await safe_edit(q,
            f"✅ <b>Деплой успішно виконано!</b>\n📦 <code>{esc(oid)}</code>\n🔗 <code>{esc(folder1_url)}</code>",
            mkb([InlineKeyboardButton("🔗 Надіслати посилання клієнту", callback_data=f"adm_send_link:{client_uid}:{oid}")],
                back_btn("admin_panel")))

    except Exception as e:
        logger.error("adm_push_go: %s", e, exc_info=True)
        await safe_edit(q,
            f"❌ <b>Помилка під час виконання деплою:</b>\n\n<code>{esc(str(e)[:500])}</code>",
            mkb(back_btn("admin_panel")))


@admin_only
async def adm_chain_deploy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    has_gh = bool(PAGES_GH_TOKEN) and bool(os.getenv("GH_TOKEN_2"))
    status = "✅ Токени наявні" if has_gh else "❌ Відсутні токени PAGES_GH_TOKEN та GH_TOKEN_2"
    await safe_edit(q,
        f"🚀 <b>Примусове оновлення сайтів (Ланцюжок)</b>\n\nСтатус: {status}\n\n"
        "Запускає повний цикл оновлення серверів без прив'язки до замовлення.",
        mkb([InlineKeyboardButton("🚀 Запустити оновлення", callback_data="chain_deploy_run")] if has_gh else [],
            back_btn("admin_panel")))


@admin_only
async def adm_chain_deploy_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_edit(q, "⏳ <b>Виконується глобальний деплой...</b>")
    try:
        result = await asyncio.to_thread(chain_deploy.run_full_chain)
        folder2_url = result["folder2_url"]
        folder1_url = result["folder1_url"]
        log_action("chain_deploy", q.from_user.id, result)
        await safe_edit(q,
            f"🚀 <b>Глобальне оновлення завершено!</b>\n\n"
            f"📁 Сервер 2: {esc(folder2_url)}\n"
            f"📁 Сервер 1 (Головний): <b>{esc(folder1_url)}</b>\n\n"
            f"📷 Новий QR-код збережено.",
            mkb(back_btn("admin_panel")))
    except Exception as e:
        logger.error("chain_deploy_run error: %s", e, exc_info=True)
        await safe_edit(q, f"❌ <b>Виникла помилка:</b>\n\n<code>{esc(str(e)[:500])}</code>",
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
        await q.answer("❌ URL не знайдено", show_alert=True)
        return
    try:
        await context.bot.send_message(client_uid,
            f"✅ <b>Ваш персональний кабінет:</b>\n\n🔗 {url}\n\n📋 Замовлення: <code>{esc(oid)}</code>",
            parse_mode="HTML")
        await q.answer("✅ Посилання надіслано!", show_alert=True)
    except Exception as e:
        await q.answer(f"Помилка: {e}", show_alert=True)


# ── Admin: users ───────────────────────────────────────────────────────────────

@admin_only
async def adm_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users = _load(USERS_KEY)
    sorted_u = sorted(users.items(), key=lambda x: x[1].get("joined_date",""), reverse=True)[:15]
    text = f"👥 <b>База користувачів ({len(users)})</b>\n\n"
    for uid2, u in sorted_u:
        badges = ("👑" if u.get("vip") else "") + ("🚫" if u.get("banned") else "")
        text += f"{badges} <b>{esc(u.get('first_name','?'))}</b> (@{esc(u.get('username','?'))})\n"
        text += f"   🆔 <code>{uid2}</code> | Баланс: {u.get('balance',0)}₴\n\n"
    await safe_edit(q, text, mkb(
        [InlineKeyboardButton("🔍 Пошук користувача", callback_data="adm:search"),
         InlineKeyboardButton("💰 Змінити баланс", callback_data="adm:balance")],
        back_btn("admin_panel"),
    ))


@admin_only
async def adm_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_USER_SEARCH
    await safe_edit(update.callback_query, "🔍 Введіть @username, Telegram ID або ім'я:", mkb(back_btn("admin_panel")))


@admin_only
async def adm_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_BALANCE_UID
    await safe_edit(update.callback_query, "💰 Введіть ID користувача для зміни балансу:", mkb(back_btn("admin_panel")))


async def _send_user_card(update, context, uid2: str, u: dict):
    orders = _load(ORDERS_KEY)
    count  = sum(1 for o in orders.values() if o.get("user_id") == uid2)
    text = (
        f"👤 <b>Картка користувача: {esc(u.get('first_name','?'))}</b>\n"
        f"🆔 ID: <code>{uid2}</code>\n"
        f"📱 Юзернейм: @{esc(u.get('username','?'))}\n"
        f"💰 Баланс: <b>{u.get('balance',0)}₴</b>\n"
        f"📦 Всього замовлень: <b>{count}</b>\n"
        f"👥 Запрошено: <b>{u.get('ref_count',0)}</b>\n"
        f"👑 VIP: {'Так' if u.get('vip') else 'Ні'} | 🚫 Бан: {'Так' if u.get('banned') else 'Ні'}"
    )
    kb = mkb(
        [InlineKeyboardButton("🔓 Розбанити" if u.get("banned") else "🚫 Забанити", callback_data=f"adm_ban:{uid2}"),
         InlineKeyboardButton("Зняти VIP" if u.get("vip") else "👑 Дати VIP", callback_data=f"adm_vip:{uid2}")],
        [InlineKeyboardButton("💰 Поповнити баланс", callback_data="adm:balance"),
         InlineKeyboardButton("💬 Написати клієнту", callback_data=f"adm_msg:{uid2}")],
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
        await q.answer("Статус блокування змінено!", show_alert=True)
        await _send_user_card(update, context, uid2, users[uid2])


@admin_only
async def adm_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    users = _load(USERS_KEY)
    if uid2 in users:
        users[uid2]["vip"] = not users[uid2].get("vip", False)
        _save(USERS_KEY, users)
        await q.answer("VIP статус змінено!", show_alert=True)
        await _send_user_card(update, context, uid2, users[uid2])


@admin_only
async def adm_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    context.user_data["state"]        = AWAIT_REPLY_TO_USER
    context.user_data["reply_to_uid"] = uid2
    context.user_data.pop("reply_fb_id", None)
    await safe_edit(q, f"💬 <b>Написати користувачу {uid2}</b>\n\nВведіть текст вашого повідомлення:", mkb(back_btn("admin_panel")))


@admin_only
async def adm_withdraw_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    uid2, amount = parts[1], int(parts[2])
    users = _load(USERS_KEY)
    if uid2 in users:
        users[uid2]["balance"] = max(0, users[uid2].get("balance", 0) - amount)
        _save(USERS_KEY, users)
        try:
            await context.bot.send_message(uid2,
                f"🎉 <b>Виплату {amount}₴ успішно підтверджено!</b>\nКошти відправлені на вашу картку. 🌸",
                parse_mode="HTML")
        except Exception:
            pass
        log_action("withdraw_confirmed", q.from_user.id, {"uid": uid2, "amount": amount})
    await safe_edit(q, f"✅ Виплатa {amount}₴ для користувача <code>{uid2}</code> підтверджена.")


# ── Admin: tariffs ─────────────────────────────────────────────────────────────

@admin_only
async def adm_tariffs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = load_tariffs()
    text = "💰 <b>Управління тарифами</b>\n\n"
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
    kb_rows.append([InlineKeyboardButton("➕ Додати новий тариф", callback_data="tariff_add")])
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
    await safe_edit(q, f"✏️ Редагування: {t.get('emoji','📦')} <b>{esc(t.get('name',''))}</b>\n\nЩо саме змінити?",
        mkb([InlineKeyboardButton("📝 Назва", callback_data=f"tedit_name:{key}"),
              InlineKeyboardButton("💰 Ціна",  callback_data=f"tedit_price:{key}")],
            [InlineKeyboardButton("😊 Емодзi", callback_data=f"tedit_emoji:{key}")],
            back_btn("adm:tariffs")))


@admin_only
async def tedit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_tariff_key"] = update.callback_query.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_NAME
    await safe_edit(update.callback_query, "📝 Введіть нову назву тарифу:")


@admin_only
async def tedit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_tariff_key"] = update.callback_query.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_PRICE
    await safe_edit(update.callback_query, "💰 Введіть нову ціну у гривнях (₴):")


@admin_only
async def tedit_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["edit_tariff_key"] = update.callback_query.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_EMOJI
    await safe_edit(update.callback_query, "😊 Введіть нове емодзi тарифу:")


@admin_only
async def tariff_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_TARIFF_NAME
    await safe_edit(update.callback_query, "➕ <b>Створення тарифу</b>\n\nКрок 1/4: Введіть назву:")


# ── Admin: promos ──────────────────────────────────────────────────────────────

@admin_only
async def adm_promos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    promos = load_promos()
    text = "🎟 <b>Управління промокодами</b>\n\n"
    kb_rows = []
    for code, p in promos.items():
        st = "✅" if p.get("active", True) else "❌"
        text += f"{st} <code>{esc(code)}</code> — <b>{p.get('discount')}%</b> (Використано: {p.get('uses',0)}/{'∞' if not p.get('max_uses') else p.get('max_uses')})\n"
        kb_rows.append([
            InlineKeyboardButton(f"🗑 Видалити {code}", callback_data=f"promo_del:{code}")
        ])
    kb_rows.append([InlineKeyboardButton("➕ Створити промокод", callback_data="promo_add")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def promo_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.callback_query.data.split(":")[1]
    promos = load_promos()
    if code in promos:
        del promos[code]
        save_promos(promos)
    await adm_promos(update, context)


@admin_only
async def promo_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(update.callback_query, "🎟 <b>Створення промокоду</b>\n\nВведіть слово-код (наприклад: DISCOUNT20):")


# ── Admin: Broadcast & Settings ────────────────────────────────────────────────

@admin_only
async def adm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_BROADCAST
    await safe_edit(update.callback_query, "📢 <b>Масова розсилка</b>\n\nВведіть текст повідомлення для всіх користувачів бота:")


@admin_only
async def broadcast_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    text = context.user_data.get("broadcast_text")
    if not text:
        await q.answer("❌ Текст порожній", show_alert=True)
        return

    await safe_edit(q, "⏳ <b>Розсилка запущена...</b>")
    users = _load(USERS_KEY)
    success, failed = 0, 0

    for uid in users.keys():
        try:
            await context.bot.send_message(uid, text, parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await safe_edit(q,
        f"✅ <b>Розсилку завершено!</b>\n\nУспішно надіслано: <b>{success}</b>\nПомилок/Заблоковано: <b>{failed}</b>",
        mkb(back_btn("admin_panel"))
    )


@admin_only
async def adm_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings()
    text = (
        "⚙️ <b>Налаштування системи</b>\n\n"
        f"• Технічні роботи: {'🔴 Включено' if s.get('maintenance_mode') else '🟢 Виключено'}\n"
        f"• Прийом нових замовлень: {'🟢 Дозволено' if s.get('new_orders_enabled') else '🔴 Призупинено'}\n"
        f"• AI верифікація чеків: {'🟢 Включено' if s.get('ai_check_receipts') else '🔴 Виключено'}\n"
        f"• AI Авто-деплой: {'🟢 Включено' if s.get('ai_auto_deploy') else '🔴 Виключено'}\n\n"
        f"💳 Картка: <code>{esc(s.get('payment_card'))}</code>\n"
        f"👤 Отримувач: <b>{esc(s.get('payment_holder'))}</b>"
    )
    kb = mkb(
        [InlineKeyboardButton("🛠 Тех. роботи (Вкл/Викл)", callback_data="sett_toggle:maintenance_mode")],
        [InlineKeyboardButton("📦 Прийом замовлень (Вкл/Викл)", callback_data="sett_toggle:new_orders_enabled")],
        [InlineKeyboardButton("🤖 AI перевірка чеків", callback_data="sett_toggle:ai_check_receipts")],
        [InlineKeyboardButton("🚀 AI авто-деплой", callback_data="sett_toggle:ai_auto_deploy")],
        [InlineKeyboardButton("💳 Змінити реквізити", callback_data="sett_edit_payment")],
        [InlineKeyboardButton("👋 Змінити привітання", callback_data="sett_edit_welcome")],
        back_btn("admin_panel")
    )
    await safe_edit(q, text, kb)


@admin_only
async def sett_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = update.callback_query.data.split(":")[1]
    s = load_settings()
    s[key] = not s.get(key, False)
    save_settings(s)
    await adm_settings(update, context)


@admin_only
async def sett_edit_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_CUSTOM_PAYMENT_TEXT
    await safe_edit(update.callback_query,
        "💳 <b>Зміна реквізитів</b>\n\n"
        "Надішліть нові реквізити форматом (кожен пункт з нового рядка):\n"
        "<code>Номер картки</code>\n"
        "<code>ПІБ отримувача</code>\n"
        "<code>Посилання на банку (опційно)</code>"
    )


@admin_only
async def sett_edit_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_WELCOME_TEXT
    await safe_edit(update.callback_query, "👋 <b>Введіть новий текст привітання для команди /start:</b>")


@admin_only
async def adm_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        logs = _db.get_recent_logs(20)
        text = "📜 <b>Останні системні події:</b>\n\n"
        for l in logs[:15]:
            text += f"• <i>{l.get('created_at','')[:16]}</i> | <b>{esc(l.get('action'))}</b> ({esc(l.get('user_id','sys'))})\n"
        await safe_edit(q, text or "📭 Логи порожні.", mkb(back_btn("admin_panel")))
    except Exception as e:
        await safe_edit(q, f"❌ Помилка читання логів: {e}", mkb(back_btn("admin_panel")))


@admin_only
async def adm_export_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = {
        "users": _load(USERS_KEY),
        "orders": _load(ORDERS_KEY),
        "tariffs": load_tariffs(),
        "settings": load_settings(),
        "promos": load_promos()
    }
    bio = io.BytesIO(json.dumps(data, ensure_ascii=False, indent=2).encode())
    bio.name = f"db_export_{now_fmt('%Y%m%d_%H%M')}.json"
    await context.bot.send_document(q.message.chat_id, document=bio, caption="📥 Дамп бази даних бота")
    await q.answer("✅ БД завантажено!")


# ── Common Callbacks Dispatcher ───────────────────────────────────────────────

async def cb_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data

    if data == "home":
        await cmd_start(update, context)
    elif data == "profile":
        await show_profile(update, context)
    elif data == "my_orders":
        await my_orders_handler(update, context)
    elif data.startswith("user_order_view:"):
        await user_order_view(update, context)
    elif data.startswith("show_qr:"):
        await show_qr_handler(update, context)
    elif data == "catalog":
        await show_catalog(update, context)
    elif data.startswith("tar:"):
        await select_tariff(update, context)
    elif data.startswith("sex:"):
        await select_sex(update, context)
    elif data.startswith("rights:"):
        await select_rights(update, context)
    elif data.startswith("zagran:"):
        await select_zagran(update, context)
    elif data.startswith("diploma:"):
        await select_diploma(update, context)
    elif data == "ref_menu":
        await ref_menu_handler(update, context)
    elif data == "withdraw":
        await withdraw_start(update, context)
    elif data == "promo_enter":
        context.user_data["state"] = AWAIT_PROMO_CODE
        await safe_edit(q, "🎟 <b>Введіть свій промокод:</b>", mkb(back_btn("home")))
    elif data == "feedback":
        context.user_data["state"] = AWAIT_FEEDBACK
        await safe_edit(q, "💬 <b>Задайте питання або напишіть відгук:</b>\n\nОператор відповість найближчим часом.", mkb(back_btn("home")))
    elif data == "about":
        await safe_edit(q,
            "ℹ️ <b>Про сервіс FunsDiia</b>\n\n"
            "Автоматизований інструмент швидкого оформлення веб-документів.\n"
            "• Миттєва генерація\n• Автоматична перевірка квитанцій AI\n• Цілодобовий доступ\n\n"
            "З усіх питань звертайтесь через кнопку Підтримки.",
            mkb(back_btn("home"))
        )
    # Admin Routers
    elif data == "admin_panel":
        await admin_panel(update, context)
    elif data == "adm:stats":
        await adm_stats(update, context)
    elif data == "adm:orders":
        await adm_orders(update, context)
    elif data.startswith("adm_order_view:"):
        await adm_order_view(update, context)
    elif data.startswith("adm_order_filter:"):
        await adm_order_filter(update, context)
    elif data.startswith("adm_approve_deploy:"):
        await adm_approve_deploy(update, context)
    elif data.startswith("adm_approve:"):
        await adm_approve(update, context)
    elif data.startswith("adm_reject:"):
        await adm_reject(update, context)
    elif data.startswith("adm_complete:"):
        await adm_complete(update, context)
    elif data.startswith("adm_push_pages:"):
        await adm_push_pages(update, context)
    elif data.startswith("adm_push_go:"):
        await adm_push_go(update, context)
    elif data == "adm:chain_deploy":
        await adm_chain_deploy_menu(update, context)
    elif data == "chain_deploy_run":
        await adm_chain_deploy_run(update, context)
    elif data.startswith("adm_send_link:"):
        await adm_send_link(update, context)
    elif data == "adm:users":
        await adm_users(update, context)
    elif data == "adm:search":
        await adm_search(update, context)
    elif data == "adm:balance":
        await adm_balance(update, context)
    elif data.startswith("adm_ban:"):
        await adm_ban(update, context)
    elif data.startswith("adm_vip:"):
        await adm_vip(update, context)
    elif data.startswith("adm_msg:"):
        await adm_msg(update, context)
    elif data.startswith("adm_withdraw_ok:"):
        await adm_withdraw_ok(update, context)
    elif data == "adm:tariffs":
        await adm_tariffs(update, context)
    elif data.startswith("tariff_toggle:"):
        await tariff_toggle(update, context)
    elif data.startswith("tariff_del:"):
        await tariff_del(update, context)
    elif data.startswith("tariff_edit:"):
        await tariff_edit(update, context)
    elif data.startswith("tedit_name:"):
        await tedit_name(update, context)
    elif data.startswith("tedit_price:"):
        await tedit_price(update, context)
    elif data.startswith("tedit_emoji:"):
        await tedit_emoji(update, context)
    elif data == "tariff_add":
        await tariff_add(update, context)
    elif data == "adm:promos":
        await adm_promos(update, context)
    elif data.startswith("promo_del:"):
        await promo_del(update, context)
    elif data == "promo_add":
        await promo_add(update, context)
    elif data == "adm:broadcast":
        await adm_broadcast(update, context)
    elif data == "broadcast_go":
        await broadcast_go(update, context)
    elif data == "adm:settings":
        await adm_settings(update, context)
    elif data.startswith("sett_toggle:"):
        await sett_toggle(update, context)
    elif data == "sett_edit_payment":
        await sett_edit_payment(update, context)
    elif data == "sett_edit_welcome":
        await sett_edit_welcome(update, context)
    elif data == "adm:logs":
        await adm_logs(update, context)
    elif data == "adm:export_db":
        await adm_export_db(update, context)
    elif data.startswith("reply_fb:"):
        fid = data.split(":")[1]
        fb = _load(FEEDBACK_KEY).get(fid, {})
        if fb:
            context.user_data["state"]        = AWAIT_REPLY_TO_USER
            context.user_data["reply_to_uid"] = fb.get("user_id")
            context.user_data["reply_fb_id"]  = fid
            await safe_edit(q, f"💬 Відповідь на відгук #{fid}\n\nВведіть текст відповідин:")


# ── Main entry point ───────────────────────────────────────────────────────────

def main():
    global _DB
    logger.info("Starting FunsDiia Bot...")

    # Инициализация БД
    _db.init_db()
    _DB = {
        USERS_KEY:    {"load": _db.load_users,    "save": _db.save_users},
        ORDERS_KEY:   {"load": _db.load_orders,   "save": _db.save_orders},
        FEEDBACK_KEY: {"load": _db.load_feedback, "save": _db.save_feedback},
        TARIFFS_KEY:  {"load": _db.load_tariffs,  "save": _db.save_tariffs},
        PROMOS_KEY:   {"load": _db.load_promos,   "save": _db.save_promos},
        SETTINGS_KEY: {"load": _db.load_settings, "save": _db.save_settings},
    }

    app = Application.builder().token(TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("stats", adm_stats))

    app.add_handler(CallbackQueryHandler(cb_router))

    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, handle_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Job Queue для подписок
    if app.job_queue:
        app.job_queue.run_repeating(subscription_check_job, interval=3600, first=10)

    logger.info("Bot started successfully!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
