"""
FunsDiia Bot — Преміум версія з керуванням даними у Профілі
────────────────────────────────────────────────────────────
Основні покращення:
1. Повне управління персональними даними у розділі "Профіль".
2. Миттєвий передеплой кабінету безпосередньо з Профілю.
3. Покращений UI/UX із преміальними меню та індикаторами стану.
4. Повний захист від Race Conditions та невалідних даних.
5. Розширена адмін-панель: глибока статистика, управління тарифами, промокодами, вивантаження БД.
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
from typing import Optional, Dict, Any, List, Tuple

import pytz
import requests as _req
from dotenv import load_dotenv

from google import genai
from google.genai import types
from google.genai.errors import APIError

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, Update,
    InputFile, BotCommand
)
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

import chain_deploy
import db as _db

load_dotenv()

# ── Config & Environment ───────────────────────────────────────────────────────

def _env_int(key: str, default: int) -> int:
    v = os.getenv(key, "").strip()
    return int(v) if v.lstrip("-").isdigit() else default


def _env_int_list(s: str) -> List[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip().lstrip("-").isdigit()]


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("❌ TELEGRAM_BOT_TOKEN не знайдено в змінних середовища!")

ADMIN_IDS: List[int] = _env_int_list(os.getenv("ADMIN_IDS", os.getenv("ADMIN_CHAT_ID", "")))
if not ADMIN_IDS:
    raise ValueError("❌ ADMIN_IDS не задано в змінних середовища!")

_raw_group = os.getenv("GROUP_CHAT_ID", "").strip()
GROUP_CHAT_ID: Optional[int] = int(_raw_group) if _raw_group.lstrip("-").isdigit() else None

PAGES_GH_TOKEN: str = os.getenv("PAGES_GH_TOKEN", "")
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "") or os.getenv("DEEPSEEK_API_KEY", "")
AI_ENABLED = bool(GEMINI_API_KEY)

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

# ── FSM States ─────────────────────────────────────────────────────────────────

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
    EDIT_PROFILE_INPUT_VALUE, EDIT_PROFILE_AWAIT_PHOTO,
) = range(30)

# ── Defaults & Constants ───────────────────────────────────────────────────────

DEFAULT_TARIFFS = {
    "1_day":    {"name": "1 день",   "price": 20,  "days": 1,    "emoji": "🌙", "active": True},
    "30_days":  {"name": "30 днів",  "price": 70,  "days": 30,   "emoji": "📅", "active": True},
    "90_days":  {"name": "90 днів",  "price": 150, "days": 90,   "emoji": "🌿", "active": True},
    "180_days": {"name": "180 днів", "price": 190, "days": 180,  "emoji": "🌟", "active": True},
    "forever":  {"name": "Назавжди", "price": 250, "days": None, "emoji": "💎", "active": True},
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

# ── DB Integration Layer ───────────────────────────────────────────────────────

_DB: Dict[str, Any] = {}

USERS_KEY    = "users"
ORDERS_KEY   = "orders"
FEEDBACK_KEY = "feedback"
TARIFFS_KEY  = "tariffs"
PROMOS_KEY   = "promos"
SETTINGS_KEY = "settings"


def _load_sync(key: str, default=None):
    if default is None:
        default = {}
    loader = _DB.get(key, {}).get("load")
    if loader:
        try:
            return loader() or default
        except Exception as e:
            logger.error("DB Load error for %s: %s", key, e)
            return default
    return default


def _save_sync(key: str, data) -> bool:
    saver = _DB.get(key, {}).get("save")
    if saver:
        try:
            return saver(data)
        except Exception as e:
            logger.error("DB Save error for %s: %s", key, e)
            return False
    return False


async def async_load(key: str, default=None):
    return await asyncio.to_thread(_load_sync, key, default)


async def async_save(key: str, data) -> bool:
    return await asyncio.to_thread(_save_sync, key, data)


# ── Formatting & Utility Helpers ───────────────────────────────────────────────

def now_str() -> str:
    return datetime.now(TIMEZONE).isoformat()


def now_fmt(fmt="%d.%m.%Y %H:%M") -> str:
    return datetime.now(TIMEZONE).strftime(fmt)


def gen_id(prefix="") -> str:
    return prefix + hashlib.sha256(f"{time.time()}{random.random()}".encode()).hexdigest()[:8]


def is_admin(uid) -> bool:
    try:
        return int(uid) in ADMIN_IDS
    except (ValueError, TypeError):
        return False


def esc(text) -> str:
    return _html_escape.escape(str(text or ""))


def log_action(action: str, uid=None, details: dict = None):
    try:
        _db.log_action_db(now_str(), action, str(uid) if uid else None, details or {})
    except Exception as e:
        logger.error("Log action error: %s", e)


# ── Tariffs & Settings Helpers ─────────────────────────────────────────────────

async def load_tariffs() -> dict:
    raw = await async_load(TARIFFS_KEY, DEFAULT_TARIFFS)
    for v in raw.values():
        if "text" in v and "name" not in v:
            v["name"] = v.pop("text")
        v.setdefault("emoji", "📦")
    return raw


async def save_tariffs(t):
    await async_save(TARIFFS_KEY, t)


async def active_tariffs() -> dict:
    t = await load_tariffs()
    return {k: v for k, v in t.items() if v.get("active", True)}


async def load_settings() -> dict:
    st = await async_load(SETTINGS_KEY, {})
    return {**DEFAULT_SETTINGS, **st}


async def save_settings(s):
    await async_save(SETTINGS_KEY, s)


async def get_setting(key):
    s = await load_settings()
    return s.get(key, DEFAULT_SETTINGS.get(key))


# ── Promos Helpers ─────────────────────────────────────────────────────────────

async def load_promos() -> dict:
    return await async_load(PROMOS_KEY, {})


async def save_promos(p):
    await async_save(PROMOS_KEY, p)


async def check_promo(code: str, uid: str) -> dict:
    promos = await load_promos()
    code = code.upper().strip()
    if code not in promos:
        return {"ok": False, "discount": 0, "msg": "❌ Промо-код не знайдено"}
    p = promos[code]
    if not p.get("active", True):
        return {"ok": False, "discount": 0, "msg": "❌ Промо-код вже не активний"}
    if p.get("max_uses", 0) and p.get("uses", 0) >= p["max_uses"]:
        return {"ok": False, "discount": 0, "msg": "❌ Ліміт використання промо-коду вичерпано"}
    if uid in p.get("used_by", []):
        return {"ok": False, "discount": 0, "msg": "❌ Ви вже використали цей промо-код"}
    expires = p.get("expires")
    if expires:
        try:
            if datetime.fromisoformat(expires) < datetime.now(TIMEZONE):
                return {"ok": False, "discount": 0, "msg": "❌ Термін дії промо-коду закінчився"}
        except Exception:
            pass
    return {"ok": True, "discount": p.get("discount", 0), "msg": f"✅ Активовано знижку {p.get('discount', 0)}%!"}


async def apply_promo(code: str, uid: str):
    promos = await load_promos()
    code = code.upper().strip()
    if code in promos:
        promos[code].setdefault("used_by", []).append(uid)
        promos[code]["uses"] = promos[code].get("uses", 0) + 1
        await save_promos(promos)


# ── Google Gemini Integration ──────────────────────────────────────────────────

_GEMINI_MODEL = "gemini-3.5-flash"
ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

_SYS_SUPPORT = (
    "Ти — ввічливий, дружній та швидкий асистент чат-підтримки сервісу FunsDiia.\n"
    "Твоя мета — допомагати користувачам із запитаннями щодо послуг, оплати та замовлень.\n"
    "Правила:\n"
    "1. Відповідай виключно українською мовою.\n"
    "2. Будь лаконічним, але вичерпним (2-4 речення).\n"
    "3. Якщо запитання стосується оплати або реквізитів — вкажи, що реквізити надаються після оформлення замовлення.\n"
    "4. Не вигадуй інформацію, якої не знаєш."
)

_SYS_RECEIPT = (
    "Ти — експерт-верифікатор фінансових чеків та квитанцій. Аналізуй зображення чека.\n"
    "Перевір:\n"
    "1. Чи це справжня квитанція банку (Приват24, Monobank, Ощад тощо).\n"
    "2. Чи збігається фактична сума на чеку з очікуваною.\n"
    "3. Чи немає явних ознак підробки.\n"
    "Поверни результат строго у форматі JSON."
)

_RECEIPT_SCHEMA = types.Schema(
    type=types.Type.OBJECT,
    properties={
        "ok": types.Schema(type=types.Type.BOOLEAN, description="true, якщо це справжній чек і сума збігається"),
        "confidence": types.Schema(type=types.Type.INTEGER, description="Впевненість від 0 до 100%"),
        "amount": types.Schema(type=types.Type.NUMBER, description="Фактично виявлена сума на чеку або null"),
        "bank_name": types.Schema(type=types.Type.STRING, description="Назва банку або платіжної системи"),
        "reason": types.Schema(type=types.Type.STRING, description="Опис результату аналізу українською мовою"),
    },
    required=["ok", "confidence", "amount", "reason"],
)

_SYS_TRANSLIT = (
    "Ти — офіційний транслітератор за стандартом КМУ (Постанова №55 від 27 січня 2010 р.).\n"
    "Перекладай українські ПІБ латиницею точно за цим стандартом.\n"
    "Відповідай ТІЛЬКИ результатом транслітерації без лапок, пояснень чи розділових знаків."
)


async def ai_check_receipt(photo_bytes: bytes, expected_amount: int, mime_type: str = "image/jpeg") -> dict:
    default_result = {
        "ok": False, "confidence": 0, "amount": None,
        "bank_name": None, "reason": "AI відключено або відсутнє фото", "auto_approved": False,
    }
    if not AI_ENABLED or not ai_client or not photo_bytes:
        return default_result

    try:
        image_part = types.Part.from_bytes(data=photo_bytes, mime_type=mime_type)
        prompt = f"Очікувана сума платежу: {expected_amount} UAH. Проаналізуй цей чек."
        config = types.GenerateContentConfig(
            system_instruction=_SYS_RECEIPT, temperature=0.1,
            response_mime_type="application/json", response_schema=_RECEIPT_SCHEMA,
        )
        response = await ai_client.aio.models.generate_content(
            model=_GEMINI_MODEL, contents=[image_part, prompt], config=config,
        )
        if not response.text:
            default_result["reason"] = "Порожня відповідь від моделі"
            return default_result

        parsed = json.loads(response.text)
        parsed["auto_approved"] = bool(parsed.get("ok")) and int(parsed.get("confidence", 0)) >= 85
        return parsed
    except APIError as e:
        logger.error("Gemini API error during receipt check: %s", e)
        default_result["reason"] = f"Помилка сервісу AI: {e.message}"
        return default_result
    except Exception as e:
        logger.error("Unexpected error in ai_check_receipt: %s", e)
        default_result["reason"] = "Помилка обробки чека"
        return default_result


async def ai_support_reply(text: str, history: list = None) -> str:
    if not AI_ENABLED or not ai_client or not text.strip():
        return ""
    try:
        formatted_contents = []
        if history:
            for msg in history:
                role = "user" if msg.get("role") == "user" else "model"
                formatted_contents.append(
                    types.Content(role=role, parts=[types.Part.from_text(text=msg.get("content", ""))])
                )
        formatted_contents.append(
            types.Content(role="user", parts=[types.Part.from_text(text=text)])
        )
        config = types.GenerateContentConfig(
            system_instruction=_SYS_SUPPORT, temperature=0.4, max_output_tokens=500,
        )
        response = await ai_client.aio.models.generate_content(
            model=_GEMINI_MODEL, contents=formatted_contents, config=config,
        )
        return response.text.strip() if response.text else ""
    except Exception as e:
        logger.error("Gemini support reply error: %s", e)
        return ""


async def ai_transliterate(fio_ua: str) -> str:
    if not AI_ENABLED or not ai_client or not fio_ua.strip():
        return fio_ua.strip()
    try:
        config = types.GenerateContentConfig(
            system_instruction=_SYS_TRANSLIT, temperature=0.0, max_output_tokens=100,
        )
        response = await ai_client.aio.models.generate_content(
            model=_GEMINI_MODEL, contents=[f"Транслітеруй: '{fio_ua.strip()}'"], config=config,
        )
        result = response.text.strip("\"' \n\t") if response.text else ""
        return result or fio_ua
    except Exception as e:
        logger.error("Gemini transliteration error: %s", e)
        return fio_ua


# ── Document Generator Functions ───────────────────────────────────────────────

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
    sex_ua = "Ч" if sex in ("M", "Ч") else "Ж"
    sex_en = "M" if sex in ("M", "Ч") else "W"

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


# ── Subscription Checking Background Job ───────────────────────────────────────

REMINDER_DAYS      = (7, 3, 2, 1, 0)    # за скільки днів нагадувати (0 = у день закінчення)
GRACE_DAYS         = 0                  # скільки днів після кінця ще не блокувати
SEND_DELAY         = 0.05               # пауза між повідомленнями (ліміт TG ~30/с)
MAX_SEND_RETRIES   = 3
SUB_SKIP_STATUSES  = {"expired", "rejected", "pending", "cancelled"}
SUB_EXPIRE_STATUSES = {"deployed"}

_sub_job_lock = asyncio.Lock()


def plural_days(n: int) -> str:
    n = abs(n)
    if n % 10 == 1 and n % 100 != 11:
        return "день"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return "дні"
    return "днів"


def _parse_end_dt(subscription_end: str) -> Optional[datetime]:
    try:
        end_dt = datetime.fromisoformat(subscription_end)
        if end_dt.tzinfo is None:
            end_dt = TIMEZONE.localize(end_dt)
        return end_dt
    except Exception:
        return None


def calc_subscription_end(tariff_key: str, tariffs: dict,
                          current_end: Optional[str] = None) -> Optional[str]:
    """Дата закінчення. Якщо передано current_end і підписка ще активна —
    дні додаються до неї (дострокове продовження без втрати залишку)."""
    t = tariffs.get(tariff_key, {})
    days = t.get("days")
    if not days:
        return None
    base = datetime.now(TIMEZONE)
    if current_end:
        cur = _parse_end_dt(current_end)
        if cur and cur > base:
            base = cur
    return (base + timedelta(days=days)).isoformat()


def days_until_expiry(subscription_end: str) -> int:
    end_dt = _parse_end_dt(subscription_end)
    if end_dt is None:
        return 999
    today   = datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end_dt.astimezone(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    return (end_day - today).days


def _fmt_end(subscription_end: str) -> str:
    end_dt = _parse_end_dt(subscription_end)
    return end_dt.astimezone(TIMEZONE).strftime("%d.%m.%Y") if end_dt else "—"


def _renew_kb(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="catalog")]])


def _reminder_text(days_left: int, order: dict, sub_end: str) -> str:
    tariff_name = esc(order.get("tariff_name", ""))
    pages_url   = order.get("pages_url", "")
    if days_left <= 0:
        return (
            "🔴 <b>Сьогодні закінчується підписка!</b>\n\n"
            f"📦 Тариф: {tariff_name}\n\n"
            "Без продовження доступ до кабінету буде заблоковано."
        )
    cabinet = f"🔗 Ваш кабінет: {pages_url}\n\n" if pages_url else ""
    return (
        f"⏰ <b>Підписка закінчується через {days_left} {plural_days(days_left)}!</b>\n\n"
        f"📦 Тариф: {tariff_name}\n"
        f"📅 Дійсна до: {_fmt_end(sub_end)}\n\n"
        f"{cabinet}"
        "Щоб продовжити доступ — оберіть тариф нижче 👇"
    )


SEND_OK, SEND_BLOCKED, SEND_FAILED = "ok", "blocked", "failed"


async def _safe_send(bot, chat_id, text: str, **kwargs) -> str:
    """Надсилання з ретраями. Повертає ok / blocked / failed."""
    kwargs.setdefault("parse_mode", "HTML")
    kwargs.setdefault("disable_web_page_preview", True)
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            await bot.send_message(chat_id, text, **kwargs)
            await asyncio.sleep(SEND_DELAY)
            return SEND_OK
        except RetryAfter as e:
            wait = e.retry_after
            wait = wait.total_seconds() if hasattr(wait, "total_seconds") else float(wait)
            logger.warning("safe_send [%s]: flood, sleep %.1fs", chat_id, wait)
            await asyncio.sleep(wait + 1)
        except Forbidden:
            return SEND_BLOCKED
        except BadRequest as e:
            logger.error("safe_send [%s]: %s", chat_id, e)
            return SEND_BLOCKED if "chat not found" in str(e).lower() else SEND_FAILED
        except (TimedOut, NetworkError) as e:
            logger.warning("safe_send [%s]: network %d/%d: %s", chat_id, attempt, MAX_SEND_RETRIES, e)
            await asyncio.sleep(2 * attempt)
        except Exception as e:
            logger.error("safe_send [%s]: %s", chat_id, e)
            return SEND_FAILED
    return SEND_FAILED


class _SubStats:
    def __init__(self):
        self.checked = self.skipped = self.reminders = self.today = 0
        self.expired = self.blocked = self.send_failed = self.errors = 0
        self.duration = 0.0

    @property
    def events(self) -> int:
        return self.reminders + self.today + self.expired + self.send_failed + self.errors

    def as_text(self) -> str:
        return (
            "📊 <b>Перевірка підписок</b>\n\n"
            f"Перевірено: {self.checked} (пропущено: {self.skipped})\n"
            f"⏰ Нагадувань: {self.reminders}\n"
            f"🔴 «Сьогодні кінець»: {self.today}\n"
            f"❌ Завершено: {self.expired}\n"
            f"🚫 Заблокували бота: {self.blocked}\n"
            f"⚠️ Помилок надсилання: {self.send_failed}\n"
            f"💥 Внутрішніх помилок: {self.errors}\n"
            f"⏱ {self.duration:.1f} с"
        )


def _new_patch(expected_end: str) -> dict:
    return {"expected_end": expected_end, "add_notified": [], "set": {}}


def _apply_patch(order: dict, patch: dict) -> bool:
    """Накладає патч на свіже замовлення. False — якщо підписку вже змінили."""
    if order.get("subscription_end") != patch["expected_end"]:
        return False
    lst = order.setdefault("notified_days", [])
    for d in patch["add_notified"]:
        if d not in lst:
            lst.append(d)
    order.update(patch["set"])
    return True


async def _process_sub_order(bot, oid: str, order: dict, users: dict,
                             stats: _SubStats) -> Optional[dict]:
    sub_end = order.get("subscription_end")
    status  = order.get("status", "")
    uid2    = order.get("user_id", "")

    if not sub_end or not uid2 or status in SUB_SKIP_STATUSES:
        stats.skipped += 1
        return None
    if users.get(uid2, {}).get("banned"):
        stats.skipped += 1
        return None
    if _parse_end_dt(sub_end) is None:
        logger.error("sub_check [%s]: bad subscription_end=%r", oid, sub_end)
        stats.errors += 1
        return None

    stats.checked += 1
    days_left = days_until_expiry(sub_end)
    notified  = list(order.get("notified_days", []))
    patch     = _new_patch(sub_end)

    # 1) Нагадування (з наздоганянням: якщо бот лежав — шлемо найактуальніше)
    if days_left >= 0:
        due = [d for d in REMINDER_DAYS if days_left <= d and d not in notified]
        if due:
            label  = "🔄 Продовжити зараз" if days_left == 0 else "🔄 Продовжити підписку"
            result = await _safe_send(
                bot, uid2, _reminder_text(days_left, order, sub_end),
                reply_markup=_renew_kb(label),
            )
            if result == SEND_OK:
                patch["add_notified"].extend(due)
                if days_left == 0:
                    stats.today += 1
                    log_action("sub_expiring_today", uid2, {"oid": oid})
                else:
                    stats.reminders += 1
                    log_action("sub_reminder", uid2, {"oid": oid, "days_left": days_left})
            elif result == SEND_BLOCKED:
                patch["add_notified"].extend(due)      # не спамимо повторами
                patch["set"]["bot_blocked"] = True
                stats.blocked += 1
            else:
                stats.send_failed += 1                 # спробуємо при наступному запуску

    # 2) Завершення підписки (з урахуванням grace-періоду)
    if days_left < -GRACE_DAYS and status in SUB_EXPIRE_STATUSES:
        patch["set"]["status"]     = "expired"
        patch["set"]["expired_at"] = datetime.now(TIMEZONE).isoformat()
        stats.expired += 1
        log_action("sub_expired", uid2, {"oid": oid, "days_overdue": -days_left})

        if not order.get("bot_blocked"):
            result = await _safe_send(
                bot, uid2,
                "❌ <b>Підписка закінчилася!</b>\n\n"
                "Для відновлення доступу — оформіть нове замовлення 👇",
                reply_markup=_renew_kb("🔄 Відновити доступ"),
            )
            if result == SEND_BLOCKED:
                patch["set"]["bot_blocked"] = True
                stats.blocked += 1
            elif result == SEND_FAILED:
                stats.send_failed += 1

    return patch if (patch["add_notified"] or patch["set"]) else None


async def subscription_check_job(context: ContextTypes.DEFAULT_TYPE):
    if _sub_job_lock.locked():
        logger.warning("subscription_check_job: already running, skip")
        return

    async with _sub_job_lock:
        started = time.monotonic()
        stats   = _SubStats()
        patches: Dict[str, dict] = {}

        try:
            orders = await async_load(ORDERS_KEY, {})
            users  = await async_load(USERS_KEY, {})

            for oid, order in list(orders.items()):
                try:
                    patch = await _process_sub_order(context.bot, oid, order, users, stats)
                    if patch:
                        patches[oid] = patch
                except Exception as e:      # одне замовлення не ламає весь цикл
                    stats.errors += 1
                    logger.exception("sub_check [%s]: %s", oid, e)
        finally:
            # Зберігаємо через свіжу копію, щоб не затерти нові замовлення/продовження
            if patches:
                try:
                    fresh   = await async_load(ORDERS_KEY, {})
                    applied = 0
                    for oid, patch in patches.items():
                        if oid in fresh and _apply_patch(fresh[oid], patch):
                            applied += 1
                        else:
                            logger.info("sub_check [%s]: patch skipped (order changed)", oid)
                    if applied:
                        await async_save(ORDERS_KEY, fresh)
                except Exception as e:
                    stats.errors += 1
                    logger.exception("sub_check: save failed: %s", e)

            stats.duration = time.monotonic() - started
            logger.info(
                "sub_check: checked=%d reminders=%d today=%d expired=%d blocked=%d "
                "send_failed=%d errors=%d (%.1fs)",
                stats.checked, stats.reminders, stats.today, stats.expired,
                stats.blocked, stats.send_failed, stats.errors, stats.duration,
            )

        if stats.events:
            for admin_id in ADMIN_IDS:
                await _safe_send(context.bot, admin_id, stats.as_text())


# ── UI Helpers ─────────────────────────────────────────────────────────────────

def mkb(*rows): return InlineKeyboardMarkup(list(rows))
def back_btn(cb): return [InlineKeyboardButton("🔙 Назад", callback_data=cb)]


async def safe_edit(query, text: str, kb=None, **kw):
    kw.setdefault("parse_mode", "HTML")
    if kb:
        kw["reply_markup"] = kb
    try:
        await query.edit_message_text(text, **kw)
    except BadRequest:
        try:
            await query.message.reply_text(text, **kw)
        except Exception as e:
            logger.error("safe_edit reply_text error: %s", e)
    except Exception as e:
        logger.error("safe_edit error: %s", e)


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


# ── Profile Helper Functions ───────────────────────────────────────────────────

async def get_latest_order_for_user(uid: str) -> tuple[Optional[str], Optional[dict]]:
    orders = await async_load(ORDERS_KEY, {})
    my_orders = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("user_id") == uid],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    if not my_orders:
        return None, None
    return my_orders[0]


async def sync_profile_to_latest_order(uid: str, user_profile: dict):
    orders = await async_load(ORDERS_KEY, {})
    oid, last_order = await get_latest_order_for_user(uid)
    if not oid or not last_order:
        return

    vd = last_order.get("values_data", {})
    vd["fio"] = user_profile.get("doc_fio", vd.get("fio", ""))
    vd["fio_en"] = user_profile.get("doc_fio_en", vd.get("fio_en", ""))
    vd["birth"] = user_profile.get("doc_dob", vd.get("birth", ""))
    
    sex = user_profile.get("doc_sex", "Ч")
    vd["sex"] = sex
    vd["sex_en"] = "M" if sex in ("Ч", "M") else "W"

    vd["bank_adress"] = user_profile.get("doc_address", vd.get("bank_adress", ""))
    vd["isRightsEnabled"] = user_profile.get("doc_is_rights", vd.get("isRightsEnabled", True))
    vd["isZagranEnabled"] = user_profile.get("doc_is_zagran", vd.get("isZagranEnabled", True))
    
    val_dip = user_profile.get("doc_is_diploma", vd.get("isDiplomaEnabled", False))
    vd["isDiplomaEnabled"] = val_dip
    vd["isStudyEnabled"] = val_dip

    last_order["values_data"] = vd
    last_order["js_content"] = values_to_js(vd)
    
    if user_profile.get("doc_photo_path"):
        last_order["photo_path"] = user_profile["doc_photo_path"]

    orders[oid] = last_order
    await async_save(ORDERS_KEY, orders)


# ── Command Handlers ───────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    users = await async_load(USERS_KEY, {})
    settings = await load_settings()

    ref_by = None
    if context.args:
        pot = context.args[0]
        if pot != uid and pot in users:
            ref_by = pot

    if uid not in users:
        users[uid] = {
            "username":        update.effective_user.username,
            "first_name":      update.effective_user.first_name,
            "balance":         0,
            "referred_by":     ref_by,
            "ref_count":       0,
            "has_bought":      False,
            "joined_date":     now_str(),
            "total_spent":     0,
            "total_orders":    0,
            "banned":          False,
            "vip":             False,
            "doc_fio":         update.effective_user.first_name or "",
            "doc_fio_en":      "",
            "doc_dob":         "01.01.2000",
            "doc_sex":         "Ч",
            "doc_address":     "м. Харків, вул. Сумська, 1",
            "doc_is_rights":   True,
            "doc_is_zagran":   True,
            "doc_is_diploma":  False,
            "doc_photo_path":  "",
        }
        await async_save(USERS_KEY, users)
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
        "🪪 <b>FunsDiia</b> — генерація та зручне управління документами.\n"
        "⚡️ Ваш інтерактивний кабінет готовий за лічені хвилини."
    )

    kb_rows = [
        [InlineKeyboardButton("🛒 Замовити тариф", callback_data="catalog")],
        [InlineKeyboardButton("👤 Профіль та Дані", callback_data="profile"),
         InlineKeyboardButton("📂 Мої замовлення", callback_data="my_orders")],
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


# ── Profile & Orders Management Handlers (Extended) ───────────────────────────

def _calc_profile_completion(u: dict, photo_path: str = "") -> int:
    """Обчислює відсоток заповненості профілю користувача."""
    fields = [
        bool(u.get("doc_fio")),
        bool(u.get("doc_dob")),
        bool(u.get("doc_address")),
        bool(photo_path or u.get("doc_photo_path")),
    ]
    return int((sum(fields) / len(fields)) * 100)


def _render_progress_bar(percent: int) -> str:
    """Генерує візуальний прогрес-бар."""
    filled = int(percent / 10)
    return "🟩" * filled + "⬜" * (10 - filled) + f" {percent}%"


async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})

    oid, last_order = await get_latest_order_for_user(uid)
    
    fio = u.get("doc_fio") or (last_order.get("values_data", {}).get("fio") if last_order else "Не вказано")
    fio_en = u.get("doc_fio_en") or (last_order.get("values_data", {}).get("fio_en") if last_order else "—")
    dob = u.get("doc_dob") or (last_order.get("values_data", {}).get("birth") if last_order else "Не вказано")
    sex = "Чоловік ♂️" if u.get("doc_sex", "Ч") in ("Ч", "M") else "Жінка ♀️"
    addr = u.get("doc_address") or (last_order.get("values_data", {}).get("bank_adress") if last_order else "Не вказано")
    
    r = "✅ Увімкнено" if u.get("doc_is_rights", True) else "❌ Вимкнено"
    z = "✅ Увімкнено" if u.get("doc_is_zagran", True) else "❌ Вимкнено"
    d = "✅ Увімкнено" if u.get("doc_is_diploma", False) else "❌ Вимкнено"
    
    photo_path = u.get("doc_photo_path") or (last_order.get("photo_path") if last_order else "")
    photo_st = "✅ Завантажено" if photo_path else "❌ Відсутнє"

    completion = _calc_profile_completion(u, photo_path)
    progress_bar = _render_progress_bar(completion)

    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    vip_label = "👑 VIP Клієнт" if u.get("vip") else "👤 Стандартний профіль"

    pages_url = last_order.get("pages_url") if last_order else None
    cabinet_status = ""
    if pages_url:
        sub_end = last_order.get("subscription_end")
        days_left = days_until_expiry(sub_end) if sub_end else None
        expiry_info = f" (залишилось {days_left} дн.)" if days_left is not None else " (безстроково)"
        cabinet_status = f"\n🌐 <b>Активний кабінет:</b> <a href='{pages_url}'>Відкрити</a>{expiry_info}"

    text = (
        f"👤 <b>ОСОБИСТИЙ КАБІНЕТ & ПРОФІЛЬ</b>  [{vip_label}]\n"
        f"────────────────────────────\n"
        f"🆔 <b>ID:</b> <code>{uid}</code>\n"
        f"💰 <b>Баланс:</b> <code>{u.get('balance', 0)}₴</code>\n"
        f"👥 <b>Запрошено рефералів:</b> <code>{u.get('ref_count', 0)}</code>{cabinet_status}\n\n"
        f"📊 <b>Заповненість профілю:</b>\n{progress_bar}\n\n"
        f"📋 <b>ПЕРСОНАЛЬНІ ДАНІ ДОКУМЕНТІВ:</b>\n"
        f"▫️ <b>ПІБ (Укр):</b> {esc(fio)}\n"
        f"▫️ <b>ПІБ (Eng):</b> <code>{esc(fio_en)}</code>\n"
        f"▫️ <b>Дата народження:</b> {esc(dob)}\n"
        f"▫️ <b>Стать:</b> {sex}\n"
        f"▫️ <b>Адреса:</b> {esc(addr)}\n"
        f"▫️ <b>Водійські права:</b> {r}\n"
        f"▫️ <b>Закордонний паспорт:</b> {z}\n"
        f"▫️ <b>Диплом / Студентський:</b> {d}\n"
        f"▫️ <b>Фото 3x4:</b> {photo_st}\n\n"
        f"🔗 <b>Ваше реферальне посилання:</b>\n<code>{ref_link}</code>"
    )

    kb_rows = [
        [InlineKeyboardButton("✏️ Редагувати дані документа", callback_data="profile_edit_menu")],
    ]
    
    if photo_path and os.path.exists(photo_path):
        kb_rows.append([InlineKeyboardButton("🖼 Переглянути поточне фото", callback_data="profile_view_photo")])

    if pages_url and last_order.get("status") in ("deployed", "completed", "approved"):
        kb_rows.append([InlineKeyboardButton("🚀 Застосувати зміни та оновити сайт", callback_data="profile_redeploy")])
    
    kb_rows.extend([
        [InlineKeyboardButton("💸 Вивести кошти", callback_data="withdraw"),
         InlineKeyboardButton("👥 Реферальна система", callback_data="ref_menu")],
        back_btn("home")
    ])

    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows), disable_web_page_preview=True)


async def profile_view_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Відображення поточного аватра/фото 3х4 з можливістю його швидкої заміни."""
    q = update.callback_query
    uid = str(q.from_user.id)
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})

    oid, last_order = await get_latest_order_for_user(uid)
    photo_path = u.get("doc_photo_path") or (last_order.get("photo_path") if last_order else "")

    if not photo_path or not os.path.exists(photo_path):
        await q.answer("❌ Фото не знайдено або файл видалено.", show_alert=True)
        return

    try:
        await q.message.delete()
    except Exception:
        pass

    kb = mkb(
        [InlineKeyboardButton("📸 Завантажити нове фото", callback_data="p_edit:photo")],
        [InlineKeyboardButton("🔙 Повернутися до профілю", callback_data="profile")]
    )

    with open(photo_path, "rb") as f:
        await context.bot.send_photo(
            chat_id=q.message.chat_id,
            photo=f,
            caption="🖼 <b>Ваше поточне фото 3×4 для документів.</b>",
            reply_markup=kb,
            parse_mode="HTML"
        )


async def profile_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})

    sex = "Чоловік ♂️" if u.get("doc_sex", "Ч") in ("Ч", "M") else "Жінка ♀️"
    r = "✅ Увімкнено" if u.get("doc_is_rights", True) else "❌ Вимкнено"
    z = "✅ Увімкнено" if u.get("doc_is_zagran", True) else "❌ Вимкнено"
    d = "✅ Увімкнено" if u.get("doc_is_diploma", False) else "❌ Вимкнено"

    fio_ua = u.get('doc_fio', '—')
    fio_en = u.get('doc_fio_en', 'Автоматично')

    text = (
        f"✏️ <b>РЕДАГУВАННЯ ДАНИХ ДОКУМЕНТА</b>\n"
        f"────────────────────────────\n"
        f"Тут ви можете змінити дані, які автоматично підтягуються у ваш генерувальний кабінет.\n\n"
        f"📝 <b>ПІБ (Укр):</b> {esc(fio_ua)}\n"
        f"🔤 <b>ПІБ (Lat):</b> {esc(fio_en)}\n"
        f"📅 <b>Дата народження:</b> {esc(u.get('doc_dob', '—'))}\n"
        f"👤 <b>Стать:</b> {sex}\n"
        f"🏠 <b>Адреса:</b> {esc(u.get('doc_address', '—'))}\n\n"
        f"⚙️ <b>АКТИВНІ ДОКУМЕНТИ В КАБІНЕТІ:</b>\n"
        f"🚗 <b>Водійське посвідчення:</b> {r}\n"
        f"🌍 <b>Закордонний паспорт:</b> {z}\n"
        f"🎓 <b>Диплом / Студентський:</b> {d}"
    )

    kb_rows = [
        [InlineKeyboardButton("📝 Змінити ПІБ", callback_data="p_edit:fio"),
         InlineKeyboardButton("📅 Змінити ДН", callback_data="p_edit:dob")],
        [InlineKeyboardButton(f"👤 Стать: {sex}", callback_data="p_toggle:sex"),
         InlineKeyboardButton("🏠 Змінити адресу", callback_data="p_edit:addr")],
        [InlineKeyboardButton(f"🚗 Права: {r}", callback_data="p_toggle:rights"),
         InlineKeyboardButton(f"🌍 Загран: {z}", callback_data="p_toggle:zagran")],
        [InlineKeyboardButton(f"🎓 Диплом: {d}", callback_data="p_toggle:diploma")],
        [InlineKeyboardButton("📸 Оновити фото 3×4", callback_data="p_edit:photo")],
        [InlineKeyboardButton("🚀 Застосувати та оновити сайт", callback_data="profile_redeploy")],
        [InlineKeyboardButton("🔙 Повернутися до профілю", callback_data="profile")],
    ]

    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


async def profile_toggle_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    parts = q.data.split(":")
    if len(parts) < 2: 
        return
    field = parts[1]

    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})

    if field == "sex":
        curr = u.get("doc_sex", "Ч")
        u["doc_sex"] = "Ж" if curr in ("Ч", "M") else "Ч"
    elif field == "rights":
        u["doc_is_rights"] = not u.get("doc_is_rights", True)
    elif field == "zagran":
        u["doc_is_zagran"] = not u.get("doc_is_zagran", True)
    elif field == "diploma":
        u["doc_is_diploma"] = not u.get("doc_is_diploma", False)

    await async_save(USERS_KEY, users)
    await sync_profile_to_latest_order(uid, u)
    await profile_edit_menu(update, context)


async def profile_input_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: 
        return
    field = parts[1]

    context.user_data["edit_profile_field"] = field

    prompts = {
        "fio": "📝 Введіть новий **ПІБ** (українською мовою):\n\n<i>Приклад: Шевченко Тарас Григорович</i>\n\n💡 Латинська версія створиться автоматично за допомогою AI.",
        "dob": "📅 Введіть нову **дату народження**:\n\n<i>Формат: ДД.ММ.РРРР (наприклад: 15.05.1998)</i>",
        "addr": "🏠 Введіть нову **адресу прописки**:\n\n<i>Приклад: м. Київ, вул. Хрещатик, буд. 1, кв. 10</i>",
        "photo": "📸 Надішліть **нове фото 3×4** (зображення файлом або фотографією):\n\n<i>Рекомендовано: чітке обличчя, світлий фон, без сонцезахисних окулярів.</i>",
    }

    if field == "photo":
        context.user_data["state"] = EDIT_PROFILE_AWAIT_PHOTO
    else:
        context.user_data["state"] = EDIT_PROFILE_INPUT_VALUE

    await safe_edit(q, prompts.get(field, "Введіть нове значення:"), mkb([InlineKeyboardButton("❌ Скасувати", callback_data="profile_edit_menu")]))


async def profile_redeploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)

    oid, last_order = await get_latest_order_for_user(uid)
    if not oid or not last_order:
        await q.answer("❌ У вас немає активних замовлень для деплою.", show_alert=True)
        return

    if last_order.get("status") not in ("deployed", "completed", "approved"):
        await q.answer("⚠️ Ваше замовлення ще очікує підтвердження оплати або знаходиться в обробці.", show_alert=True)
        return

    await safe_edit(q, "🚀 <b>Синхронізація даних та оновлення сайту...</b>\n\n⏳ Зачекайте, проводиться деплой змін (30–60 сек).")

    try:
        users = await async_load(USERS_KEY, {})
        u = users.get(uid, {})
        await sync_profile_to_latest_order(uid, u)
        
        orders = await async_load(ORDERS_KEY, {})
        updated_order = orders.get(oid, last_order)

        folder1_url = await _run_chain_deploy(oid, updated_order)

        orders[oid]["pages_url"] = folder1_url
        orders[oid]["updated_at"] = now_str()
        await async_save(ORDERS_KEY, orders)

        log_action("user_profile_redeploy", uid, {"oid": oid, "url": folder1_url})

        await safe_edit(q,
            f"🎉 <b>Кабінет успішно оновлено!</b>\n\n"
            f"📦 <b>Замовлення:</b> <code>#{oid}</code>\n"
            f"🔗 <a href='{folder1_url}'>Відкрити оновлений кабінет</a>\n\n"
            f"⏱ <i>Кеш браузера може оновлюватися протягом 1–2 хвилин.</i>",
            mkb(
                [InlineKeyboardButton("🔗 Відкрити кабінет", url=folder1_url)],
                [InlineKeyboardButton("👤 Повернутися до профілю", callback_data="profile")]
            ),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error("User profile redeploy error [%s]: %s", oid, e, exc_info=True)
        await safe_edit(q,
            f"❌ <b>Помилка під час оновлення сайту.</b>\n\n<code>{esc(str(e)[:300])}</code>",
            mkb(
                [InlineKeyboardButton("🔄 Спробувати знову", callback_data="profile_redeploy")],
                back_btn("profile")
            )
        )


# ── Orders Section (Extended) ──────────────────────────────────────────────────

async def my_orders_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    orders = await async_load(ORDERS_KEY, {})
    tariffs = await load_tariffs()
    
    my = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("user_id") == uid],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    
    if not my:
        await safe_edit(
            q, 
            "📭 <b>У вас ще немає замовлень.</b>\n\nОберіть тариф у каталозі, щоб створити свій перший інтерактивний кабінет!",
            mkb([InlineKeyboardButton("🛒 Перейти до каталогу", callback_data="catalog")], back_btn("home"))
        )
        return

    status_map = {
        "pending": "⏳ На перевірці",
        "approved": "✅ Сплачено",
        "completed": "🎉 Виконано",
        "rejected": "❌ Відхилено",
        "deployed": "🌐 Активний",
        "expired": "🔴 Прострочено"
    }

    total_spent = sum(o.get("final_price", 0) for _, o in my if o.get("status") in ("deployed", "completed", "approved"))

    text = (
        f"📂 <b>ІСТОРІЯ ЗАМОВЛЕНЬ</b>\n"
        f"────────────────────────────\n"
        f"📊 Всього замовлень: <b>{len(my)}</b>\n"
        f"💰 Інвестовано в доступ: <b>{total_spent}₴</b>\n\n"
        f"Оберіть замовлення зі списку для перегляду деталей:"
    )

    kb_rows = []
    for oid, o in my[:12]:
        st_icon = status_map.get(o.get("status", ""), "·").split()[0]
        t_name = tariffs.get(o.get("tariff", ""), {}).get("name", o.get("tariff_name", "?"))
        date_str = o.get("created_at", "")[:10]
        btn_text = f"{st_icon} #{oid} | {esc(t_name)} ({date_str})"
        kb_rows.append([InlineKeyboardButton(btn_text, callback_data=f"user_ord_view:{oid}")])

    kb_rows.append(back_btn("home"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


async def user_ord_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    parts = q.data.split(":")
    if len(parts) < 2: 
        return
    oid = parts[1]

    orders = await async_load(ORDERS_KEY, {})
    o = orders.get(oid)
    if not o or o.get("user_id") != uid:
        await q.answer("❌ Замовлення не знайдено або належить іншому користувачу.", show_alert=True)
        return

    status_map = {
        "pending": "⏳ На обробці (перевірка чека)",
        "approved": "✅ Сплачено (підтверджено)",
        "completed": "🎉 Виконано",
        "rejected": "❌ Відхилено",
        "deployed": "🌐 Активний (задеплоєний)",
        "expired": "🔴 Термін підписки закінчився"
    }

    st = status_map.get(o.get("status", ""), o.get("status", "?"))
    url = esc(o.get("pages_url", ""))
    pages_line = f"\n🔗 <b>Ссылка на кабінет:</b> <a href='{url}'>{url}</a>" if url else ""

    sub_end = o.get("subscription_end")
    if sub_end:
        dt_end = datetime.fromisoformat(sub_end).strftime('%d.%m.%Y о %H:%M')
        days_left = days_until_expiry(sub_end)
        sub_line = f"\n📅 <b>Підписка дійсна до:</b> {dt_end} <i>(залишилось {max(0, days_left)} дн.)</i>"
    else:
        sub_line = "\n♾ <b>Тип підписки:</b> Безстрокова"

    created_date = datetime.fromisoformat(o.get("created_at", now_str())).strftime('%d.%m.%Y %H:%M')

    vd = o.get("values_data", {})
    docs_summary = []
    if vd.get("isRightsEnabled", True): docs_summary.append("🚗 Права")
    if vd.get("isZagranEnabled", True): docs_summary.append("🌍 Загран")
    if vd.get("isDiplomaEnabled", False): docs_summary.append("🎓 Диплом")
    docs_text = ", ".join(docs_summary) if docs_summary else "Базовий пакет"

    text = (
        f"📦 <b>ДЕТАЛІ ЗАМОВЛЕННЯ #{esc(oid)}</b>\n"
        f"────────────────────────────\n"
        f"📊 <b>Статус:</b> {st}\n"
        f"💎 <b>Тариф:</b> {esc(o.get('tariff_name', '?'))}\n"
        f"💰 <b>Вартість:</b> {o.get('final_price', '?')}₴\n"
        f"📅 <b>Створено:</b> {created_date}{sub_line}\n"
        f"📑 <b>Включені документи:</b> {docs_text}{pages_line}\n\n"
        f"👤 <b>Дані на момент видачі:</b>\n"
        f"▫️ ПІБ: {esc(vd.get('fio', o.get('fio', '—')))}\n"
        f"▫️ ДН: {esc(vd.get('birth', o.get('dob', '—')))}"
    )

    kb_rows = []
    if url and o.get("status") in ("deployed", "completed"):
        kb_rows.append([InlineKeyboardButton("🔗 Відкрити кабінет у браузері", url=o["pages_url"])])
        kb_rows.append([InlineKeyboardButton("🔄 Оновити цей кабінет даними з профілю", callback_data="profile_redeploy")])

    kb_rows.append([InlineKeyboardButton("✏️ Змінити мої дані у профілі", callback_data="profile_edit_menu")])
    kb_rows.append(back_btn("my_orders"))

    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows), disable_web_page_preview=True)


# ── Catalog & Purchase Flow Handlers (Extended) ─────────────────────────────

async def show_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = await active_tariffs()
    
    if not tariffs:
        await safe_edit(
            q,
            "📭 <b>Наразі немає активних тарифів.</b>\n\nСпробуйте завітати пізніше або зверніться до підтримки.",
            mkb(back_btn("home"))
        )
        return

    text = (
        "🛒 <b>ОБЕРІТЬ ТАРИФ ПІДПИСКИ</b>\n"
        "────────────────────────────\n"
        "Оберіть оптимальний термін дії вашого кабінету.\n"
        "Усі тарифи включають повний доступ до підключених документів.\n\n"
    )
    
    kb_rows = []
    for k, t in tariffs.items():
        days_str = "∞ безстроково" if not t.get("days") else f"{t['days']} дн."
        text += f"{t.get('emoji','📦')} <b>{esc(t.get('name'))}</b> — <b>{t.get('price')}₴</b> <i>({days_str})</i>\n"
        
        btn_label = f"{t.get('emoji','📦')} {t.get('name')} — {t.get('price')}₴"
        kb_rows.append([InlineKeyboardButton(btn_label, callback_data=f"tar:{k}")])

    # Перевірка наявності активованої знижки з контексту
    promo_discount = context.user_data.get("promo_discount", 0)
    promo_code = context.user_data.get("promo_code")
    if promo_discount > 0:
        text += f"\n🎟️ <b>Активовано промо-код <code>{promo_code}</code>:</b> -{promo_discount}%\n"

    kb_rows.append([InlineKeyboardButton("🎟️ Ввести промо-код", callback_data="promo_enter")])
    kb_rows.append(back_btn("home"))
    
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


async def select_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    
    new_enabled = await get_setting("new_orders_enabled")
    if not new_enabled and not is_admin(uid):
        await q.answer("❌ Прийом нових замовлень тимчасово призупинено адміністратором.", show_alert=True)
        return

    parts = q.data.split(":")
    if len(parts) < 2:
        return
    key = parts[1]
    
    tariffs = await active_tariffs()
    if key not in tariffs:
        await q.answer("❌ Цей тариф більше недоступний.", show_alert=True)
        return

    t = tariffs[key]
    discount = context.user_data.get("promo_discount", 0)
    base_price = t.get("price", 0)
    final_price = int(base_price * (100 - discount) / 100) if discount else base_price

    context.user_data.update({
        "tariff": key,
        "tariff_name": t.get("name"),
        "tariff_price": base_price,
        "final_price": final_price,
    })

    # Перевірка, чи є у користувача вже збережені дані в Профілі
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})
    has_profile_data = bool(u.get("doc_fio") and u.get("doc_dob"))

    if has_profile_data:
        # Пропонуємо заповнення в 1-клік з профілю
        fio = u.get("doc_fio")
        dob = u.get("doc_dob")
        sex_str = "Чоловік ♂️" if u.get("doc_sex", "Ч") in ("Ч", "M") else "Жінка ♀️"
        price_text = f"<s>{base_price}₴</s> <b>{final_price}₴</b>" if discount else f"<b>{final_price}₴</b>"

        text = (
            f"{t.get('emoji','📦')} <b>Обрано тариф: {esc(t.get('name'))}</b> ({price_text})\n"
            f"────────────────────────────\n"
            f"💡 <b>У вас є збережені дані в Профілі:</b>\n\n"
            f"▫️ <b>ПІБ:</b> {esc(fio)}\n"
            f"▫️ <b>ДН:</b> {esc(dob)}\n"
            f"▫️ <b>Стать:</b> {sex_str}\n"
            f"▫️ <b>Адреса:</b> {esc(u.get('doc_address', 'Автогенерація'))}\n\n"
            f"Використати ці дані для швидкого оформлення?"
        )

        kb = mkb(
            [InlineKeyboardButton("⚡️ Використати дані з Профілю", callback_data="buy_use_profile")],
            [InlineKeyboardButton("✏️ Ввести нові дані вручну", callback_data="buy_start_manual")],
            [InlineKeyboardButton("🔙 До каталогу", callback_data="catalog")]
        )
        await safe_edit(q, text, kb)
        return

    # Якщо даних немає, переходимо до кроку 1
    context.user_data["state"] = AWAIT_FIO
    await safe_edit(
        q,
        f"{t.get('emoji','📦')} <b>Тариф: {esc(t.get('name'))}</b> — {final_price}₴\n\n"
        "📝 <b>Крок 1/7</b> — Введіть ПІБ українською мовою:\n"
        "<i>Приклад: Шевченко Тарас Григорович</i>",
        mkb([InlineKeyboardButton("❌ Скасувати", callback_data="catalog")])
    )


async def buy_use_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Швидке заповнення майстра з даних Профілю користувача."""
    q = update.callback_query
    uid = str(q.from_user.id)
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})

    context.user_data.update({
        "fio": u.get("doc_fio"),
        "fio_en": u.get("doc_fio_en", ""),
        "dob": u.get("doc_dob"),
        "sex": "M" if u.get("doc_sex", "Ч") in ("Ч", "M") else "W",
        "address": u.get("doc_address", ""),
        "is_rights": u.get("doc_is_rights", True),
        "is_zagran": u.get("doc_is_zagran", True),
        "is_diploma": u.get("doc_is_diploma", False),
        "is_study": u.get("doc_is_diploma", False),
    })

    # Перевірка наявності збереженого фото
    photo_path = u.get("doc_photo_path")
    if photo_path and os.path.exists(photo_path):
        # Якщо фото існує, одразу показуємо підсумкове підтвердження
        await _show_order_summary(update, context)
    else:
        # Якщо фото немає, просимо лише надіслати фото
        context.user_data["state"] = AWAIT_PHOTO
        await safe_edit(
            q,
            "📸 <b>Останній крок</b> — Надішліть фото 3×4\n"
            "<i>Надішліть якісний портрет на світлому фоні</i>",
            mkb([InlineKeyboardButton("❌ Скасувати", callback_data="catalog")])
        )


async def buy_start_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Примусовий старт ручного введення даних."""
    q = update.callback_query
    context.user_data["state"] = AWAIT_FIO
    t_name = context.user_data.get("tariff_name", "")
    t_price = context.user_data.get("final_price", 0)

    await safe_edit(
        q,
        f"📦 <b>Тариф: {esc(t_name)}</b> — {t_price}₴\n\n"
        "📝 <b>Крок 1/7</b> — Введіть ПІБ українською мовою:\n"
        "<i>Приклад: Шевченко Тарас Григорович</i>",
        mkb([InlineKeyboardButton("❌ Скасувати", callback_data="catalog")])
    )


async def select_sex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not context.user_data or context.user_data.get("state") != AWAIT_SEX:
        await safe_edit(q, "⚠️ Сесію вичерпано. Почніть спочатку через /start", mkb(back_btn("home")))
        return

    parts = q.data.split(":")
    if len(parts) < 2:
        return
    
    context.user_data["sex"] = parts[1]
    context.user_data["state"] = AWAIT_ADDRESS
    sex_text = "Чоловік ♂️" if context.user_data["sex"] == "M" else "Жінка ♀️"

    kb = mkb(
        [InlineKeyboardButton("⏩ Пропустити (автогенерація)", callback_data="skip_address")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="catalog")]
    )

    await safe_edit(
        q,
        f"✅ Стать: <b>{sex_text}</b>\n\n"
        "🏠 <b>Крок 4/7</b> — Адреса прописки\n"
        "<i>Приклад: м. Харків, вул. Сумська, 5, кв. 12</i>\n\n"
        "<i>Натисніть кнопку нижче або введіть /skip для автогенерації</i>",
        kb
    )


async def skip_address_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка пропуску введення адреси через inline-кнопку."""
    q = update.callback_query
    context.user_data["address"] = ""
    context.user_data["state"] = AWAIT_RIGHTS_CHOICE
    await _ask_rights(update, context)


async def _ask_rights(update, context):
    kb = mkb(
        [InlineKeyboardButton("✅ Так", callback_data="rights:yes"),
         InlineKeyboardButton("❌ Ні",  callback_data="rights:no")],
        [InlineKeyboardButton("❌ Скасувати замовлення", callback_data="catalog")]
    )
    text = "🚗 <b>Крок 5/7</b> — Відображати водійські права у кабінеті?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def _ask_zagran(update, context):
    kb = mkb(
        [InlineKeyboardButton("✅ Так", callback_data="zagran:yes"),
         InlineKeyboardButton("❌ Ні",  callback_data="zagran:no")],
        [InlineKeyboardButton("❌ Скасувати замовлення", callback_data="catalog")]
    )
    text = "🌍 <b>Крок 6/7</b> — Відображати закордонний паспорт у кабінеті?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def _ask_diploma(update, context):
    kb = mkb(
        [InlineKeyboardButton("✅ Так", callback_data="diploma:yes"),
         InlineKeyboardButton("❌ Ні",  callback_data="diploma:no")],
        [InlineKeyboardButton("❌ Скасувати замовлення", callback_data="catalog")]
    )
    text = "🎓 <b>Крок 7/7</b> — Відображати диплом / студентський квиток?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


async def select_rights(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["is_rights"] = (len(parts) > 1 and parts[1] == "yes")
    context.user_data["state"] = AWAIT_ZAGRAN_CHOICE
    await _ask_zagran(update, context)


async def select_zagran(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["is_zagran"] = (len(parts) > 1 and parts[1] == "yes")
    context.user_data["state"] = AWAIT_DIPLOMA_CHOICE
    await _ask_diploma(update, context)


async def select_diploma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    val = (len(parts) > 1 and parts[1] == "yes")
    context.user_data["is_diploma"] = val
    context.user_data["is_study"] = val
    context.user_data["state"] = AWAIT_PHOTO

    kb = mkb([InlineKeyboardButton("❌ Скасувати замовлення", callback_data="catalog")])
    text = (
        "📸 <b>Останній крок</b> — Надішліть фото 3×4\n\n"
        "<i>Надішліть зображення (портрет на світлому фоні).</i>"
    )
    await safe_edit(q, text, kb)


async def _show_order_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ підсумкового екрану перевірки перед генерацією та створенням замовлення."""
    q = update.callback_query
    d = context.user_data

    fio = d.get("fio", "—")
    dob = d.get("dob", "—")
    sex = "Чоловік ♂️" if d.get("sex") == "M" else "Жінка ♀️"
    addr = d.get("address") or "Харківська область (авто)"
    
    r = "✅ Так" if d.get("is_rights") else "❌ Ні"
    z = "✅ Так" if d.get("is_zagran") else "❌ Ні"
    dip = "✅ Так" if d.get("is_diploma") else "❌ Ні"

    price = d.get("final_price", d.get("tariff_price", 0))

    text = (
        f"📋 <b>ПЕРЕВІРКА ДАНИХ ЗАМОВЛЕННЯ</b>\n"
        f"────────────────────────────\n"
        f"💎 <b>Тариф:</b> {esc(d.get('tariff_name'))}\n"
        f"💳 <b>До сплати:</b> <b>{price}₴</b>\n\n"
        f"📝 <b>ПІБ:</b> {esc(fio)}\n"
        f"📅 <b>Дата народження:</b> {esc(dob)}\n"
        f"👤 <b>Стать:</b> {sex}\n"
        f"🏠 <b>Адреса:</b> {esc(addr)}\n"
        f"🚗 <b>Права:</b> {r} | 🌍 <b>Загран:</b> {z} | 🎓 <b>Диплом:</b> {dip}\n\n"
        f"Все вірно? Натисніть кнопку нижче для підтвердження."
    )

    kb = mkb(
        [InlineKeyboardButton("✅ Все вірно — Підтвердити", callback_data="confirm_order_final")],
        [InlineKeyboardButton("✏️ Заповнити заново", callback_data="buy_start_manual")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="catalog")]
    )

    if q:
        await safe_edit(q, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")

# ── Text Message Handler ───────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    users = await async_load(USERS_KEY, {})
    if users.get(uid, {}).get("banned"):
        return

    state = context.user_data.get("state")
    text  = (update.message.text or "").strip()

    if state == EDIT_PROFILE_INPUT_VALUE:
        field = context.user_data.get("edit_profile_field")
        u = users.get(uid, {})

        if field == "fio":
            if len(text.split()) < 2:
                await update.message.reply_text("❌ Введіть як мінімум 2 слова (Прізвище Ім'я).")
                return
            u["doc_fio"] = text
            if AI_ENABLED:
                u["doc_fio_en"] = await ai_transliterate(text)
            else:
                u["doc_fio_en"] = text

        elif field == "dob":
            if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", text):
                await update.message.reply_text("❌ Формат: ДД.ММ.РРРР (наприклад: 12.04.1995)")
                return
            u["doc_dob"] = text

        elif field == "addr":
            u["doc_address"] = text

        await async_save(USERS_KEY, users)
        await sync_profile_to_latest_order(uid, u)

        context.user_data["state"] = None
        context.user_data.pop("edit_profile_field", None)

        await update.message.reply_text(
            f"✅ <b>Дані успішно збережено в Профілі!</b>\n\nНатисніть 🚀 <b>Застосувати та оновити сайт</b> для публікації змін.",
            reply_markup=mkb([InlineKeyboardButton("✏️ Повернутися до редагування", callback_data="profile_edit_menu")],
                             [InlineKeyboardButton("🚀 Оновити сайт кабінету", callback_data="profile_redeploy")]),
            parse_mode="HTML"
        )
        return

    if is_admin(uid) and update.message.reply_to_message:
        await _handle_admin_reply_msg(update, context)
        return

    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context)
        return

    if state == AWAIT_PROMO_CODE and not is_admin(uid):
        result = await check_promo(text, uid)
        if result["ok"]:
            await apply_promo(text, uid)
            context.user_data["promo_discount"] = result["discount"]
            context.user_data["promo_code"]     = text.upper().strip()
            log_action("promo_used", uid, {"code": text})
        context.user_data["state"] = None
        await update.message.reply_text(result["msg"], parse_mode="HTML")
        return

    if state == AWAIT_FEEDBACK:
        fid = gen_id("fb_")
        feedbacks = await async_load(FEEDBACK_KEY, {})
        feedbacks[fid] = {
            "user_id":    uid,
            "username":   update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "feedback":   text,
            "created_at": now_str(),
            "status":     "new",
        }
        await async_save(FEEDBACK_KEY, feedbacks)
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
        await update.message.reply_text("✅ <b>Дякуємо за відгук!</b> Ми зв'яжемося з вами найближчим часом. 🌸", parse_mode="HTML")
        return

    if state == AWAIT_FIO:
        if len(text.split()) < 2:
            await update.message.reply_text("❌ Введіть мінімум 2 слова (Прізвище Ім'я).")
            return
        context.user_data["fio"] = text
        context.user_data["state"] = AWAIT_DOB
        await update.message.reply_text("📅 <b>Крок 2/7</b> — Дата народження\nФормат: <b>ДД.ММ.РРРР</b>", parse_mode="HTML")
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

    if is_admin(uid):
        await _handle_admin_state(update, context, state, text, uid)
        return

    ai_sup = await get_setting("ai_support")
    if AI_ENABLED and ai_sup and text and not state:
        history = context.user_data.get("ai_history", [])
        reply = await ai_support_reply(text, history[-8:])
        if reply:
            history.append({"role": "user", "content": text})
            history.append({"role": "assistant", "content": reply})
            context.user_data["ai_history"] = history[-16:]
            await update.message.reply_text(
                f"🤖 {reply}\n\n<i>Для меню натисніть /start</i>", parse_mode="HTML")
            return

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


# ── Media Handler ──────────────────────────────────────────────────────────────

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    state = context.user_data.get("state")

    if state == EDIT_PROFILE_AWAIT_PHOTO and update.message.photo:
        users = await async_load(USERS_KEY, {})
        u = users.get(uid, {})

        photo_file  = await update.message.photo[-1].get_file()
        photo_bytes = bytes(await photo_file.download_as_bytearray())

        os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)
        photo_path = os.path.join(ORDER_PHOTOS_DIR, f"user_{uid}.png")
        await asyncio.to_thread(lambda: open(photo_path, "wb").write(photo_bytes))

        u["doc_photo_path"] = photo_path
        await async_save(USERS_KEY, users)
        await sync_profile_to_latest_order(uid, u)

        context.user_data["state"] = None
        context.user_data.pop("edit_profile_field", None)

        await update.message.reply_text(
            "✅ <b>Фото профілю оновлено!</b>\n\nНатисніть 🚀 <b>Застосувати та оновити сайт</b> для публікації змін.",
            reply_markup=mkb([InlineKeyboardButton("✏️ Повернутися до редагування", callback_data="profile_edit_menu")],
                             [InlineKeyboardButton("🚀 Оновити сайт кабінету", callback_data="profile_redeploy")]),
            parse_mode="HTML"
        )
        return

    elif state == AWAIT_PHOTO and update.message.photo:
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
    
    def save_photo_file():
        with open(photo_path, "wb") as f:
            f.write(photo_bytes)

    await asyncio.to_thread(save_photo_file)

    fio_ua = context.user_data.get("fio", "")
    if AI_ENABLED and fio_ua:
        context.user_data["fio_en"] = await ai_transliterate(fio_ua)

    users = await async_load(USERS_KEY, {})
    if uid in users:
        users[uid].update({
            "doc_fio":         context.user_data.get("fio"),
            "doc_fio_en":      context.user_data.get("fio_en"),
            "doc_dob":         context.user_data.get("dob"),
            "doc_sex":         "Ч" if context.user_data.get("sex") == "M" else "Ж",
            "doc_address":     context.user_data.get("address", ""),
            "doc_is_rights":   context.user_data.get("is_rights", True),
            "doc_is_zagran":   context.user_data.get("is_zagran", True),
            "doc_is_diploma":  context.user_data.get("is_diploma", False),
            "doc_photo_path": photo_path,
        })
        await async_save(USERS_KEY, users)

    values_data = gen_values_dict({**context.user_data, "order_id": oid})
    js_content  = values_to_js(values_data)

    discount    = context.user_data.get("promo_discount", 0)
    base_price  = context.user_data.get("tariff_price", 0)
    final_price = int(base_price * (100 - discount) / 100)

    orders = await async_load(ORDERS_KEY, {})
    orders[oid] = {
        "user_id":      uid,
        "tariff":       context.user_data.get("tariff"),
        "tariff_name":  context.user_data.get("tariff_name"),
        "fio":          context.user_data.get("fio"),
        "dob":          context.user_data.get("dob"),
        "sex":          context.user_data.get("sex"),
        "address":      context.user_data.get("address", ""),
        "is_rights":    context.user_data.get("is_rights", True),
        "is_zagran":    context.user_data.get("is_zagran", True),
        "is_diploma":   context.user_data.get("is_diploma", False),
        "is_study":     context.user_data.get("is_study", False),
        "promo":        context.user_data.get("promo_code"),
        "discount":     discount,
        "price":        base_price,
        "final_price":  final_price,
        "created_at":   now_str(),
        "status":       "pending",
        "photo_path":   photo_path,
        "values_data":  values_data,
        "js_content":   js_content,
    }
    await async_save(ORDERS_KEY, orders)

    if uid in users:
        users[uid]["total_orders"] = users[uid].get("total_orders", 0) + 1
        await async_save(USERS_KEY, users)

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
            await context.bot.send_photo(admin_id, buf, caption=caption, reply_markup=admin_kb, parse_mode="HTML")
            js_buf = io.BytesIO(js_content.encode()); js_buf.name = f"values_{oid}.js"
            await context.bot.send_document(admin_id, js_buf, caption=f"📄 values.js для #{oid}")
        except Exception as e:
            logger.error("Admin notify error: %s", e)

    await notify_group_photo(context.bot, photo_bytes, caption, admin_kb)
    await update.message.reply_text(
        f"✅ <b>Замовлення #{esc(oid)} прийнято!</b>\n\n"
        f"💳 До сплати: <b>{price_text}</b>\n\n"
        "📌 Подальші кроки:\n1️⃣ Отримайте реквізити\n2️⃣ Надішліть фото чека\n3️⃣ Отримайте посилання на сайт ⚡️",
        parse_mode="HTML",
    )
    context.user_data.clear()


async def _forward_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    orders = await async_load(ORDERS_KEY, {})
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

    receipt_bytes = b""
    if update.message.photo:
        f = await update.message.photo[-1].get_file()
        receipt_bytes = bytes(await f.download_as_bytearray())
    elif update.message.document and (update.message.document.mime_type or "").startswith("image/"):
        f = await update.message.document.get_file()
        receipt_bytes = bytes(await f.download_as_bytearray())

    settings = await load_settings()
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
        await async_save(ORDERS_KEY, orders)
        log_action("ai_receipt_approved", uid, {"oid": last_oid, "confidence": confidence})

        await update.message.reply_text(
            f"🎉 <b>Оплату підтверджено автоматично!</b>\n🤖 AI ({confidence}% впевненість)\n⏳ Готуємо кабінет...",
            parse_mode="HTML")

        try:
            folder1_url = await _run_chain_deploy(last_oid, last_order)
            orders = await async_load(ORDERS_KEY, {})
            tariffs = await load_tariffs()
            sub_end = calc_subscription_end(orders[last_oid].get("tariff", ""), tariffs)
            orders[last_oid].update({
                "pages_url": folder1_url, "status": "deployed",
                "deployed_at": now_str(), "subscription_end": sub_end,
                "notified_days": [],
            })
            await async_save(ORDERS_KEY, orders)

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
        "✅ <b>Чек отримано!</b>\nАдміністратор перевірить його найближчим часом. 🌸", parse_mode="HTML")


# ── Chain Deploy Execution ─────────────────────────────────────────────────────

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

    result = await asyncio.to_thread(chain_deploy.run_full_chain, values_data, order_id=oid)
    return result["folder1_url"]


async def _handle_referral_bonus(context, uid: str):
    users = await async_load(USERS_KEY, {})
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
    await async_save(USERS_KEY, users)


# ── Admin Handlers ─────────────────────────────────────────────────────────────

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
                feedbacks = await async_load(FEEDBACK_KEY, {})
                if fid in feedbacks:
                    feedbacks[fid]["status"]      = "replied"
                    feedbacks[fid]["admin_reply"] = update.message.text
                    await async_save(FEEDBACK_KEY, feedbacks)
            await update.message.reply_text(f"✅ Відповідь надіслана → {target}")
        except Exception as e:
            await update.message.reply_text(f"❌ Помилка: {e}")
    context.user_data["state"] = None
    context.user_data.pop("reply_to_uid", None)
    context.user_data.pop("reply_fb_id",  None)


async def _handle_admin_state(update, context, state, text, uid):
    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context); return

    if state == AWAIT_WELCOME_TEXT:
        s = await load_settings(); s["welcome_text"] = text; await save_settings(s)
        context.user_data["state"] = None
        await update.message.reply_text("✅ Текст привітання оновлено!"); return

    if state == AWAIT_BROADCAST:
        context.user_data["broadcast_text"] = text
        context.user_data["state"] = None
        users = await async_load(USERS_KEY, {})
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
            promos = await load_promos()
            promos[code] = {"discount": context.user_data["new_promo_discount"],
                            "max_uses": uses, "uses": 0, "active": True,
                            "used_by": [], "created_at": now_str(), "created_by": uid}
            await save_promos(promos)
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
        users = await async_load(USERS_KEY, {})
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
            users = await async_load(USERS_KEY, {})
            if target_uid not in users:
                await update.message.reply_text("❌ Не знайдено.")
            else:
                users[target_uid]["balance"] = max(0, users[target_uid].get("balance", 0) + amount)
                await async_save(USERS_KEY, users)
                context.user_data["state"] = None
                await update.message.reply_text(
                    f"✅ Баланс {target_uid}: {'+' if amount>=0 else ''}{amount}₴\nНовий: {users[target_uid]['balance']}₴")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_CUSTOM_PAYMENT_TEXT:
        parts = text.split("\n")
        if len(parts) >= 2:
            s = await load_settings()
            s["payment_card"]   = parts[0].strip()
            s["payment_holder"] = parts[1].strip()
            if len(parts) >= 3:
                s["payment_link"] = parts[2].strip()
            await save_settings(s)
            context.user_data["state"] = None
            await update.message.reply_text("✅ Реквізити оновлено!")
        else:
            await update.message.reply_text("❌ Мінімум 2 рядки: картка + отримувач")
        return

    if state == AWAIT_REJECT_REASON:
        oid = context.user_data.get("reject_oid")
        client_uid = context.user_data.get("reject_uid")
        if oid and client_uid:
            orders = await async_load(ORDERS_KEY, {})
            if oid in orders:
                orders[oid]["status"] = "rejected"
                orders[oid]["reject_reason"] = text
                await async_save(ORDERS_KEY, orders)
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
        tariffs = await load_tariffs()
        if key in tariffs:
            if state == AWAIT_TARIFF_EDIT_PRICE:
                try:
                    tariffs[key]["price"] = int(text)
                    await save_tariffs(tariffs)
                    context.user_data["state"] = None
                    await update.message.reply_text(f"✅ Ціна → {text}₴")
                except ValueError:
                    await update.message.reply_text("❌ Введіть число!")
            elif state == AWAIT_TARIFF_EDIT_NAME:
                tariffs[key]["name"] = text
                await save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Назва → {esc(text)}")
            else:
                tariffs[key]["emoji"] = text.strip()
                await save_tariffs(tariffs)
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
        tariffs = await load_tariffs()
        base, c = key, 1
        while key in tariffs:
            key = f"{base}_{c}"; c += 1
        tariffs[key] = {"name": name, "price": price, "days": days, "emoji": emj, "active": True}
        await save_tariffs(tariffs)
        context.user_data["state"] = None
        log_action("tariff_created", uid, {"key": key})
        await update.message.reply_text(
            f"✅ Тариф: {emj} {esc(name)} — {price}₴  ({'∞' if not days else f'{days} дн.'})"); return


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

        orders = await async_load(ORDERS_KEY, {})
        if oid in orders:
            orders[oid]["status"] = "completed"
            orders[oid]["completed_at"] = now_str()
            await async_save(ORDERS_KEY, orders)
            users = await async_load(USERS_KEY, {})
            if client_uid in users:
                users[client_uid]["total_spent"] = (
                    users[client_uid].get("total_spent", 0) + orders[oid].get("final_price", 0))
                await async_save(USERS_KEY, users)

        log_action("order_completed", None, {"oid": oid})
        await update.message.reply_text(f"✅ Файли надіслані клієнту {client_uid} (#{oid})")
        context.user_data["state"] = None
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: {e}")


# ── Admin Handlers (Extended & Hardened) ───────────────────────────────────────

async def _handle_admin_reply_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка швидкої відповіді адміна через Reply на переслане повідомлення/чек."""
    reply_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    
    # Шукаємо pattern ID користувача або замовлення
    m_uid = re.search(r"🆔\s*(\d+)", reply_text)
    m_oid = re.search(r"#([a-zA-Z0-9_]{6,12})", reply_text)
    
    if not m_uid:
        await update.message.reply_text("⚠️ Не вдалося розпізнати ID клієнта (🆔 XXXXXXXXX) у повідомленні.")
        return

    client_id = m_uid.group(1)
    oid_str = f" [до #{m_oid.group(1)}]" if m_oid else ""
    
    try:
        sent_msg = await context.bot.send_message(
            chat_id=client_id,
            text=f"💬 <b>Відповідь адміністратора{oid_str}:</b>\n\n{esc(update.message.text)}\n\n🌸 <i>З повагою, команда підтримки</i>",
            parse_mode="HTML"
        )
        log_action("admin_reply_msg", update.effective_user.id, {"target_uid": client_id, "text": update.message.text})
        await update.message.reply_text(f"✅ Відповідь успішно надіслано клієнту <code>{client_id}</code>", parse_mode="HTML")
    except Forbidden:
        await update.message.reply_text(f"❌ Помилка: Клієнт <code>{client_id}</code> заблокував бота.", parse_mode="HTML")
    except Exception as e:
        logger.error("Admin reply error: %s", e)
        await update.message.reply_text(f"❌ Помилка надсилання: {esc(str(e))}")


async def _do_reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Надсилання відповіді клієнту у режимі активного FSM-стану AWAIT_REPLY_TO_USER."""
    target = context.user_data.get("reply_to_uid")
    fid = context.user_data.get("reply_fb_id")

    if not target:
        await update.message.reply_text("❌ Помилка: Цільовий ID користувача втрачено у сесії.")
        context.user_data["state"] = None
        return

    try:
        await context.bot.send_message(
            chat_id=target,
            text=f"💬 <b>Відповідь адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸 <i>Якщо у вас є додаткові питання — просто напишіть у цей чат!</i>",
            parse_mode="HTML"
        )
        
        # Оновлюємо статус тикета/відгуку, якщо reply був прив'язаний до feedback_id
        if fid:
            feedbacks = await async_load(FEEDBACK_KEY, {})
            if fid in feedbacks:
                feedbacks[fid]["status"] = "replied"
                feedbacks[fid]["admin_reply"] = update.message.text
                feedbacks[fid]["replied_at"] = now_str()
                feedbacks[fid]["replied_by"] = update.effective_user.id
                await async_save(FEEDBACK_KEY, feedbacks)

        log_action("admin_direct_reply", update.effective_user.id, {"target_uid": target, "fb_id": fid})
        await update.message.reply_text(f"✅ Повідомлення надіслано користувачу <code>{target}</code>", parse_mode="HTML")

    except Forbidden:
        await update.message.reply_text(f"❌ Не вдалося надіслати: Користувач <code>{target}</code> зупинив бота.", parse_mode="HTML")
    except Exception as e:
        logger.error("Error in _do_reply_to_user: %s", e)
        await update.message.reply_text(f"❌ Помилка відправки: {esc(str(e))}")

    # Скидаємо стан сесії після завершення
    context.user_data["state"] = None
    context.user_data.pop("reply_to_uid", None)
    context.user_data.pop("reply_fb_id", None)


async def _handle_admin_state(update: Update, context: ContextTypes.DEFAULT_TYPE, state: int, text: str, uid: str):
    """Маршрутизатор FSM-станів адмін-панелі."""
    
    # 1. Відповідь користувачу
    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context)
        return

    # 2. Оновлення вітального тексту
    if state == AWAIT_WELCOME_TEXT:
        s = await load_settings()
        s["welcome_text"] = text
        await save_settings(s)
        context.user_data["state"] = None
        log_action("update_welcome_text", uid)
        await update.message.reply_text("✅ <b>Текст привітання успішно оновлено!</b>", parse_mode="HTML")
        return

    # 3. Підготовка масової розсилки
    if state == AWAIT_BROADCAST:
        context.user_data["broadcast_text"] = text
        context.user_data["state"] = None
        users = await async_load(USERS_KEY, {})
        active_users = sum(1 for u in users.values() if not u.get("banned"))

        preview_text = (
            f"📢 <b>ПОПЕРЕДНІЙ ПЕРЕГЛЯД РОЗСИЛКИ:</b>\n"
            f"────────────────────────────\n"
            f"{text}\n"
            f"────────────────────────────\n"
            f"👥 <b>Отримувачів (активних):</b> {active_users} із {len(users)}"
        )
        kb = mkb(
            [InlineKeyboardButton("✅ Підтвердити та надіслати", callback_data="broadcast_go")],
            [InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")]
        )
        await update.message.reply_text(preview_text, reply_markup=kb, parse_mode="HTML")
        return

    # 4. Промокоди: Назва -> Знижка -> Кількість
    if state == AWAIT_PROMO_CODE:
        code_clean = text.upper().strip()
        if not re.match(r"^[A-Z0-9_\-]{2,20}$", code_clean):
            await update.message.reply_text("❌ Невалідний код. Використовуйте лише латинські літери та цифри (2-20 символів).")
            return

        promos = await load_promos()
        warn_msg = "\n⚠️ <i>Увага: Промокод із такою назвою вже існує і буде перезаписаний!</i>" if code_clean in promos else ""

        context.user_data["new_promo_code"] = code_clean
        context.user_data["state"] = AWAIT_PROMO_DISCOUNT
        await update.message.reply_text(
            f"✅ Код: <b><code>{esc(code_clean)}</code></b>{warn_msg}\n\n"
            f"Введіть розмір знижки у відсотках (від 1 до 100):",
            parse_mode="HTML"
        )
        return

    if state == AWAIT_PROMO_DISCOUNT:
        try:
            val = int(text)
            if not (1 <= val <= 100):
                raise ValueError
            context.user_data["new_promo_discount"] = val
            context.user_data["state"] = AWAIT_PROMO_USES
            await update.message.reply_text(
                f"✅ Знижка: <b>{val}%</b>\n\n"
                f"Введіть максимальну кількість використань (введіть <b>0</b> для необмеженої кількості):",
                parse_mode="HTML"
            )
        except ValueError:
            await update.message.reply_text("❌ Будь ласка, введіть ціле число від 1 до 100.")
        return

    if state == AWAIT_PROMO_USES:
        try:
            uses = int(text)
            if uses < 0:
                raise ValueError
            
            code = context.user_data["new_promo_code"]
            discount = context.user_data["new_promo_discount"]
            
            promos = await load_promos()
            promos[code] = {
                "discount": discount,
                "max_uses": uses,
                "uses": 0,
                "active": True,
                "used_by": [],
                "created_at": now_str(),
                "created_by": uid
            }
            await save_promos(promos)
            context.user_data["state"] = None
            log_action("promo_created", uid, {"code": code, "discount": discount, "max_uses": uses})
            
            uses_str = "необмежено (∞)" if uses == 0 else f"{uses} разів"
            await update.message.reply_text(
                f"🎉 <b>Промо-код успішно створено!</b>\n\n"
                f"🎟️ Код: <code>{esc(code)}</code>\n"
                f"💰 Знижка: <b>{discount}%</b>\n"
                f"👥 Лиміт використань: <b>{uses_str}</b>",
                parse_mode="HTML"
            )
        except ValueError:
            await update.message.reply_text("❌ Введіть ціле невід'ємне число (0 або більше).")
        return

    # 5. Пошук користувача
    if state == AWAIT_USER_SEARCH:
        context.user_data["state"] = None
        users = await async_load(USERS_KEY, {})
        q_raw = text.strip().lstrip("@").lower()
        
        found = []
        for uid2, u in users.items():
            uname = (u.get("username") or "").lower()
            fname = (u.get("first_name") or "").lower()
            doc_fio = (u.get("doc_fio") or "").lower()
            
            if q_raw in uname or q_raw in str(uid2) or q_raw in fname or q_raw in doc_fio:
                found.append((uid2, u))

        if not found:
            await update.message.reply_text("🔍 Користувача за вашим запитом не знайдено.")
            return

        if len(found) == 1:
            await _send_user_card(update, context, found[0][0], found[0][1])
        else:
            text_list = f"🔍 <b>Знайдено {len(found)} користувачів:</b>\n\n"
            kb_rows = []
            for uid2, u in found[:10]:
                text_list += f"• <b>{esc(u.get('first_name','?'))}</b> (@{esc(u.get('username','—'))}) — ID: <code>{uid2}</code>\n"
                kb_rows.append([InlineKeyboardButton(f"👤 {u.get('first_name','?')} ({uid2})", callback_data=f"adm_user_card:{uid2}")])
            
            kb_rows.append([InlineKeyboardButton("🔙 В адмін-панель", callback_data="admin_panel")])
            await update.message.reply_text(text_list, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML")
        return

    # 6. Зміна балансу
    if state == AWAIT_BALANCE_UID:
        target_uid = text.strip()
        users = await async_load(USERS_KEY, {})
        if target_uid not in users:
            await update.message.reply_text("❌ Користувача з таким ID не знайдено в базі.")
            return
        
        context.user_data["balance_target_uid"] = target_uid
        context.user_data["state"] = AWAIT_BALANCE_AMOUNT
        curr_bal = users[target_uid].get("balance", 0)
        await update.message.reply_text(
            f"👤 Користувач: <code>{target_uid}</code>\n"
            f"💰 Поточний баланс: <b>{curr_bal}₴</b>\n\n"
            f"Введіть суму для зміни (наприклад <b>100</b> для нарахування або <b>-50</b> для списання):",
            parse_mode="HTML"
        )
        return

    if state == AWAIT_BALANCE_AMOUNT:
        try:
            amount = int(text)
            target_uid = context.user_data.get("balance_target_uid")
            users = await async_load(USERS_KEY, {})
            
            if target_uid not in users:
                await update.message.reply_text("❌ Помилка: Сесія втрачена.")
            else:
                old_bal = users[target_uid].get("balance", 0)
                new_bal = max(0, old_bal + amount)
                users[target_uid]["balance"] = new_bal
                await async_save(USERS_KEY, users)
                
                context.user_data["state"] = None
                log_action("balance_change", uid, {"target": target_uid, "delta": amount, "new_balance": new_bal})
                
                # Повідомляємо користувача
                try:
                    sign = "+" if amount > 0 else ""
                    await context.bot.send_message(
                        target_uid,
                        f"💳 <b>Зміна балансу!</b>\n\nОперація: <b>{sign}{amount}₴</b>\nВаш новий баланс: <b>{new_bal}₴</b>",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

                await update.message.reply_text(
                    f"✅ <b>Баланс оновлено!</b>\n\n"
                    f"🆔 Користувач: <code>{target_uid}</code>\n"
                    f"📊 Було: {old_bal}₴  ➜  <b>Стало: {new_bal}₴</b>",
                    parse_mode="HTML"
                )
        except ValueError:
            await update.message.reply_text("❌ Введіть коректне ціле число.")
        return

    # 7. Налаштування реквізитів
    if state == AWAIT_CUSTOM_PAYMENT_TEXT:
        parts = [p.strip() for p in text.split("\n") if p.strip()]
        if len(parts) >= 2:
            s = await load_settings()
            s["payment_card"] = parts[0]
            s["payment_holder"] = parts[1]
            if len(parts) >= 3:
                s["payment_link"] = parts[2]
            await save_settings(s)
            
            context.user_data["state"] = None
            log_action("update_payment_details", uid)
            await update.message.reply_text(
                f"✅ <b>Реквізити збережено!</b>\n\n"
                f"💳 Картка: <code>{esc(parts[0])}</code>\n"
                f"👤 Отримувач: <b>{esc(parts[1])}</b>\n"
                f"🔗 Посилання: {esc(parts[2] if len(parts)>=3 else '—')}",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text("❌ Потрібно ввести мінімум 2 рядки:\n1-й рядок: Номер картки\n2-й рядок: ПІБ/Назва банку отримувача")
        return

    # 8. Відхилення замовлення з причиною
    if state == AWAIT_REJECT_REASON:
        oid = context.user_data.get("reject_oid")
        client_uid = context.user_data.get("reject_uid")
        
        if oid and client_uid:
            orders = await async_load(ORDERS_KEY, {})
            if oid in orders:
                orders[oid]["status"] = "rejected"
                orders[oid]["reject_reason"] = text
                orders[oid]["rejected_at"] = now_str()
                orders[oid]["rejected_by"] = uid
                await async_save(ORDERS_KEY, orders)

            try:
                await context.bot.send_message(
                    client_uid,
                    f"❌ <b>Ваше замовлення #{esc(oid)} відхилено</b>\n\n"
                    f"💬 <b>Причина:</b> {esc(text)}\n\n"
                    f"<i>Якщо ви вважаєте це помилкою, зв'яжіться з підтримкою через розділ 💬 Підтримка.</i>",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error("Error notifying user of reject: %s", e)

            context.user_data["state"] = None
            log_action("order_rejected", uid, {"oid": oid, "reason": text})
            await update.message.reply_text(f"✅ Замовлення <code>#{oid}</code> відхилено, клієнта сповіщено.", parse_mode="HTML")
        return

    # 9. Редагування тарифу
    if state in (AWAIT_TARIFF_EDIT_PRICE, AWAIT_TARIFF_EDIT_NAME, AWAIT_TARIFF_EDIT_EMOJI):
        key = context.user_data.get("edit_tariff_key")
        tariffs = await load_tariffs()
        if key in tariffs:
            if state == AWAIT_TARIFF_EDIT_PRICE:
                try:
                    new_p = int(text)
                    tariffs[key]["price"] = new_p
                    await save_tariffs(tariffs)
                    context.user_data["state"] = None
                    await update.message.reply_text(f"✅ Ціну тарифу <b>{esc(tariffs[key]['name'])}</b> змінено на <b>{new_p}₴</b>", parse_mode="HTML")
                except ValueError:
                    await update.message.reply_text("❌ Введіть коректну ціну числом.")
            elif state == AWAIT_TARIFF_EDIT_NAME:
                tariffs[key]["name"] = text.strip()
                await save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Назву тарифу оновлено на <b>{esc(text)}</b>", parse_mode="HTML")
            elif state == AWAIT_TARIFF_EDIT_EMOJI:
                tariffs[key]["emoji"] = text.strip()
                await save_tariffs(tariffs)
                context.user_data["state"] = None
                await update.message.reply_text(f"✅ Емоджі тарифу змінено на {text.strip()}")
        return

    # 10. Створення нового тарифу
    if state == AWAIT_TARIFF_NAME:
        context.user_data["new_t_name"] = text.strip()
        context.user_data["state"] = AWAIT_TARIFF_PRICE
        await update.message.reply_text("💰 Введіть ціну тарифу в гривнях (числом):")
        return

    if state == AWAIT_TARIFF_PRICE:
        try:
            val = int(text)
            context.user_data["new_t_price"] = val
            context.user_data["state"] = AWAIT_TARIFF_DAYS
            await update.message.reply_text("📅 Введіть кількість днів дії підписки (введіть <b>0</b> для безстрокового тарифу):", parse_mode="HTML")
        except ValueError:
            await update.message.reply_text("❌ Введіть ціле число.")
        return

    if state == AWAIT_TARIFF_DAYS:
        try:
            days_val = int(text)
            context.user_data["new_t_days"] = days_val if days_val > 0 else None
            context.user_data["state"] = AWAIT_TARIFF_EMOJI
            await update.message.reply_text("😊 Надішліть емоджі для тарифу (наприклад: 🌟, 💎, 📅):")
        except ValueError:
            await update.message.reply_text("❌ Введіть ціле число.")
        return

    if state == AWAIT_TARIFF_EMOJI:
        name = context.user_data["new_t_name"]
        price = context.user_data["new_t_price"]
        days = context.user_data.get("new_t_days")
        emj = text.strip() or "📦"
        
        # Генерація унікального ключа
        key = re.sub(r"[^a-z0-9]", "_", name.lower())[:20]
        tariffs = await load_tariffs()
        base, c = key, 1
        while key in tariffs:
            key = f"{base}_{c}"
            c += 1

        tariffs[key] = {
            "name": name,
            "price": price,
            "days": days,
            "emoji": emj,
            "active": True
        }
        await save_tariffs(tariffs)
        context.user_data["state"] = None
        log_action("tariff_created", uid, {"key": key, "name": name, "price": price})
        
        days_str = "безстроково (∞)" if not days else f"{days} днів"
        await update.message.reply_text(
            f"🎉 <b>Тариф успішно створено!</b>\n\n"
            f"{emj} <b>{esc(name)}</b> — <b>{price}₴</b> ({days_str})",
            parse_mode="HTML"
        )
        return


async def _process_complete_order_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка відправки готових файлів/документів від адміна клієнту."""
    oid = context.user_data.get("complete_oid")
    client_uid = context.user_data.get("complete_uid")
    
    if not oid or not client_uid:
        await update.message.reply_text("❌ Помилка: Контекст замовлення втрачено.")
        context.user_data["state"] = None
        return

    caption = f"📁 <b>Ваше замовлення #{esc(oid)} виконано!</b>\n\nДякуємо, що обираєте наш сервіс 🌸"

    try:
        if update.message.document:
            await context.bot.send_document(chat_id=client_uid, document=update.message.document.file_id, caption=caption, parse_mode="HTML")
        elif update.message.photo:
            await context.bot.send_photo(chat_id=client_uid, photo=update.message.photo[-1].file_id, caption=caption, parse_mode="HTML")
        elif update.message.video:
            await context.bot.send_video(chat_id=client_uid, video=update.message.video.file_id, caption=caption, parse_mode="HTML")
        elif update.message.audio:
            await context.bot.send_audio(chat_id=client_uid, audio=update.message.audio.file_id, caption=caption, parse_mode="HTML")
        else:
            await update.message.reply_text("⚠️ Непідтримуваний тип файлу. Надішліть документ, фото або відео.")
            return

        # Оновлюємо статус у БД
        orders = await async_load(ORDERS_KEY, {})
        if oid in orders:
            orders[oid]["status"] = "completed"
            orders[oid]["completed_at"] = now_str()
            orders[oid]["completed_by"] = update.effective_user.id
            await async_save(ORDERS_KEY, orders)

            # Нараховуємо загальну суму витрат користувачу
            users = await async_load(USERS_KEY, {})
            if client_uid in users:
                spent = orders[oid].get("final_price", 0)
                users[client_uid]["total_spent"] = users[client_uid].get("total_spent", 0) + spent
                await async_save(USERS_KEY, users)

        log_action("order_completed_files", update.effective_user.id, {"oid": oid, "client_uid": client_uid})
        await update.message.reply_text(f"✅ Файли успішно відправлені клієнту <code>{client_uid}</code>. Замовлення <code>#{oid}</code> закрито!", parse_mode="HTML")
        context.user_data["state"] = None

    except Forbidden:
        await update.message.reply_text(f"❌ Клієнт <code>{client_uid}</code> заблокував бота.", parse_mode="HTML")
    except Exception as e:
        logger.error("Error sending complete order files: %s", e)
        await update.message.reply_text(f"❌ Помилка при відправці файлів: {esc(str(e))}")


# ── Admin User Management Handlers (Extended) ──────────────────────────────────

USERS_PER_PAGE = 10

@admin_only
async def adm_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список користувачів з підтримкою пагінації."""
    q = update.callback_query
    page = 1
    if q and q.data and ":" in q.data:
        try:
            page = int(q.data.split(":")[1])
        except ValueError:
            page = 1

    users = await async_load(USERS_KEY, {})
    sorted_u = sorted(users.items(), key=lambda x: x[1].get("joined_date", ""), reverse=True)
    
    total_users = len(sorted_u)
    total_pages = max(1, (total_users + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))

    start_idx = (page - 1) * USERS_PER_PAGE
    end_idx = start_idx + USERS_PER_PAGE
    page_users = sorted_u[start_idx:end_idx]

    text = f"👥 <b>УПРАВЛІННЯ КОРИСТУВАЧАМИ</b> (Стор. {page}/{total_pages})\n"
    text += f"────────────────────────────\n"
    text += f"📊 Всього у базі: <b>{total_users}</b>\n\n"

    for uid2, u in page_users:
        badges = ("👑 " if u.get("vip") else "") + ("🚫 " if u.get("banned") else "") + ("💰 " if u.get("has_bought") else "🆕 ")
        fname = esc(u.get('first_name', 'Користувач'))
        uname = f"@{esc(u.get('username'))}" if u.get('username') else "без username"
        
        text += f"{badges}<b>{fname}</b> ({uname})\n"
        text += f"└ 🆔 <code>{uid2}</code> | 💳 {u.get('balance', 0)}₴ | 👥 {u.get('ref_count', 0)}\n\n"

    kb_rows = []
    
    # Кнопки пагінації
    nav_btns = []
    if page > 1:
        nav_btns.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"adm:users:{page - 1}"))
    nav_btns.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_btns.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"adm:users:{page + 1}"))
    kb_rows.append(nav_btns)

    kb_rows.append([
        InlineKeyboardButton("🔍 Пошук", callback_data="adm:search"),
        InlineKeyboardButton("💰 Змінити баланс", callback_data="adm:balance")
    ])
    kb_rows.append(back_btn("admin_panel"))

    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def adm_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_USER_SEARCH
    await safe_edit(
        update.callback_query,
        "🔍 <b>ПОШУК КОРИСТУВАЧА</b>\n────────────────────────────\n"
        "Введіть @username, Telegram ID або ПІБ для пошуку:",
        mkb(back_btn("admin_panel"))
    )


@admin_only
async def adm_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_BALANCE_UID
    await safe_edit(
        update.callback_query,
        "💰 <b>ЗМІНА БАЛАНСУ</b>\n────────────────────────────\n"
        "Введіть Telegram ID користувача:",
        mkb(back_btn("admin_panel"))
    )


async def _send_user_card(update: Update, context: ContextTypes.DEFAULT_TYPE, uid2: str, u: dict):
    """Детальна картка користувача з повним функціоналом керування."""
    orders = await async_load(ORDERS_KEY, {})
    user_orders = [o for o in orders.values() if o.get("user_id") == uid2]
    
    completed_orders = sum(1 for o in user_orders if o.get("status") in ("completed", "deployed"))
    total_spent = u.get("total_spent") or sum(o.get("final_price", 0) for o in user_orders if o.get("status") in ("completed", "deployed"))

    joined = u.get("joined_date", "невідомо")[:10] if u.get("joined_date") else "невідомо"
    username_str = f"@{esc(u.get('username'))}" if u.get('username') else "не вказано"

    text = (
        f"👤 <b>КАРТКА КОРИСТУВАЧА</b>\n"
        f"────────────────────────────\n"
        f"Ім'я: <b>{esc(u.get('first_name', '?'))}</b>\n"
        f"🆔 ID: <code>{uid2}</code>\n"
        f"📱 Юзернейм: {username_str}\n"
        f"📅 Реєстрація: <code>{joined}</code>\n\n"
        f"📊 <b>СТАТИСТИКА ТА БАЛАНС:</b>\n"
        f"💰 Баланс: <b>{u.get('balance', 0)}₴</b>\n"
        f"💳 Всього витрачено: <b>{total_spent}₴</b>\n"
        f"📦 Всього замовлень: <b>{len(user_orders)}</b> (активних/виконаних: {completed_orders})\n"
        f"👥 Запрошено рефералів: <b>{u.get('ref_count', 0)}</b>\n\n"
        f"⚙️ <b>СТАТУСИ:</b>\n"
        f"👑 VIP: {'✅ Так' if u.get('vip') else '❌ Ні'}\n"
        f"🚫 Блокування: {'🔴 Заблокований' if u.get('banned') else '🟢 Активний'}"
    )

    ban_btn_label = "🔓 Розблокувати" if u.get("banned") else "🚫 Заблокувати"
    vip_btn_label = "👤 Зняти VIP" if u.get("vip") else "👑 Надати VIP"

    kb = mkb(
        [InlineKeyboardButton(ban_btn_label, callback_data=f"adm_ban:{uid2}"),
         InlineKeyboardButton(vip_btn_label, callback_data=f"adm_vip:{uid2}")],
        [InlineKeyboardButton("💰 Поповнити / Списати", callback_data=f"adm_direct_balance:{uid2}"),
         InlineKeyboardButton("💬 Написати", callback_data=f"adm_msg:{uid2}")],
        [InlineKeyboardButton("📜 Замовлення клієнта", callback_data=f"adm_user_orders:{uid2}")],
        back_btn("adm:users")
    )

    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")


@admin_only
async def adm_direct_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Прямий перехід до зміни балансу з картки користувача."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    uid2 = parts[1]

    context.user_data["balance_target_uid"] = uid2
    context.user_data["state"] = AWAIT_BALANCE_AMOUNT

    users = await async_load(USERS_KEY, {})
    curr_bal = users.get(uid2, {}).get("balance", 0)

    await safe_edit(
        q,
        f"💰 <b>ЗМІНА БАЛАНСУ</b> для <code>{uid2}</code>\n"
        f"Поточний баланс: <b>{curr_bal}₴</b>\n\n"
        f"Введіть суму змін (наприклад <b>100</b> або <b>-50</b>):",
        mkb(back_btn(f"adm_user_card:{uid2}"))
    )


@admin_only
async def adm_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    uid2 = parts[1]
    
    users = await async_load(USERS_KEY, {})
    if uid2 in users:
        users[uid2]["banned"] = not users[uid2].get("banned", False)
        await async_save(USERS_KEY, users)
        
        is_banned = users[uid2]["banned"]
        action_text = "заблоковано" if is_banned else "розблоковано"
        log_action("admin_ban_toggle", q.from_user.id, {"target_uid": uid2, "banned": is_banned})
        
        await q.answer(f"Користувача успішно {action_text}!", show_alert=True)
        await _send_user_card(update, context, uid2, users[uid2])


@admin_only
async def adm_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    uid2 = parts[1]
    
    users = await async_load(USERS_KEY, {})
    if uid2 in users:
        is_vip = not users[uid2].get("vip", False)
        users[uid2]["vip"] = is_vip
        await async_save(USERS_KEY, users)
        
        try:
            msg = "👑 <b>Вам надано VIP-статус!</b>\nДякуємо за співпрацю 🌸" if is_vip else "ℹ️ Ваш VIP-статус було деактивовано."
            await context.bot.send_message(uid2, msg, parse_mode="HTML")
        except Exception:
            pass
            
        log_action("admin_vip_toggle", q.from_user.id, {"target_uid": uid2, "vip": is_vip})
        await q.answer("VIP-статус змінено!", show_alert=True)
        await _send_user_card(update, context, uid2, users[uid2])


@admin_only
async def adm_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    uid2 = parts[1]
    
    context.user_data["state"] = AWAIT_REPLY_TO_USER
    context.user_data["reply_to_uid"] = uid2
    context.user_data.pop("reply_fb_id", None)
    
    await safe_edit(
        q,
        f"💬 <b>НОВЕ ПОВІДОМЛЕННЯ ДЛЯ <code>{uid2}</code></b>\n────────────────────────────\n"
        f"Введіть текст повідомлення, яке буде відправлено клієнту від імені бота:",
        mkb(back_btn(f"adm_user_card:{uid2}"))
    )


@admin_only
async def adm_confirm_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Підтвердження виводу реферальних коштів адміністратором."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 3: return
    uid2, amount = parts[1], int(parts[2])
    
    users = await async_load(USERS_KEY, {})
    if uid2 in users:
        users[uid2]["balance"] = max(0, users[uid2].get("balance", 0) - amount)
        await async_save(USERS_KEY, users)
        
        try:
            await context.bot.send_message(
                uid2,
                f"🎉 <b>Запит на вивід коштів оброблено!</b>\n\n"
                f"💰 Сума <b>{amount}₴</b> успішно перерахована за вашими реквізитами. Дякуємо! 🌸",
                parse_mode="HTML"
            )
        except Exception:
            pass
            
        log_action("withdraw_confirmed", q.from_user.id, {"uid": uid2, "amount": amount})
        await safe_edit(q, f"✅ Вивід <b>{amount}₴</b> для користувача <code>{uid2}</code> підтверджено.")


@admin_only
async def adm_reject_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Відхилення виводу коштів із можливістю повернення/збереження балансу."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 3: return
    uid2, amount = parts[1], int(parts[2])

    try:
        await context.bot.send_message(
            uid2,
            f"❌ <b>Запит на вивід коштів ({amount}₴) відхилено.</b>\n\n"
            f"Кошти залишено на вашому внутрішньому балансі. За деталями зверніться в підтримку.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    log_action("withdraw_rejected", q.from_user.id, {"uid": uid2, "amount": amount})
    await safe_edit(q, f"❌ Вивід {amount}₴ для користувача <code>{uid2}</code> відхилено.")


# ── Admin Tariff Handlers (Extended) ───────────────────────────────────────────

@admin_only
async def adm_tariffs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = await load_tariffs()
    
    text = (
        "💰 <b>УПРАВЛІННЯ ТАРИФАМИ ПІДПИСКИ</b>\n"
        "────────────────────────────\n"
        "Нижче наведено список усіх тарифів бота:\n\n"
    )
    
    kb_rows = []
    for k, t in tariffs.items():
        st = "✅" if t.get("active", True) else "❌"
        d = "безстроково" if not t.get("days") else f"{t['days']}дн"
        
        text += f"{st} {t.get('emoji','📦')} <b>{esc(t.get('name',''))}</b> — <b>{t.get('price')}₴</b> <i>({d})</i>\n"
        
        kb_rows.append([
            InlineKeyboardButton(f"{st} {t.get('name')}", callback_data=f"tariff_toggle:{k}"),
            InlineKeyboardButton("✏️ Ред.", callback_data=f"tariff_edit:{k}"),
            InlineKeyboardButton("🗑️ Видалити", callback_data=f"tariff_del_confirm:{k}"),
        ])
        
    kb_rows.append([InlineKeyboardButton("➕ Додати новий тариф", callback_data="tariff_add")])
    kb_rows.append(back_btn("admin_panel"))
    
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def tariff_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.callback_query.data.split(":")
    if len(parts) > 1:
        key = parts[1]
        tariffs = await load_tariffs()
        if key in tariffs:
            tariffs[key]["active"] = not tariffs[key].get("active", True)
            await save_tariffs(tariffs)
            log_action("tariff_toggle", update.effective_user.id, {"key": key, "active": tariffs[key]["active"]})
    await adm_tariffs(update, context)


@admin_only
async def tariff_del_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запит на підтвердження видалення тарифу."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    key = parts[1]

    tariffs = await load_tariffs()
    t = tariffs.get(key)
    if not t:
        await q.answer("❌ Тариф не знайдено", show_alert=True)
        return

    text = (
        f"⚠️ <b>ВИДАЛЕННЯ ТАРИФУ</b>\n────────────────────────────\n"
        f"Ви дійсно бажаєте видалити тариф <b>{t.get('emoji','📦')} {esc(t.get('name'))}</b> ({t.get('price')}₴)?"
    )
    
    kb = mkb(
        [InlineKeyboardButton("🔥 Так, видалити", callback_data=f"tariff_del_do:{key}")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="adm:tariffs")]
    )
    await safe_edit(q, text, kb)


@admin_only
async def tariff_del_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Остаточне видалення тарифу."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) > 1:
        key = parts[1]
        tariffs = await load_tariffs()
        if key in tariffs:
            del tariffs[key]
            await save_tariffs(tariffs)
            log_action("tariff_deleted", q.from_user.id, {"key": key})
            await q.answer("Тариф видалено!", show_alert=True)
    await adm_tariffs(update, context)


@admin_only
async def tariff_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    key = parts[1]
    
    context.user_data["edit_tariff_key"] = key
    tariffs = await load_tariffs()
    t = tariffs.get(key, {})
    
    days_str = "безстроковий" if not t.get('days') else f"{t.get('days')} дн."
    text = (
        f"✏️ <b>РЕДАГУВАННЯ ТАРИФУ</b>\n────────────────────────────\n"
        f"Тариф: {t.get('emoji','📦')} <b>{esc(t.get('name',''))}</b>\n"
        f"Ціна: <b>{t.get('price')}₴</b>\n"
        f"Термін: <b>{days_str}</b>\n\n"
        f"Оберіть параметр для зміни:"
    )
    
    kb = mkb(
        [InlineKeyboardButton("📝 Змінити назву", callback_data=f"tedit_name:{key}"),
         InlineKeyboardButton("💰 Змінити ціну", callback_data=f"tedit_price:{key}")],
        [InlineKeyboardButton("😊 Змінити емоджі", callback_data=f"tedit_emoji:{key}")],
        back_btn("adm:tariffs")
    )
    await safe_edit(q, text, kb)


@admin_only
async def tedit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.callback_query.data.split(":")
    if len(parts) > 1:
        context.user_data["edit_tariff_key"] = parts[1]
        context.user_data["state"] = AWAIT_TARIFF_EDIT_NAME
        await safe_edit(update.callback_query, "📝 Введіть нову назву тарифу:", mkb(back_btn("adm:tariffs")))


@admin_only
async def tedit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.callback_query.data.split(":")
    if len(parts) > 1:
        context.user_data["edit_tariff_key"] = parts[1]
        context.user_data["state"] = AWAIT_TARIFF_EDIT_PRICE
        await safe_edit(update.callback_query, "💰 Введіть нову ціну тарифу (в гривнях):", mkb(back_btn("adm:tariffs")))


@admin_only
async def tedit_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.callback_query.data.split(":")
    if len(parts) > 1:
        context.user_data["edit_tariff_key"] = parts[1]
        context.user_data["state"] = AWAIT_TARIFF_EDIT_EMOJI
        await safe_edit(update.callback_query, "😊 Надішліть новий емоджі для тарифу:", mkb(back_btn("adm:tariffs")))


@admin_only
async def tariff_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_TARIFF_NAME
    await safe_edit(
        update.callback_query,
        "➕ <b>СТВОРЕННЯ НОВОГО ТАРИФУ</b>\n────────────────────────────\n"
        "Крок 1/4: Введіть назву тарифу (наприклад: <i>Преміум 30 днів</i>):",
        mkb(back_btn("adm:tariffs"))
    )


# ── Admin Promo Handlers (Extended) ───────────────────────────────────────────

@admin_only
async def adm_promos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    promos = await load_promos()
    
    text = (
        f"🎟️ <b>УПРАВЛІННЯ ПРОМО-КОДАМИ ({len(promos)})</b>\n"
        f"────────────────────────────\n"
        f"Нижче наведено список усіх створених промо-кодів:\n\n"
    )
    
    kb_rows = []
    if not promos:
        text += "📭 <i>Активних або збережених промо-кодів не знайдено.</i>\n\n"
    else:
        for code, p in promos.items():
            st = "🟢" if p.get("active", True) else "🔴"
            max_u = p.get('max_uses', 0)
            uses_str = "∞" if not max_u else str(max_u)
            text += f"{st} <code>{esc(code)}</code> — <b>{p.get('discount',0)}%</b> <i>({p.get('uses',0)}/{uses_str} вик.)</i>\n"
            
            kb_rows.append([
                InlineKeyboardButton(f"{st} {code}", callback_data=f"promo_toggle:{code}"),
                InlineKeyboardButton("📊 Статистика", callback_data=f"promo_stat:{code}"),
                InlineKeyboardButton("🗑️ Видалити", callback_data=f"promo_del_confirm:{code}")
            ])

    kb_rows.append([InlineKeyboardButton("➕ Створити новий промо-код", callback_data="adm_create_promo")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def promo_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.callback_query.data.split(":")
    if len(parts) > 1:
        code = parts[1]
        promos = await load_promos()
        if code in promos:
            promos[code]["active"] = not promos[code].get("active", True)
            await save_promos(promos)
            log_action("promo_toggle", update.effective_user.id, {"code": code, "active": promos[code]["active"]})
    await adm_promos(update, context)


@admin_only
async def promo_stat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Детальна статистика використання конкретного промокоду."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    code = parts[1]

    promos = await load_promos()
    p = promos.get(code)
    if not p:
        await q.answer("❌ Промо-код не знайдено", show_alert=True)
        return

    used_by = p.get("used_by", [])
    used_str = ", ".join([f"<code>{u}</code>" for u in used_by[:15]]) if used_by else "ніхто не використовував"

    text = (
        f"📊 <b>СТАТИСТИКА ПРОМО-КОДУ: <code>{esc(code)}</code></b>\n"
        f"────────────────────────────\n"
        f"💰 Знижка: <b>{p.get('discount', 0)}%</b>\n"
        f"Статус: <b>{'🟢 Активний' if p.get('active') else '🔴 Деактивований'}</b>\n"
        f"👥 Використано: <b>{p.get('uses', 0)} / {'∞' if not p.get('max_uses') else p.get('max_uses')}</b>\n"
        f"📅 Створено: <code>{p.get('created_at', '—')[:16]}</code>\n\n"
        f"👤 <b>Останні користувачі:</b>\n{used_str}"
    )

    kb = mkb(
        [InlineKeyboardButton("🔄 Статус (On/Off)", callback_data=f"promo_toggle:{code}")],
        [InlineKeyboardButton("🗑️ Видалити", callback_data=f"promo_del_confirm:{code}")],
        back_btn("adm:promos")
    )
    await safe_edit(q, text, kb)


@admin_only
async def promo_del_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    code = parts[1]

    text = f"⚠️ Ви дійсно бажаєте видалити промо-код <code>{esc(code)}</code>?"
    kb = mkb(
        [InlineKeyboardButton("🔥 Так, видалити", callback_data=f"promo_del_do:{code}")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="adm:promos")]
    )
    await safe_edit(q, text, kb)


@admin_only
async def promo_del_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) > 1:
        code = parts[1]
        promos = await load_promos()
        if code in promos:
            del promos[code]
            await save_promos(promos)
            log_action("promo_deleted", q.from_user.id, {"code": code})
            await q.answer("Промо-код видалено!", show_alert=True)
    await adm_promos(update, context)


@admin_only
async def adm_create_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(
        update.callback_query,
        "🎟️ <b>СТВОРЕННЯ ПРОМО-КОДУ</b>\n────────────────────────────\n"
        "Введіть назву коду (наприклад: <code>SALE2026</code> або <code>SUMMER</code>):",
        mkb(back_btn("adm:promos"))
    )


# ── Admin Broadcast Handlers (With Ready Templates) ───────────────────────────

BROADCAST_TEMPLATES = {
    "tmpl_discount": {
        "title": "🔥 Знижка на всі тарифи (-20%)",
        "text": (
            "🔥 <b>ГАРАЧА АКЦІЯ ТІЛЬКИ СЬОГОДНІ!</b>\n\n"
            "Привіт, {first_name}! 👋\n\n"
            "Отримайте знижку <b>-20%</b> на оновлення або продовження вашого інтерактивного кабінету!\n\n"
            "🎟️ Скористайтесь промокодом: <code>SALE20</code>\n\n"
            "Поспішайте, пропозиція діє обмежений час! ⏳"
        )
    },
    "tmpl_update": {
        "title": "🚀 Велике оновлення функціоналу",
        "text": (
            "🚀 <b>ВЕЛИКЕ ОНОВЛЕННЯ СЕРВІСУ!</b>\n\n"
            "Вітаємо, {first_name}! Ми підготували для вас нові можливості:\n\n"
            "✅ Додано миттєве оновлення фото у профілі\n"
            "✅ Прискорено генерацію та деплой кабінетів у 2 рази\n"
            "✅ Покращено стабільність та захист даних\n\n"
            "Перевірте нові функції прямо зараз у розділі 👤 <b>Профіль</b>!"
        )
    },
    "tmpl_maint": {
        "title": "🛠 Попередження про тех. роботи",
        "text": (
            "🛠 <b>ТЕХНІЧНЕ ОБСЛУГОВУВАННЯ</b>\n\n"
            "Шановний(а) {first_name}!\n\n"
            "Сьогодні з <b>02:00 до 04:00</b> будуть проводитися планові технічні роботи на сервері.\n\n"
            "У цей період деплой нових сайтів може бути тимчасово недоступним. Дякуємо за розуміння! 🌸"
        )
    },
    "tmpl_referral": {
        "title": "👥 Запрошуй друзів та заробляй",
        "text": (
            "💰 <b>ЗАРОБЛЯЙТЕ РАЗОМ З НАМИ!</b>\n\n"
            "Привіт, {first_name}! Нагадуємо про нашу реферальну програму 🎉\n\n"
            "Діліться вашим реферальним посиланням із друзями та отримуйте відсоток з кожного їхнього замовлення на свій баланс!\n\n"
            "Заходьте в розділ <b>👥 Реферали</b> та забирайте своє посилання!"
        )
    }
}


@admin_only
async def adm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users = await async_load(USERS_KEY, {})
    active = sum(1 for u in users.values() if not u.get("banned"))

    text = (
        f"📢 <b>РОЗСИЛКА ПОВІДОМЛЕНЬ</b>\n"
        f"────────────────────────────\n"
        f"👥 Активних отримувачів: <b>{active}</b> із {len(users)}\n\n"
        f"Виберіть готовий шаблон розсилки або натисніть <b>'✍️ Написати свій текст'</b>:\n"
    )

    kb_rows = []
    for t_id, t_info in BROADCAST_TEMPLATES.items():
        kb_rows.append([InlineKeyboardButton(f"📄 {t_info['title']}", callback_data=f"cast_tmpl:{t_id}")])

    kb_rows.append([InlineKeyboardButton("✍️ Написати власний текст", callback_data="cast_custom")])
    kb_rows.append(back_btn("admin_panel"))

    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def cast_select_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка вибору готового шаблону розсилки."""
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    tmpl_id = parts[1]

    tmpl = BROADCAST_TEMPLATES.get(tmpl_id)
    if not tmpl:
        await q.answer("❌ Шаблон не знайдено", show_alert=True)
        return

    context.user_data["broadcast_text"] = tmpl["text"]
    context.user_data["state"] = None

    users = await async_load(USERS_KEY, {})
    active_users = sum(1 for u in users.values() if not u.get("banned"))

    # Показ попереднього перегляду із заповненими тегами
    sample_text = tmpl["text"].format(first_name=q.from_user.first_name or "Користувач", user_id=q.from_user.id)

    preview_text = (
        f"📢 <b>ПОПЕРЕДНІЙ ПЕРЕГЛЯД РОЗСИЛКИ ({tmpl['title']}):</b>\n"
        f"────────────────────────────\n"
        f"{sample_text}\n"
        f"────────────────────────────\n"
        f"👥 Отримають користувачів: <b>{active_users}</b>\n"
        f"💡 <i>Теги {{first_name}} та {{user_id}} будуть автоматично підставлені під кожного клієнта.</i>"
    )

    kb = mkb(
        [InlineKeyboardButton("✅ Підтвердити та надіслати", callback_data="broadcast_go")],
        [InlineKeyboardButton("✏️ Відредагувати текст", callback_data="cast_custom_edit")],
        [InlineKeyboardButton("❌ Скасувати", callback_data="adm_broadcast")]
    )
    await safe_edit(q, preview_text, kb)


@admin_only
async def cast_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = AWAIT_BROADCAST
    await safe_edit(
        update.callback_query,
        "✍️ <b>ВВЕДЕННЯ ТЕКСТУ РОЗСИЛКИ</b>\n────────────────────────────\n"
        "Надішліть текст для розсилки (HTML-розмітка підтримується).\n\n"
        "💡 Доступні теги для підстановки:\n"
        "• <code>{first_name}</code> — ім'я користувача\n"
        "• <code>{user_id}</code> — ID користувача",
        mkb(back_btn("adm_broadcast"))
    )


@admin_only
async def cast_custom_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редагування обраного шаблону."""
    q = update.callback_query
    curr_text = context.user_data.get("broadcast_text", "")
    context.user_data["state"] = AWAIT_BROADCAST

    await safe_edit(
        q,
        f"✏️ <b>ПОТОЧНИЙ ТЕКСТ:</b>\n<code>{esc(curr_text)}</code>\n\n"
        f"Надішліть новий відредагований варіант повідомлення:",
        mkb(back_btn("adm_broadcast"))
    )


@admin_only
async def broadcast_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    raw_text = context.user_data.pop("broadcast_text", "")

    if not raw_text:
        await q.answer("❌ Текст розсилки порожній!", show_alert=True)
        return

    users = await async_load(USERS_KEY, {})
    success = failed = blocked = 0
    total = len(users)

    await safe_edit(q, "📢 <b>Запуск розсилки...</b>\n⏳ Будь ласка, зачекайте. Це може зайняти певний час.")

    for idx, (uid2, u) in enumerate(users.items(), 1):
        if u.get("banned"):
            blocked += 1
            continue

        fname = u.get("first_name", "Клієнт")
        # Форматування під конкретного користувача
        formatted_text = raw_text.format(first_name=esc(fname), user_id=uid2)

        try:
            await context.bot.send_message(chat_id=uid2, text=formatted_text, parse_mode="HTML")
            success += 1
        except Forbidden:
            blocked += 1
        except Exception as e:
            logger.warning("Broadcast failed for %s: %s", uid2, e)
            failed += 1

        # Захист від Rate Limit Telegram (30 повідомлень на секунду max)
        if idx % 20 == 0:
            await asyncio.sleep(1.0)

    log_action("broadcast", q.from_user.id, {"success": success, "failed": failed, "blocked": blocked})

    report_text = (
        f"🎉 <b>РОЗСИЛКА УСПІШНО ЗАВЕРШЕНА!</b>\n"
        f"────────────────────────────\n"
        f"✅ Доставлено: <b>{success}</b>\n"
        f"🔇 Заблокували бота / Неактивні: <b>{blocked}</b>\n"
        f"❌ Помилки відправки: <b>{failed}</b>\n"
        f"📊 Оброблено всього: <b>{total}</b>"
    )

    await context.bot.send_message(
        chat_id=q.from_user.id,
        text=report_text,
        reply_markup=mkb(back_btn("admin_panel")),
        parse_mode="HTML"
    )


# ── Admin Feedback Handlers (Extended) ─────────────────────────────────────────

@admin_only
async def adm_feedbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    feedbacks = await async_load(FEEDBACK_KEY, {})
    sorted_fb = sorted(feedbacks.items(), key=lambda x: x[1].get("created_at", ""), reverse=True)[:15]

    text = f"💬 <b>ВІДГУКИ ТА ЗВЕРНЕННЯ ({len(feedbacks)})</b>\n────────────────────────────\n\n"
    kb_rows = []

    if not sorted_fb:
        text += "📭 <i>Нових звернень від користувачів немає.</i>"
    else:
        for fid, f in sorted_fb:
            st_icon = {"new": "🟢 Новий", "read": "🔵 Прочитано", "replied": "🟣 Відповідно"}.get(f.get("status", "new"), "⚪")
            fname = esc(f.get('first_name', 'Клієнт'))
            fb_snippet = esc(f.get('feedback', '')[:35])
            
            text += f"{st_icon} <b>#{esc(fid)}</b> від <b>{fname}</b>\n└ <i>\"{fb_snippet}...\"</i>\n\n"
            kb_rows.append([InlineKeyboardButton(f"💬 Переглянути #{fid}", callback_data=f"reply_fb:{fid}")])

    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))


@admin_only
async def reply_fb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    fid = parts[1]

    feedbacks = await async_load(FEEDBACK_KEY, {})
    fb = feedbacks.get(fid, {})
    if not fb:
        await q.answer("❌ Відгук не знайдено", show_alert=True)
        return

    # Позначаємо як прочитаний
    feedbacks[fid]["status"] = "read"
    await async_save(FEEDBACK_KEY, feedbacks)

    context.user_data["reply_to_uid"] = fb.get("user_id")
    context.user_data["reply_fb_id"] = fid
    context.user_data["state"] = AWAIT_REPLY_TO_USER

    created_at = fb.get("created_at", "—")[:16]
    text = (
        f"✍️ <b>ВІДПОВІДЬ НА ЗВЕРНЕННЯ #{esc(fid)}</b>\n"
        f"────────────────────────────\n"
        f"👤 Від кого: <b>{esc(fb.get('first_name','?'))}</b> (ID: <code>{fb.get('user_id')}</code>)\n"
        f"📅 Дата: <code>{created_at}</code>\n\n"
        f"💬 <b>Текст звернення:</b>\n"
        f"<i>\"{esc(fb.get('feedback','?'))}\"</i>\n\n"
        f"Введіть текст відповіді для користувача:"
    )

    await safe_edit(q, text, mkb(back_btn("adm_feedbacks")))


# ── Admin Settings Handlers (Extended) ─────────────────────────────────────────

@admin_only
async def adm_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = await load_settings()

    text = (
        f"⚙️ <b>СИСТЕМНІ НАЛАШТУВАННЯ БОТА</b>\n"
        f"────────────────────────────\n"
        f"🛠 Тех. обслуговування: <b>{'🔴 УВІМКНЕНО (доступ закрито)' if s.get('maintenance_mode') else '🟢 ВИМКНЕНО (бот працює)'}</b>\n"
        f"📦 Прийом замовлень: <b>{'✅ Дозволено' if s.get('new_orders_enabled', True) else '⛔️ Призупинено'}</b>\n\n"
        f"🤖 <b>AI МОДУЛІ & ІНТЕГРАЦІЇ:</b>\n"
        f"  · ШІ Перевірка чеків: <b>{'✅ Увімк.' if s.get('ai_check_receipts', True) else '❌ Вимк.'}</b>\n"
        f"  · Авто-деплой сайтів: <b>{'✅ Увімк.' if s.get('ai_auto_deploy', True) else '❌ Вимк.'}</b>\n"
        f"  · ШІ Чат-підтримка: <b>{'✅ Увімк.' if s.get('ai_support', True) else '❌ Вимк.'}</b>\n\n"
        f"💳 <b>РЕКВІЗИТИ ОПЛАТИ:</b>\n"
        f"  · Картка: <code>{esc(s.get('payment_card','Не задано'))}</code>\n"
        f"  · Отримувач: <b>{esc(s.get('payment_holder','Не задано'))}</b>"
    )

    kb = mkb(
        [InlineKeyboardButton("🛠 Тех. обслуговування", callback_data="toggle_maintenance"),
         InlineKeyboardButton("📦 Замовлення", callback_data="toggle_orders")],
        [InlineKeyboardButton("🤖 AI Чеки", callback_data="toggle_ai_receipts"),
         InlineKeyboardButton("🚀 Авто-деплой", callback_data="toggle_ai_deploy")],
        [InlineKeyboardButton("💬 AI Підтримка", callback_data="toggle_ai_support")],
        [InlineKeyboardButton("💳 Змінити реквізити", callback_data="edit_payment"),
         InlineKeyboardButton("📝 Текст привітання", callback_data="edit_welcome")],
        back_btn("admin_panel")
    )
    await safe_edit(q, text, kb)


@admin_only
async def toggle_maintenance(update, context):
    s = await load_settings()
    s["maintenance_mode"] = not s.get("maintenance_mode", False)
    await save_settings(s)
    log_action("toggle_maintenance", update.effective_user.id, {"state": s["maintenance_mode"]})
    await adm_settings(update, context)


@admin_only
async def toggle_orders(update, context):
    s = await load_settings()
    s["new_orders_enabled"] = not s.get("new_orders_enabled", True)
    await save_settings(s)
    log_action("toggle_orders", update.effective_user.id, {"state": s["new_orders_enabled"]})
    await adm_settings(update, context)


@admin_only
async def toggle_ai_receipts(update, context):
    s = await load_settings()
    s["ai_check_receipts"] = not s.get("ai_check_receipts", True)
    await save_settings(s)
    await adm_settings(update, context)


@admin_only
async def toggle_ai_deploy(update, context):
    s = await load_settings()
    s["ai_auto_deploy"] = not s.get("ai_auto_deploy", True)
    await save_settings(s)
    await adm_settings(update, context)


@admin_only
async def toggle_ai_support(update, context):
    s = await load_settings()
    s["ai_support"] = not s.get("ai_support", True)
    await save_settings(s)
    await adm_settings(update, context)


@admin_only
async def edit_payment(update, context):
    context.user_data["state"] = AWAIT_CUSTOM_PAYMENT_TEXT
    await safe_edit(
        update.callback_query,
        "💳 <b>ВВЕДЕННЯ НОВИХ РЕКВІЗИТІВ</b>\n────────────────────────────\n"
        "Надішліть дані у 3 рядки:\n"
        "<b>1-й рядок:</b> Номер картки (напр. 4149 0000 0000 0000)\n"
        "<b>2-й рядок:</b> ПІБ отримувача (напр. Шевченко Т. Г.)\n"
        "<b>3-й рядок:</b> Посилання на Monobank / банка (необов'язково)",
        mkb(back_btn("adm_settings"))
    )


@admin_only
async def edit_welcome(update, context):
    context.user_data["state"] = AWAIT_WELCOME_TEXT
    await safe_edit(
        update.callback_query,
        "📝 <b>ЗМІНА ВІТАЛЬНОГО ТЕКСТУ (/start)</b>\n────────────────────────────\n"
        "Надішліть новий вітальний текст бота (HTML підтримується):",
        mkb(back_btn("adm_settings"))
    )


# ── Admin Logs & DB Export Handlers (Extended) ────────────────────────────────

@admin_only
async def adm_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    logs = await asyncio.to_thread(_db.load_logs_db)
    if not isinstance(logs, list):
        logs = []

    text = f"📜 <b>ОСТАННІ ДІЇ В СИСТЕМІ ({len(logs)})</b>\n────────────────────────────\n\n"
    for entry in logs[-15:][::-1]:
        ts = entry.get('ts', '')[11:16]
        action = esc(entry.get('action', '?'))
        uid2 = entry.get('uid', '?')
        text += f"🕐 <code>{ts}</code> | <b>{action}</b> | User: <code>{uid2}</code>\n"

    await safe_edit(q, text, mkb(back_btn("admin_panel")))


@admin_only
async def adm_export_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await safe_edit(
        q,
        "📥 <b>ВИВАНТАЖЕННЯ ТА БЕКАП БАЗИ ДАНИХ</b>\n────────────────────────────\n"
        "Оберіть потрібний модуль для завантаження JSON або завантажте повну БД у ZIP-архіві:",
        mkb(
            [InlineKeyboardButton("👥 Користувачі (JSON)", callback_data="export:users")],
            [InlineKeyboardButton("📦 Замовлення (JSON)", callback_data="export:orders")],
            [InlineKeyboardButton("🎟 Промо-коди (JSON)", callback_data="export:promos")],
            [InlineKeyboardButton("💬 Відгуки (JSON)", callback_data="export:feedback")],
            [InlineKeyboardButton("📜 Логи дій (JSON)", callback_data="export:logs")],
            [InlineKeyboardButton("📊 Повний бекап усієї БД (ZIP)", callback_data="export:all")],
            back_btn("admin_panel")
        )
    )


@admin_only
async def adm_export_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    if len(parts) < 2: return
    export_type = parts[1]
    await q.answer("⏳ Формуємо файл для завантаження...")
    admin_id = q.from_user.id

    try:
        if export_type == "users":
            data = await async_load(USERS_KEY, {})
            filename = f"users_{now_fmt('%Y%m%d_%H%M')}.json"
            content = json.dumps(data, ensure_ascii=False, indent=2).encode()
            caption = f"👥 Користувачі: {len(data)} записів"

        elif export_type == "orders":
            data = await async_load(ORDERS_KEY, {})
            export_data = {
                oid: {k: v for k, v in o.items() if k not in ("js_content", "values_data")}
                for oid, o in data.items()
            }
            filename = f"orders_{now_fmt('%Y%m%d_%H%M')}.json"
            content = json.dumps(export_data, ensure_ascii=False, indent=2).encode()
            caption = f"📦 Замовлення: {len(data)} записів"

        elif export_type == "promos":
            data = await load_promos()
            filename = f"promos_{now_fmt('%Y%m%d_%H%M')}.json"
            content = json.dumps(data, ensure_ascii=False, indent=2).encode()
            caption = f"🎟 Промо-коди: {len(data)} записів"

        elif export_type == "feedback":
            data = await async_load(FEEDBACK_KEY, {})
            filename = f"feedback_{now_fmt('%Y%m%d_%H%M')}.json"
            content = json.dumps(data, ensure_ascii=False, indent=2).encode()
            caption = f"💬 Відгуки: {len(data)} записів"

        elif export_type == "logs":
            logs = await asyncio.to_thread(_db.load_logs_db)
            filename = f"logs_{now_fmt('%Y%m%d_%H%M')}.json"
            content = json.dumps(logs, ensure_ascii=False, indent=2).encode()
            caption = f"📜 Логи дій: {len(logs)} записів"

        elif export_type == "all":
            import zipfile
            zip_buf = io.BytesIO()

            def make_zip():
                with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    orders_data = _load_sync(ORDERS_KEY, {})
                    orders_export = {
                        oid: {k: v for k, v in o.items() if k not in ("js_content", "values_data")}
                        for oid, o in orders_data.items()
                    }
                    zf.writestr("users.json", json.dumps(_load_sync(USERS_KEY, {}), ensure_ascii=False, indent=2))
                    zf.writestr("orders.json", json.dumps(orders_export, ensure_ascii=False, indent=2))
                    zf.writestr("feedback.json", json.dumps(_load_sync(FEEDBACK_KEY, {}), ensure_ascii=False, indent=2))
                    zf.writestr("promos.json", json.dumps(_load_sync(PROMOS_KEY, {}), ensure_ascii=False, indent=2))
                    zf.writestr("settings.json", json.dumps(_load_sync(SETTINGS_KEY, {}), ensure_ascii=False, indent=2))
                    zf.writestr("tariffs.json", json.dumps(_load_sync(TARIFFS_KEY, {}), ensure_ascii=False, indent=2))
                    zf.writestr("logs.json", json.dumps(_db.load_logs_db(), ensure_ascii=False, indent=2))

            await asyncio.to_thread(make_zip)
            zip_buf.seek(0)
            filename = f"full_db_backup_{now_fmt('%Y%m%d_%H%M')}.zip"
            content = zip_buf.read()
            caption = f"📊 Повний бекап бази даних | {now_fmt()}"
        else:
            await q.answer("❌ Невідомий тип експорту", show_alert=True)
            return

        buf = io.BytesIO(content)
        buf.name = filename
        await context.bot.send_document(chat_id=admin_id, document=buf, caption=caption, parse_mode="HTML")
        log_action("db_export", admin_id, {"type": export_type})

    except Exception as e:
        logger.error("Export error: %s", e, exc_info=True)
        await context.bot.send_message(
            chat_id=admin_id,
            text=f"❌ <b>Помилка під час формування бекапу:</b>\n\n<code>{esc(str(e)[:300])}</code>",
            parse_mode="HTML"
        )

# ── Other Callbacks (Extended) ──────────────────────────────────────────────────

async def promo_enter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перехід до режиму введення промо-коду під час оформлення замовлення."""
    q = update.callback_query
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(
        q,
        "🎟️ <b>ВВЕДЕННЯ ПРОМО-КОДУ</b>\n────────────────────────────\n"
        "Введіть ваш промо-код у чат для активації знижки:",
        mkb(back_btn("catalog"))
    )


async def ref_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Розширена реферальна програма з детальною статистикою."""
    q = update.callback_query
    uid = str(q.from_user.id)
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})
    
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    ref_count = u.get("ref_count", 0)
    balance = u.get("balance", 0)
    earned_total = u.get("ref_earned_total", ref_count * REFERRAL_REWARD)

    text = (
        f"👥 <b>РЕФЕРАЛЬНА ПРОГРАМА</b>\n"
        f"────────────────────────────\n"
        f"Запрошуйте друзів та отримуйте пасивний дохід!\n\n"
        f"🎁 Ви отримуєте <b>{REFERRAL_REWARD}₴</b> за кожного активного користувача.\n"
        f"💰 Поточний баланс: <b>{balance}₴</b>\n"
        f"👥 Запрошено друзів: <b>{ref_count}</b>\n"
        f"📊 Всього зароблено: <b>{earned_total}₴</b>\n"
        f"💳 Мінімум для виводу: <b>{MIN_WITHDRAW}₴</b>\n\n"
        f"🔗 <b>Ваше реферальне посилання:</b>\n"
        f"<code>{ref_link}</code>"
    )

    kb = mkb(
        [InlineKeyboardButton("💸 Вивести кошти", callback_data="withdraw")],
        back_btn("home")
    )
    await safe_edit(q, text, kb, disable_web_page_preview=True)


async def withdraw_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка запиту на вивід реферальних коштів."""
    q = update.callback_query
    uid = str(q.from_user.id)
    users = await async_load(USERS_KEY, {})
    u = users.get(uid, {})
    bal = u.get("balance", 0)

    if bal < MIN_WITHDRAW:
        await safe_edit(
            q,
            f"❌ <b>Недостатньо коштів для виводу.</b>\n\n"
            f"Мінімальна сума: <b>{MIN_WITHDRAW}₴</b>\n"
            f"Ваш баланс: <b>{bal}₴</b>\n\n"
            f"Запросіть більше друзів за реферальним посиланням!",
            mkb(back_btn("ref_menu"))
        )
        return

    # Відправляємо сповіщення усім адмінам з кнопками обробки
    fname = esc(q.from_user.first_name or "Користувач")
    uname = f"@{q.from_user.username}" if q.from_user.username else "без username"

    admin_msg = (
        f"💰 <b>НОВИЙ ЗАПИТ НА ВИВІД КОШТІВ!</b>\n"
        f"────────────────────────────\n"
        f"👤 Користувач: <b>{fname}</b> ({uname})\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"💳 Сума виводу: <b>{bal}₴</b>\n"
        f"📅 Дата запиту: <code>{now_fmt()}</code>"
    )

    kb_admin = mkb(
        [InlineKeyboardButton("✅ Підтвердити вивід", callback_data=f"confirm_withdraw:{uid}:{bal}"),
         InlineKeyboardButton("❌ Відхилити", callback_data=f"reject_withdraw:{uid}:{bal}")]
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id, admin_msg, reply_markup=kb_admin, parse_mode="HTML")
        except Exception as e:
            logger.error("Error sending withdraw alert to admin %s: %s", admin_id, e)

    log_action("withdraw_request", uid, {"amount": bal})

    await safe_edit(
        q,
        f"✅ <b>Запит на вивід {bal}₴ успішно створено!</b>\n\n"
        f"Адміністратор обробить ваш виплатний запит протягом 24 годин. Кошти будуть перераховані за картковими реквізитами, вказаними в підтримці. 🌸",
        mkb(back_btn("ref_menu"))
    )


async def feedback_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню зворотного зв'язку та чату підтримки."""
    context.user_data["state"] = AWAIT_FEEDBACK
    await safe_edit(
        update.callback_query,
        "💬 <b>СЛУЖБА ПІДТРИМКИ & ЗВОРОТНИЙ ЗВ'ЯЗОК</b>\n"
        "────────────────────────────\n"
        "Введіть ваше запитання, пропозицію або повідомлення про помилку у чат:\n\n"
        "<i>Оператор відповість вам у найкоротший термін!</i>",
        mkb(back_btn("home"))
    )


async def about_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Інформація про сервіс та інструкція."""
    s = await load_settings()
    card = esc(s.get('payment_card', 'За запитом'))
    holder = esc(s.get('payment_holder', 'FunsDiia Service'))

    text = (
        f"🪪 <b>FunsDiia Bot</b> — Автоматизований сервіс генерації та деплою персональних веб-кабінетів документів.\n\n"
        f"<b>ЯК ЦЕ ПРАЦЮЄ:</b>\n"
        f"1️⃣ Заповнюйте свої дані один раз у розділі 👤 <b>Профіль</b>\n"
        f"2️⃣ Обирайте бажаний термін дії у 🛒 <b>Каталозі</b>\n"
        f"3️⃣ Оплачуйте замовлення та отримуйте ваше індивідуальне посилання\n"
        f"4️⃣ Оновлюйте дані сайту в 1-клік прямо з Профілю в будь-який момент!\n\n"
        f"💳 <b>Офіційні реквізити оплати:</b>\n"
        f"<code>{card}</code> ({holder})\n\n"
        f"⚡️ <i>Захищене з'єднання та 100% збереження ваших даних.</i>"
    )

    await safe_edit(update.callback_query, text, mkb(back_btn("home")))


# ── Global Router (Extended & Hardened) ─────────────────────────────────────────

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Центральний маршрутизатор усіх CallbackQuery оновлень бота."""
    q = update.callback_query
    d = q.data
    uid = str(q.from_user.id)

    try:
        await q.answer()
    except Exception:
        pass

    # Перевірка на заблокованого користувача
    users = await async_load(USERS_KEY, {})
    if users.get(uid, {}).get("banned") and not is_admin(uid):
        try:
            await q.message.reply_text("🚫 Ваш акаунт заблоковано адміністратором за порушення правил.")
        except Exception:
            pass
        return

    try:
        # Точні стататичні маршрути
        routes = {
            "home":                 cmd_start,
            "catalog":              show_catalog,
            "profile":              show_profile,
            "profile_view_photo":   profile_view_photo,
            "profile_edit_menu":    profile_edit_menu,
            "profile_redeploy":     profile_redeploy,
            "buy_use_profile":      buy_use_profile,
            "buy_start_manual":     buy_start_manual,
            "skip_address":         skip_address_callback,
            "my_orders":            my_orders_handler,
            "ref_menu":             ref_menu,
            "withdraw":             withdraw_handler,
            "feedback":             feedback_menu,
            "about":                about_handler,
            "promo_enter":          promo_enter,
            "admin_panel":          admin_panel,
            "adm:stats":            adm_stats,
            "adm:orders":           adm_orders,
            "adm:users":            adm_users,
            "adm:search":           adm_search,
            "adm:balance":          adm_balance,
            "adm:tariffs":          adm_tariffs,
            "adm:promos":           adm_promos,
            "adm:broadcast":        adm_broadcast,
            "adm:feedbacks":        adm_feedbacks,
            "adm:settings":         adm_settings,
            "adm:logs":             adm_logs,
            "adm:export_db":        adm_export_db,
            "adm:chain_deploy":     adm_chain_deploy_menu,
            "chain_deploy_run":     adm_chain_deploy_run,
            "cast_custom":          cast_custom,
            "cast_custom_edit":     cast_custom_edit,
            "broadcast_go":         broadcast_go,
            "tariff_add":           tariff_add,
            "adm_create_promo":     adm_create_promo,
            "toggle_maintenance":   toggle_maintenance,
            "toggle_orders":        toggle_orders,
            "toggle_ai_receipts":   toggle_ai_receipts,
            "toggle_ai_deploy":     toggle_ai_deploy,
            "toggle_ai_support":    toggle_ai_support,
            "edit_payment":         edit_payment,
            "edit_welcome":         edit_welcome,
        }

        if d in routes:
            return await routes[d](update, context)

        # Динамічні префіксні маршрути
        if d.startswith("p_toggle:"):          return await profile_toggle_field(update, context)
        if d.startswith("p_edit:"):            return await profile_input_prompt(update, context)
        if d.startswith("user_ord_view:"):    return await user_ord_view(update, context)

        if d.startswith("tar:"):               return await select_tariff(update, context)
        if d.startswith("sex:"):               return await select_sex(update, context)
        if d.startswith("rights:"):            return await select_rights(update, context)
        if d.startswith("zagran:"):            return await select_zagran(update, context)
        if d.startswith("diploma:"):           return await select_diploma(update, context)

        # Адмінські динамічні маршрути
        if d.startswith("adm:users:"):         return await adm_users(update, context)
        if d.startswith("adm_user_card:"):     
            parts = d.split(":")
            if len(parts) > 1 and parts[1] in users:
                return await _send_user_card(update, context, parts[1], users[parts[1]])
        if d.startswith("adm_direct_balance:"):return await adm_direct_balance(update, context)
        if d.startswith("adm_approve_deploy:"): return await adm_approve_deploy(update, context)
        if d.startswith("adm_approve:"):       return await adm_approve(update, context)
        if d.startswith("adm_reject:"):        return await adm_reject(update, context)
        if d.startswith("adm_complete:"):      return await adm_complete(update, context)
        if d.startswith("adm_push_pages:"):    return await adm_push_pages(update, context)
        if d.startswith("adm_push_go:"):       return await adm_push_go(update, context)
        if d.startswith("adm_send_link:"):     return await adm_send_link(update, context)
        if d.startswith("confirm_withdraw:"):  return await adm_confirm_withdraw(update, context)
        if d.startswith("reject_withdraw:"):   return await adm_reject_withdraw(update, context)
        if d.startswith("adm_order_view:"):    return await adm_order_view(update, context)
        if d.startswith("adm_order_filter:"):  return await adm_order_filter(update, context)

        # Управління тарифами
        if d.startswith("tariff_toggle:"):     return await tariff_toggle(update, context)
        if d.startswith("tariff_edit:"):       return await tariff_edit(update, context)
        if d.startswith("tariff_del_confirm:"):return await tariff_del_confirm(update, context)
        if d.startswith("tariff_del_do:"):     return await tariff_del_do(update, context)
        if d.startswith("tedit_name:"):        return await tedit_name(update, context)
        if d.startswith("tedit_price:"):       return await tedit_price(update, context)
        if d.startswith("tedit_emoji:"):       return await tedit_emoji(update, context)

        # Промокоди та фідбек
        if d.startswith("promo_toggle:"):      return await promo_toggle(update, context)
        if d.startswith("promo_stat:"):        return await promo_stat(update, context)
        if d.startswith("promo_del_confirm:"): return await promo_del_confirm(update, context)
        if d.startswith("promo_del_do:"):      return await promo_del_do(update, context)
        if d.startswith("cast_tmpl:"):         return await cast_select_template(update, context)
        if d.startswith("reply_fb:"):          return await reply_fb(update, context)
        if d.startswith("adm_ban:"):           return await adm_ban(update, context)
        if d.startswith("adm_vip:"):           return await adm_vip(update, context)
        if d.startswith("adm_msg:"):           return await adm_msg(update, context)
        if d.startswith("export:"):            return await adm_export_do(update, context)

        logger.warning("Unhandled callback data received: %s", d)

    except Exception as e:
        logger.error("button_handler routing error [%s]: %s", d, e, exc_info=True)
        try:
            await q.message.reply_text("😔 Сталася помилка при обробці команди. Спробуйте виконати /start.")
        except Exception:
            pass


# ── Additional Admin Commands (Extended) ───────────────────────────────────────

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /admin для швидкого виклику адмін-панелі."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ У вас немає прав доступу до адміністративної панелі.")
        return

    users = await async_load(USERS_KEY, {})
    orders = await async_load(ORDERS_KEY, {})
    pending = sum(1 for o in orders.values() if o.get("status") == "pending")

    await update.message.reply_text(
        f"👑 <b>АДМІНІСТРАТИВНА ПАНЕЛЬ</b>\n"
        f"────────────────────────────\n"
        f"👥 Всього користувачів: <b>{len(users)}</b>\n"
        f"⏳ В очікуванні перевірки: <b>{pending}</b>\n"
        f"🕐 Системний час: <code>{now_fmt()}</code>",
        reply_markup=mkb(
            [InlineKeyboardButton("📊 Статистика", callback_data="adm:stats"),
             InlineKeyboardButton("📋 Замовлення", callback_data="adm:orders")],
            [InlineKeyboardButton("👥 Користувачі", callback_data="adm:users"),
             InlineKeyboardButton("💰 Тарифи", callback_data="adm:tariffs")],
            [InlineKeyboardButton("🎟 Промо-коди", callback_data="adm:promos"),
             InlineKeyboardButton("📢 Розсилка", callback_data="adm:broadcast")],
            [InlineKeyboardButton("⚙️ Налаштування", callback_data="adm:settings"),
             InlineKeyboardButton("🚀 Деплой", callback_data="adm:chain_deploy")],
            [InlineKeyboardButton("📥 Вивантажити БД", callback_data="adm:export_db")]
        ),
        parse_mode="HTML"
    )


async def cmd_deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Прямий виклик повного ланцюжка деплою через /deploy."""
    if not is_admin(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ <b>Запускаємо деплой ланцюжком...</b>", parse_mode="HTML")
    try:
        result = await asyncio.to_thread(chain_deploy.run_full_chain)
        log_action("chain_deploy_cmd", update.effective_user.id, result)
        
        await msg.edit_text(
            f"🚀 <b>Деплой успішно завершено!</b>\n\n"
            f"📁 Папка 2 (Вихідний код):\n🔗 {esc(result['folder2_url'])}\n\n"
            f"📁 Папка 1 (Кабінет з QR):\n🔗 <b>{esc(result['folder1_url'])}</b>\n\n"
            f"⏱ <i>Зміни з'являться на сайті протягом 1–2 хвилин.</i>",
            parse_mode="HTML"
        )
    except chain_deploy.DeployError as e:
        await msg.edit_text(f"❌ <b>Помилка під час деплою:</b>\n\n<code>{esc(str(e))}</code>", parse_mode="HTML")
    except Exception as e:
        logger.error("cmd_deploy error: %s", e, exc_info=True)
        await msg.edit_text(f"❌ <b>Непередбачена помилка деплою:</b>\n\n<code>{esc(str(e)[:400])}</code>", parse_mode="HTML")


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Швидкий виклик вивантаження БД через /export."""
    if not is_admin(update.effective_user.id):
        return
        
    await update.message.reply_text(
        "📥 <b>Оберіть модуль для експорту:</b>",
        reply_markup=mkb(
            [InlineKeyboardButton("👥 Користувачі", callback_data="export:users")],
            [InlineKeyboardButton("📦 Замовлення", callback_data="export:orders")],
            [InlineKeyboardButton("📊 Вся БД (ZIP)", callback_data="export:all")]
        ),
        parse_mode="HTML"
    )


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /ban <user_id>."""
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Використання: <code>/ban &lt;user_id&gt;</code>", parse_mode="HTML")
        return
        
    uid2 = context.args[0].strip()
    users = await async_load(USERS_KEY, {})
    if uid2 not in users:
        await update.message.reply_text("❌ Користувача з таким ID не знайдено.")
        return
        
    users[uid2]["banned"] = True
    await async_save(USERS_KEY, users)
    log_action("cmd_ban", update.effective_user.id, {"target_uid": uid2})
    await update.message.reply_text(f"🚫 Користувача <code>{uid2}</code> успішно заблоковано.", parse_mode="HTML")


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /unban <user_id>."""
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Використання: <code>/unban &lt;user_id&gt;</code>", parse_mode="HTML")
        return
        
    uid2 = context.args[0].strip()
    users = await async_load(USERS_KEY, {})
    if uid2 not in users:
        await update.message.reply_text("❌ Користувача з таким ID не знайдено.")
        return
        
    users[uid2]["banned"] = False
    await async_save(USERS_KEY, users)
    log_action("cmd_unban", update.effective_user.id, {"target_uid": uid2})
    await update.message.reply_text(f"✅ Користувача <code>{uid2}</code> успішно розблоковано.", parse_mode="HTML")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /balance <user_id> <amount>."""
    if not is_admin(update.effective_user.id):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Використання: <code>/balance &lt;user_id&gt; &lt;amount&gt;</code>", parse_mode="HTML")
        return
        
    uid2 = context.args[0].strip()
    try:
        amount = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Сума має бути цілим числом.")
        return

    users = await async_load(USERS_KEY, {})
    if uid2 not in users:
        await update.message.reply_text("❌ Користувача з таким ID не знайдено.")
        return

    new_bal = max(0, users[uid2].get("balance", 0) + amount)
    users[uid2]["balance"] = new_bal
    await async_save(USERS_KEY, users)
    
    log_action("cmd_balance", update.effective_user.id, {"target_uid": uid2, "amount": amount, "new_balance": new_bal})
    await update.message.reply_text(f"✅ Баланс <code>{uid2}</code> змінено! Новий баланс: <b>{new_bal}₴</b>", parse_mode="HTML")


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальний обробник помилок бота з інформуванням адміністраторів."""
    logger.error("Global Error Caught: %s", context.error, exc_info=True)
    
    error_msg = (
        f"❌ <b>КРИТИЧНА ПОМИЛКА БОТА!</b>\n"
        f"────────────────────────────\n"
        f"<code>{esc(str(context.error)[:350])}</code>"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id, error_msg, parse_mode="HTML")
        except Exception:
            pass


# ── Main Entrypoint (Extended) ─────────────────────────────────────────────────

def main():
    """Точка запуску Telegram-бота FunsDiia."""
    # Створення директорії під фото замовлень
    os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)

    # Ініціалізація структури бази даних
    _db.init_db()
    logger.info("✅ Базу даних ініціалізовано успішно.")

    # Маппінг завантаження та збереження модулів
    _DB.update({
        USERS_KEY:    {"load": _db.load_users,       "save": _db.save_users},
        ORDERS_KEY:   {"load": _db.load_orders,      "save": _db.save_orders},
        FEEDBACK_KEY: {"load": _db.load_feedback,    "save": _db.save_feedback},
        TARIFFS_KEY:  {"load": _db.load_tariffs_db,  "save": _db.save_tariffs_db},
        PROMOS_KEY:   {"load": _db.load_promos_db,   "save": _db.save_promos_db},
        SETTINGS_KEY: {"load": _db.load_settings_db, "save": _db.save_settings_db},
    })

    # Перевірка наявності необхідних токенів
    if not PAGES_GH_TOKEN:
        logger.warning("⚠️ PAGES_GH_TOKEN не встановлено у змінних оточення!")
    if not os.getenv("GH_TOKEN_2"):
        logger.warning("⚠️ GH_TOKEN_2 не встановлено у змінних оточення!")
    if not GROUP_CHAT_ID:
        logger.warning("⚠️ GROUP_CHAT_ID не встановлено у змінних оточення!")

    # Побудова додатку python-telegram-bot
    app = Application.builder().token(TOKEN).build()

    # Фонова задача перевірки підписок щогодини
    if app.job_queue:
        app.job_queue.run_repeating(
            subscription_check_job,
            interval=3600,
            first=60,
            name="subscription_check",
        )

    # Реєстрація команд
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("help",    cmd_start))
    app.add_handler(CommandHandler("admin",   cmd_admin))
    app.add_handler(CommandHandler("deploy",  cmd_deploy))
    app.add_handler(CommandHandler("export",  cmd_export))
    app.add_handler(CommandHandler("ban",     cmd_ban))
    app.add_handler(CommandHandler("unban",   cmd_unban))
    app.add_handler(CommandHandler("balance", cmd_balance))

    # Реєстрація CallbackQuery та повідомлень
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VIDEO, handle_media))
    
    # Реєстрація глобальної помилки
    app.add_error_handler(error_handler)

    logger.info("🌸 FunsDiia Bot успішно запущено! Адміни: %s | Група: %s", ADMIN_IDS, GROUP_CHAT_ID)
    
    # Запуск бота у режимі long polling
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
