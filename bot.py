"""
FunsDiia Bot — AI-автоматизація (DeepSeek V3)
──────────────────────────────────────────────
• GH токен ТІЛЬКИ з env (PAGES_GH_TOKEN) — не з коду
• Підтвердження деплою + чеки → ГРУПА (GROUP_CHAT_ID)
• Адмін-панель → особистий чат з ботом
• DeepSeek V3 аналізує чеки автоматично
• DeepSeek V3 відповідає на питання користувачів (підтримка)
• Авто-деплой після підтвердженого чека
• Адмін тільки підтверджує сумнівні чеки (1 кнопка)
• Безпека: rate-limit, HTML-escape, безпечне збереження
"""

import os, json, logging, io, random, re, pytz, time, hashlib, asyncio, base64
import html as _html_escape
from datetime import datetime, timedelta
from typing import Optional

import requests as _requests_lib
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters,
)
from telegram.error import Forbidden, BadRequest

# ─────────────────────────────────────────
#  КОНФІГ
# ─────────────────────────────────────────
load_dotenv()

# Допоміжна функція для безпечного отримання чисел
def _get_env_int(key: str, default: int) -> int:
    val = os.getenv(key, "").strip()
    return int(val) if val.isdigit() else default

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("❌ Токен бота не знайдено! Встановіть TELEGRAM_BOT_TOKEN у secrets.")

def _parse_int_list(s: str) -> list[int]:
    # Використовуємо .strip() для коректної обробки
    return [int(x.strip()) for x in s.split(",") if x.strip().lstrip("-").isdigit()]

ADMIN_IDS: list[int]   = _parse_int_list(os.getenv("ADMIN_IDS", os.getenv("ADMIN_CHAT_ID", "")))
if not ADMIN_IDS:
    raise ValueError("❌ ADMIN_IDS не задано!")

# ID групи
_raw_group = os.getenv("GROUP_CHAT_ID", "").strip()
GROUP_CHAT_ID: Optional[int] = int(_raw_group) if _raw_group.lstrip("-").isdigit() else None

# GitHub та DeepSeek конфіг
PAGES_GH_TOKEN: str = os.getenv("PAGES_GH_TOKEN", "")
DEEPSEEK_API_KEY: str = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL   = "deepseek-chat"
DEEPSEEK_URL     = "https://api.deepseek.com/chat/completions"
AI_ENABLED       = bool(DEEPSEEK_API_KEY)

TIMEZONE        = pytz.timezone("Europe/Kyiv")
BOT_USERNAME    = os.getenv("BOT_USERNAME", "FunsDiia_bot")

# Використовуємо безпечне отримання чисел
REFERRAL_REWARD = _get_env_int("REFERRAL_REWARD", 19)
MIN_WITHDRAW    = _get_env_int("MIN_WITHDRAW", 50)

# Файли БД
USERS_FILE         = "users_data.json"
ORDERS_FILE        = "orders_data.json"
FEEDBACK_FILE      = "feedback_data.json"
TARIFFS_FILE       = "tariffs_data.json"
PROMO_FILE         = "promo_data.json"
LOGS_FILE          = "action_logs.json"
SETTINGS_FILE      = "bot_settings.json"
ORDER_PHOTOS_DIR   = "order_photos"
SITE_TEMPLATE_DIR  = "1"          # папка з шаблоном (values.js, index.html, etc.)

# ── Стани FSM ──
(
    AWAIT_FIO, AWAIT_DOB, AWAIT_SEX, AWAIT_PHOTO, AWAIT_FEEDBACK,
    AWAIT_TARIFF_NAME, AWAIT_TARIFF_PRICE, AWAIT_TARIFF_DAYS, AWAIT_TARIFF_EMOJI,
    AWAIT_BROADCAST, AWAIT_PROMO_CODE, AWAIT_PROMO_DISCOUNT, AWAIT_PROMO_USES,
    AWAIT_USER_SEARCH, AWAIT_BALANCE_UID, AWAIT_BALANCE_AMOUNT,
    AWAIT_ORDER_COMPLETE_FILE, AWAIT_REPLY_TO_USER, AWAIT_CUSTOM_PAYMENT_TEXT,
    AWAIT_REJECT_REASON, AWAIT_TARIFF_EDIT_PRICE, AWAIT_TARIFF_EDIT_NAME,
    AWAIT_TARIFF_EDIT_EMOJI,
    AWAIT_ADDRESS, AWAIT_RIGHTS_CHOICE, AWAIT_ZAGRAN_CHOICE,
    AWAIT_DIPLOMA_CHOICE, AWAIT_STUDY_CHOICE,
    AWAIT_WELCOME_TEXT,
) = range(29)

DEFAULT_TARIFFS = {
    "1_day":   {"name": "1 день",   "price": 20,  "days": 1,    "emoji": "🌙", "active": True},
    "30_days": {"name": "30 днів",  "price": 70,  "days": 30,   "emoji": "📅", "active": True},
    "90_days": {"name": "90 днів",  "price": 150, "days": 90,   "emoji": "🌿", "active": True},
    "180_days":{"name": "180 днів", "price": 190, "days": 180,  "emoji": "🌟", "active": True},
    "forever": {"name": "Назавжди", "price": 250, "days": None, "emoji": "💎", "active": True},
}

DEFAULT_SETTINGS = {
    "bot_enabled":       True,
    "payment_card":      "5355 5732 5047 6310",
    "payment_holder":    "SenseBank",
    "payment_link":      "https://send.monobank.ua/jar/6R3gd9Ew8w",
    "welcome_text":      "",
    "maintenance_mode":  False,
    "new_orders_enabled":True,
    "ai_check_receipts": True,   # DeepSeek аналізує чеки
    "ai_auto_deploy":    True,   # авто-деплой після підтвердженого чека
    "ai_support":        True,   # відповідає на питання користувачів
}

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  DEEPSEEK AI — МОДУЛЬ
# ─────────────────────────────────────────
_AI_SYSTEM_SUPPORT = """Ти — чат-підтримка сервісу FunsDiia. 
Сервіс генерує персональні кабінети у стилі Дія (демонстраційні, навчальні цілі).
Відповідай коротко, дружньо, українською мовою. 
Якщо питання про оплату — скажи що реквізити надає адміністратор після підтвердження замовлення.
Якщо питання поза темою — ввічливо поверни до теми сервісу.
НЕ обіцяй того чого не можеш зробити. НЕ давай особисті поради."""

_AI_SYSTEM_RECEIPT = """Ти — верифікатор платіжних чеків для сервісу FunsDiia.
Проаналізуй зображення чека/скріншота оплати.
Дай відповідь ТІЛЬКИ у форматі JSON без зайвого тексту:
{"ok": true/false, "confidence": 0-100, "amount": число_або_null, "reason": "короткий опис"}

ok=true якщо це схоже на реальний банківський переказ/чек.
ok=false якщо: не схоже на чек, порожнє фото, скріншот чогось іншого.
confidence — впевненість у відсотках.
amount — сума переказу якщо видно, або null.
reason — 1-2 речення пояснення."""


def _deepseek_request(messages: list, system: str = "", max_tokens: int = 500) -> str:
    """Синхронний запит до DeepSeek API."""
    if not DEEPSEEK_API_KEY:
        return ""
    payload = {
        "model": DEEPSEEK_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system}] + messages if system else messages,
        "temperature": 0.3,
    }
    try:
        resp = _requests_lib.post(
            DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error("DeepSeek API error: %s", e)
        return ""


async def ai_check_receipt(photo_bytes: bytes, expected_amount: int) -> dict:
    """
    Перевіряє фото чека через DeepSeek Vision.
    Повертає dict: {ok, confidence, amount, reason, auto_approved}
    """
    if not AI_ENABLED or not photo_bytes:
        return {"ok": None, "confidence": 0, "amount": None, "reason": "AI вимкнено", "auto_approved": False}

    b64 = base64.b64encode(photo_bytes).decode()
    messages = [{
        "role": "user",
        "content": [
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
            },
            {
                "type": "text",
                "text": f"Очікувана сума оплати: {expected_amount}₴. Проаналізуй цей чек.",
            },
        ],
    }]

    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: _deepseek_request(messages, _AI_SYSTEM_RECEIPT, 300)
    )

    result = {"ok": None, "confidence": 0, "amount": None, "reason": raw or "Немає відповіді", "auto_approved": False}
    try:
        # Парсимо JSON з відповіді
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            result.update(parsed)
            # Авто-підтвердження якщо впевненість >= 80%
            result["auto_approved"] = bool(parsed.get("ok")) and int(parsed.get("confidence", 0)) >= 80
    except Exception as e:
        logger.warning("Receipt JSON parse error: %s | raw=%s", e, raw[:200])

    return result


async def ai_support_reply(user_message: str, user_history: list = None) -> str:
    """Відповідь підтримки через DeepSeek."""
    if not AI_ENABLED:
        return ""
    messages = (user_history or []) + [{"role": "user", "content": user_message}]
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _deepseek_request(messages, _AI_SYSTEM_SUPPORT, 400)
    )


async def ai_generate_fio_en(fio_ua: str) -> str:
    """Транслітерація ПІБ українською → латиницею через DeepSeek."""
    if not AI_ENABLED or not fio_ua:
        return fio_ua
    messages = [{"role": "user", "content":
        f"Транслітеруй українське ПІБ латиницею (стандарт КМУ 2010): '{fio_ua}'. "
        f"Відповідь ТІЛЬКИ транслітерація, нічого більше."}]
    result = await asyncio.get_event_loop().run_in_executor(
        None, lambda: _deepseek_request(messages, "", 50)
    )
    return result.strip('"\'').strip() or fio_ua

# ─────────────────────────────────────────
#  DB УТИЛІТИ
# ─────────────────────────────────────────
def safe_load(filename: str, default=None):
    if default is None:
        default = {}
    if not os.path.exists(filename):
        return default
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Load error %s: %s", filename, e)
        return default

def safe_save(filename: str, data) -> bool:
    try:
        tmp = filename + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, filename)
        return True
    except Exception as e:
        logger.error("Save error %s: %s", filename, e)
        return False

def now_str() -> str:
    return datetime.now(TIMEZONE).isoformat()

def now_fmt(fmt="%d.%m.%Y %H:%M") -> str:
    return datetime.now(TIMEZONE).strftime(fmt)

def gen_id(prefix="") -> str:
    return prefix + hashlib.sha256(f"{time.time()}{random.random()}".encode()).hexdigest()[:8]

def is_admin(uid) -> bool:
    return int(uid) in ADMIN_IDS

def esc(text) -> str:
    """HTML-escape для безпечного вставлення в parse_mode=HTML."""
    return _html_escape.escape(str(text))

# ─────────────────────────────────────────
#  ЛОГУВАННЯ ДІЙ
# ─────────────────────────────────────────
def log_action(action: str, uid=None, details: dict = None):
    logs = safe_load(LOGS_FILE, [])
    if not isinstance(logs, list):
        logs = []
    logs.insert(0, {
        "ts": now_str(), "action": action,
        "uid": str(uid) if uid else None,
        "details": details or {},
    })
    safe_save(LOGS_FILE, logs[:500])

# ─────────────────────────────────────────
#  ТАРИФИ / НАЛАШТУВАННЯ
# ─────────────────────────────────────────
def load_tariffs() -> dict:
    raw = safe_load(TARIFFS_FILE, DEFAULT_TARIFFS)
    for v in raw.values():
        if "text" in v and "name" not in v:
            v["name"] = v.pop("text")
        v.setdefault("emoji", "📦")
    return raw

def save_tariffs(t): safe_save(TARIFFS_FILE, t)
def active_tariffs() -> dict: return {k: v for k, v in load_tariffs().items() if v.get("active", True)}
def fmt_tariff(key, t) -> str: return f"{t.get('emoji','📦')} {t.get('name', key)} — {t.get('price', 0)}₴"

def load_settings() -> dict:
    return {**DEFAULT_SETTINGS, **safe_load(SETTINGS_FILE, {})}

def save_settings(s): safe_save(SETTINGS_FILE, s)
def get_setting(key): return load_settings().get(key, DEFAULT_SETTINGS.get(key))

# ─────────────────────────────────────────
#  ГЕНЕРАЦІЯ ДОКУМЕНТІВ
# ─────────────────────────────────────────
def gen_rnokpp():   return "".join(str(random.randint(0, 9)) for _ in range(10))
def gen_passport(): return "".join(str(random.randint(0, 9)) for _ in range(9))
def gen_uznr():     return f"{random.randint(1990,2010)}0128-{random.randint(10000,99999)}"
def gen_prava():    return f"AUX{random.randint(100000,999999)}"
def gen_zagran():   return f"FX{random.randint(100000,999999)}"

def gen_address() -> str:
    districts = ["Харківський", "Чугуївський", "Ізюмський", "Лозівський", "Богодухівський"]
    cities    = ["м. Харків", "м. Чугуїв", "м. Мерефа", "м. Люботин", "смт Пісочин"]
    streets   = ["Гарібальді", "Сумська", "Пушкінська", "Полтавський Шлях", "пр. Науки", "Клочківська"]
    return (
        f"Харківська область, {random.choice(districts)} район "
        f"{random.choice(cities)}, вул. {random.choice(streets)}, "
        f"буд. {random.randint(1,150)}, кв. {random.randint(1,250)}"
    )

def gen_values_dict(data: dict) -> dict:
    """
    Генерує повний словник значень для values.js.
    Зберігається в order['values_data'] і пушиться у репо.
    """
    rnokpp     = gen_rnokpp()
    pass_num   = gen_passport()
    uznr       = gen_uznr()
    prava_num  = gen_prava()
    zagran_num = gen_zagran()
    bank_addr  = data.get("address") or gen_address()

    sex = data.get("sex", "M")
    sex_ua, sex_en = ("Ч", "M") if sex == "M" else ("Ж", "W")
    date_now = now_fmt("%d.%m.%Y")
    date_out = (datetime.now(TIMEZONE) + timedelta(days=3650)).strftime("%d.%m.%Y")

    universities = ["ХНУ імені Каразіна", "НТУ ХПІ", "ХНЕУ імені С. Кузнеця", "ХНМУ", "ХНУРЕ"]
    faculties    = ["Фізико-технічний", "Комп'ютерних наук", "Економічний", "Медичний", "Радіоелектроніки"]
    univ    = random.choice(universities)
    fak     = random.choice(faculties)
    diploma = f"MT-{random.randint(100000, 999999)}"
    student_num = f"{random.randint(2020,2024)}{random.randint(100000,999999)}"

    date_give_z = (datetime.now(TIMEZONE) - timedelta(days=random.randint(1000, 2000))).strftime("%d.%m.%Y")
    date_out_z  = (datetime.now(TIMEZONE) + timedelta(days=random.randint(3000, 4000))).strftime("%d.%m.%Y")

    fio_ua = data.get("fio", "")
    fio_en = data.get("fio_en") or fio_ua

    return {
        "fio":              fio_ua,
        "fio_en":           fio_en,
        "birth":            data.get("dob", ""),
        "date_give":        date_now,
        "date_out":         date_out,
        "organ":            "0512",
        "rnokpp":           rnokpp,
        "uznr":             uznr,
        "pass_number":      pass_num,
        "registeredOn":     date_now,
        "legalAdress":      "Харківська область",
        "live":             "Харківська область",
        "bank_adress":      bank_addr,
        "sex":              sex_ua,
        "sex_en":           sex_en,
        "rights_categories":"A, B",
        "prava_number":     prava_num,
        "prava_date_give":  date_now,
        "prava_date_out":   date_out,
        "pravaOrgan":       "0512",
        "university":       univ,
        "fakultet":         fak,
        "stepen_dip":       "Магістра",
        "univer_dip":       univ,
        "dayout_dip":       date_out,
        "special_dip":      "Прикладна математика",
        "number_dip":       diploma,
        "form":             "Очна",
        "zagran_number":    zagran_num,
        "dateGiveZ":        date_give_z,
        "dateOutZ":         date_out_z,
        "student_number":   student_num,
        "student_date_give":date_now,
        "student_date_out": date_out,
        "isRightsEnabled":  data.get("is_rights",  True),
        "isZagranEnabled":  data.get("is_zagran",  True),
        "isDiplomaEnabled": data.get("is_diploma", False),
        "isStudyEnabled":   data.get("is_study",   False),
        "isRojdenie":       False,
        "photo_passport":   "1.png",
        "photo_rights":     "1.png",
        "photo_students":   "1.png",
        "photo_zagran":     "1.png",
        "signPng":          "sign.png",
        "order_id":         data.get("order_id", ""),
        "generated_at":     now_str(),
    }

def values_dict_to_js(d: dict) -> str:
    """Перетворює словник значень у values.js для index.html."""
    lines = [f"// Автоматично згенеровано: {d.get('generated_at','')}", ""]
    for key, val in d.items():
        if isinstance(val, bool):
            lines.append(f"var {key} = {'true' if val else 'false'};")
        elif isinstance(val, (int, float)):
            lines.append(f"var {key} = {val};")
        else:
            escaped = str(val).replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'var {key} = "{escaped}";')
    return "\n".join(lines) + "\n"

# ─────────────────────────────────────────
#  ПРОМО-КОДИ
# ─────────────────────────────────────────
def load_promos() -> dict: return safe_load(PROMO_FILE, {})
def save_promos(p): safe_save(PROMO_FILE, p)

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
    return {"ok": True, "discount": p.get("discount", 0), "msg": f"✅ Код активовано! Знижка {p.get('discount',0)}%"}

def apply_promo(code: str, uid: str):
    promos = load_promos()
    code = code.upper().strip()
    if code not in promos:
        return
    promos[code].setdefault("used_by", []).append(uid)
    promos[code]["uses"] = promos[code].get("uses", 0) + 1
    save_promos(promos)

# ─────────────────────────────────────────
#  UI ХЕЛПЕРИ
# ─────────────────────────────────────────
def back_btn(cb): return [InlineKeyboardButton("🔙 Назад", callback_data=cb)]
def mkb(*rows): return InlineKeyboardMarkup(list(rows))

async def safe_edit(query, text: str, kb=None, **kw):
    kw.setdefault("parse_mode", "HTML")
    if kb:
        kw["reply_markup"] = kb
    try:
        await query.edit_message_text(text, **kw)
    except BadRequest:
        await query.message.reply_text(text, **kw)

def admin_check(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_admin(uid):
            if update.callback_query:
                await update.callback_query.answer("❌ Немає доступу", show_alert=True)
            else:
                await update.message.reply_text("❌ У вас немає доступу.")
            return
        return await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper

async def notify_group(bot, text: str, kb=None, **kw):
    """Надіслати повідомлення в групу (якщо GROUP_CHAT_ID встановлено)."""
    if not GROUP_CHAT_ID:
        return None
    kw.setdefault("parse_mode", "HTML")
    if kb:
        kw["reply_markup"] = kb
    try:
        return await bot.send_message(GROUP_CHAT_ID, text, **kw)
    except Exception as e:
        logger.error("notify_group error: %s", e)
        return None

async def notify_group_photo(bot, photo_bytes: bytes, caption: str, kb=None):
    """Надіслати фото в групу."""
    if not GROUP_CHAT_ID:
        return None
    try:
        p_io = io.BytesIO(photo_bytes)
        p_io.name = "photo.png"
        kw = {"caption": caption, "parse_mode": "HTML"}
        if kb:
            kw["reply_markup"] = kb
        return await bot.send_photo(GROUP_CHAT_ID, p_io, **kw)
    except Exception as e:
        logger.error("notify_group_photo error: %s", e)
        return None

# ─────────────────────────────────────────
#  /start
# ─────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    users = safe_load(USERS_FILE)
    settings = load_settings()

    ref_by = None
    if context.args:
        pot = context.args[0]
        if pot != uid and pot in users:
            ref_by = pot

    if uid not in users:
        users[uid] = {
            "username":   update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "balance":    0,
            "referred_by":ref_by,
            "ref_count":  0,
            "has_bought": False,
            "joined_date":now_str(),
            "total_spent":0,
            "total_orders":0,
            "banned":     False,
            "vip":        False,
            "notes":      "",
        }
        safe_save(USERS_FILE, users)
        log_action("new_user", uid, {"ref_by": ref_by})
        if ref_by:
            try:
                await context.bot.send_message(
                    ref_by,
                    f"👋 <b>Новий реферал!</b>\n\n"
                    f"Користувач {esc(update.effective_user.first_name)} приєднався за вашим посиланням!\n"
                    f"Ви отримаєте {REFERRAL_REWARD}₴ після його першого замовлення.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

    u = users.get(uid, {})
    if u.get("banned"):
        await update.effective_message.reply_text(
            "🚫 <b>Ваш акаунт заблоковано.</b>\n\nЗв'яжіться з адміністратором.",
            parse_mode="HTML",
        )
        return

    if settings.get("maintenance_mode") and not is_admin(uid):
        await update.effective_message.reply_text(
            "🛠 <b>Бот тимчасово на технічному обслуговуванні.</b>\n\nСпробуйте пізніше.",
            parse_mode="HTML",
        )
        return

    welcome = settings.get("welcome_text") or (
        f"🌸 <b>Вітаємо, {esc(update.effective_user.first_name)}!</b>\n\n"
        "Раді вас бачити у <b>FunsDiia</b> — вашому помічнику у генерації документів.\n\n"
        "✨ <b>Що ми пропонуємо:</b>\n"
        "• 📄 Генерація документів будь-якої складності\n"
        "• ⚡️ Швидке виконання — до 10 хвилин\n"
        "• 💰 Вигідна реферальна програма\n"
        "• 🎁 Промо-коди та знижки\n\n"
        "Оберіть потрібний розділ 👇"
    )

    vip_badge = " 👑" if u.get("vip") else ""
    bal = u.get("balance", 0)
    bal_text = f" | 💰 {bal}₴" if bal > 0 else ""

    kb_rows = [
        [InlineKeyboardButton("🛍️ Каталог тарифів", callback_data="catalog")],
        [InlineKeyboardButton("🎟️ Промо-код", callback_data="promo_enter"),
         InlineKeyboardButton("👥 Реферали", callback_data="ref_menu")],
        [InlineKeyboardButton("📦 Мої замовлення", callback_data="my_orders"),
         InlineKeyboardButton("💬 Зв'язок", callback_data="feedback")],
        [InlineKeyboardButton(f"👤 Профіль{vip_badge}{bal_text}", callback_data="profile")],
        [InlineKeyboardButton("ℹ️ Про нас", callback_data="about")],
    ]
    if is_admin(uid):
        kb_rows.append([InlineKeyboardButton("👑 Адмін-панель", callback_data="admin_panel")])

    await update.effective_message.reply_text(
        welcome, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="HTML",
    )
    context.user_data.clear()

# ─────────────────────────────────────────
#  ПРОФІЛЬ
# ─────────────────────────────────────────
async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    users = safe_load(USERS_FILE)
    u = users.get(uid, {})
    orders = safe_load(ORDERS_FILE)
    my_orders = [o for o in orders.values() if o.get("user_id") == uid]
    done    = sum(1 for o in my_orders if o.get("status") == "completed")
    pending = sum(1 for o in my_orders if o.get("status") == "pending")
    vip = "👑 VIP" if u.get("vip") else "👤 Звичайний"
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    text = (
        f"👤 <b>Ваш профіль</b>\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📛 Ім'я: {esc(u.get('first_name','—'))}\n"
        f"🏅 Статус: {vip}\n\n"
        f"💰 Баланс: <b>{u.get('balance',0)}₴</b>\n"
        f"👥 Рефералів: <b>{u.get('ref_count',0)}</b>\n"
        f"💸 Витрачено: <b>{u.get('total_spent',0)}₴</b>\n\n"
        f"📦 Замовлень всього: <b>{len(my_orders)}</b>\n"
        f"   ✅ Виконано: {done}\n"
        f"   ⏳ В обробці: {pending}\n\n"
        f"📅 З нами з: {u.get('joined_date','')[:10]}\n\n"
        f"🔗 Реф. посилання:\n<code>{ref_link}</code>"
    )
    kb = mkb(
        [InlineKeyboardButton("💰 Вивести кошти", callback_data="withdraw")],
        back_btn("home"),
    )
    await safe_edit(q, text, kb, disable_web_page_preview=True)

# ─────────────────────────────────────────
#  МОЇ ЗАМОВЛЕННЯ
# ─────────────────────────────────────────
async def my_orders_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    orders = safe_load(ORDERS_FILE)
    my = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("user_id") == uid],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    if not my:
        await safe_edit(q,
            "📭 <b>У вас ще немає замовлень.</b>\n\nОберіть тариф у каталозі!",
            mkb(back_btn("home")),
        )
        return
    status_map = {
        "pending": "⏳ Очікує", "approved": "✅ Підтверджено",
        "completed": "🎉 Виконано", "rejected": "❌ Відхилено",
        "paid": "💳 Оплачено", "deployed": "🌐 Опубліковано",
    }
    tariffs = load_tariffs()
    text = "📦 <b>Ваші замовлення</b>\n\n"
    for oid, o in my[:10]:
        st = status_map.get(o.get("status", "pending"), o.get("status", "?"))
        t_name = tariffs.get(o.get("tariff", ""), {}).get("name", o.get("tariff", "?"))
        text += f"🔖 <b>#{esc(oid)}</b> — {esc(t_name)}\n"
        text += f"   {st} | {o.get('created_at','')[:10]}\n"
        if o.get("pages_url"):
            text += f"   🔗 <a href='{esc(o['pages_url'])}'>Відкрити</a>\n"
        text += "\n"
    await safe_edit(q, text, mkb(back_btn("home")), disable_web_page_preview=True)

# ─────────────────────────────────────────
#  КАТАЛОГ → ЗАМОВЛЕННЯ
# ─────────────────────────────────────────
async def show_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    tariffs = active_tariffs()
    text = "🛍️ <b>Наші тарифи</b>\n\n"
    for k, t in tariffs.items():
        d = "безстроково" if not t.get("days") else f"{t['days']} днів"
        text += f"{t.get('emoji','📦')} <b>{esc(t.get('name'))}</b> — {t.get('price')}₴ ({d})\n"
    text += "\n<i>Оберіть тариф нижче 👇</i>"
    kb_rows = [[InlineKeyboardButton(fmt_tariff(k, t), callback_data=f"tar:{k}")] for k, t in tariffs.items()]
    kb_rows.append(back_btn("home"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))

async def select_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    settings = load_settings()
    if not settings.get("new_orders_enabled", True) and not is_admin(uid):
        await q.answer("❌ Прийом замовлень тимчасово призупинено", show_alert=True)
        return
    key = q.data.split(":")[1]
    tariffs = active_tariffs()
    if key not in tariffs:
        await q.answer("❌ Тариф недоступний", show_alert=True)
        return
    t = tariffs[key]
    context.user_data.update({
        "tariff":       key,
        "tariff_name":  t.get("name"),
        "tariff_price": t.get("price"),
        "state":        AWAIT_FIO,
    })
    await safe_edit(q,
        f"{t.get('emoji','📦')} <b>Тариф обрано:</b> {esc(t.get('name'))} — {t.get('price')}₴\n\n"
        "✍️ <b>Крок 1/7 — ПІБ</b>\n\nВведіть повне ПІБ українською:\n"
        "<i>Наприклад: Іванов Іван Іванович</i>",
    )

# ─────────────────────────────────────────
#  ПРОМО-КОД (публічний)
# ─────────────────────────────────────────
async def promo_enter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(q,
        "🎟️ <b>Введіть промо-код</b>\n\nНапишіть ваш промо-код для отримання знижки:",
        mkb(back_btn("home")),
    )

# ─────────────────────────────────────────
#  РЕФЕРАЛИ / ВИВІД
# ─────────────────────────────────────────
async def ref_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    u = safe_load(USERS_FILE).get(uid, {})
    ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
    text = (
        f"👥 <b>Реферальна програма</b>\n\n"
        "Запрошуйте друзів — отримуйте бонуси! 🎁\n\n"
        f"💰 Бонус за кожного реферала: <b>{REFERRAL_REWARD}₴</b>\n"
        f"💎 Мінімальний вивід: <b>{MIN_WITHDRAW}₴</b>\n\n"
        f"📊 <b>Ваша статистика:</b>\n"
        f"• Запрошено: <b>{u.get('ref_count',0)}</b>\n"
        f"• Баланс: <b>{u.get('balance',0)}₴</b>\n\n"
        f"🔗 <b>Ваше посилання:</b>\n<code>{ref_link}</code>"
    )
    await safe_edit(q, text, mkb(
        [InlineKeyboardButton("💰 Вивести кошти", callback_data="withdraw")],
        back_btn("home"),
    ), disable_web_page_preview=True)

async def withdraw_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = str(q.from_user.id)
    bal = safe_load(USERS_FILE).get(uid, {}).get("balance", 0)
    if bal < MIN_WITHDRAW:
        await safe_edit(q,
            f"❌ <b>Недостатньо коштів</b>\n\n"
            f"Мінімум для виведення: {MIN_WITHDRAW}₴\nВаш баланс: {bal}₴",
            mkb(back_btn("ref_menu")),
        )
        return
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                f"💰 <b>Запит на вивід</b>\n\n"
                f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
                f"🆔 ID: {uid}\n💳 Сума: {bal}₴\n📅 {now_fmt()}",
                reply_markup=mkb([InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_withdraw:{uid}:{bal}")]),
                parse_mode="HTML",
            )
        except Exception:
            pass
    await safe_edit(q,
        "✅ <b>Запит відправлено!</b>\n\nАдміністратор обробить ваш запит протягом 24 годин. 🌸",
        mkb(back_btn("ref_menu")),
    )

# ─────────────────────────────────────────
#  ЗВОРОТНИЙ ЗВ'ЯЗОК
# ─────────────────────────────────────────
async def feedback_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_FEEDBACK
    await safe_edit(q,
        "💬 <b>Зворотній зв'язок</b>\n\n"
        "Напишіть ваше повідомлення, відгук або запитання.\nМи відповімо якнайшвидше! 🌸",
        mkb(back_btn("home")),
    )

async def about_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    settings = load_settings()
    await safe_edit(q,
        "ℹ️ <b>Про FunsDiia</b>\n\n"
        "Ми — команда професіоналів у генерації документів.\n\n"
        "📌 <b>Як це працює:</b>\n"
        "1️⃣ Обираєте тариф\n"
        "2️⃣ Вводите ПІБ, дату народження, стать\n"
        "3️⃣ Надсилаєте фото 3×4\n"
        "4️⃣ Оплачуєте та отримуєте посилання на ваш кабінет\n\n"
        f"💳 <b>Оплата:</b>\n{esc(settings.get('payment_card','—'))}\n"
        f"👤 {esc(settings.get('payment_holder','—'))}\n\n"
        "⚡️ Виконання: до 10 хвилин",
        mkb(back_btn("home")),
    )

# ─────────────────────────────────────────
#  КРОКИ АНКЕТИ (callback-кнопки)
# ─────────────────────────────────────────
async def select_sex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["sex"] = q.data.split(":")[1]
    context.user_data["state"] = AWAIT_ADDRESS
    sex_text = "Чоловік ♂️" if context.user_data["sex"] == "M" else "Жінка ♀️"
    await safe_edit(q,
        f"✅ Стать: <b>{sex_text}</b>\n\n"
        "🏠 <b>Крок 4/7 — Адреса реєстрації</b>\n\n"
        "Введіть адресу прописки:\n"
        "<i>Наприклад: Харківська область, м. Харків, вул. Сумська, буд. 5, кв. 12</i>\n\n"
        "<i>Або надішліть /skip для автоматичної генерації</i>",
    )

async def ask_rights(update, context):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так, є водійські права", callback_data="rights:yes")],
        [InlineKeyboardButton("❌ Ні", callback_data="rights:no")],
    ])
    text = "🚗 <b>Крок 5/7 — Водійські права</b>\n\nУ вас є водійські права?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")

async def ask_zagran(update, context):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так, є закордонний паспорт", callback_data="zagran:yes")],
        [InlineKeyboardButton("❌ Ні", callback_data="zagran:no")],
    ])
    text = "🌍 <b>Крок 6/7 — Закордонний паспорт</b>\n\nУ вас є закордонний паспорт?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")

async def ask_diploma(update, context):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так, є диплом / студентський", callback_data="diploma:yes")],
        [InlineKeyboardButton("❌ Ні", callback_data="diploma:no")],
    ])
    text = "🎓 <b>Крок 7/7 — Диплом / студентський</b>\n\nДодати диплом або студентський квиток?"
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")

async def ask_photo(update, context):
    text = (
        "📸 <b>Останній крок — Фото</b>\n\n"
        "Надішліть фото 3×4 (обличчя на світлому фоні).\n"
        "<i>Це буде фото у вашому документі.</i>"
    )
    if update.callback_query:
        await safe_edit(update.callback_query, text)
    else:
        await update.message.reply_text(text, parse_mode="HTML")

async def select_rights(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["is_rights"] = (q.data.split(":")[1] == "yes")
    context.user_data["state"] = AWAIT_ZAGRAN_CHOICE
    await ask_zagran(update, context)

async def select_zagran(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["is_zagran"] = (q.data.split(":")[1] == "yes")
    context.user_data["state"] = AWAIT_DIPLOMA_CHOICE
    await ask_diploma(update, context)

async def select_diploma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    val = q.data.split(":")[1] == "yes"
    context.user_data["is_diploma"] = val
    context.user_data["is_study"]   = val
    context.user_data["state"] = AWAIT_PHOTO
    await ask_photo(update, context)

# ─────────────────────────────────────────
#  ОБРОБНИК ПОВІДОМЛЕНЬ
# ─────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    users = safe_load(USERS_FILE)
    if users.get(uid, {}).get("banned"):
        return

    state = context.user_data.get("state")
    text  = (update.message.text or "").strip()

    # Адмін відповідає через Reply
    if is_admin(uid) and update.message.reply_to_message:
        await handle_admin_reply(update, context)
        return

    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context)
        return

    # ── Публічні стани ──
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
        feedbacks = safe_load(FEEDBACK_FILE)
        feedbacks[fid] = {
            "user_id":    uid,
            "username":   update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "feedback":   text,
            "created_at": now_str(),
            "status":     "new",
        }
        safe_save(FEEDBACK_FILE, feedbacks)
        log_action("feedback", uid, {"fid": fid})
        msg_text = (
            f"💬 <b>Новий відгук #{esc(fid)}</b>\n\n"
            f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
            f"🆔 {uid}\n📝 {esc(text)}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id, msg_text,
                    reply_markup=mkb([InlineKeyboardButton("✍️ Відповісти", callback_data=f"reply_fb:{fid}")]),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        context.user_data["state"] = None
        await update.message.reply_text("✅ <b>Дякуємо за відгук!</b>\n\nМи відповімо найближчим часом. 🌸", parse_mode="HTML")
        return

    if state == AWAIT_FIO:
        if len(text.split()) < 2:
            await update.message.reply_text("❌ Введіть мінімум 2 слова (Прізвище Ім'я).", parse_mode="HTML")
            return
        context.user_data["fio"] = text
        context.user_data["state"] = AWAIT_DOB
        await update.message.reply_text(
            "📅 <b>Крок 2/7 — Дата народження</b>\n\nФормат: <b>ДД.ММ.РРРР</b>\nНаприклад: 15.06.1995",
            parse_mode="HTML",
        )
        return

    if state == AWAIT_DOB:
        if not re.match(r"^\d{2}\.\d{2}\.\d{4}$", text):
            await update.message.reply_text("❌ Неправильний формат. Використовуйте ДД.ММ.РРРР", parse_mode="HTML")
            return
        context.user_data["dob"] = text
        context.user_data["state"] = AWAIT_SEX
        await update.message.reply_text(
            "👤 <b>Крок 3/7 — Стать</b>",
            reply_markup=mkb([
                InlineKeyboardButton("♂️ Чоловік", callback_data="sex:M"),
                InlineKeyboardButton("♀️ Жінка",   callback_data="sex:W"),
            ]),
            parse_mode="HTML",
        )
        return

    if state == AWAIT_ADDRESS:
        context.user_data["address"] = "" if text.lower() in ("/skip", "skip") else text
        context.user_data["state"] = AWAIT_RIGHTS_CHOICE
        await ask_rights(update, context)
        return

    # ── Адмін-стани ──
    if is_admin(uid):
        await handle_admin_state(update, context, state, text, uid)
        return

    # ── AI підтримка (відповідає на питання юзерів) ──
    settings = load_settings()
    if AI_ENABLED and settings.get("ai_support", True) and text and not state:
        # Зберігаємо короткий history (максимум 4 повідомлення)
        history = context.user_data.get("ai_history", [])
        reply = await ai_support_reply(text, history[-8:])  # last 4 pairs
        if reply:
            history.append({"role": "user", "content": text})
            history.append({"role": "assistant", "content": reply})
            context.user_data["ai_history"] = history[-16:]  # тримаємо 8 пар
            await update.message.reply_text(
                f"🤖 {reply}\n\n<i>Для оформлення замовлення натисніть /start</i>",
                parse_mode="HTML",
            )
            return

    # Fallback — пересилаємо адміну
    try:
        fwd = await update.message.forward(ADMIN_IDS[0])
        await context.bot.send_message(
            ADMIN_IDS[0],
            f"📩 <b>Повідомлення від користувача</b>\n"
            f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
            f"🆔 {uid}\n📅 {now_fmt()}",
            reply_to_message_id=fwd.message_id,
            parse_mode="HTML",
        )
        await update.message.reply_text("✉️ <b>Повідомлення передано адміністратору.</b>", parse_mode="HTML")
    except Exception as e:
        logger.error("Forward error: %s", e)


# ─────────────────────────────────────────
#  ОБРОБКА МЕДІА (фото / чек)
# ─────────────────────────────────────────
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    state = context.user_data.get("state")

    if state == AWAIT_PHOTO and update.message.photo:
        await process_order(update, context, uid)
    elif is_admin(uid) and state == AWAIT_ORDER_COMPLETE_FILE:
        await process_complete_order_files(update, context)
    else:
        await forward_receipt(update, context, uid)

async def process_order(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    photo_file  = await update.message.photo[-1].get_file()
    photo_bytes: bytes = bytes(await photo_file.download_as_bytearray())

    oid = gen_id("ord_")
    context.user_data["order_id"] = oid

    os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)
    photo_path = os.path.join(ORDER_PHOTOS_DIR, f"{oid}.png")
    with open(photo_path, "wb") as f:
        f.write(photo_bytes)

    # AI транслітерація ПІБ (якщо ввімкнено)
    fio_ua = context.user_data.get("fio", "")
    if AI_ENABLED and fio_ua and not context.user_data.get("fio_en"):
        fio_en = await ai_generate_fio_en(fio_ua)
        context.user_data["fio_en"] = fio_en

    values_data = gen_values_dict({**context.user_data, "order_id": oid})
    js_content  = values_dict_to_js(values_data)

    discount    = context.user_data.get("promo_discount", 0)
    base_price  = context.user_data.get("tariff_price", 0)
    final_price = int(base_price * (100 - discount) / 100)

    orders = safe_load(ORDERS_FILE)
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
    safe_save(ORDERS_FILE, orders)

    users = safe_load(USERS_FILE)
    if uid in users:
        users[uid]["total_orders"] = users[uid].get("total_orders", 0) + 1
        safe_save(USERS_FILE, users)

    await handle_referral_bonus(context, uid)
    log_action("new_order", uid, {"oid": oid, "tariff": context.user_data.get("tariff")})

    price_text = f"{final_price}₴" + (f" (зі знижкою {discount}%)" if discount else "")
    caption = (
        f"📦 <b>НОВЕ ЗАМОВЛЕННЯ #{esc(oid)}</b>\n\n"
        f"👤 {esc(update.effective_user.first_name)} (@{esc(update.effective_user.username or '')})\n"
        f"🆔 {uid}\n"
        f"💎 Тариф: {esc(context.user_data.get('tariff_name',''))} — {price_text}\n"
        f"📝 ПІБ: {esc(context.user_data.get('fio',''))}\n"
        f"📅 ДН: {esc(context.user_data.get('dob',''))}\n"
        f"👤 Стать: {'Чоловік' if context.user_data.get('sex')=='M' else 'Жінка'}\n"
        f"🚗 Права: {'Так' if context.user_data.get('is_rights') else 'Ні'}\n"
        f"🌍 Загран: {'Так' if context.user_data.get('is_zagran') else 'Ні'}\n"
        f"🎓 Диплом: {'Так' if context.user_data.get('is_diploma') else 'Ні'}\n"
        f"⏰ {now_fmt()}"
    )

    has_gh = bool(PAGES_GH_TOKEN)

    # Кнопки для адміна
    admin_kb_rows = [
        [InlineKeyboardButton("✅ Підтвердити оплату", callback_data=f"adm_approve:{uid}:{oid}")],
        [InlineKeyboardButton("❌ Відхилити", callback_data=f"adm_reject:{uid}:{oid}")],
    ]
    if has_gh:
        admin_kb_rows.append([InlineKeyboardButton("🚀 Деплой на GitHub Pages", callback_data=f"adm_push_pages:{uid}:{oid}")])
    admin_kb_rows.append([InlineKeyboardButton("📨 Надіслати файли вручну", callback_data=f"adm_complete:{uid}:{oid}")])

    # Надсилаємо адмінам у особистий чат
    for admin_id in ADMIN_IDS:
        try:
            p_io = io.BytesIO(photo_bytes); p_io.name = f"photo_{oid}.png"
            await context.bot.send_photo(
                admin_id, p_io, caption=caption,
                reply_markup=InlineKeyboardMarkup(admin_kb_rows), parse_mode="HTML",
            )
            js_io = io.BytesIO(js_content.encode()); js_io.name = f"values_{oid}.js"
            await context.bot.send_document(admin_id, js_io, caption=f"📄 values.js для #{oid}")
        except Exception as e:
            logger.error("Admin notify error: %s", e)

    # Надсилаємо в групу
    await notify_group_photo(
        context.bot, photo_bytes, caption,
        InlineKeyboardMarkup(admin_kb_rows),
    )

    await update.message.reply_text(
        f"✅ <b>Замовлення #{esc(oid)} прийнято!</b>\n\n"
        "📌 <b>Що далі:</b>\n"
        "1️⃣ Адміністратор перевірить дані\n"
        "2️⃣ Ви отримаєте реквізити для оплати\n"
        "3️⃣ Надішліть чек після оплати\n"
        "4️⃣ Отримаєте посилання на ваш кабінет\n\n"
        f"💳 Ціна: <b>{price_text}</b>\n\nОчікуйте на повідомлення! 🌸",
        parse_mode="HTML",
    )
    context.user_data.clear()

async def forward_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    """
    Юзер надіслав чек.
    1. DeepSeek V3 аналізує фото чека
    2. Якщо впевненість >= 80% → авто-деплой, юзер отримує посилання
    3. Якщо < 80% → адмін отримує чек з кнопкою підтвердження (1 натискання)
    """
    settings = load_settings()
    first_name = esc(update.effective_user.first_name)
    username   = esc(update.effective_user.username or "")

    # Шукаємо останнє активне замовлення
    orders = safe_load(ORDERS_FILE)
    user_orders = sorted(
        [(oid, o) for oid, o in orders.items()
         if o.get("user_id") == uid and o.get("status") in ("pending", "approved")],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )

    if not user_orders:
        await update.message.reply_text(
            "⚠️ <b>Активне замовлення не знайдено.</b>\n\nСпочатку оформіть замовлення через /start",
            parse_mode="HTML",
        )
        return

    last_oid, last_order = user_orders[0]
    expected_price = last_order.get("final_price", 0)

    # ── Повідомлення юзеру поки AI аналізує ──
    await update.message.reply_text(
        "✅ <b>Чек отримано!</b>\n\n🤖 Перевіряємо оплату автоматично...",
        parse_mode="HTML",
    )

    # ── Завантажуємо фото чека ──
    receipt_bytes = b""
    if update.message.photo:
        f = await update.message.photo[-1].get_file()
        receipt_bytes = bytes(await f.download_as_bytearray())
    elif update.message.document and update.message.document.mime_type and \
         update.message.document.mime_type.startswith("image/"):
        f = await update.message.document.get_file()
        receipt_bytes = bytes(await f.download_as_bytearray())

    # ── AI перевірка ──
    ai_result = {"ok": None, "confidence": 0, "amount": None, "reason": "", "auto_approved": False}
    if AI_ENABLED and settings.get("ai_check_receipts", True) and receipt_bytes:
        ai_result = await ai_check_receipt(receipt_bytes, expected_price)
        logger.info("AI receipt check uid=%s oid=%s: %s", uid, last_oid, ai_result)

    auto_ok   = ai_result.get("auto_approved", False) and settings.get("ai_auto_deploy", True) and bool(PAGES_GH_TOKEN)
    confidence = ai_result.get("confidence", 0)
    ai_amount  = ai_result.get("amount")
    ai_reason  = ai_result.get("reason", "")

    ai_badge = (
        f"🤖 <b>AI-аналіз чека:</b>\n"
        f"{'✅ Схоже на чек' if ai_result.get('ok') else '⚠️ Сумнівний чек'} | "
        f"Впевненість: <b>{confidence}%</b>\n"
        f"{'💰 Сума: ' + str(ai_amount) + '₴' if ai_amount else ''}\n"
        f"<i>{ai_reason}</i>\n\n"
    )

    info_text = (
        f"📑 <b>Чек від клієнта</b>\n"
        f"👤 {first_name} (@{username})\n"
        f"🆔 {uid}\n"
        f"📦 Замовлення: <code>{esc(last_oid)}</code>\n"
        f"💰 Очікувана сума: {expected_price}₴\n"
        f"📅 {now_fmt()}\n\n"
        f"{ai_badge if AI_ENABLED else ''}"
        f"<i>Reply → відповісти клієнту</i>"
    )

    # ══════════════════════════════════════
    #  АВТО-ДЕПЛОЙ (AI впевнений >= 80%)
    # ══════════════════════════════════════
    if auto_ok:
        log_action("ai_receipt_approved", uid, {"oid": last_oid, "confidence": confidence})

        await update.message.reply_text(
            f"🎉 <b>Оплату підтверджено автоматично!</b>\n\n"
            f"🤖 AI перевірив чек ({confidence}% впевненість)\n"
            f"⏳ Готуємо ваш кабінет, зачекайте...",
            parse_mode="HTML",
        )

        # Оновлюємо статус
        orders[last_oid]["status"] = "approved"
        orders[last_oid]["receipt_ai"] = ai_result
        safe_save(ORDERS_FILE, orders)

        # Отримуємо дані для деплою
        js_content = last_order.get("js_content", "")
        if not js_content:
            vd = gen_values_dict({
                "fio": last_order.get("fio",""), "dob": last_order.get("dob",""),
                "sex": last_order.get("sex","M"), "is_rights": last_order.get("is_rights",True),
                "is_zagran": last_order.get("is_zagran",True), "is_diploma": last_order.get("is_diploma",False),
                "is_study": last_order.get("is_study",False), "address": last_order.get("address",""),
                "order_id": last_oid,
            })
            js_content = values_dict_to_js(vd)

        photo_bytes = b""
        photo_path = last_order.get("photo_path","")
        if photo_path and os.path.exists(photo_path):
            with open(photo_path,"rb") as f:
                photo_bytes = f.read()

        try:
            pages_url = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: push_order_to_pages(uid, last_oid, js_content, photo_bytes),
            )
            orders = safe_load(ORDERS_FILE)
            orders[last_oid]["pages_url"]   = pages_url
            orders[last_oid]["status"]      = "deployed"
            orders[last_oid]["deployed_at"] = now_str()
            safe_save(ORDERS_FILE, orders)
            log_action("pages_deployed_auto", uid, {"oid": last_oid, "url": pages_url})

            # Юзеру — посилання
            await update.message.reply_text(
                f"✅ <b>Ваш кабінет готовий!</b>\n\n"
                f"🔗 <b>Посилання:</b>\n{pages_url}\n\n"
                f"⏱ Якщо сайт ще не відкривається — зачекайте 1-2 хвилини.\n"
                f"📋 Замовлення: <code>{esc(last_oid)}</code>",
                parse_mode="HTML",
                disable_web_page_preview=False,
            )

            # В групу — авто-звіт
            repo_name = build_repo_name(uid, last_oid)
            await notify_group(
                context.bot,
                f"🤖 <b>АВТО-ДЕПЛОЙ завершено!</b>\n\n"
                f"👤 {first_name} (@{username}) | <code>{uid}</code>\n"
                f"📦 Замовлення: <code>{esc(last_oid)}</code>\n"
                f"💰 Сума: {expected_price}₴\n"
                f"🤖 AI впевненість: {confidence}%\n"
                f"📁 Репо: <code>{esc(repo_name)}</code>\n\n"
                f"🔗 {pages_url}\n\n"
                f"✅ Посилання надіслано клієнту автоматично.",
                mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{uid}:{last_oid}")]),
            )

            # Адмінам — інфо
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        f"🤖 <b>Авто-деплой</b> #{esc(last_oid)}\n"
                        f"👤 {first_name} | AI: {confidence}%\n🔗 {pages_url}",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            return

        except Exception as e:
            logger.error("Auto-deploy error: %s", e, exc_info=True)
            # Якщо деплой не вдався — fallback до ручного
            await update.message.reply_text(
                "⚠️ Оплату підтверджено, але виникла технічна помилка при деплої.\n"
                "Адміністратор виправить вручну.",
                parse_mode="HTML",
            )

    # ══════════════════════════════════════
    #  РУЧНЕ ПІДТВЕРДЖЕННЯ (AI не впевнений або AI вимкнено)
    # ══════════════════════════════════════
    kb_rows = []
    if PAGES_GH_TOKEN:
        kb_rows.append([InlineKeyboardButton(
            "✅ Підтвердити і задеплоїти",
            callback_data=f"adm_approve_deploy:{uid}:{last_oid}",
        )])
    kb_rows.append([InlineKeyboardButton(
        "✅ Підтвердити (без деплою)",
        callback_data=f"adm_approve:{uid}:{last_oid}",
    )])
    kb_rows.append([InlineKeyboardButton(
        "❌ Відхилити чек",
        callback_data=f"adm_reject:{uid}:{last_oid}",
    )])
    if last_order.get("pages_url"):
        kb_rows.append([InlineKeyboardButton(
            "🔗 Надіслати посилання",
            callback_data=f"adm_send_link:{uid}:{last_oid}",
        )])

    receipt_kb = InlineKeyboardMarkup(kb_rows)

    # Надсилаємо адмінам
    for admin_id in ADMIN_IDS:
        try:
            if receipt_bytes:
                p_io = io.BytesIO(receipt_bytes); p_io.name = "receipt.jpg"
                await context.bot.send_photo(
                    admin_id, p_io, caption=info_text,
                    reply_markup=receipt_kb, parse_mode="HTML",
                )
            else:
                fwd = await update.message.forward(admin_id)
                await context.bot.send_message(
                    admin_id, info_text,
                    reply_to_message_id=fwd.message_id,
                    reply_markup=receipt_kb, parse_mode="HTML",
                )
        except Exception as e:
            logger.error("receipt fwd (admin %s): %s", admin_id, e)

    # В групу
    if GROUP_CHAT_ID:
        try:
            if receipt_bytes:
                p_io = io.BytesIO(receipt_bytes); p_io.name = "receipt.jpg"
                await context.bot.send_photo(
                    GROUP_CHAT_ID, p_io, caption=info_text,
                    reply_markup=receipt_kb, parse_mode="HTML",
                )
            else:
                await update.message.copy_to(GROUP_CHAT_ID)
                await context.bot.send_message(
                    GROUP_CHAT_ID, info_text,
                    reply_markup=receipt_kb, parse_mode="HTML",
                )
        except Exception as e:
            logger.error("receipt fwd (group): %s", e)

    if not auto_ok and AI_ENABLED and ai_result.get("ok") is not None:
        reason_txt = f"\n\n⚠️ AI не зміг авто-підтвердити ({confidence}%). Перевірте вручну." if confidence < 80 else ""
        await update.message.reply_text(
            f"✅ <b>Чек отримано!</b>{reason_txt}\n\nАдміністратор перевірить найближчим часом. 🌸",
            parse_mode="HTML",
        )

async def handle_referral_bonus(context, uid: str):
    users = safe_load(USERS_FILE)
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
                f"💰 <b>Реферальний бонус!</b>\n\nВаш реферал зробив перше замовлення!\n"
                f"Нараховано: <b>{REFERRAL_REWARD}₴</b>\nБаланс: <b>{users[ref_by]['balance']}₴</b>",
                parse_mode="HTML",
            )
        except Exception:
            pass
    users[uid]["has_bought"] = True
    safe_save(USERS_FILE, users)

# ─────────────────────────────────────────
#  АДМІН: REPLY → ВІДПОВІДЬ КЛІЄНТУ
# ─────────────────────────────────────────
async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    m = re.search(r"🆔\s*(\d+)", reply_text)
    if m:
        client_id = m.group(1)
        try:
            await context.bot.send_message(
                client_id,
                f"💬 <b>Відповідь адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸 Гарного дня!",
                parse_mode="HTML",
            )
            await update.message.reply_text(f"✅ Відповідь надіслано → {client_id}")
        except Exception as e:
            await update.message.reply_text(f"❌ Помилка: {e}")
    else:
        await update.message.reply_text("⚠️ Не знайдено ID клієнта в цьому повідомленні.")

async def _do_reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = context.user_data.get("reply_to_uid")
    fid    = context.user_data.get("reply_fb_id")
    if target:
        try:
            await context.bot.send_message(
                target,
                f"💬 <b>Відповідь адміністратора:</b>\n\n{esc(update.message.text)}\n\n🌸",
                parse_mode="HTML",
            )
            if fid:
                feedbacks = safe_load(FEEDBACK_FILE)
                if fid in feedbacks:
                    feedbacks[fid]["status"]      = "replied"
                    feedbacks[fid]["admin_reply"] = update.message.text
                    safe_save(FEEDBACK_FILE, feedbacks)
            await update.message.reply_text(f"✅ Відповідь надіслана → {target}")
        except Exception as e:
            await update.message.reply_text(f"❌ Помилка: {e}")
    context.user_data["state"] = None
    context.user_data.pop("reply_to_uid", None)
    context.user_data.pop("reply_fb_id",  None)

# ─────────────────────────────────────────
#  АДМІН-СТАНИ (текстовий ввід)
# ─────────────────────────────────────────
async def handle_admin_state(update, context, state, text, uid):
    if state == AWAIT_REPLY_TO_USER:
        await _do_reply_to_user(update, context); return

    if state == AWAIT_WELCOME_TEXT:
        s = load_settings(); s["welcome_text"] = text; save_settings(s)
        context.user_data["state"] = None
        await update.message.reply_text("✅ Текст привітання оновлено!"); return

    if state == AWAIT_BROADCAST:
        context.user_data["broadcast_text"] = text
        context.user_data["state"] = None
        users = safe_load(USERS_FILE)
        await update.message.reply_text(
            f"📢 <b>Попередній перегляд:</b>\n\n{text}\n\n👥 Отримають: <b>{len(users)}</b> користувачів",
            reply_markup=mkb(
                [InlineKeyboardButton("✅ Надіслати", callback_data="broadcast_go"),
                 InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")],
            ),
            parse_mode="HTML",
        ); return

    if state == AWAIT_PROMO_CODE:
        context.user_data["new_promo_code"] = text.upper().strip()
        context.user_data["state"] = AWAIT_PROMO_DISCOUNT
        await update.message.reply_text(
            f"✅ Код: <b>{esc(context.user_data['new_promo_code'])}</b>\n\nВведіть знижку у % (наприклад: 20):",
            parse_mode="HTML",
        ); return

    if state == AWAIT_PROMO_DISCOUNT:
        try:
            context.user_data["new_promo_discount"] = int(text)
            context.user_data["state"] = AWAIT_PROMO_USES
            await update.message.reply_text(f"✅ Знижка: <b>{text}%</b>\n\nМакс. використань (0 = необмежено):", parse_mode="HTML")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_PROMO_USES:
        try:
            uses = int(text)
            promos = load_promos()
            code = context.user_data["new_promo_code"]
            promos[code] = {
                "discount": context.user_data["new_promo_discount"],
                "max_uses": uses, "uses": 0,
                "active":   True, "used_by": [],
                "created_at": now_str(), "created_by": uid,
            }
            save_promos(promos)
            context.user_data["state"] = None
            log_action("promo_created", uid, {"code": code})
            await update.message.reply_text(
                f"✅ <b>Промо-код створено!</b>\n\n🎟️ Код: <code>{esc(code)}</code>\n"
                f"💰 Знижка: {context.user_data['new_promo_discount']}%\n"
                f"👥 Ліміт: {'∞' if not uses else uses}",
                parse_mode="HTML",
            )
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_USER_SEARCH:
        context.user_data["state"] = None
        users = safe_load(USERS_FILE)
        q_text = text.strip().lstrip("@")
        found = [(uid2, u) for uid2, u in users.items()
                 if q_text in (u.get("username") or "") or q_text in str(uid2) or q_text in (u.get("first_name") or "")]
        if not found:
            await update.message.reply_text("🔍 Користувача не знайдено."); return
        await send_user_card(update, context, found[0][0], found[0][1]); return

    if state == AWAIT_BALANCE_UID:
        context.user_data["balance_target_uid"] = text.strip()
        context.user_data["state"] = AWAIT_BALANCE_AMOUNT
        await update.message.reply_text("💰 Введіть суму (+ або - для списання):"); return

    if state == AWAIT_BALANCE_AMOUNT:
        try:
            amount = int(text)
            target_uid = context.user_data.get("balance_target_uid")
            users = safe_load(USERS_FILE)
            if target_uid not in users:
                await update.message.reply_text("❌ Користувача не знайдено.")
            else:
                users[target_uid]["balance"] = max(0, users[target_uid].get("balance", 0) + amount)
                safe_save(USERS_FILE, users)
                context.user_data["state"] = None
                log_action("balance_edit", uid, {"target": target_uid, "amount": amount})
                await update.message.reply_text(
                    f"✅ Баланс змінено!\n👤 {target_uid}\n"
                    f"💰 {'+' if amount>=0 else ''}{amount}₴\n"
                    f"📊 Новий баланс: {users[target_uid]['balance']}₴",
                )
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
            await update.message.reply_text("✅ Реквізити оплати оновлено!")
        else:
            await update.message.reply_text("❌ Мінімум 2 рядки: картка + отримувач")
        return

    if state == AWAIT_TARIFF_EDIT_PRICE:
        try:
            price = int(text)
            key = context.user_data.get("edit_tariff_key")
            tariffs = load_tariffs()
            if key in tariffs:
                tariffs[key]["price"] = price
                save_tariffs(tariffs)
            context.user_data["state"] = None
            await update.message.reply_text(f"✅ Ціна → {price}₴")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_TARIFF_EDIT_NAME:
        key = context.user_data.get("edit_tariff_key")
        tariffs = load_tariffs()
        if key in tariffs:
            tariffs[key]["name"] = text
            save_tariffs(tariffs)
        context.user_data["state"] = None
        await update.message.reply_text(f"✅ Назву змінено → {esc(text)}"); return

    if state == AWAIT_TARIFF_EDIT_EMOJI:
        key = context.user_data.get("edit_tariff_key")
        tariffs = load_tariffs()
        if key in tariffs:
            tariffs[key]["emoji"] = text.strip()
            save_tariffs(tariffs)
        context.user_data["state"] = None
        await update.message.reply_text(f"✅ Емоджі змінено → {text}"); return

    if state == AWAIT_REJECT_REASON:
        oid = context.user_data.get("reject_oid")
        client_uid = context.user_data.get("reject_uid")
        if oid and client_uid:
            orders = safe_load(ORDERS_FILE)
            if oid in orders:
                orders[oid]["status"]        = "rejected"
                orders[oid]["reject_reason"] = text
                safe_save(ORDERS_FILE, orders)
            try:
                await context.bot.send_message(
                    client_uid,
                    f"❌ <b>Замовлення #{esc(oid)} відхилено</b>\n\nПричина: {esc(text)}\n\nЗ питань звертайтеся до адміна.",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            context.user_data["state"] = None
            await update.message.reply_text(f"✅ Замовлення #{oid} відхилено, клієнт сповіщений.")
        return

    if state == AWAIT_TARIFF_NAME:
        context.user_data["new_t_name"] = text
        context.user_data["state"] = AWAIT_TARIFF_PRICE
        await update.message.reply_text("💰 Введіть ціну (₴):"); return

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
            days = int(text) or None
            context.user_data["new_t_days"] = days
            context.user_data["state"] = AWAIT_TARIFF_EMOJI
            await update.message.reply_text("😊 Введіть емоджі (наприклад: 🌟):")
        except ValueError:
            await update.message.reply_text("❌ Введіть число!")
        return

    if state == AWAIT_TARIFF_EMOJI:
        emj   = text.strip() or "📦"
        name  = context.user_data["new_t_name"]
        price = context.user_data["new_t_price"]
        days  = context.user_data.get("new_t_days")
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
            f"✅ Тариф додано!\n{emj} {esc(name)} — {price}₴\n"
            f"Термін: {'Назавжди' if not days else f'{days} днів'}",
        )

# ─────────────────────────────────────────
#  АДМІН: ЗАВЕРШЕННЯ ЗАМОВЛЕННЯ (файли вручну)
# ─────────────────────────────────────────
async def process_complete_order_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    oid = context.user_data.get("complete_oid")
    client_uid = context.user_data.get("complete_uid")
    if not oid or not client_uid:
        return
    try:
        caption = f"📁 Ваші файли за замовленням #{esc(oid)}\n\nДякуємо! 🌸"
        if update.message.document:
            await context.bot.send_document(client_uid, update.message.document.file_id, caption=caption)
        elif update.message.photo:
            await context.bot.send_photo(client_uid, update.message.photo[-1].file_id, caption=caption)
        elif update.message.video:
            await context.bot.send_video(client_uid, update.message.video.file_id, caption=caption)

        orders = safe_load(ORDERS_FILE)
        if oid in orders:
            orders[oid]["status"]       = "completed"
            orders[oid]["completed_at"] = now_str()
            safe_save(ORDERS_FILE, orders)
            users = safe_load(USERS_FILE)
            if client_uid in users:
                users[client_uid]["total_spent"] = (
                    users[client_uid].get("total_spent", 0) + orders[oid].get("final_price", 0)
                )
                safe_save(USERS_FILE, users)

        log_action("order_completed", None, {"oid": oid})
        await update.message.reply_text(f"✅ Файли надіслані клієнту {client_uid} (#{oid})")
        context.user_data["state"] = None
    except Exception as e:
        await update.message.reply_text(f"❌ Помилка надсилання: {e}")

# ─────────────────────────────────────────
#  АДМІН-ПАНЕЛЬ
# ─────────────────────────────────────────
@admin_check
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users  = safe_load(USERS_FILE)
    orders = safe_load(ORDERS_FILE)
    pending = sum(1 for o in orders.values() if o.get("status") == "pending")
    gh_status = "✅ встановлено" if PAGES_GH_TOKEN else "❌ не встановлено"
    group_status = f"<code>{GROUP_CHAT_ID}</code>" if GROUP_CHAT_ID else "❌ не задано"
    text = (
        f"👑 <b>Адмін-панель FunsDiia</b>\n\n"
        f"👥 Користувачів: <b>{len(users)}</b>\n"
        f"📦 Замовлень в черзі: <b>{pending}</b>\n"
        f"🔑 GitHub Token: {gh_status}\n"
        f"💬 Група: {group_status}\n"
        f"🕐 {now_fmt()}"
    )
    kb = mkb(
        [InlineKeyboardButton("📊 Статистика",    callback_data="adm:stats"),
         InlineKeyboardButton("📋 Замовлення",    callback_data="adm:orders")],
        [InlineKeyboardButton("👥 Користувачі",   callback_data="adm:users"),
         InlineKeyboardButton("🔍 Пошук",         callback_data="adm:search")],
        [InlineKeyboardButton("💰 Тарифи",        callback_data="adm:tariffs"),
         InlineKeyboardButton("🎟️ Промо-коди",    callback_data="adm:promos")],
        [InlineKeyboardButton("📢 Розсилка",      callback_data="adm:broadcast"),
         InlineKeyboardButton("💬 Відгуки",       callback_data="adm:feedbacks")],
        [InlineKeyboardButton("⚙️ Налаштування",  callback_data="adm:settings"),
         InlineKeyboardButton("📜 Логи",          callback_data="adm:logs")],
        [InlineKeyboardButton("🌐 GitHub Pages",  callback_data="adm:pages")],
        back_btn("home"),
    )
    await safe_edit(q, text, kb)

@admin_check
async def adm_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users     = safe_load(USERS_FILE)
    orders    = safe_load(ORDERS_FILE)
    feedbacks = safe_load(FEEDBACK_FILE)
    tariffs   = load_tariffs()

    total_u    = len(users)
    active_u   = sum(1 for u in users.values() if not u.get("banned"))
    vip_u      = sum(1 for u in users.values() if u.get("vip"))
    total_o    = len(orders)
    done_o     = sum(1 for o in orders.values() if o.get("status") == "completed")
    pending_o  = sum(1 for o in orders.values() if o.get("status") == "pending")
    rejected_o = sum(1 for o in orders.values() if o.get("status") == "rejected")
    deployed_o = sum(1 for o in orders.values() if o.get("status") == "deployed")
    revenue    = sum(o.get("final_price", 0) for o in orders.values() if o.get("status") in ("completed", "deployed"))
    total_bal  = sum(u.get("balance", 0) for u in users.values())
    new_fb     = sum(1 for f in feedbacks.values() if f.get("status") == "new")

    t_stats = {}
    for o in orders.values():
        k = o.get("tariff", "?"); t_stats[k] = t_stats.get(k, 0) + 1
    t_text = "".join(
        f"  {t.get('emoji','📦')} {esc(t.get('name','?'))}: {t_stats.get(k, 0)}\n"
        for k, t in tariffs.items()
    )

    yesterday = datetime.now(TIMEZONE) - timedelta(hours=24)
    new_users_24h  = sum(1 for u in users.values() if u.get("joined_date","") > yesterday.isoformat())
    new_orders_24h = sum(1 for o in orders.values() if o.get("created_at","") > yesterday.isoformat())

    text = (
        f"📊 <b>Статистика бота</b>\n📅 {now_fmt()}\n\n"
        f"👥 <b>Користувачі</b>\n"
        f"  Всього: {total_u} | Активних: {active_u} | VIP: {vip_u}\n"
        f"  Нові за 24г: +{new_users_24h}\n\n"
        f"📦 <b>Замовлення</b>\n"
        f"  Всього: {total_o}\n"
        f"  Виконано: {done_o} | Задеплоєно: {deployed_o}\n"
        f"  В черзі: {pending_o} | Відхилено: {rejected_o}\n"
        f"  Нові за 24г: +{new_orders_24h}\n\n"
        f"💰 <b>Фінанси</b>\n  Дохід: {revenue}₴ | Баланс юзерів: {total_bal}₴\n\n"
        f"📋 <b>По тарифах</b>\n{t_text}\n"
        f"💬 Нові відгуки: {new_fb}"
    )
    await safe_edit(q, text, mkb(back_btn("admin_panel")))

@admin_check
async def adm_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    orders = safe_load(ORDERS_FILE)
    status_filter = context.user_data.get("orders_filter", "pending")
    filtered = sorted(
        [(oid, o) for oid, o in orders.items() if o.get("status") == status_filter],
        key=lambda x: x[1].get("created_at", ""), reverse=True,
    )
    status_map = {"pending":"⏳","approved":"✅","completed":"🎉","rejected":"❌","paid":"💳","deployed":"🌐"}
    st_emoji = status_map.get(status_filter, "📋")
    text = f"📦 <b>Замовлення ({st_emoji} {status_filter})</b>\n\nВсього: {len(filtered)}\n\n"
    kb_rows = []
    for oid, o in filtered[:15]:
        text += f"🔖 <b>#{esc(oid)}</b> — {esc(o.get('tariff_name','?'))}\n"
        text += f"   👤 {esc(o.get('fio','?'))} | {o.get('created_at','')[:10]}\n\n"
        kb_rows.append([InlineKeyboardButton(f"#{oid} — {o.get('fio','?')[:15]}", callback_data=f"adm_order_view:{oid}")])

    kb_rows.append([
        InlineKeyboardButton("⏳", callback_data="adm_order_filter:pending"),
        InlineKeyboardButton("✅", callback_data="adm_order_filter:approved"),
        InlineKeyboardButton("🎉", callback_data="adm_order_filter:completed"),
        InlineKeyboardButton("🌐", callback_data="adm_order_filter:deployed"),
        InlineKeyboardButton("❌", callback_data="adm_order_filter:rejected"),
    ])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text or "📭 Замовлень немає.", InlineKeyboardMarkup(kb_rows))

@admin_check
async def adm_order_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    oid = q.data.split(":")[1]
    orders = safe_load(ORDERS_FILE)
    o = orders.get(oid, {})
    uid2 = o.get("user_id", "?")
    u = safe_load(USERS_FILE).get(uid2, {})
    text = (
        f"📦 <b>Замовлення #{esc(oid)}</b>\n\n"
        f"👤 {esc(o.get('fio','?'))}\n"
        f"🆔 {uid2} | @{esc(u.get('username','?'))}\n"
        f"📅 ДН: {esc(o.get('dob','?'))}\n"
        f"💎 Тариф: {esc(o.get('tariff_name','?'))}\n"
        f"💰 Ціна: {o.get('final_price','?')}₴\n"
        f"🎟️ Промо: {esc(str(o.get('promo','-')))}\n"
        f"📊 Статус: {o.get('status','?')}\n"
        f"📅 Дата: {o.get('created_at','')[:16]}"
    )
    if o.get("pages_url"):
        text += f"\n🔗 Pages: {esc(o['pages_url'])}"

    kb_rows = [
        [InlineKeyboardButton("✅ Підтвердити", callback_data=f"adm_approve:{uid2}:{oid}"),
         InlineKeyboardButton("❌ Відхилити",   callback_data=f"adm_reject:{uid2}:{oid}")],
    ]
    if PAGES_GH_TOKEN:
        kb_rows.append([InlineKeyboardButton("🚀 Деплой на GitHub Pages", callback_data=f"adm_push_pages:{uid2}:{oid}")])
    kb_rows.append([InlineKeyboardButton("📨 Надіслати файли вручну", callback_data=f"adm_complete:{uid2}:{oid}")])
    kb_rows.append([InlineKeyboardButton("💬 Написати клієнту",       callback_data=f"adm_msg:{uid2}")])
    if o.get("pages_url"):
        kb_rows.append([InlineKeyboardButton("🔗 Надіслати посилання", callback_data=f"adm_send_link:{uid2}:{oid}")])
    kb_rows.append(back_btn("adm:orders"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))

@admin_check
async def adm_order_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["orders_filter"] = q.data.split(":")[1]
    await adm_orders(update, context)

@admin_check
@admin_check
async def adm_approve_deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Адмін вручну підтверджує чек І одразу деплоїть (1 кнопка)."""
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    await safe_edit(q, "⏳ <b>Підтверджуємо оплату і деплоємо...</b>")

    orders = safe_load(ORDERS_FILE)
    order = orders.get(oid, {})
    if not order:
        await safe_edit(q, "❌ Замовлення не знайдено.")
        return

    # Підтверджуємо
    orders[oid]["status"] = "approved"
    safe_save(ORDERS_FILE, orders)
    log_action("receipt_manually_approved", q.from_user.id, {"oid": oid, "client": client_uid})

    # Повідомляємо клієнта
    try:
        await context.bot.send_message(
            client_uid,
            f"✅ <b>Оплату підтверджено!</b>\n\n⏳ Готуємо ваш кабінет...\n📋 Замовлення: <code>{esc(oid)}</code>",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Client notify error: %s", e)

    # Деплой
    js_content = order.get("js_content","")
    if not js_content:
        vd = gen_values_dict({
            "fio": order.get("fio",""), "dob": order.get("dob",""),
            "sex": order.get("sex","M"), "is_rights": order.get("is_rights",True),
            "is_zagran": order.get("is_zagran",True), "is_diploma": order.get("is_diploma",False),
            "is_study": order.get("is_study",False), "address": order.get("address",""),
            "order_id": oid,
        })
        js_content = values_dict_to_js(vd)

    photo_bytes = b""
    photo_path = order.get("photo_path","")
    if photo_path and os.path.exists(photo_path):
        with open(photo_path,"rb") as f:
            photo_bytes = f.read()

    try:
        pages_url = await asyncio.get_event_loop().run_in_executor(
            None, lambda: push_order_to_pages(client_uid, oid, js_content, photo_bytes),
        )
        orders = safe_load(ORDERS_FILE)
        orders[oid]["pages_url"]   = pages_url
        orders[oid]["status"]      = "deployed"
        orders[oid]["deployed_at"] = now_str()
        safe_save(ORDERS_FILE, orders)
        log_action("pages_deployed", q.from_user.id, {"oid": oid, "url": pages_url})

        repo_name = build_repo_name(client_uid, oid)

        # Клієнту посилання
        try:
            await context.bot.send_message(
                client_uid,
                f"✅ <b>Ваш кабінет готовий!</b>\n\n🔗 <b>Посилання:</b>\n{pages_url}\n\n"
                f"⏱ Якщо сайт ще не відкривається — зачекайте 1-2 хвилини.\n"
                f"📋 Замовлення: <code>{esc(oid)}</code>",
                parse_mode="HTML", disable_web_page_preview=False,
            )
        except Exception as e:
            logger.error("Client notify error: %s", e)

        # В групу
        await notify_group(
            context.bot,
            f"🚀 <b>Деплой завершено (вручну підтверджено)</b>\n\n"
            f"📦 <code>{esc(oid)}</code> | 👤 <code>{client_uid}</code>\n"
            f"📁 Репо: <code>{esc(repo_name)}</code>\n🔗 {pages_url}",
            mkb([InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")]),
        )

        await safe_edit(q,
            f"✅ <b>Підтверджено і задеплоєно!</b>\n\n"
            f"📦 <code>{esc(oid)}</code>\n🔗 {esc(pages_url)}\n"
            f"📤 Посилання надіслано клієнту.",
            mkb(
                [InlineKeyboardButton("🔗 Надіслати ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")],
                back_btn("admin_panel"),
            ),
        )
    except Exception as e:
        logger.error("adm_approve_deploy error: %s", e, exc_info=True)
        await safe_edit(q,
            f"⚠️ Оплату підтверджено, але деплой не вдався:\n<code>{esc(str(e)[:300])}</code>",
            mkb(
                [InlineKeyboardButton("🔄 Спробувати деплой знову", callback_data=f"adm_push_pages:{client_uid}:{oid}")],
                back_btn("admin_panel"),
            ),
        )


async def adm_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    orders = safe_load(ORDERS_FILE)
    if oid in orders:
        orders[oid]["status"] = "approved"
        safe_save(ORDERS_FILE, orders)
    settings  = load_settings()
    price     = orders.get(oid, {}).get("final_price", "?")
    payment_text = (
        f"✅ <b>Замовлення #{esc(oid)} підтверджено!</b>\n\n"
        f"💳 <b>Реквізити для оплати:</b>\n"
        f"Картка: <code>{esc(settings.get('payment_card','—'))}</code>\n"
        f"Отримувач: {esc(settings.get('payment_holder','—'))}\n\n"
        f"🔗 Monobank: {settings.get('payment_link','—')}\n\n"
        f"💰 Сума до сплати: <b>{price}₴</b>\n\n"
        f"📤 Після оплати надішліть скріншот у цей чат!"
    )
    try:
        await context.bot.send_message(client_uid, payment_text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        await q.answer(f"Помилка: {e}", show_alert=True)
        return
    log_action("order_approved", q.from_user.id, {"oid": oid, "client": client_uid})

    # Повідомляємо в групу про підтвердження
    await notify_group(
        context.bot,
        f"✅ <b>Замовлення #{esc(oid)} підтверджено</b>\n👤 Клієнт: <code>{client_uid}</code>\n💰 Сума: {price}₴\n⏳ Очікуємо оплату...",
    )

    kb_rows = []
    if PAGES_GH_TOKEN:
        kb_rows.append([InlineKeyboardButton("🚀 Деплой на GitHub Pages", callback_data=f"adm_push_pages:{client_uid}:{oid}")])
    kb_rows.append([InlineKeyboardButton("📨 Надіслати файли вручну", callback_data=f"adm_complete:{client_uid}:{oid}")])
    kb_rows.append(back_btn("adm:orders"))

    await safe_edit(q,
        f"✅ Реквізити надіслані клієнту {client_uid}\n\nТепер виберіть спосіб надання доступу:",
        InlineKeyboardMarkup(kb_rows),
    )

@admin_check
async def adm_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["reject_uid"] = parts[1]
    context.user_data["reject_oid"] = parts[2]
    context.user_data["state"]      = AWAIT_REJECT_REASON
    await safe_edit(q, f"❌ <b>Відхилення замовлення #{esc(parts[2])}</b>\n\nВведіть причину відхилення:")

@admin_check
async def adm_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    context.user_data["complete_uid"] = parts[1]
    context.user_data["complete_oid"] = parts[2]
    context.user_data["state"]        = AWAIT_ORDER_COMPLETE_FILE
    await safe_edit(q,
        f"📨 <b>Надсилання файлів (#{esc(parts[2])})</b>\n\n"
        f"Надішліть файли, фото або ZIP для клієнта {parts[1]}.\n"
        "Вони будуть автоматично переслані.",
    )

@admin_check
async def adm_confirm_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    parts = q.data.split(":")
    uid2, amount = parts[1], int(parts[2])
    users = safe_load(USERS_FILE)
    if uid2 in users:
        users[uid2]["balance"] = max(0, users[uid2].get("balance", 0) - amount)
        safe_save(USERS_FILE, users)
        try:
            await context.bot.send_message(
                uid2,
                f"💰 <b>Виведення підтверджено!</b>\n\nСума {amount}₴ буде відправлена найближчим часом. 🌸",
                parse_mode="HTML",
            )
        except Exception:
            pass
        log_action("withdraw_confirmed", q.from_user.id, {"uid": uid2, "amount": amount})
    await safe_edit(q, f"✅ Вивід {amount}₴ для {uid2} підтверджено.")

# ─────────────────────────────────────────
#  АДМІН: КОРИСТУВАЧІ
# ─────────────────────────────────────────
@admin_check
async def adm_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users = safe_load(USERS_FILE)
    sorted_u = sorted(users.items(), key=lambda x: x[1].get("joined_date",""), reverse=True)[:15]
    text = f"👥 <b>Користувачі ({len(users)} всього)</b>\n\n"
    for uid2, u in sorted_u:
        badges = ("👑" if u.get("vip") else "") + ("🚫" if u.get("banned") else "") + ("💰" if u.get("has_bought") else "🆕")
        text += f"{badges} <b>{esc(u.get('first_name','?'))}</b> (@{esc(u.get('username','?'))})\n"
        text += f"   🆔 {uid2} | 💳 {u.get('balance',0)}₴ | 👥 {u.get('ref_count',0)}\n\n"
    kb = mkb(
        [InlineKeyboardButton("🔍 Пошук",              callback_data="adm:search"),
         InlineKeyboardButton("💰 Нарахувати баланс",  callback_data="adm:balance")],
        back_btn("admin_panel"),
    )
    await safe_edit(q, text, kb)

@admin_check
async def adm_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_USER_SEARCH
    await safe_edit(q, "🔍 <b>Пошук користувача</b>\n\nВведіть @username, ID або ім'я:", mkb(back_btn("admin_panel")))

@admin_check
async def adm_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_BALANCE_UID
    await safe_edit(q, "💰 <b>Зміна балансу</b>\n\nВведіть ID користувача:", mkb(back_btn("admin_panel")))

async def send_user_card(update, context, uid2: str, u: dict):
    orders = safe_load(ORDERS_FILE)
    user_orders = [o for o in orders.values() if o.get("user_id") == uid2]
    text = (
        f"👤 <b>Профіль {esc(u.get('first_name','?'))}</b>\n\n"
        f"🆔 ID: <code>{uid2}</code>\n📱 @{esc(u.get('username','?'))}\n"
        f"💰 Баланс: {u.get('balance',0)}₴\n"
        f"👥 Рефералів: {u.get('ref_count',0)}\n"
        f"💸 Витрачено: {u.get('total_spent',0)}₴\n"
        f"📦 Замовлень: {len(user_orders)}\n"
        f"🏅 VIP: {'Так' if u.get('vip') else 'Ні'}\n"
        f"🚫 Бан: {'Так' if u.get('banned') else 'Ні'}\n"
        f"📅 З нами: {u.get('joined_date','')[:10]}"
    )
    ban_label = "🔓 Розблокувати" if u.get("banned") else "🚫 Заблокувати"
    vip_label = "👤 Зняти VIP"    if u.get("vip")    else "👑 Дати VIP"
    kb = mkb(
        [InlineKeyboardButton(ban_label, callback_data=f"adm_ban:{uid2}"),
         InlineKeyboardButton(vip_label, callback_data=f"adm_vip:{uid2}")],
        [InlineKeyboardButton("💰 Змінити баланс", callback_data="adm:balance")],
        [InlineKeyboardButton("💬 Написати",       callback_data=f"adm_msg:{uid2}")],
        back_btn("adm:users"),
    )
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")

@admin_check
async def adm_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    users = safe_load(USERS_FILE)
    if uid2 in users:
        users[uid2]["banned"] = not users[uid2].get("banned", False)
        safe_save(USERS_FILE, users)
        action = "заблоковано" if users[uid2]["banned"] else "розблоковано"
        log_action(f"user_{action}", q.from_user.id, {"target": uid2})
        await q.answer(f"Користувача {action}!", show_alert=True)
        await send_user_card(update, context, uid2, users[uid2])

@admin_check
async def adm_vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    users = safe_load(USERS_FILE)
    if uid2 in users:
        users[uid2]["vip"] = not users[uid2].get("vip", False)
        safe_save(USERS_FILE, users)
        try:
            await context.bot.send_message(
                uid2,
                f"👑 <b>{'Вам надано VIP-статус!' if users[uid2]['vip'] else 'VIP-статус знято.'}</b>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        await q.answer("Статус VIP змінено!", show_alert=True)
        await send_user_card(update, context, uid2, users[uid2])

@admin_check
async def adm_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid2 = q.data.split(":")[1]
    context.user_data["state"]        = AWAIT_REPLY_TO_USER
    context.user_data["reply_to_uid"] = uid2
    context.user_data.pop("reply_fb_id", None)
    await safe_edit(q, f"💬 <b>Повідомлення клієнту {uid2}</b>\n\nВведіть текст:", mkb(back_btn("admin_panel")))

# ─────────────────────────────────────────
#  АДМІН: ТАРИФИ
# ─────────────────────────────────────────
@admin_check
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
    kb_rows.append([InlineKeyboardButton("➕ Додати тариф", callback_data="tariff_add")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text, InlineKeyboardMarkup(kb_rows))

@admin_check
async def tariff_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    key = q.data.split(":")[1]
    tariffs = load_tariffs()
    if key in tariffs:
        tariffs[key]["active"] = not tariffs[key].get("active", True)
        save_tariffs(tariffs)
    await adm_tariffs(update, context)

@admin_check
async def tariff_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    key = q.data.split(":")[1]
    tariffs = load_tariffs()
    if key in tariffs:
        del tariffs[key]
        save_tariffs(tariffs)
        log_action("tariff_deleted", q.from_user.id, {"key": key})
    await adm_tariffs(update, context)

@admin_check
async def tariff_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    key = q.data.split(":")[1]
    tariffs = load_tariffs()
    t = tariffs.get(key, {})
    context.user_data["edit_tariff_key"] = key
    await safe_edit(q,
        f"✏️ <b>Редагування тарифу</b>\n\n{t.get('emoji','📦')} {esc(t.get('name',''))} — {t.get('price')}₴\n\nЩо змінити?",
        mkb(
            [InlineKeyboardButton("📝 Назва",  callback_data=f"tedit_name:{key}"),
             InlineKeyboardButton("💰 Ціна",   callback_data=f"tedit_price:{key}")],
            [InlineKeyboardButton("😊 Емоджі", callback_data=f"tedit_emoji:{key}")],
            back_btn("adm:tariffs"),
        ),
    )

@admin_check
async def tedit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["edit_tariff_key"] = q.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_NAME
    await safe_edit(q, "📝 Введіть нову назву:")

@admin_check
async def tedit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["edit_tariff_key"] = q.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_PRICE
    await safe_edit(q, "💰 Введіть нову ціну (₴):")

@admin_check
async def tedit_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["edit_tariff_key"] = q.data.split(":")[1]
    context.user_data["state"] = AWAIT_TARIFF_EDIT_EMOJI
    await safe_edit(q, "😊 Введіть нове емоджі:")

@admin_check
async def tariff_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_TARIFF_NAME
    await safe_edit(q, "➕ <b>Новий тариф</b>\n\nКрок 1/4: Введіть назву:")

# ─────────────────────────────────────────
#  АДМІН: ПРОМО-КОДИ
# ─────────────────────────────────────────
@admin_check
async def adm_promos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    promos = load_promos()
    text = f"🎟️ <b>Промо-коди ({len(promos)} всього)</b>\n\n"
    kb_rows = []
    for code, p in promos.items():
        st   = "✅" if p.get("active", True) else "❌"
        used = p.get("uses", 0)
        max_u= p.get("max_uses", 0)
        text += f"{st} <code>{esc(code)}</code> — {p.get('discount',0)}% ({used}/{max_u if max_u else '∞'})\n"
        kb_rows.append([
            InlineKeyboardButton(f"{st} {code}", callback_data=f"promo_toggle:{code}"),
            InlineKeyboardButton("🗑️", callback_data=f"promo_del:{code}"),
        ])
    kb_rows.append([InlineKeyboardButton("➕ Створити промо-код", callback_data="adm_create_promo")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text or "📭 Промо-кодів немає.", InlineKeyboardMarkup(kb_rows))

@admin_check
async def promo_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    code = q.data.split(":")[1]
    promos = load_promos()
    if code in promos:
        promos[code]["active"] = not promos[code].get("active", True)
        save_promos(promos)
    await adm_promos(update, context)

@admin_check
async def promo_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    code = q.data.split(":")[1]
    promos = load_promos()
    if code in promos:
        del promos[code]
        save_promos(promos)
    await adm_promos(update, context)

@admin_check
async def adm_create_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_PROMO_CODE
    await safe_edit(q,
        "🎟️ <b>Створення промо-коду</b>\n\nВведіть назву коду (наприклад: SALE20):",
        mkb(back_btn("adm:promos")),
    )

# ─────────────────────────────────────────
#  АДМІН: РОЗСИЛКА
# ─────────────────────────────────────────
@admin_check
async def adm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    users = safe_load(USERS_FILE)
    active = sum(1 for u in users.values() if not u.get("banned"))
    context.user_data["state"] = AWAIT_BROADCAST
    await safe_edit(q,
        f"📢 <b>Розсилка</b>\n\nОтримають: <b>{active}</b> активних користувачів\n\n"
        "Напишіть текст розсилки (HTML підтримується):",
        mkb(back_btn("admin_panel")),
    )

@admin_check
async def broadcast_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    text  = context.user_data.get("broadcast_text", "")
    users = safe_load(USERS_FILE)
    success, failed, blocked = 0, 0, 0
    await safe_edit(q, "📢 <b>Розсилка розпочата...</b>")
    for uid2, u in users.items():
        if u.get("banned"):
            blocked += 1; continue
        try:
            await context.bot.send_message(uid2, text, parse_mode="HTML")
            success += 1
            if success % 25 == 0:
                await asyncio.sleep(1)
        except Forbidden:
            failed += 1
        except Exception:
            failed += 1
    log_action("broadcast", q.from_user.id, {"success": success, "failed": failed})
    await context.bot.send_message(
        q.from_user.id,
        f"📢 <b>Розсилка завершена!</b>\n\n✅ Успішно: {success}\n❌ Помилок: {failed}\n🔇 Заблоковано: {blocked}",
        parse_mode="HTML",
    )
    context.user_data.pop("broadcast_text", None)

# ─────────────────────────────────────────
#  АДМІН: ВІДГУКИ
# ─────────────────────────────────────────
@admin_check
async def adm_feedbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    feedbacks = safe_load(FEEDBACK_FILE)
    sorted_fb = sorted(feedbacks.items(), key=lambda x: x[1].get("created_at",""), reverse=True)[:10]
    text = f"💬 <b>Відгуки ({len(feedbacks)} всього)</b>\n\n"
    kb_rows = []
    for fid, f in sorted_fb:
        st    = {"new":"🟢","read":"🔵","replied":"🟣"}.get(f.get("status","new"),"⚪")
        short = f.get("feedback","")[:30] + ("..." if len(f.get("feedback","")) > 30 else "")
        text += f"{st} <b>#{esc(fid)}</b> — {esc(f.get('first_name','?'))}\n{esc(short)}\n\n"
        kb_rows.append([InlineKeyboardButton(f"✍️ #{fid}", callback_data=f"reply_fb:{fid}")])
    kb_rows.append(back_btn("admin_panel"))
    await safe_edit(q, text or "📭 Відгуків немає.", InlineKeyboardMarkup(kb_rows))

@admin_check
async def reply_fb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    fid = q.data.split(":")[1]
    feedbacks = safe_load(FEEDBACK_FILE)
    fb = feedbacks.get(fid, {})
    if fb:
        feedbacks[fid]["status"] = "read"
        safe_save(FEEDBACK_FILE, feedbacks)
    context.user_data["reply_to_uid"] = fb.get("user_id")
    context.user_data["reply_fb_id"]  = fid
    context.user_data["state"]        = AWAIT_REPLY_TO_USER
    await safe_edit(q,
        f"✍️ <b>Відповідь на відгук #{esc(fid)}</b>\n\n"
        f"Від: {esc(fb.get('first_name','?'))}\nТекст: {esc(fb.get('feedback','?'))}\n\nВведіть відповідь:",
    )

# ─────────────────────────────────────────
#  АДМІН: НАЛАШТУВАННЯ
# ─────────────────────────────────────────
@admin_check
async def adm_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings()
    ai_status = "✅ Встановлено" if AI_ENABLED else "❌ Не задано (DEEPSEEK_API_KEY у secrets)"
    text = (
        f"⚙️ <b>Налаштування бота</b>\n\n"
        f"🛠 Тех. обслуговування: {'🔴 Так' if s.get('maintenance_mode') else '🟢 Ні'}\n"
        f"📦 Нові замовлення: {'✅' if s.get('new_orders_enabled') else '❌'}\n\n"
        f"🤖 <b>AI (DeepSeek V3):</b> {ai_status}\n"
        f"  └ Авто-перевірка чеків: {'✅' if s.get('ai_check_receipts',True) else '❌'}\n"
        f"  └ Авто-деплой: {'✅' if s.get('ai_auto_deploy',True) else '❌'}\n"
        f"  └ AI підтримка: {'✅' if s.get('ai_support',True) else '❌'}\n\n"
        f"💳 <b>Реквізити:</b>\n{esc(s.get('payment_card','—'))}\n{esc(s.get('payment_holder','—'))}\n"
        f"🔗 {esc(s.get('payment_link','—'))}\n\n"
        f"💬 Група: {'✅ ' + str(GROUP_CHAT_ID) if GROUP_CHAT_ID else '❌ Не задано (GROUP_CHAT_ID у secrets)'}\n"
        f"🔑 GH Token: {'✅ Встановлено' if PAGES_GH_TOKEN else '❌ Не задано (PAGES_GH_TOKEN у secrets)'}"
    )
    kb = mkb(
        [InlineKeyboardButton("🛠 Тех. обслуговування", callback_data="toggle_maintenance"),
         InlineKeyboardButton("📦 Нові замовлення",     callback_data="toggle_orders")],
        [InlineKeyboardButton("🤖 AI чеки: " + ("✅" if s.get("ai_check_receipts",True) else "❌"),
                              callback_data="toggle_ai_receipts"),
         InlineKeyboardButton("🚀 Авто-деплой: " + ("✅" if s.get("ai_auto_deploy",True) else "❌"),
                              callback_data="toggle_ai_deploy")],
        [InlineKeyboardButton("💬 AI підтримка: " + ("✅" if s.get("ai_support",True) else "❌"),
                              callback_data="toggle_ai_support")],
        [InlineKeyboardButton("💳 Змінити реквізити",   callback_data="edit_payment")],
        [InlineKeyboardButton("📝 Текст привітання",    callback_data="edit_welcome")],
        back_btn("admin_panel"),
    )
    await safe_edit(q, text, kb, disable_web_page_preview=True)

@admin_check
async def toggle_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings(); s["maintenance_mode"] = not s.get("maintenance_mode", False); save_settings(s)
    await adm_settings(update, context)

@admin_check
async def toggle_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings(); s["new_orders_enabled"] = not s.get("new_orders_enabled", True); save_settings(s)
    await adm_settings(update, context)

@admin_check
async def toggle_ai_receipts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings(); s["ai_check_receipts"] = not s.get("ai_check_receipts", True); save_settings(s)
    await adm_settings(update, context)

@admin_check
async def toggle_ai_deploy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings(); s["ai_auto_deploy"] = not s.get("ai_auto_deploy", True); save_settings(s)
    await adm_settings(update, context)

@admin_check
async def toggle_ai_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    s = load_settings(); s["ai_support"] = not s.get("ai_support", True); save_settings(s)
    await adm_settings(update, context)

@admin_check
async def edit_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_CUSTOM_PAYMENT_TEXT
    await safe_edit(q,
        "💳 <b>Оновлення реквізитів</b>\n\nВведіть 3 рядки:\n"
        "1) Номер картки\n2) Ім'я отримувача\n3) Посилання Mono (необов'язково)",
    )

@admin_check
async def edit_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    context.user_data["state"] = AWAIT_WELCOME_TEXT
    await safe_edit(q, "📝 Введіть новий текст привітання (HTML підтримується):")

# ─────────────────────────────────────────
#  АДМІН: ЛОГИ
# ─────────────────────────────────────────
@admin_check
async def adm_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    logs = safe_load(LOGS_FILE, [])
    if not isinstance(logs, list): logs = []
    text = f"📜 <b>Останні дії ({len(logs)} всього)</b>\n\n"
    for entry in logs[:20]:
        ts     = entry.get("ts","")[:16]
        action = esc(entry.get("action","?"))
        uid2   = entry.get("uid","?")
        text  += f"🕐 {ts} | <code>{action}</code> | {uid2}\n"
    await safe_edit(q, text, mkb(back_btn("admin_panel")))

# ─────────────────────────────────────────
#  GITHUB PAGES — МЕНЮ (інфо)
# ─────────────────────────────────────────
@admin_check
async def pages_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    token_status  = "✅ Встановлено (з env/secrets)" if PAGES_GH_TOKEN else "❌ Не встановлено"
    group_status  = f"✅ <code>{GROUP_CHAT_ID}</code>" if GROUP_CHAT_ID else "❌ Не задано"
    text = (
        f"🌐 <b>GitHub Pages</b>\n\n"
        f"🔑 PAGES_GH_TOKEN: {token_status}\n"
        f"💬 GROUP_CHAT_ID: {group_status}\n\n"
        f"<i>Токен встановлюється тільки через GitHub Secrets (PAGES_GH_TOKEN).\n"
        f"Нікому не надсилайте токен у чат!</i>\n\n"
        f"<b>Як встановити:</b>\n"
        f"1. github.com → Settings → Developer settings → Tokens\n"
        f"2. Створити токен з правами: <code>repo</code> + <code>pages</code>\n"
        f"3. Додати в GitHub Secrets репо бота як <code>PAGES_GH_TOKEN</code>\n"
        f"4. Для групи — додати <code>GROUP_CHAT_ID</code> (від'ємне число для супергрупи)"
    )
    await safe_edit(q, text, mkb(back_btn("admin_panel")))

# ─────────────────────────────────────────
#  GITHUB: ДЕПЛОЙ (виконання)
# ─────────────────────────────────────────
def _gh(method: str, path: str, **kwargs):
    """GitHub API запит з токеном з env."""
    resp = _requests_lib.request(
        method, f"https://api.github.com{path}",
        headers={
            "Authorization": f"token {PAGES_GH_TOKEN}",
            "Accept":        "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
        **kwargs,
    )
    resp.raise_for_status()
    return resp.json() if resp.text else {}

def _gh_get_sha(username, repo, path, branch) -> Optional[str]:
    try:
        d = _gh("GET", f"/repos/{username}/{repo}/contents/{path}", params={"ref": branch})
        return d.get("sha")
    except Exception:
        return None

def _gh_push_file(username, repo, rel_path, content_bytes, branch):
    sha = _gh_get_sha(username, repo, rel_path, branch)
    payload = {
        "message": f"deploy: {rel_path}",
        "content": base64.b64encode(content_bytes).decode(),
        "branch":  branch,
    }
    if sha:
        payload["sha"] = sha
    _gh("PUT", f"/repos/{username}/{repo}/contents/{rel_path}", json=payload)

def build_repo_name(user_id: str, order_id: str) -> str:
    return f"diia-{user_id}-{order_id}".lower().replace("_", "-")[:80]

def push_order_to_pages(user_id: str, order_id: str, js_content: str, photo_bytes: bytes) -> str:
    """
    Бере файли з папки SITE_TEMPLATE_DIR ('1'),
    замінює values.js та фото,
    пушить все на GitHub і вмикає Pages.
    Повертає URL.
    """
    if not PAGES_GH_TOKEN:
        raise RuntimeError("PAGES_GH_TOKEN не встановлено в середовищі (GitHub Secrets)")

    branch    = "main"
    user_data = _gh("GET", "/user")
    username  = user_data["login"]
    repo_name = build_repo_name(user_id, order_id)

    # Створити репо якщо немає
    try:
        _gh("GET", f"/repos/{username}/{repo_name}")
    except _requests_lib.HTTPError as e:
        if e.response.status_code == 404:
            _gh("POST", "/user/repos", json={
                "name":        repo_name,
                "description": f"Order {order_id}",
                "private":     False,
                "auto_init":   False,
            })
            time.sleep(2)
        else:
            raise

    template_root = SITE_TEMPLATE_DIR
    if not os.path.isdir(template_root):
        raise FileNotFoundError(
            f"Папка шаблону '{template_root}' не знайдена поряд з bot.py. "
            f"Переконайтеся, що папка '1' скопійована в репо бота."
        )

    files_to_push: list[tuple[str, bytes]] = []
    for root, dirs, files in os.walk(template_root):
        dirs[:] = [d for d in dirs if d not in (".git", "__pycache__")]
        for fname in files:
            abs_path = os.path.join(root, fname)
            rel_path = os.path.relpath(abs_path, template_root).replace("\\", "/")
            if rel_path == "values.js":
                files_to_push.append((rel_path, js_content.encode("utf-8")))
            elif rel_path in ("1.png", "sign.png", "sig.png") and photo_bytes:
                files_to_push.append((rel_path, photo_bytes))
            else:
                with open(abs_path, "rb") as f:
                    files_to_push.append((rel_path, f.read()))

    logger.info("Pushing %d files to %s/%s", len(files_to_push), username, repo_name)
    for rel_path, content in files_to_push:
        _gh_push_file(username, repo_name, rel_path, content, branch)

    pages_url = f"https://{username}.github.io/{repo_name}/"
    try:
        _gh("GET", f"/repos/{username}/{repo_name}/pages")
    except _requests_lib.HTTPError:
        try:
            _gh("POST", f"/repos/{username}/{repo_name}/pages",
                json={"source": {"branch": branch, "path": "/"}})
        except Exception as e:
            logger.warning("Enable pages: %s", e)

    return pages_url

@admin_check
async def adm_push_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати підтвердження деплою."""
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]

    if not PAGES_GH_TOKEN:
        await safe_edit(q,
            "❌ <b>GitHub токен не встановлено!</b>\n\n"
            "Додайте <code>PAGES_GH_TOKEN</code> у <b>GitHub Secrets</b> репо бота.\n"
            "Токен ніколи не надсилається в чат — це безпечно.",
            mkb(back_btn("admin_panel")),
        )
        return

    orders = safe_load(ORDERS_FILE)
    order  = orders.get(oid, {})
    repo_name = build_repo_name(client_uid, oid)

    confirm_text = (
        f"🚀 <b>Підтвердіть деплой</b>\n\n"
        f"📦 Замовлення: <code>{esc(oid)}</code>\n"
        f"👤 Клієнт: <code>{client_uid}</code>\n"
        f"📝 ПІБ: {esc(order.get('fio','?'))}\n"
        f"💎 Тариф: {esc(order.get('tariff_name','?'))}\n"
        f"📁 Репо: <code>{esc(repo_name)}</code>\n\n"
        f"✅ Натисніть підтвердити — бот запушить файли з папки <code>1/</code> і надішле посилання клієнту і в групу."
    )
    await safe_edit(q, confirm_text, mkb(
        [InlineKeyboardButton("✅ Підтвердити деплой", callback_data=f"adm_push_go:{client_uid}:{oid}")],
        [InlineKeyboardButton("❌ Скасувати",          callback_data="admin_panel")],
    ))

@admin_check
async def adm_push_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Виконати деплой на GitHub Pages."""
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    await safe_edit(q, "⏳ <b>Деплоємо на GitHub Pages...</b>\n\nЦе може зайняти 30-60 секунд.")

    orders = safe_load(ORDERS_FILE)
    order  = orders.get(oid, {})
    if not order:
        await safe_edit(q, "❌ Замовлення не знайдено.")
        return

    # Беремо збережений js_content або генеруємо заново
    js_content = order.get("js_content")
    if not js_content:
        values_data = gen_values_dict({
            "fio":        order.get("fio", ""),
            "dob":        order.get("dob", ""),
            "sex":        order.get("sex", "M"),
            "is_rights":  order.get("is_rights", True),
            "is_zagran":  order.get("is_zagran", True),
            "is_diploma": order.get("is_diploma", False),
            "is_study":   order.get("is_study", False),
            "address":    order.get("address", ""),
            "order_id":   oid,
        })
        js_content = values_dict_to_js(values_data)

    # Фото
    photo_bytes = b""
    photo_path  = order.get("photo_path", "")
    if photo_path and os.path.exists(photo_path):
        with open(photo_path, "rb") as f:
            photo_bytes = f.read()

    try:
        pages_url = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: push_order_to_pages(client_uid, oid, js_content, photo_bytes),
        )

        orders[oid]["pages_url"]    = pages_url
        orders[oid]["status"]       = "deployed"
        orders[oid]["deployed_at"]  = now_str()
        safe_save(ORDERS_FILE, orders)

        log_action("pages_deployed", q.from_user.id, {"oid": oid, "url": pages_url})
        repo_name = build_repo_name(client_uid, oid)

        # ── Клієнту ──
        client_msg = (
            f"✅ <b>Ваш кабінет готовий!</b>\n\n"
            f"🔗 <b>Посилання:</b>\n{pages_url}\n\n"
            f"⏱ Якщо сайт ще не відкривається — зачекайте 1-2 хвилини.\n"
            f"📋 Замовлення: <code>{esc(oid)}</code>"
        )
        try:
            await context.bot.send_message(client_uid, client_msg, parse_mode="HTML", disable_web_page_preview=False)
        except Exception as e:
            logger.error("Client notify error: %s", e)

        # ── В групу ── підтвердження пуша + посилання
        group_msg = (
            f"🚀 <b>Деплой завершено!</b>\n\n"
            f"📦 Замовлення: <code>{esc(oid)}</code>\n"
            f"👤 Клієнт: <code>{client_uid}</code>\n"
            f"📝 ПІБ: {esc(order.get('fio',''))}\n"
            f"📁 Репо: <code>{esc(repo_name)}</code>\n\n"
            f"🔗 <b>GitHub Pages:</b>\n{pages_url}\n\n"
            f"✅ Посилання надіслано клієнту автоматично."
        )
        group_kb = mkb(
            [InlineKeyboardButton("🔗 Надіслати посилання ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")],
        )
        await notify_group(context.bot, group_msg, group_kb)

        # ── Адміну в особистий чат ──
        await safe_edit(q,
            f"✅ <b>Деплой успішний!</b>\n\n"
            f"📦 Замовлення: <code>{esc(oid)}</code>\n"
            f"📁 Репо: <code>{esc(repo_name)}</code>\n\n"
            f"🔗 <b>GitHub Pages:</b>\n<code>{esc(pages_url)}</code>\n\n"
            f"📤 Посилання надіслано клієнту <code>{client_uid}</code>\n"
            f"📢 Підтвердження надіслано в групу\n"
            f"⏱ Сайт активний через ~1-2 хв.",
            mkb(
                [InlineKeyboardButton("🔗 Надіслати посилання ще раз", callback_data=f"adm_send_link:{client_uid}:{oid}")],
                back_btn("admin_panel"),
            ),
        )

    except Exception as e:
        logger.error("adm_push_go error: %s", e, exc_info=True)
        await safe_edit(q,
            f"❌ <b>Помилка деплою</b>\n\n<code>{esc(str(e)[:500])}</code>\n\n"
            f"Перевірте:\n• PAGES_GH_TOKEN має права repo + pages\n"
            f"• Папка <code>1/</code> є в репо бота",
            mkb(back_btn("admin_panel")),
        )

@admin_check
async def adm_send_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Надіслати збережене посилання клієнту одним натисканням."""
    q = update.callback_query
    parts = q.data.split(":")
    client_uid, oid = parts[1], parts[2]
    orders    = safe_load(ORDERS_FILE)
    pages_url = orders.get(oid, {}).get("pages_url", "")
    fio       = orders.get(oid, {}).get("fio", "")

    if not pages_url:
        await q.answer("❌ URL не знайдено — спочатку зробіть деплой", show_alert=True)
        return

    client_msg = (
        f"✅ <b>Ваш кабінет готовий!</b>\n\n"
        f"🔗 <b>Посилання:</b>\n{pages_url}\n\n"
        f"📋 Замовлення: <code>{esc(oid)}</code>"
    )
    try:
        await context.bot.send_message(client_uid, client_msg, parse_mode="HTML", disable_web_page_preview=False)
        # Сповіщення в групу
        await notify_group(
            context.bot,
            f"📤 <b>Посилання надіслано клієнту</b>\n\n"
            f"👤 <code>{client_uid}</code>\n"
            f"📝 ПІБ: {esc(fio)}\n"
            f"📦 Замовлення: <code>{esc(oid)}</code>\n\n"
            f"🔗 {pages_url}",
        )
        await safe_edit(q,
            f"✅ Посилання надіслано клієнту <code>{client_uid}</code>\n"
            f"📝 ПІБ: {esc(fio)}\n\n🔗 {esc(pages_url)}",
            mkb(back_btn("admin_panel")),
        )
    except Exception as e:
        await q.answer(f"Помилка: {e}", show_alert=True)

# ─────────────────────────────────────────
#  CALLBACK ROUTER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    d   = q.data
    uid = str(q.from_user.id)

    if safe_load(USERS_FILE).get(uid, {}).get("banned") and not is_admin(uid):
        await q.answer("🚫 Ваш акаунт заблоковано", show_alert=True)
        return

    await q.answer()

    try:
        routes = {
            "home":              cmd_start,
            "catalog":           show_catalog,
            "ref_menu":          ref_menu,
            "withdraw":          withdraw_handler,
            "feedback":          feedback_menu,
            "about":             about_handler,
            "profile":           show_profile,
            "my_orders":         my_orders_handler,
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
            "adm:pages":         pages_menu,
            "broadcast_go":      broadcast_go,
            "tariff_add":        tariff_add,
            "adm_create_promo":  adm_create_promo,
            "toggle_maintenance":toggle_maintenance,
            "toggle_orders":     toggle_orders,
            "edit_payment":      edit_payment,
            "edit_welcome":      edit_welcome,
            "toggle_ai_receipts":toggle_ai_receipts,
            "toggle_ai_deploy":  toggle_ai_deploy,
            "toggle_ai_support": toggle_ai_support,
        }
        if d in routes:
            return await routes[d](update, context)

        # Prefix маршрути
        if d.startswith("tar:"):              return await select_tariff(update, context)
        if d.startswith("sex:"):              return await select_sex(update, context)
        if d.startswith("rights:"):           return await select_rights(update, context)
        if d.startswith("zagran:"):           return await select_zagran(update, context)
        if d.startswith("diploma:"):          return await select_diploma(update, context)
        if d.startswith("adm_approve:"):      return await adm_approve(update, context)
        if d.startswith("adm_approve_deploy:"): return await adm_approve_deploy(update, context)
        if d.startswith("adm_reject:"):       return await adm_reject(update, context)
        if d.startswith("adm_complete:"):     return await adm_complete(update, context)
        if d.startswith("adm_push_pages:"):   return await adm_push_pages(update, context)
        if d.startswith("adm_push_go:"):      return await adm_push_go(update, context)
        if d.startswith("adm_send_link:"):    return await adm_send_link(update, context)
        if d.startswith("confirm_withdraw:"): return await adm_confirm_withdraw(update, context)
        if d.startswith("adm_order_view:"):   return await adm_order_view(update, context)
        if d.startswith("adm_order_filter:"): return await adm_order_filter(update, context)
        if d.startswith("tariff_toggle:"):    return await tariff_toggle(update, context)
        if d.startswith("tariff_edit:"):      return await tariff_edit(update, context)
        if d.startswith("tariff_del:"):       return await tariff_del(update, context)
        if d.startswith("tedit_name:"):       return await tedit_name(update, context)
        if d.startswith("tedit_price:"):      return await tedit_price(update, context)
        if d.startswith("tedit_emoji:"):      return await tedit_emoji(update, context)
        if d.startswith("promo_toggle:"):     return await promo_toggle(update, context)
        if d.startswith("promo_del:"):        return await promo_del(update, context)
        if d.startswith("reply_fb:"):         return await reply_fb(update, context)
        if d.startswith("adm_ban:"):          return await adm_ban(update, context)
        if d.startswith("adm_vip:"):          return await adm_vip(update, context)
        if d.startswith("adm_msg:"):          return await adm_msg(update, context)

        logger.warning("Unhandled callback: %s", d)

    except Exception as e:
        logger.error("button_handler error [%s]: %s", d, e, exc_info=True)
        try:
            await q.message.reply_text("😔 Сталася помилка. Спробуйте ще раз або натисніть /start.")
        except Exception:
            pass

# ─────────────────────────────────────────
#  КОМАНДИ
# ─────────────────────────────────────────
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Немає доступу.")
        return
    users  = safe_load(USERS_FILE)
    orders = safe_load(ORDERS_FILE)
    pending = sum(1 for o in orders.values() if o.get("status") == "pending")
    kb = mkb(
        [InlineKeyboardButton("📊 Статистика",   callback_data="adm:stats"),
         InlineKeyboardButton("📋 Замовлення",   callback_data="adm:orders")],
        [InlineKeyboardButton("👥 Користувачі",  callback_data="adm:users"),
         InlineKeyboardButton("💰 Тарифи",       callback_data="adm:tariffs")],
        [InlineKeyboardButton("🎟️ Промо",        callback_data="adm:promos"),
         InlineKeyboardButton("📢 Розсилка",     callback_data="adm:broadcast")],
        [InlineKeyboardButton("⚙️ Налаштування", callback_data="adm:settings"),
         InlineKeyboardButton("🌐 GitHub Pages", callback_data="adm:pages")],
        [InlineKeyboardButton("📜 Логи",         callback_data="adm:logs")],
    )
    await update.message.reply_text(
        f"👑 <b>Адмін-панель</b>\n\n👥 {len(users)} юзерів | 📦 {pending} в черзі\n🕐 {now_fmt()}",
        reply_markup=kb, parse_mode="HTML",
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    users   = safe_load(USERS_FILE)
    orders  = safe_load(ORDERS_FILE)
    revenue = sum(o.get("final_price", 0) for o in orders.values() if o.get("status") in ("completed","deployed"))
    await update.message.reply_text(
        f"📊 <b>Швидка статистика</b>\n\n"
        f"👥 {len(users)} юзерів\n📦 {len(orders)} замовлень\n💰 Дохід: {revenue}₴\n🕐 {now_fmt()}",
        parse_mode="HTML",
    )

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    context.user_data["state"] = AWAIT_BROADCAST
    await update.message.reply_text("📢 Введіть текст розсилки:", parse_mode="HTML")

async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Використання: /ban <user_id>"); return
    uid2 = context.args[0]
    users = safe_load(USERS_FILE)
    if uid2 not in users:
        await update.message.reply_text("❌ Користувача не знайдено"); return
    users[uid2]["banned"] = True
    safe_save(USERS_FILE, users)
    log_action("ban", update.effective_user.id, {"target": uid2})
    await update.message.reply_text(f"🚫 Користувача {uid2} заблоковано.")

async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Використання: /unban <user_id>"); return
    uid2 = context.args[0]
    users = safe_load(USERS_FILE)
    if uid2 in users:
        users[uid2]["banned"] = False
        safe_save(USERS_FILE, users)
    await update.message.reply_text(f"✅ Користувача {uid2} розблоковано.")

async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if len(context.args) < 2:
        await update.message.reply_text("Використання: /balance <user_id> <amount>"); return
    uid2, amount = context.args[0], int(context.args[1])
    users = safe_load(USERS_FILE)
    if uid2 not in users:
        await update.message.reply_text("❌ Не знайдено"); return
    users[uid2]["balance"] = max(0, users[uid2].get("balance", 0) + amount)
    safe_save(USERS_FILE, users)
    log_action("balance_cmd", update.effective_user.id, {"target": uid2, "amount": amount})
    await update.message.reply_text(f"✅ Баланс {uid2} → {users[uid2]['balance']}₴")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Error: %s", context.error, exc_info=True)
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                f"❌ <b>Помилка бота</b>\n\n{esc(str(context.error)[:300])}",
                parse_mode="HTML",
            )
        except Exception:
            pass

# ─────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────
def main():
    os.makedirs(ORDER_PHOTOS_DIR, exist_ok=True)

    # Перевірки при старті
    if not PAGES_GH_TOKEN:
        logger.warning("⚠️  PAGES_GH_TOKEN не встановлено — деплой на GitHub Pages буде недоступний")
    if not GROUP_CHAT_ID:
        logger.warning("⚠️  GROUP_CHAT_ID не встановлено — повідомлення будуть тільки в особистий чат адміна")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_start))
    app.add_handler(CommandHandler("admin",     cmd_admin))
    app.add_handler(CommandHandler("stats",     cmd_stats))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("ban",       cmd_ban))
    app.add_handler(CommandHandler("unban",     cmd_unban))
    app.add_handler(CommandHandler("balance",   cmd_balance))

    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VIDEO, handle_media))
    app.add_error_handler(error_handler)

    logger.info("🌸 FunsDiia Bot starting... Admins: %s | Group: %s", ADMIN_IDS, GROUP_CHAT_ID)
    print(f"✅ FunsDiia Bot is running! Admins: {ADMIN_IDS} | Group: {GROUP_CHAT_ID}")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    import time as _time
    if os.getenv("GITHUB_ACTIONS") == "true":
        from threading import Thread
        def _heartbeat():
            while True: _time.sleep(60); logger.info("💓 alive")
        Thread(target=_heartbeat, daemon=True).start()
    main()
