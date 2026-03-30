import os
import json
import logging
import io
import random
import re
import pytz
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any, List
from contextlib import contextmanager

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler, 
    ContextTypes, filters, ConversationHandler
)
from telegram.error import TelegramError

# -------------------------
# НАЛАШТУВАННЯ
# -------------------------
load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("❌ Токен бота не знайдено в змінних оточення!")

ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "5423792783"))
TIMEZONE = pytz.timezone("Europe/Kyiv")
BOT_USERNAME = os.getenv("BOT_USERNAME", "FunsDiia_bot")

USERS_FILE = "users_data.json"
ORDERS_FILE = "orders_data.json"
FEEDBACK_FILE = "feedback_data.json"
TARIFFS_FILE = "tariffs_data.json"
REFERRAL_REWARD = 19

# Стандартні тарифи (якщо файл не існує)
DEFAULT_TARIFFS = {
    "1_day": {"name": "🌙 1 день", "price": 20, "days": 1, "emoji": "🌙", "active": True},
    "30_days": {"name": "📅 30 днів", "price": 70, "days": 30, "emoji": "📅", "active": True},
    "90_days": {"name": "🌿 90 днів", "price": 150, "days": 90, "emoji": "🌿", "active": True},
    "180_days": {"name": "🌟 180 днів", "price": 190, "days": 180, "emoji": "🌟", "active": True},
    "forever": {"name": "💎 Назавжди", "price": 250, "days": None, "emoji": "💎", "active": True}
}

PAYMENT_REQUISITES = "💳 Картка: 5355573250476310\n👤 Отримувач: SenseBank"
PAYMENT_LINK = "https://send.monobank.ua/jar/6R3gd9Ew8w"

# Состояния для ConversationHandler
AWAITING_FIO, AWAITING_DOB, AWAITING_SEX, AWAITING_PHOTO, AWAITING_FEEDBACK = range(5)
AWAITING_NEW_TARIFF_NAME, AWAITING_NEW_TARIFF_PRICE, AWAITING_NEW_TARIFF_DAYS = range(5, 8)
AWAITING_BROADCAST_MESSAGE = 8

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO"))
)
logger = logging.getLogger(__name__)

# -------------------------
# УТИЛІТИ ДЛЯ РОБОТИ З ФАЙЛАМИ
# -------------------------
@contextmanager
def file_lock(filename: str, mode: str = 'r'):
    """Просте блокування файлу для уникнення конфліктів"""
    lock_file = filename + '.lock'
    try:
        while os.path.exists(lock_file):
            time.sleep(0.1)
        open(lock_file, 'w').close()
        yield
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)

def safe_load_db(filename: str, default: dict = None) -> dict:
    """Безпечне завантаження даних з обробкою помилок"""
    if default is None:
        default = {}
    
    if not os.path.exists(filename):
        return default
    
    try:
        with file_lock(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Помилка завантаження {filename}: {e}")
        return _repair_json_file(filename) or default
    except Exception as e:
        logger.error(f"Неочікувана помилка при завантаженні {filename}: {e}")
        return default

def safe_save_db(filename: str, data: dict) -> bool:
    """Безпечне збереження даних зі створенням бекапу"""
    try:
        if os.path.exists(filename):
            backup_name = f"{filename}.backup"
            try:
                with open(filename, 'r', encoding='utf-8') as src:
                    with open(backup_name, 'w', encoding='utf-8') as dst:
                        dst.write(src.read())
            except:
                pass
        
        with file_lock(filename, 'w'):
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        return True
    except Exception as e:
        logger.error(f"Помилка збереження {filename}: {e}")
        return False

def _repair_json_file(filename: str) -> Optional[dict]:
    """Намагається відновити пошкоджений JSON файл"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        content = content.strip()
        if not content.endswith('}'):
            content += '}'
        
        last_brace = content.rfind('}')
        if last_brace > 0:
            content = content[:last_brace + 1]
        
        return json.loads(content)
    except:
        return None

# -------------------------
# ФУНКЦІЇ ДЛЯ РОБОТИ З ТАРИФАМИ
# -------------------------
def load_tariffs() -> dict:
    """Завантаження тарифів з файлу"""
    tariffs = safe_load_db(TARIFFS_FILE, DEFAULT_TARIFFS)
    
    # Конвертуємо старі тарифи в новий формат якщо потрібно
    converted = {}
    for key, value in tariffs.items():
        if isinstance(value, dict):
            if "text" in value and "name" not in value:
                # Конвертуємо старий формат
                converted[key] = {
                    "name": value.get("text", key),
                    "price": value.get("price", 0),
                    "days": value.get("days"),
                    "emoji": value.get("emoji", "📦"),
                    "active": value.get("active", True)
                }
            else:
                converted[key] = value
        else:
            converted[key] = {"name": key, "price": 0, "days": None, "emoji": "📦", "active": True}
    
    return converted

def save_tariffs(tariffs: dict) -> bool:
    """Збереження тарифів у файл"""
    return safe_save_db(TARIFFS_FILE, tariffs)

def get_active_tariffs() -> dict:
    """Отримання активних тарифів"""
    tariffs = load_tariffs()
    return {k: v for k, v in tariffs.items() if v.get("active", True)}

def format_tariff_text(tariff_key: str, tariff_data: dict) -> str:
    """Форматування тексту тарифу для відображення"""
    return f"{tariff_data.get('emoji', '📦')} {tariff_data.get('name', tariff_key)} — {tariff_data.get('price', 0)}₴"

# -------------------------
# ГЕНЕРАЦІЯ ДАНИХ
# -------------------------
def generate_rnokpp() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(10))

def generate_passport_number() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(9))

def generate_uznr() -> str:
    year = random.randint(1990, 2010)
    return f"{year}0128-{random.randint(10000, 99999)}"

def generate_prava_number() -> str:
    return f"AUX{random.randint(100000, 999999)}"

def generate_zagran_number() -> str:
    return f"FX{random.randint(100000, 999999)}"

def generate_bank_address() -> str:
    districts = ["Харківський", "Чугуївський", "Ізюмський", "Лозівський", "Богодухівський"]
    cities = ["м. Харків", "м. Чугуїв", "м. Мерефа", "м. Люботин", "смт Пісочин"]
    streets = ["Гарібальді", "Сумська", "Пушкінська", "Полтавський Шлях", "пр. Науки", "Клочківська"]
    
    district = random.choice(districts)
    city = random.choice(cities)
    street = random.choice(streets)
    building = random.randint(1, 150)
    apartment = random.randint(1, 250)
    
    return f"Харківська область, {district} район {city}, вул. {street}, буд. {building}, кв. {apartment}"

def generate_js_content(data: dict) -> str:
    """Генерація вмісту JS файлу з даними"""
    try:
        rnokpp = generate_rnokpp()
        pass_num = generate_passport_number()
        uznr = generate_uznr()
        prava_num = generate_prava_number()
        zagran_num = generate_zagran_number()
        bank_addr = generate_bank_address()

        u_sex = data.get("sex", "Ж")
        sex_ua, sex_en = ("Ч", "M") if u_sex == "M" else ("Ж", "W")
        date_now = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
        date_out = (datetime.now(TIMEZONE) + timedelta(days=3650)).strftime("%d.%m.%Y")
        
        student_number = f"{random.randint(2020, 2024)}{random.randint(100000, 999999)}"
        diploma_number = f"MT-{random.randint(100000, 999999)}"
        
        universities = ["ХНУ імені Каразіна", "НТУ ХПІ", "ХНЕУ імені С. Кузнеця", "ХНМУ", "ХНУРЕ"]
        faculties = ["Фізико-технічний", "Комп'ютерних наук", "Економічний", "Медичний", "Радіоелектроніки"]
        
        university = random.choice(universities)
        fakultet = random.choice(faculties)
        
        date_give_z = (datetime.now(TIMEZONE) - timedelta(days=random.randint(1000, 2000))).strftime("%d.%m.%Y")
        date_out_z = (datetime.now(TIMEZONE) + timedelta(days=random.randint(3000, 4000))).strftime("%d.%m.%Y")
        
        is_rights_enabled = random.choice([True, True, True, False])
        is_zagran_enabled = random.choice([True, True, False])
        is_diploma_enabled = random.choice([True, False])
        is_study_enabled = random.choice([True, True, False])

        return f"""// ========================================
// АВТОМАТИЧНО ЗГЕНЕРОВАНИЙ ФАЙЛ
// ========================================
// Дата: {date_now}
// Замовлення: {data.get('order_id', 'unknown')}
// ========================================

// === ОСНОВНІ ДАНІ ===
var fio                = "{data.get('fio', '')}";
var fio_en             = "{data.get('fio_en', data.get('fio', ''))}";
var birth              = "{data.get('dob', '')}";
var date_give          = "{date_now}";
var date_out           = "{date_out}";
var organ              = "0512";
var rnokpp             = "{rnokpp}";
var uznr               = "{uznr}";
var pass_number        = "{pass_num}";

// === ПРОПИСКА ===
var legalAdress        = "Харківська область";
var live               = "Харківська область";
var bank_adress        = "{bank_addr}";

// === СТАТЬ ===
var sex                = "{sex_ua}";
var sex_en             = "{sex_en}";

// === ВОДІЙСЬКІ ПРАВА ===
var rights_categories  = "A, B";
var prava_number       = "{prava_num}";
var prava_date_give    = "{date_now}";
var prava_date_out     = "{date_out}";
var pravaOrgan         = "0512";

// === ОСВІТА ===
var university         = "{university}";
var fakultet           = "{fakultet}";
var stepen_dip         = "Магістра";
var univer_dip         = "{university}";
var dayout_dip         = "{date_out}";
var special_dip        = "Прикладна математика";
var number_dip         = "{diploma_number}";
var form               = "Очна";

// === ЗАГРАНПАСПОРТ ===
var zagran_number      = "{zagran_num}";
var dateGiveZ          = "{date_give_z}";
var dateOutZ           = "{date_out_z}";

// === СТУДЕНТСЬКИЙ ===
var student_number     = "{student_number}";
var student_date_give  = "{date_now}";
var student_date_out   = "{date_out}";

// === НАЛАШТУВАННЯ ===
var isRightsEnabled    = {str(is_rights_enabled).lower()};
var isZagranEnabled    = {str(is_zagran_enabled).lower()};
var isDiplomaEnabled   = {str(is_diploma_enabled).lower()};
var isStudyEnabled     = {str(is_study_enabled).lower()};

// === ФАЙЛИ ===
var photo_passport     = "1.png";
var photo_rights       = "1.png";
var photo_students     = "1.png";
var photo_zagran       = "1.png";
var signPng            = "sign.png";

// ========================================
"""
    except Exception as e:
        logger.error(f"Помилка генерації JS: {e}")
        return "// Помилка генерації даних"

# -------------------------
# ОСНОВНІ ОБРОБНИКИ
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник команди /start"""
    try:
        uid = str(update.effective_user.id)
        users = safe_load_db(USERS_FILE)
        
        ref_by = None
        if context.args and context.args[0]:
            potential_ref = context.args[0]
            if potential_ref != uid and potential_ref in users:
                ref_by = potential_ref
        
        if uid not in users:
            users[uid] = {
                "username": update.effective_user.username,
                "first_name": update.effective_user.first_name,
                "balance": 0,
                "referred_by": ref_by,
                "ref_count": 0,
                "has_bought": False,
                "joined_date": datetime.now(TIMEZONE).isoformat(),
                "total_spent": 0,
                "language": "uk",
                "blocked": False
            }
            safe_save_db(USERS_FILE, users)
            
            if ref_by:
                try:
                    await context.bot.send_message(
                        ref_by,
                        f"👋 <b>Чудова новина!</b>\n\n"
                        f"Користувач {update.effective_user.first_name} приєднався за вашим посиланням!\n"
                        f"Щойно він зробить перше замовлення, ви отримаєте {REFERRAL_REWARD}₴ на рахунок.",
                        parse_mode="HTML"
                    )
                except:
                    pass

        # Головне меню з красивими кнопками
        kb = [
            [InlineKeyboardButton("🛍️ КАТАЛОГ ТАРИФІВ", callback_data="catalog")],
            [InlineKeyboardButton("👥 РЕФЕРАЛЬНА ПРОГРАМА", callback_data="ref_menu")],
            [InlineKeyboardButton("💬 ЗВОРОТНИЙ ЗВ'ЯЗОК", callback_data="feedback")],
            [InlineKeyboardButton("ℹ️ ПРО НАС", callback_data="about")]
        ]
        
        # Вітальне повідомлення
        welcome_text = (
            f"🌸 <b>Вітаємо, {update.effective_user.first_name}!</b>\n\n"
            f"Раді вітати вас у <b>FunsDiia</b> — вашому надійному помічнику в генерації документів.\n\n"
            f"✨ <b>Що ми пропонуємо:</b>\n"
            f"• 📄 Генерація документів будь-якої складності\n"
            f"• ⚡️ Швидке виконання замовлень\n"
            f"• 💰 Вигідна реферальна програма\n"
            f"• 🎯 Індивідуальний підхід до кожного клієнта\n\n"
            f"Оберіть потрібний розділ нижче 👇"
        )
        
        await update.effective_message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Помилка в start: {e}")
        await update.effective_message.reply_text(
            "😔 <b>Сталася помилка</b>\n\n"
            "Будь ласка, спробуйте пізніше або зв'яжіться з адміністратором.",
            parse_mode="HTML"
        )

async def about_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Інформація про бота"""
    query = update.callback_query
    await query.answer()
    
    text = (
        "ℹ️ <b>Про бота FunsDiia</b>\n\n"
        "Ми — команда професіоналів, яка допомагає людям отримувати необхідні документи швидко та якісно.\n\n"
        "📌 <b>Як це працює:</b>\n"
        "1️⃣ Оберіть відповідний тариф у каталозі\n"
        "2️⃣ Введіть свої дані (ПІБ, дату народження, стать)\n"
        "3️⃣ Надішліть фото 3x4\n"
        "4️⃣ Отримайте готові файли після підтвердження\n\n"
        "💡 <b>Чому обирають нас:</b>\n"
        "• ⚡️ Швидкість виконання — до 10 хвилин\n"
        "• 🎯 Висока якість генерації\n"
        "• 💰 Вигідні ціни та бонуси\n"
        "• 🤝 Індивідуальний підхід\n\n"
        "📞 <b>Контакти для зв'язку:</b>\n"
        "• Адміністратор: @admin\n\n"
        "💰 <b>Оплата:</b>\n"
        "• Картка SenseBank\n"
        "• Monobank (миттєво)\n\n"
        "Дякуємо, що обираєте нас! 🌟"
    )
    
    kb = [[InlineKeyboardButton("🔙 НАЗАД ДО ГОЛОВНОГО", callback_data="home")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML",
        disable_web_page_preview=True
    )

async def feedback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник зворотного зв'язку"""
    query = update.callback_query
    await query.answer()
    
    text = (
        "💬 <b>Зворотний зв'язок</b>\n\n"
        "Ми завжди раді почути вашу думку! 🌸\n\n"
        "📝 <b>Ви можете:</b>\n"
        "• Залишити відгук про роботу бота\n"
        "• Повідомити про помилку або неточність\n"
        "• Запропонувати ідею для покращення\n"
        "• Поставити запитання адміністратору\n\n"
        "✍️ <b>Напишіть ваше повідомлення нижче</b>\n"
        "Ми відповімо вам найближчим часом (зазвичай протягом 30 хвилин)."
    )
    
    kb = [[InlineKeyboardButton("🔙 НАЗАД", callback_data="home")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )
    
    context.user_data["state"] = AWAITING_FEEDBACK

async def handle_feedback_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка повідомлення зі зворотним зв'язком"""
    try:
        uid = str(update.effective_user.id)
        feedback_text = update.message.text
        
        feedbacks = safe_load_db(FEEDBACK_FILE)
        
        feedback_id = hashlib.md5(f"{uid}{time.time()}".encode()).hexdigest()[:8]
        feedbacks[feedback_id] = {
            "user_id": uid,
            "username": update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "feedback": feedback_text,
            "created_at": datetime.now(TIMEZONE).isoformat(),
            "status": "new"
        }
        safe_save_db(FEEDBACK_FILE, feedbacks)
        
        # Відправляємо відгук в адмін-групу
        kb = [[InlineKeyboardButton("✍️ ВІДПОВІСТИ", callback_data=f"reply_feedback:{feedback_id}")]]
        admin_message = (
            f"💬 <b>Новий відгук #{feedback_id}</b>\n\n"
            f"👤 <b>Від:</b> {update.effective_user.first_name}\n"
            f"📱 <b>Username:</b> @{update.effective_user.username}\n"
            f"🆔 <b>ID:</b> {uid}\n"
            f"📅 <b>Час:</b> {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}\n\n"
            f"📝 <b>Повідомлення:</b>\n{feedback_text}\n\n"
            f"⬇️ <i>Натисніть кнопку нижче або зробіть Reply, щоб відповісти</i>"
        )
        
        await context.bot.send_message(
            ADMIN_CHAT_ID,
            admin_message,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode="HTML"
        )
        
        # Підтверджуємо користувачу
        await update.message.reply_text(
            "✅ <b>Дякуємо за ваш відгук!</b>\n\n"
            "Ваше повідомлення отримано. Ми розглянемо його найближчим часом і обов'язково відповімо.\n\n"
            "Гарного дня! 🌸",
            parse_mode="HTML"
        )
        
        context.user_data.clear()
        
    except Exception as e:
        logger.error(f"Помилка в handle_feedback_message: {e}")
        await update.message.reply_text(
            "😔 <b>Помилка</b>\n\n"
            "Не вдалося відправити відгук. Спробуйте пізніше або зв'яжіться з адміністратором напряму.",
            parse_mode="HTML"
        )

async def ref_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню реферальної системи"""
    try:
        query = update.callback_query
        await query.answer()
        
        uid = str(update.effective_user.id)
        users = safe_load_db(USERS_FILE)
        u = users.get(uid, {"balance": 0, "ref_count": 0})
        
        ref_link = f"https://t.me/{BOT_USERNAME}?start={uid}"
        potential_earnings = u.get('ref_count', 0) * REFERRAL_REWARD
        
        text = (
            f"👥 <b>Реферальна програма</b>\n\n"
            f"Запрошуйте друзів та отримуйте бонуси! 🎁\n\n"
            f"💰 <b>Бонус за кожного друга:</b> {REFERRAL_REWARD}₴\n"
            f"💎 <b>Мінімальний вивід:</b> 50₴\n\n"
            f"📊 <b>Ваша статистика:</b>\n"
            f"• 👤 Запрошено друзів: <b>{u.get('ref_count', 0)}</b>\n"
            f"• 💰 Потенційний заробіток: <b>{potential_earnings}₴</b>\n"
            f"• 💳 Поточний баланс: <b>{u.get('balance', 0)}₴</b>\n\n"
            f"🔗 <b>Ваше реферальне посилання:</b>\n"
            f"<code>{ref_link}</code>\n\n"
            f"📱 <i>Поділіться цим посиланням з друзями та заробляйте разом з нами!</i>"
        )
        
        kb = [
            [InlineKeyboardButton("💰 ВИВЕСТИ КОШТИ", callback_data="withdraw")],
            [InlineKeyboardButton("🔙 НАЗАД", callback_data="home")]
        ]
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Помилка в ref_menu: {e}")

async def withdraw_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запит на виведення коштів"""
    query = update.callback_query
    await query.answer()
    
    uid = str(update.effective_user.id)
    users = safe_load_db(USERS_FILE)
    balance = users.get(uid, {}).get("balance", 0)
    
    if balance < 50:
        await query.edit_message_text(
            "❌ <b>Недостатньо коштів</b>\n\n"
            f"Мінімальна сума для виведення: 50₴\n"
            f"Ваш баланс: {balance}₴\n\n"
            f"Запрошуйте більше друзів, щоб накопичити потрібну суму! 🌸",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 НАЗАД", callback_data="ref_menu")
            ]]),
            parse_mode="HTML"
        )
        return
    
    kb = [[InlineKeyboardButton("✅ ПІДТВЕРДИТИ", callback_data=f"confirm_withdraw:{uid}:{balance}")]]
    await context.bot.send_message(
        ADMIN_CHAT_ID,
        f"💰 <b>Запит на виведення коштів</b>\n\n"
        f"👤 <b>Користувач:</b> {update.effective_user.first_name}\n"
        f"📱 <b>Username:</b> @{update.effective_user.username}\n"
        f"🆔 <b>ID:</b> {uid}\n"
        f"💳 <b>Сума:</b> {balance}₴\n"
        f"📅 <b>Час:</b> {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )
    
    await query.edit_message_text(
        "✅ <b>Запит відправлено!</b>\n\n"
        "Ваш запит на виведення коштів передано адміністратору.\n"
        "Очікуйте на зарахування протягом 24 годин.\n\n"
        "Дякуємо за співпрацю! 🌸",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 НАЗАД", callback_data="ref_menu")
        ]]),
        parse_mode="HTML"
    )

async def show_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ каталогу тарифів"""
    query = update.callback_query
    
    tariffs = get_active_tariffs()
    
    kb = []
    for key, tariff in tariffs.items():
        kb.append([InlineKeyboardButton(
            format_tariff_text(key, tariff), 
            callback_data=f"tar:{key}"
        )])
    
    kb.append([InlineKeyboardButton("🔙 НАЗАД", callback_data="home")])
    
    text = (
        "🛍️ <b>Наші тарифи</b>\n\n"
        "Оберіть відповідний пакет:\n\n"
    )
    
    for key, tariff in tariffs.items():
        days_text = "безстроково" if tariff.get('days') is None else f"{tariff.get('days')} днів"
        text += f"{tariff.get('emoji', '📦')} <b>{tariff.get('name')}</b> — {tariff.get('price')}₴ ({days_text})\n"
    
    text += "\nПісля вибору тарифу вам потрібно буде ввести:\n"
    text += "• 📝 ПІБ (українською)\n"
    text += "• 📅 Дату народження\n"
    text += "• 👤 Стать\n"
    text += "• 📸 Фото 3x4\n\n"
    text += "Тисніть на кнопку з потрібним тарифом 👇"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def select_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вибір тарифу"""
    query = update.callback_query
    tariff_key = query.data.split(":")[1]
    
    tariffs = get_active_tariffs()
    
    if tariff_key in tariffs:
        tariff = tariffs[tariff_key]
        context.user_data["tariff"] = tariff_key
        context.user_data["tariff_text"] = format_tariff_text(tariff_key, tariff)
        context.user_data["state"] = AWAITING_FIO
        
        await query.edit_message_text(
            f"{tariff.get('emoji', '📦')} <b>Ви обрали тариф:</b> {tariff.get('name')} — {tariff.get('price')}₴\n\n"
            f"✍️ <b>Введіть ваше ПІБ</b>\n"
            f"(українською мовою, наприклад: Іванов Іван Іванович)\n\n"
            f"📝 <i>Будь ласка, перевірте правильність написання</i>",
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            "❌ <b>Тариф не знайдено</b>\n\n"
            "Спробуйте обрати інший тариф.",
            parse_mode="HTML"
        )

async def select_sex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вибір статі"""
    query = update.callback_query
    context.user_data["sex"] = query.data.split(":")[1]
    context.user_data["state"] = AWAITING_PHOTO
    
    sex_text = "чоловік" if context.user_data["sex"] == "M" else "жінка"
    
    await query.edit_message_text(
        f"✅ <b>Стать обрано:</b> {sex_text}\n\n"
        f"📸 <b>Надішліть ваше фото</b>\n\n"
        f"Вимоги до фото:\n"
        f"• 📏 Формат 3x4\n"
        f"• 👤 Обличчя має бути добре видно\n"
        f"• 🎨 Бажано на світлому фоні\n"
        f"• 📱 Можна зробити фото на телефон\n\n"
        f"<i>Надішліть фото одним повідомленням</i>",
        parse_mode="HTML"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник текстових повідомлень"""
    try:
        if update.effective_chat.id == ADMIN_CHAT_ID and update.message.reply_to_message:
            await handle_admin_reply(update, context)
            return

        state = context.user_data.get("state")
        
        # Обробка адмін-функцій
        if state in [AWAITING_NEW_TARIFF_NAME, AWAITING_NEW_TARIFF_PRICE, AWAITING_NEW_TARIFF_DAYS]:
            await handle_new_tariff_input(update, context)
            return
        
        if state == AWAITING_BROADCAST_MESSAGE:
            await handle_broadcast_message(update, context)
            return
        
        if state == AWAITING_FEEDBACK:
            await handle_feedback_message(update, context)
            return
        
        if state == AWAITING_FIO:
            fio = update.message.text.strip()
            if len(fio.split()) < 2:
                await update.message.reply_text(
                    "❌ <b>Помилка</b>\n\n"
                    "Будь ласка, введіть повне ПІБ (мінімум 2 слова).\n"
                    "Наприклад: Іванов Іван Іванович",
                    parse_mode="HTML"
                )
                return
            
            context.user_data["fio"] = fio
            context.user_data["state"] = AWAITING_DOB
            await update.message.reply_text(
                "📅 <b>Дата народження</b>\n\n"
                "Введіть дату у форматі: <b>ДД.ММ.РРРР</b>\n"
                "Наприклад: 01.01.1990\n\n"
                "<i>Переконайтеся, що дата введена правильно</i>",
                parse_mode="HTML"
            )
            
        elif state == AWAITING_DOB:
            dob = update.message.text.strip()
            date_pattern = r'^\d{2}\.\d{2}\.\d{4}$'
            
            if not re.match(date_pattern, dob):
                await update.message.reply_text(
                    "❌ <b>Неправильний формат</b>\n\n"
                    "Використовуйте формат: <b>ДД.ММ.РРРР</b>\n"
                    "Наприклад: 01.01.1990",
                    parse_mode="HTML"
                )
                return
            
            try:
                day, month, year = map(int, dob.split('.'))
                if not (1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2024):
                    raise ValueError
                
                context.user_data["dob"] = dob
                context.user_data["state"] = AWAITING_SEX
                
                kb = [
                    [
                        InlineKeyboardButton("Чоловік ♂️", callback_data="sex:M"),
                        InlineKeyboardButton("Жінка ♀️", callback_data="sex:W")
                    ]
                ]
                
                await update.message.reply_text(
                    "👤 <b>Виберіть стать:</b>",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode="HTML"
                )
            except:
                await update.message.reply_text(
                    "❌ <b>Неправильна дата</b>\n\n"
                    "Будь ласка, введіть коректну дату народження.",
                    parse_mode="HTML"
                )
        
        else:
            if update.effective_chat.id != ADMIN_CHAT_ID:
                # Пересилаємо повідомлення адміну
                await update.message.forward(ADMIN_CHAT_ID)
                await update.message.reply_text(
                    "💬 <b>Повідомлення передано адміністратору</b>\n\n"
                    "Очікуйте на відповідь найближчим часом.",
                    parse_mode="HTML"
                )
                
    except Exception as e:
        logger.error(f"Помилка в handle_message: {e}")
        await update.message.reply_text(
            "😔 <b>Сталася помилка</b>\n\n"
            "Будь ласка, спробуйте пізніше.",
            parse_mode="HTML"
        )

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник медіа-повідомлень"""
    try:
        uid = str(update.effective_user.id)
        state = context.user_data.get("state")
        
        if state == AWAITING_PHOTO and update.message.photo:
            await process_order_photo(update, context, uid)
        else:
            await forward_receipt(update, context, uid)
            
    except Exception as e:
        logger.error(f"Помилка в handle_media: {e}")
        await update.message.reply_text(
            "😔 <b>Помилка при обробці медіа</b>\n\n"
            "Спробуйте ще раз або зв'яжіться з адміністратором.",
            parse_mode="HTML"
        )

async def process_order_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    """Обробка фото для замовлення"""
    try:
        photo_file = await update.message.photo[-1].get_file()
        photo_bytes = await photo_file.download_as_bytearray()
        
        order_id = hashlib.md5(f"{uid}{time.time()}".encode()).hexdigest()[:8]
        context.user_data["order_id"] = order_id
        
        js_content = generate_js_content(context.user_data)
        
        p_io = io.BytesIO(photo_bytes)
        js_io = io.BytesIO(js_content.encode('utf-8'))
        
        p_io.name = f"photo_{order_id}.png"
        js_io.name = f"values_{order_id}.js"
        
        orders = safe_load_db(ORDERS_FILE)
        orders[order_id] = {
            "user_id": uid,
            "tariff": context.user_data.get("tariff"),
            "fio": context.user_data.get("fio"),
            "dob": context.user_data.get("dob"),
            "sex": context.user_data.get("sex"),
            "created_at": datetime.now(TIMEZONE).isoformat(),
            "status": "pending"
        }
        safe_save_db(ORDERS_FILE, orders)
        
        await process_referral_bonus(update, context, uid)
        
        kb = [[InlineKeyboardButton("✅ ПІДТВЕРДИТИ", callback_data=f"adm_ok:{uid}:{order_id}")]]
        caption = (
            f"📦 <b>Нове замовлення #{order_id}</b>\n\n"
            f"👤 <b>ID:</b> {uid}\n"
            f"💎 <b>Тариф:</b> {context.user_data.get('tariff_text')}\n"
            f"📝 <b>ПІБ:</b> {context.user_data['fio']}\n"
            f"📅 <b>Дата народження:</b> {context.user_data['dob']}\n"
            f"👤 <b>Стать:</b> {'Чоловік' if context.user_data.get('sex') == 'M' else 'Жінка'}\n"
            f"⏰ <b>Час:</b> {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}"
        )
        
        await context.bot.send_document(
            ADMIN_CHAT_ID,
            p_io,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode="HTML"
        )
        
        await context.bot.send_document(ADMIN_CHAT_ID, js_io)
        
        await update.message.reply_text(
            "✅ <b>Дані отримано!</b>\n\n"
            "Дякуємо за замовлення! 🌸\n\n"
            "📌 <b>Що далі?</b>\n"
            "1️⃣ Адміністратор перевірить ваші дані (зазвичай до 10 хвилин)\n"
            "2️⃣ Ви отримаєте реквізити для оплати\n"
            "3️⃣ Після оплати надішліть чек сюди\n"
            "4️⃣ Отримаєте готові файли\n\n"
            "Очікуйте на повідомлення!",
            parse_mode="HTML"
        )
        
        context.user_data.clear()
        
    except Exception as e:
        logger.error(f"Помилка в process_order_photo: {e}")
        await update.message.reply_text(
            "❌ <b>Помилка при обробці замовлення</b>\n\n"
            "Будь ласка, спробуйте ще раз або зв'яжіться з адміністратором.",
            parse_mode="HTML"
        )

async def forward_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    """Пересилання чека адміністратору"""
    try:
        forwarded = await update.message.forward(ADMIN_CHAT_ID)
        
        user_info = (
            f"📑 <b>Чек від користувача</b>\n\n"
            f"👤 <b>ID:</b> {uid}\n"
            f"📱 <b>Username:</b> @{update.effective_user.username}\n"
            f"💫 <b>Ім'я:</b> {update.effective_user.first_name}\n"
            f"📅 <b>Час:</b> {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}\n\n"
            f"⬇️ <i>Зробіть Reply на це повідомлення, щоб відповісти</i>"
        )
        
        await context.bot.send_message(
            ADMIN_CHAT_ID,
            user_info,
            reply_to_message_id=forwarded.message_id,
            parse_mode="HTML"
        )
        
        await update.message.reply_text(
            "✅ <b>Чек отримано!</b>\n\n"
            "Дякуємо! Чек передано адміністратору для перевірки.\n"
            "Після підтвердження оплати ви отримаєте готові файли.\n\n"
            "Очікуйте, будь ласка. 🌸",
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Помилка в forward_receipt: {e}")

async def process_referral_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE, uid: str):
    """Обробка реферального бонусу"""
    try:
        users = safe_load_db(USERS_FILE)
        
        if uid in users and not users[uid].get("has_bought", False):
            ref_by = users[uid].get("referred_by")
            
            if ref_by and ref_by in users:
                users[ref_by]["balance"] += REFERRAL_REWARD
                users[ref_by]["ref_count"] += 1
                
                users[uid]["has_bought"] = True
                tariff = context.user_data.get("tariff")
                tariffs = load_tariffs()
                if tariff and tariff in tariffs:
                    users[uid]["total_spent"] = tariffs[tariff]["price"]
                
                safe_save_db(USERS_FILE, users)
                
                try:
                    await context.bot.send_message(
                        ref_by,
                        f"💰 <b>Вітаємо!</b>\n\n"
                        f"Ваш реферал зробив перше замовлення! 🎉\n"
                        f"Вам нараховано <b>{REFERRAL_REWARD}₴</b>\n"
                        f"Поточний баланс: <b>{users[ref_by]['balance']}₴</b>\n\n"
                        f"Дякуємо за співпрацю! 🌸",
                        parse_mode="HTML"
                    )
                except:
                    pass
            else:
                users[uid]["has_bought"] = True
                safe_save_db(USERS_FILE, users)
                
    except Exception as e:
        logger.error(f"Помилка в process_referral_bonus: {e}")

async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка відповідей адміністратора"""
    try:
        reply_msg = update.message.reply_to_message
        text_to_scan = reply_msg.text or reply_msg.caption or ""
        
        # Шукаємо ID користувача в тексті
        found_id = re.search(r"ID:\s*(\d+)", text_to_scan)
        if found_id:
            client_id = found_id.group(1)
            
            await context.bot.send_message(
                client_id,
                f"💬 <b>Відповідь адміністратора:</b>\n\n{update.message.text}\n\n"
                f"🌸 Гарного дня!",
                parse_mode="HTML"
            )
            
            await update.message.reply_text(
                f"✅ Відповідь надіслано клієнту {client_id}",
                parse_mode="HTML"
            )
        else:
            # Перевіряємо, чи це відповідь на відгук
            if "reply_to_user" in context.user_data:
                user_id = context.user_data.get("reply_to_user")
                feedback_id = context.user_data.get("feedback_id")
                
                await context.bot.send_message(
                    user_id,
                    f"💬 <b>Відповідь на ваш відгук:</b>\n\n{update.message.text}\n\n"
                    f"Дякуємо за звернення! 🌸",
                    parse_mode="HTML"
                )
                
                # Оновлюємо статус відгуку
                feedbacks = safe_load_db(FEEDBACK_FILE)
                if feedback_id in feedbacks:
                    feedbacks[feedback_id]["status"] = "replied"
                    feedbacks[feedback_id]["replied_at"] = datetime.now(TIMEZONE).isoformat()
                    feedbacks[feedback_id]["admin_reply"] = update.message.text
                    safe_save_db(FEEDBACK_FILE, feedbacks)
                
                await update.message.reply_text(
                    f"✅ Відповідь на відгук #{feedback_id} надіслано",
                    parse_mode="HTML"
                )
                
                context.user_data.pop("reply_to_user", None)
                context.user_data.pop("feedback_id", None)
            else:
                await update.message.reply_text(
                    "❌ Не вдалося знайти ID клієнта",
                    parse_mode="HTML"
                )
            
    except Exception as e:
        logger.error(f"Помилка в handle_admin_reply: {e}")
        await update.message.reply_text(
            "❌ Помилка при відправці",
            parse_mode="HTML"
        )

async def admin_reply_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник відповіді адміністратора на відгук"""
    try:
        query = update.callback_query
        await query.answer()
        
        feedback_id = query.data.split(":")[1]
        feedbacks = safe_load_db(FEEDBACK_FILE)
        
        if feedback_id in feedbacks:
            feedbacks[feedback_id]["status"] = "read"
            safe_save_db(FEEDBACK_FILE, feedbacks)
            
            user_id = feedbacks[feedback_id]["user_id"]
            
            context.user_data["reply_to_user"] = user_id
            context.user_data["feedback_id"] = feedback_id
            
            await query.edit_message_text(
                f"✍️ <b>Напишіть відповідь користувачу</b>\n\n"
                f"👤 ID: {user_id}\n"
                f"📝 Відгук: {feedbacks[feedback_id]['feedback'][:100]}...\n\n"
                f"<i>Введіть текст відповіді:</i>",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Помилка в admin_reply_feedback: {e}")

async def admin_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Підтвердження замовлення адміністратором"""
    try:
        query = update.callback_query
        await query.answer()
        
        data = query.data.split(":")
        if len(data) >= 2:
            uid = data[1]
            order_id = data[2] if len(data) > 2 else "unknown"
            
            orders = safe_load_db(ORDERS_FILE)
            if order_id in orders:
                orders[order_id]["status"] = "approved"
                orders[order_id]["approved_at"] = datetime.now(TIMEZONE).isoformat()
                safe_save_db(ORDERS_FILE, orders)
            
            payment_text = (
                f"✅ <b>Замовлення #{order_id} підтверджено!</b>\n\n"
                f"💳 <b>Реквізити для оплати:</b>\n"
                f"{PAYMENT_REQUISITES}\n\n"
                f"🔗 <b>Monobank:</b>\n{PAYMENT_LINK}\n\n"
                f"📤 <b>Після оплати:</b>\n"
                f"1️⃣ Зробіть скріншот успішної оплати\n"
                f"2️⃣ Надішліть його в цей чат\n"
                f"3️⃣ Отримайте готові файли\n\n"
                f"Дякуємо, що обираєте нас! 🌸"
            )
            
            await context.bot.send_message(uid, payment_text, parse_mode="HTML")
            
            await query.edit_message_text(
                f"✅ Реквізити надіслано клієнту {uid}\n"
                f"📦 Замовлення #{order_id}",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Помилка в admin_approve: {e}")

async def admin_confirm_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Підтвердження виведення коштів адміністратором"""
    try:
        query = update.callback_query
        await query.answer()
        
        data = query.data.split(":")
        if len(data) >= 3:
            uid = data[1]
            amount = int(data[2])
            
            users = safe_load_db(USERS_FILE)
            if uid in users:
                users[uid]["balance"] = 0
                safe_save_db(USERS_FILE, users)
                
                await context.bot.send_message(
                    uid,
                    f"💰 <b>Виведення коштів підтверджено!</b>\n\n"
                    f"Сума <b>{amount}₴</b> буде надіслана найближчим часом.\n"
                    f"Дякуємо за співпрацю! 🌸",
                    parse_mode="HTML"
                )
                
                await query.edit_message_text(
                    f"✅ Виведення {amount}₴ для користувача {uid} підтверджено",
                    parse_mode="HTML"
                )
    except Exception as e:
        logger.error(f"Помилка в admin_confirm_withdraw: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальний обробник помилок"""
    try:
        logger.error(f"Помилка: {context.error}")
        
        await context.bot.send_message(
            ADMIN_CHAT_ID,
            f"❌ <b>Помилка бота</b>\n\n"
            f"{str(context.error)[:200]}",
            parse_mode="HTML"
        )
    except:
        pass

# ========================
# АДМІН-ФУНКЦІЇ
# ========================

async def admin_panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для входу в адмін-панель"""
    # Перевіряємо чи це адмін (за ID)
    if str(update.effective_user.id) != str(ADMIN_CHAT_ID):
        await update.message.reply_text("❌ У вас немає доступу до адмін-панелі")
        return
    
    # Текст адмін-панелі
    text = (
        "👑 <b>Адмін-панель</b>\n\n"
        "Ласкаво просимо до панелі керування ботом!\n\n"
        "Виберіть дію:"
    )
    
    kb = [
        [InlineKeyboardButton("📊 СТАТИСТИКА", callback_data="admin_stats")],
        [InlineKeyboardButton("💰 УПРАВЛІННЯ ТАРИФАМИ", callback_data="admin_tariffs")],
        [InlineKeyboardButton("📢 РОЗСИЛКА", callback_data="admin_broadcast")],
        [InlineKeyboardButton("👥 КОРИСТУВАЧІ", callback_data="admin_users")],
        [InlineKeyboardButton("💬 ВІДГУКИ", callback_data="admin_feedback_list")],
        [InlineKeyboardButton("🔙 ВИЙТИ", callback_data="home")]
    ]
    
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Головне меню адмін-панелі (для callback)"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    text = (
        "👑 <b>Адмін-панель</b>\n\n"
        "Ласкаво просимо до панелі керування ботом!\n\n"
        "Виберіть дію:"
    )
    
    kb = [
        [InlineKeyboardButton("📊 СТАТИСТИКА", callback_data="admin_stats")],
        [InlineKeyboardButton("💰 УПРАВЛІННЯ ТАРИФАМИ", callback_data="admin_tariffs")],
        [InlineKeyboardButton("📢 РОЗСИЛКА", callback_data="admin_broadcast")],
        [InlineKeyboardButton("👥 КОРИСТУВАЧІ", callback_data="admin_users")],
        [InlineKeyboardButton("💬 ВІДГУКИ", callback_data="admin_feedback_list")],
        [InlineKeyboardButton("🔙 ВИЙТИ", callback_data="home")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика для адміна"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    users = safe_load_db(USERS_FILE)
    orders = safe_load_db(ORDERS_FILE)
    feedbacks = safe_load_db(FEEDBACK_FILE)
    tariffs = load_tariffs()
    
    total_users = len(users)
    active_users = sum(1 for u in users.values() if not u.get("blocked", False))
    total_orders = len(orders)
    completed_orders = sum(1 for o in orders.values() if o.get("status") == "approved")
    pending_orders = sum(1 for o in orders.values() if o.get("status") == "pending")
    total_balance = sum(u.get("balance", 0) for u in users.values())
    total_feedbacks = len(feedbacks)
    new_feedbacks = sum(1 for f in feedbacks.values() if f.get("status") == "new")
    
    # Статистика по тарифах
    tariff_stats = {}
    total_revenue = 0
    for o in orders.values():
        t = o.get("tariff", "unknown")
        tariff_stats[t] = tariff_stats.get(t, 0) + 1
        if o.get("status") == "approved" and t in tariffs:
            total_revenue += tariffs[t].get("price", 0)
    
    tariff_text = ""
    for t_key, t_data in tariffs.items():
        count = tariff_stats.get(t_key, 0)
        if t_data.get("active", True):
            tariff_text += f"• {t_data.get('emoji', '📦')} {t_data.get('name')}: {count} замовлень\n"
    
    current_time = datetime.now(TIMEZONE).strftime("%d.%m.%Y %H:%M:%S")
    
    text = (
        f"📊 <b>Детальна статистика</b>\n\n"
        f"⏰ <b>Час:</b> {current_time}\n\n"
        f"👥 <b>Користувачі:</b>\n"
        f"• Всього: {total_users}\n"
        f"• Активних: {active_users}\n"
        f"• Заблоковано: {total_users - active_users}\n\n"
        f"📦 <b>Замовлення:</b>\n"
        f"• Всього: {total_orders}\n"
        f"• Виконано: {completed_orders}\n"
        f"• В обробці: {pending_orders}\n\n"
        f"💰 <b>Фінанси:</b>\n"
        f"• Загальний баланс: {total_balance}₴\n"
        f"• Загальний дохід: {total_revenue}₴\n\n"
        f"📋 <b>Замовлення по тарифах:</b>\n"
        f"{tariff_text}\n"
        f"💬 <b>Відгуки:</b>\n"
        f"• Всього: {total_feedbacks}\n"
        f"• Нові: {new_feedbacks}"
    )
    
    kb = [[InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def admin_tariffs_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню управління тарифами"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    tariffs = load_tariffs()
    
    text = "💰 <b>Управління тарифами</b>\n\n"
    text += "Активні тарифи:\n\n"
    
    kb = []
    for key, tariff in tariffs.items():
        status = "✅" if tariff.get("active", True) else "❌"
        text += f"{status} {tariff.get('emoji', '📦')} <b>{tariff.get('name')}</b> — {tariff.get('price')}₴\n"
        if tariff.get('days'):
            text += f"   └ Термін: {tariff.get('days')} днів\n"
        else:
            text += f"   └ Термін: Назавжди\n"
        
        # Кнопки для кожного тарифу
        kb.append([
            InlineKeyboardButton(
                f"{'✅' if tariff.get('active') else '❌'} {tariff.get('name')}", 
                callback_data=f"tariff_toggle:{key}"
            ),
            InlineKeyboardButton(
                f"✏️ Ціна", 
                callback_data=f"tariff_edit_price:{key}"
            ),
            InlineKeyboardButton(
                f"📝 Назва", 
                callback_data=f"tariff_edit_name:{key}"
            )
        ])
    
    kb.append([InlineKeyboardButton("➕ ДОДАТИ ТАРИФ", callback_data="tariff_add")])
    kb.append([InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def admin_tariff_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввімкнення/вимкнення тарифу"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    tariff_key = query.data.split(":")[1]
    
    tariffs = load_tariffs()
    if tariff_key in tariffs:
        tariffs[tariff_key]["active"] = not tariffs[tariff_key].get("active", True)
        save_tariffs(tariffs)
        
        await query.answer(f"Тариф {'увімкнено' if tariffs[tariff_key]['active'] else 'вимкнено'}")
    
    await admin_tariffs_menu(update, context)

async def admin_tariff_edit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редагування ціни тарифу"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    tariff_key = query.data.split(":")[1]
    
    context.user_data["editing_tariff"] = tariff_key
    context.user_data["edit_type"] = "price"
    context.user_data["state"] = AWAITING_NEW_TARIFF_PRICE
    
    await query.edit_message_text(
        f"✏️ <b>Редагування ціни тарифу</b>\n\n"
        f"Введіть нову ціну (тільки цифри):\n"
        f"Наприклад: 150",
        parse_mode="HTML"
    )

async def admin_tariff_edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редагування назви тарифу"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    tariff_key = query.data.split(":")[1]
    
    context.user_data["editing_tariff"] = tariff_key
    context.user_data["edit_type"] = "name"
    context.user_data["state"] = AWAITING_NEW_TARIFF_NAME
    
    await query.edit_message_text(
        f"✏️ <b>Редагування назви тарифу</b>\n\n"
        f"Введіть нову назву:\n"
        f"Наприклад: Преміум 30 днів",
        parse_mode="HTML"
    )

async def admin_tariff_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Початок додавання нового тарифу"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    context.user_data["state"] = AWAITING_NEW_TARIFF_NAME
    context.user_data["adding_tariff"] = True
    context.user_data["edit_type"] = "new"
    
    await query.edit_message_text(
        "➕ <b>Додавання нового тарифу</b>\n\n"
        "Крок 1/3: Введіть назву тарифу\n"
        "(наприклад: Преміум 30 днів)",
        parse_mode="HTML"
    )

async def admin_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню розсилки"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    text = (
        "📢 <b>Розсилка повідомлень</b>\n\n"
        "Ви можете надіслати повідомлення всім користувачам бота.\n\n"
        "✍️ Напишіть текст повідомлення для розсилки:\n\n"
        "<i>Підтримується HTML-форматування</i>"
    )
    
    kb = [[InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )
    
    context.user_data["state"] = AWAITING_BROADCAST_MESSAGE

async def admin_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список користувачів"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    users = safe_load_db(USERS_FILE)
    
    # Сортуємо за датою реєстрації (нові перші)
    sorted_users = sorted(
        users.items(), 
        key=lambda x: x[1].get("joined_date", ""), 
        reverse=True
    )[:20]
    
    text = "👥 <b>Останні користувачі:</b>\n\n"
    
    for uid, data in sorted_users:
        status = "✅" if not data.get("blocked", False) else "❌"
        bought = "💰" if data.get("has_bought", False) else "🆕"
        text += f"{status}{bought} <b>{data.get('first_name', 'No name')}</b>\n"
        text += f"   └ ID: {uid}\n"
        text += f"   └ Баланс: {data.get('balance', 0)}₴\n"
        text += f"   └ Запрошено: {data.get('ref_count', 0)}\n"
        text += f"   └ Дата: {data.get('joined_date', '')[:10]}\n\n"
    
    kb = [[InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def admin_feedback_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список відгуків для адміна"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    feedbacks = safe_load_db(FEEDBACK_FILE)
    
    if not feedbacks:
        await query.edit_message_text(
            "📭 <b>Немає відгуків</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")
            ]]),
            parse_mode="HTML"
        )
        return
    
    sorted_feedbacks = sorted(
        feedbacks.items(), 
        key=lambda x: x[1].get("created_at", ""), 
        reverse=True
    )[:10]
    
    text = "📋 <b>Останні відгуки:</b>\n\n"
    
    kb = []
    for fid, data in sorted_feedbacks:
        status = "🟢" if data.get("status") == "new" else "🔵" if data.get("status") == "read" else "🟣"
        short_feedback = data.get('feedback', '')[:30] + "..." if len(data.get('feedback', '')) > 30 else data.get('feedback', '')
        text += f"{status} <b>Відгук #{fid}</b>\n"
        text += f"   👤 {data.get('first_name')} (@{data.get('username')})\n"
        text += f"   📝 {short_feedback}\n"
        text += f"   📅 {data.get('created_at', '')[:16]}\n\n"
        
        kb.append([InlineKeyboardButton(f"💬 Відповісти на #{fid}", callback_data=f"reply_feedback:{fid}")])
    
    kb.append([InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка повідомлення для розсилки"""
    try:
        broadcast_text = update.message.text
        users = safe_load_db(USERS_FILE)
        
        # Підтвердження розсилки
        kb = [
            [
                InlineKeyboardButton("✅ ПІДТВЕРДИТИ", callback_data="broadcast_confirm"),
                InlineKeyboardButton("❌ СКАСУВАТИ", callback_data="admin_panel")
            ]
        ]
        
        context.user_data["broadcast_message"] = broadcast_text
        
        await update.message.reply_text(
            f"📢 <b>Попередній перегляд розсилки:</b>\n\n"
            f"{broadcast_text}\n\n"
            f"👥 <b>Отримають:</b> {len(users)} користувачів\n\n"
            f"Підтвердіть розсилку:",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Помилка в handle_broadcast_message: {e}")
        await update.message.reply_text("❌ Помилка при створенні розсилки")

async def execute_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Виконання розсилки"""
    query = update.callback_query
    
    # Перевіряємо чи це адмін
    if str(query.from_user.id) != str(ADMIN_CHAT_ID):
        await query.answer("❌ У вас немає доступу", show_alert=True)
        return
    
    await query.answer()
    
    broadcast_text = context.user_data.get("broadcast_message")
    if not broadcast_text:
        await query.edit_message_text(
            "❌ <b>Помилка:</b> немає тексту для розсилки",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 НАЗАД", callback_data="admin_panel")
            ]]),
            parse_mode="HTML"
        )
        return
    
    users = safe_load_db(USERS_FILE)
    
    await query.edit_message_text(
        f"📢 <b>Розсилка розпочата</b>\n\n"
        f"Всього користувачів: {len(users)}\n"
        f"Процес може зайняти деякий час...\n\n"
        f"⏳ Будь ласка, зачекайте...",
        parse_mode="HTML"
    )
    
    success = 0
    failed = 0
    blocked = 0
    
    for uid, user_data in users.items():
        if user_data.get("blocked", False):
            blocked += 1
            continue
        
        try:
            await context.bot.send_message(
                uid,
                broadcast_text,
                parse_mode="HTML"
            )
            success += 1
            
            # Невелика затримка щоб не перевантажити API
            if success % 20 == 0:
                time.sleep(1)
                
        except Exception as e:
            failed += 1
            logger.error(f"Помилка відправки користувачу {uid}: {e}")
    
    # Результат розсилки
    result_text = (
        f"📢 <b>Розсилка завершена</b>\n\n"
        f"📊 <b>Результати:</b>\n"
        f"• ✅ Успішно: {success}\n"
        f"• ❌ Помилок: {failed}\n"
        f"• 🔇 Заблоковано: {blocked}\n"
        f"• 👥 Всього: {len(users)}\n\n"
        f"⏰ Час: {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M:%S')}"
    )
    
    await context.bot.send_message(
        ADMIN_CHAT_ID,
        result_text,
        parse_mode="HTML"
    )
    
    # Очищаємо дані
    context.user_data.clear()

async def handle_new_tariff_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка введення даних для нового тарифу або редагування"""
    state = context.user_data.get("state")
    text = update.message.text.strip()
    
    if state == AWAITING_NEW_TARIFF_NAME:
        if context.user_data.get("edit_type") == "name":
            # Редагування назви існуючого тарифу
            tariff_key = context.user_data.get("editing_tariff")
            tariffs = load_tariffs()
            
            if tariff_key in tariffs:
                tariffs[tariff_key]["name"] = text
                save_tariffs(tariffs)
                
                context.user_data.clear()
                
                await update.message.reply_text(
                    f"✅ <b>Назву тарифу успішно змінено!</b>",
                    parse_mode="HTML"
                )
                
                # Повертаємось в адмін-панель
                kb = [[InlineKeyboardButton("🔙 ДО АДМІН-ПАНЕЛІ", callback_data="admin_panel")]]
                await update.message.reply_text(
                    "👑 Оберіть дію:",
                    reply_markup=InlineKeyboardMarkup(kb),
                    parse_mode="HTML"
                )
        else:
            # Додавання нового тарифу
            context.user_data["new_tariff_name"] = text
            context.user_data["state"] = AWAITING_NEW_TARIFF_PRICE
            
            await update.message.reply_text(
                "➕ <b>Крок 2/3:</b>\n\n"
                "Введіть ціну тарифу (тільки цифри):\n"
                "Наприклад: 150",
                parse_mode="HTML"
            )
        
    elif state == AWAITING_NEW_TARIFF_PRICE:
        try:
            price = int(text)
            
            if context.user_data.get("edit_type") == "price":
                # Редагування ціни існуючого тарифу
                tariff_key = context.user_data.get("editing_tariff")
                tariffs = load_tariffs()
                
                if tariff_key in tariffs:
                    tariffs[tariff_key]["price"] = price
                    save_tariffs(tariffs)
                    
                    context.user_data.clear()
                    
                    await update.message.reply_text(
                        f"✅ <b>Ціну тарифу успішно змінено!</b>",
                        parse_mode="HTML"
                    )
                    
                    # Повертаємось в адмін-панель
                    kb = [[InlineKeyboardButton("🔙 ДО АДМІН-ПАНЕЛІ", callback_data="admin_panel")]]
                    await update.message.reply_text(
                        "👑 Оберіть дію:",
                        reply_markup=InlineKeyboardMarkup(kb),
                        parse_mode="HTML"
                    )
            else:
                # Додавання нового тарифу
                context.user_data["new_tariff_price"] = price
                context.user_data["state"] = AWAITING_NEW_TARIFF_DAYS
                
                await update.message.reply_text(
                    "➕ <b>Крок 3/3:</b>\n\n"
                    "Введіть кількість днів дії (0 - якщо назавжди):\n"
                    "Наприклад: 30",
                    parse_mode="HTML"
                )
        except ValueError:
            await update.message.reply_text(
                "❌ Помилка! Введіть число.",
                parse_mode="HTML"
            )
            
    elif state == AWAITING_NEW_TARIFF_DAYS:
        try:
            days = int(text)
            if days == 0:
                days = None
            
            # Генеруємо ключ для тарифу
            name = context.user_data["new_tariff_name"]
            price = context.user_data["new_tariff_price"]
            
            # Створюємо ключ з назви
            key = name.lower().replace(" ", "_").replace("'", "").replace('"', '')[:20]
            
            tariffs = load_tariffs()
            
            # Перевіряємо чи ключ унікальний
            base_key = key
            counter = 1
            while key in tariffs:
                key = f"{base_key}_{counter}"
                counter += 1
            
            # Емоджі для нового тарифу
            emojis = ["🌟", "✨", "🎯", "🎨", "🎭", "🎪", "🎫", "🎬", "🎤", "🎧", "🎲", "🎰", "🎳", "🎮"]
            new_emoji = emojis[len(tariffs) % len(emojis)]
            
            tariffs[key] = {
                "name": name,
                "price": price,
                "days": days,
                "emoji": new_emoji,
                "active": True
            }
            
            save_tariffs(tariffs)
            
            context.user_data.clear()
            
            await update.message.reply_text(
                f"✅ <b>Тариф успішно додано!</b>\n\n"
                f"{new_emoji} {name} — {price}₴\n"
                f"Термін: {'Назавжди' if days is None else f'{days} днів'}",
                parse_mode="HTML"
            )
            
            # Повертаємось в адмін-панель
            kb = [[InlineKeyboardButton("🔙 ДО АДМІН-ПАНЕЛІ", callback_data="admin_panel")]]
            await update.message.reply_text(
                "👑 Оберіть дію:",
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode="HTML"
            )
            
        except ValueError:
            await update.message.reply_text(
                "❌ Помилка! Введіть число.",
                parse_mode="HTML"
            )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для перегляду статистики (тільки для адміна)"""
    # Перевіряємо чи це адмін
    if str(update.effective_user.id) != str(ADMIN_CHAT_ID):
        await update.message.reply_text("❌ У вас немає доступу до статистики")
        return
    
    users = safe_load_db(USERS_FILE)
    orders = safe_load_db(ORDERS_FILE)
    feedbacks = safe_load_db(FEEDBACK_FILE)
    tariffs = load_tariffs()
    
    total_users = len(users)
    active_users = sum(1 for u in users.values() if not u.get("blocked", False))
    total_orders = len(orders)
    completed_orders = sum(1 for o in orders.values() if o.get("status") == "approved")
    pending_orders = sum(1 for o in orders.values() if o.get("status") == "pending")
    total_balance = sum(u.get("balance", 0) for u in users.values())
    total_feedbacks = len(feedbacks)
    new_feedbacks = sum(1 for f in feedbacks.values() if f.get("status") == "new")
    
    # Статистика по тарифах
    tariff_stats = {}
    total_revenue = 0
    for o in orders.values():
        t = o.get("tariff", "unknown")
        tariff_stats[t] = tariff_stats.get(t, 0) + 1
        if o.get("status") == "approved" and t in tariffs:
            total_revenue += tariffs[t].get("price", 0)
    
    tariff_text = ""
    for t_key, t_data in tariffs.items():
        count = tariff_stats.get(t_key, 0)
        if t_data.get("active", True):
            tariff_text += f"• {t_data.get('emoji', '📦')} {t_data.get('name')}: {count} замовлень\n"
    
    # Інформація про час
    current_time = datetime.now(TIMEZONE).strftime("%d.%m.%Y %H:%M:%S")
    
    text = (
        f"📊 <b>Статистика бота FunsDiia</b>\n\n"
        f"⏰ <b>Час:</b> {current_time}\n\n"
        f"👥 <b>Користувачі:</b>\n"
        f"• Всього: {total_users}\n"
        f"• Активних: {active_users}\n"
        f"• Заблоковано: {total_users - active_users}\n\n"
        f"📦 <b>Замовлення:</b>\n"
        f"• Всього: {total_orders}\n"
        f"• Виконано: {completed_orders}\n"
        f"• В обробці: {pending_orders}\n"
        f"• Скасовано: {total_orders - completed_orders - pending_orders}\n\n"
        f"💰 <b>Фінанси:</b>\n"
        f"• Загальний баланс користувачів: {total_balance}₴\n"
        f"• Загальний дохід: {total_revenue}₴\n\n"
        f"📋 <b>Замовлення по тарифах:</b>\n"
        f"{tariff_text}\n"
        f"💬 <b>Відгуки:</b>\n"
        f"• Всього: {total_feedbacks}\n"
        f"• Нові: {new_feedbacks}"
    )
    
    await update.message.reply_text(text, parse_mode="HTML")

# Оновлений button_handler з новими callback
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробник натискань на inline кнопки"""
    query = update.callback_query
    await query.answer()
    
    try:
        # Публічні callback
        if query.data == "home":
            await start(update, context)
        elif query.data == "ref_menu":
            await ref_menu(update, context)
        elif query.data == "about":
            await about_handler(update, context)
        elif query.data == "withdraw":
            await withdraw_handler(update, context)
        elif query.data == "catalog":
            await show_catalog(update, context)
        elif query.data == "feedback":
            await feedback_handler(update, context)
        elif query.data.startswith("tar:"):
            await select_tariff(update, context)
        elif query.data.startswith("sex:"):
            await select_sex(update, context)
        
        # Адмін-функції (з перевіркою доступу)
        elif query.data == "admin_panel":
            await admin_panel(update, context)
        elif query.data == "admin_stats":
            await admin_stats(update, context)
        elif query.data == "admin_tariffs":
            await admin_tariffs_menu(update, context)
        elif query.data == "admin_broadcast":
            await admin_broadcast_menu(update, context)
        elif query.data == "admin_users":
            await admin_users_list(update, context)
        elif query.data == "admin_feedback_list":
            await admin_feedback_list(update, context)
        elif query.data.startswith("adm_ok:"):
            await admin_approve(update, context)
        elif query.data.startswith("confirm_withdraw:"):
            await admin_confirm_withdraw(update, context)
        elif query.data.startswith("reply_feedback:"):
            await admin_reply_feedback(update, context)
        elif query.data.startswith("tariff_toggle:"):
            await admin_tariff_toggle(update, context)
        elif query.data.startswith("tariff_edit_price:"):
            await admin_tariff_edit_price(update, context)
        elif query.data.startswith("tariff_edit_name:"):
            await admin_tariff_edit_name(update, context)
        elif query.data == "tariff_add":
            await admin_tariff_add_start(update, context)
        elif query.data == "broadcast_confirm":
            await execute_broadcast(update, context)
            
    except Exception as e:
        logger.error(f"Помилка в button_handler: {e}")
        await query.edit_message_text(
            "😔 <b>Сталася помилка</b>\n\n"
            "Будь ласка, спробуйте пізніше.",
            parse_mode="HTML"
        )

# -------------------------
# ЗАПУСК БОТА
# -------------------------
def main():
    """Основна функція запуску бота"""
    try:
        app = Application.builder().token(TOKEN).build()
        
        # Команди для всіх
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", start))
        
        # Адмін-команди
        app.add_handler(CommandHandler("admin", admin_panel_command))
        app.add_handler(CommandHandler("stats", stats_command))
        
        # Обробники
        app.add_handler(CallbackQueryHandler(button_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, handle_media))
        
        app.add_error_handler(error_handler)
        
        logger.info("🌸 Бот FunsDiia успішно запущено!")
        print("✅ Бот запущено! Натисніть Ctrl+C для зупинки.")
        print(f"👑 Адмін-панель: /admin (тільки для адміна з ID: {ADMIN_CHAT_ID})")
        
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
    except Exception as e:
        logger.error(f"Критична помилка при запуску: {e}")
        print(f"❌ Критична помилка: {e}")
        raise

# -------------------------
# GitHub Actions Keep Alive
# -------------------------
if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        import time
        from threading import Thread
        
        def keep_alive():
            while True:
                time.sleep(60)
                logger.info("🌸 Бот працює...")
        
        Thread(target=keep_alive, daemon=True).start()
    
    main()