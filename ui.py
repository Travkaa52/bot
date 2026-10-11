"""Shared Telegram presentation primitives for the FunsDiia bot."""

from typing import Optional

DIVIDER = "━━━━━━━━━━━━━━━━━━━━"
SHORT_DIVIDER = "──────────────"


def heading(icon: str, title: str, subtitle: Optional[str] = None) -> str:
    text = f"{icon} <b>{title}</b>\n{DIVIDER}"
    if subtitle:
        text += f"\n{subtitle}"
    return text


def section(title: str) -> str:
    return f"\n<b>{title}</b>\n{SHORT_DIVIDER}"


def row(label: str, value: object, icon: str = "▫️") -> str:
    return f"{icon} <b>{label}:</b> {value}"


def status(label: str, value: str, icon: str = "●") -> str:
    return f"{icon} <b>{label}:</b> <b>{value}</b>"


def progress(value: int, width: int = 10) -> str:
    value = max(0, min(100, int(value)))
    filled = round(value / 100 * width)
    return f"{'🟩' * filled}{'⬜' * (width - filled)} {value}%"


def success(message: str, next_step: Optional[str] = None) -> str:
    text = f"✅ <b>Готово</b>\n\n{message}"
    return f"{text}\n\n➡️ {next_step}" if next_step else text


def error(message: str, next_step: Optional[str] = None) -> str:
    text = f"⚠️ <b>Не вдалося виконати дію</b>\n\n{message}"
    return f"{text}\n\n➡️ {next_step}" if next_step else text


def empty(title: str, message: str, action: Optional[str] = None) -> str:
    text = f"📭 <b>{title}</b>\n\n{message}"
    return f"{text}\n\n➡️ {action}" if action else text


def home_intro(name: str, balance: object, vip: bool) -> str:
    badge = " · VIP" if vip else ""
    return (
        heading("🌸", f"Вітаємо, {name}{badge}", "Ваш персональний центр FunsDiia")
        + f"\n\n{row('Баланс', f'{balance}₴', '💰')}"
        + "\n\nОберіть потрібний розділ нижче."
    )


def admin_overview(users: int, orders: int, pending: int, revenue: object) -> str:
    return (
        heading("🛡️", "Центр управління", "Стан системи та швидкий доступ до операцій")
        + section("Ключові показники")
        + f"\n{row('Користувачі', users, '👥')}"
        + f"\n{row('Замовлення', orders, '📦')}"
        + f"\n{row('Очікують перевірки', pending, '⏳')}"
        + f"\n{row('Підтверджений дохід', f'{revenue}₴', '💳')}"
    )
