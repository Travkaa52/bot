"""
db.py — SQLite Cloud database layer for FunsDiia Bot
=====================================================
Replaces JSON file storage with SQLite Cloud.
All public functions mirror the old safe_load/safe_save API so bot.py
requires minimal changes.

Connection string is read from env: SQLITECLOUD_URL
Format: sqlitecloud://user:password@host/database
"""

import json
import logging
import os
import threading
from contextlib import contextmanager
from typing import Any

import sqlitecloud

logger = logging.getLogger(__name__)

# ── Connection ────────────────────────────────────────────────────────────────
_SQLITECLOUD_URL = os.getenv("SQLITECLOUD_URL", "")
_local = threading.local()   # thread-local connection cache


def _get_conn() -> sqlitecloud.Connection:
    """Return a thread-local SQLite Cloud connection (create if needed)."""
    if not hasattr(_local, "conn") or _local.conn is None:
        if not _SQLITECLOUD_URL:
            raise RuntimeError(
                "SQLITECLOUD_URL env var is not set. "
                "Format: sqlitecloud://user:password@host/database"
            )
        _local.conn = sqlitecloud.connect(_SQLITECLOUD_URL)
    return _local.conn


@contextmanager
def _cursor():
    """Yield a cursor; commit on success, rollback on error."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


# ── Schema bootstrap ──────────────────────────────────────────────────────────
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id     TEXT PRIMARY KEY,
    data        TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    data        TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS feedback (
    feedback_id TEXT PRIMARY KEY,
    data        TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS tariffs (
    tariff_id   TEXT PRIMARY KEY,
    data        TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS promos (
    promo_code  TEXT PRIMARY KEY,
    data        TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL DEFAULT 'null'
);

CREATE TABLE IF NOT EXISTS action_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    action      TEXT NOT NULL,
    uid         TEXT,
    details     TEXT NOT NULL DEFAULT '{}'
);
"""

_schema_initialized = False
_schema_lock = threading.Lock()


def init_db() -> None:
    """Create tables if they don't exist. Call once at bot startup."""
    global _schema_initialized
    with _schema_lock:
        if _schema_initialized:
            return
        try:
            conn = _get_conn()
            cur = conn.cursor()
            for stmt in _SCHEMA_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            conn.commit()
            cur.close()
            _schema_initialized = True
            logger.info("✅ SQLite Cloud schema initialized.")
        except Exception as e:
            logger.error("❌ DB init error: %s", e)
            raise


# ── Generic key-value helpers ─────────────────────────────────────────────────

def _table_load(table: str, id_col: str, default: Any = None) -> dict:
    """
    Load all rows from `table` and return {id: data_dict}.
    `default` is returned on error.
    """
    if default is None:
        default = {}
    try:
        with _cursor() as cur:
            cur.execute(f"SELECT {id_col}, data FROM {table}")
            rows = cur.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}
    except Exception as e:
        logger.error("DB load error [%s]: %s", table, e)
        return default


def _table_save(table: str, id_col: str, records: dict) -> bool:
    """
    Upsert all records into `table`.
    `records` is a dict {id: data_dict}.
    """
    try:
        with _cursor() as cur:
            # Remove rows that are no longer present
            if records:
                placeholders = ",".join("?" * len(records))
                cur.execute(
                    f"DELETE FROM {table} WHERE {id_col} NOT IN ({placeholders})",
                    list(records.keys()),
                )
            else:
                cur.execute(f"DELETE FROM {table}")
            # Upsert
            for rec_id, data in records.items():
                cur.execute(
                    f"INSERT INTO {table} ({id_col}, data) VALUES (?, ?) "
                    f"ON CONFLICT({id_col}) DO UPDATE SET data=excluded.data",
                    (rec_id, json.dumps(data, ensure_ascii=False)),
                )
        return True
    except Exception as e:
        logger.error("DB save error [%s]: %s", table, e)
        return False


def _table_upsert_one(table: str, id_col: str, rec_id: str, data: dict) -> bool:
    """Upsert a single record (more efficient than rewriting the whole table)."""
    try:
        with _cursor() as cur:
            cur.execute(
                f"INSERT INTO {table} ({id_col}, data) VALUES (?, ?) "
                f"ON CONFLICT({id_col}) DO UPDATE SET data=excluded.data",
                (rec_id, json.dumps(data, ensure_ascii=False)),
            )
        return True
    except Exception as e:
        logger.error("DB upsert error [%s/%s]: %s", table, rec_id, e)
        return False


def _table_delete_one(table: str, id_col: str, rec_id: str) -> bool:
    try:
        with _cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE {id_col} = ?", (rec_id,))
        return True
    except Exception as e:
        logger.error("DB delete error [%s/%s]: %s", table, rec_id, e)
        return False


# ── USERS ─────────────────────────────────────────────────────────────────────

def load_users() -> dict:
    return _table_load("users", "user_id", {})


def save_users(users: dict) -> bool:
    return _table_save("users", "user_id", users)


def get_user(uid: str) -> dict:
    try:
        with _cursor() as cur:
            cur.execute("SELECT data FROM users WHERE user_id = ?", (str(uid),))
            row = cur.fetchone()
        return json.loads(row[0]) if row else {}
    except Exception as e:
        logger.error("get_user error [%s]: %s", uid, e)
        return {}


def save_user(uid: str, data: dict) -> bool:
    return _table_upsert_one("users", "user_id", str(uid), data)


def delete_user(uid: str) -> bool:
    return _table_delete_one("users", "user_id", str(uid))


# ── ORDERS ────────────────────────────────────────────────────────────────────

def load_orders() -> dict:
    return _table_load("orders", "order_id", {})


def save_orders(orders: dict) -> bool:
    return _table_save("orders", "order_id", orders)


def get_order(order_id: str) -> dict:
    try:
        with _cursor() as cur:
            cur.execute("SELECT data FROM orders WHERE order_id = ?", (order_id,))
            row = cur.fetchone()
        return json.loads(row[0]) if row else {}
    except Exception as e:
        logger.error("get_order error [%s]: %s", order_id, e)
        return {}


def save_order(order_id: str, data: dict) -> bool:
    return _table_upsert_one("orders", "order_id", order_id, data)


def delete_order(order_id: str) -> bool:
    return _table_delete_one("orders", "order_id", order_id)


def get_orders_by_user(uid: str) -> dict:
    """Return all orders for a specific user_id."""
    try:
        with _cursor() as cur:
            cur.execute(
                "SELECT order_id, data FROM orders WHERE json_extract(data, '$.user_id') = ?",
                (str(uid),),
            )
            rows = cur.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}
    except Exception as e:
        logger.error("get_orders_by_user error [%s]: %s", uid, e)
        return {}


def get_orders_by_status(status: str) -> dict:
    """Return all orders with a given status."""
    try:
        with _cursor() as cur:
            cur.execute(
                "SELECT order_id, data FROM orders WHERE json_extract(data, '$.status') = ?",
                (status,),
            )
            rows = cur.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}
    except Exception as e:
        logger.error("get_orders_by_status error [%s]: %s", status, e)
        return {}


# ── FEEDBACK ──────────────────────────────────────────────────────────────────

def load_feedback() -> dict:
    return _table_load("feedback", "feedback_id", {})


def save_feedback(feedbacks: dict) -> bool:
    return _table_save("feedback", "feedback_id", feedbacks)


def save_feedback_one(fid: str, data: dict) -> bool:
    return _table_upsert_one("feedback", "feedback_id", fid, data)


# ── TARIFFS ───────────────────────────────────────────────────────────────────

def load_tariffs_db() -> dict:
    return _table_load("tariffs", "tariff_id", {})


def save_tariffs_db(tariffs: dict) -> bool:
    return _table_save("tariffs", "tariff_id", tariffs)


# ── PROMOS ────────────────────────────────────────────────────────────────────

def load_promos_db() -> dict:
    return _table_load("promos", "promo_code", {})


def save_promos_db(promos: dict) -> bool:
    return _table_save("promos", "promo_code", promos)


# ── SETTINGS (single-row key/value) ──────────────────────────────────────────

def load_settings_db() -> dict:
    """Return all settings as a flat dict."""
    try:
        with _cursor() as cur:
            cur.execute("SELECT key, value FROM settings")
            rows = cur.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}
    except Exception as e:
        logger.error("load_settings_db error: %s", e)
        return {}


def save_settings_db(settings: dict) -> bool:
    """Upsert all settings key/value pairs."""
    try:
        with _cursor() as cur:
            for key, val in settings.items():
                cur.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, json.dumps(val, ensure_ascii=False)),
                )
        return True
    except Exception as e:
        logger.error("save_settings_db error: %s", e)
        return False


def get_setting_db(key: str, default: Any = None) -> Any:
    try:
        with _cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
        return json.loads(row[0]) if row else default
    except Exception as e:
        logger.error("get_setting_db error [%s]: %s", key, e)
        return default


def set_setting_db(key: str, value: Any) -> bool:
    try:
        with _cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value, ensure_ascii=False)),
            )
        return True
    except Exception as e:
        logger.error("set_setting_db error [%s]: %s", key, e)
        return False


# ── ACTION LOGS ───────────────────────────────────────────────────────────────
_LOGS_MAX = 500


def log_action_db(ts: str, action: str, uid: str = None, details: dict = None) -> None:
    """Append an action log entry (keeps last _LOGS_MAX rows)."""
    try:
        with _cursor() as cur:
            cur.execute(
                "INSERT INTO action_logs (ts, action, uid, details) VALUES (?, ?, ?, ?)",
                (ts, action, str(uid) if uid else None,
                 json.dumps(details or {}, ensure_ascii=False)),
            )
            # Prune old entries
            cur.execute(
                "DELETE FROM action_logs WHERE id NOT IN "
                f"(SELECT id FROM action_logs ORDER BY id DESC LIMIT {_LOGS_MAX})"
            )
    except Exception as e:
        logger.error("log_action_db error: %s", e)


def load_logs_db() -> list:
    """Return action logs as a list of dicts, newest first."""
    try:
        with _cursor() as cur:
            cur.execute(
                "SELECT ts, action, uid, details FROM action_logs ORDER BY id DESC"
            )
            rows = cur.fetchall()
        return [
            {"ts": r[0], "action": r[1], "uid": r[2], "details": json.loads(r[3])}
            for r in rows
        ]
    except Exception as e:
        logger.error("load_logs_db error: %s", e)
        return []


# ── One-time JSON → DB migration ──────────────────────────────────────────────

def migrate_json_to_db(
    users_json: dict = None,
    orders_json: dict = None,
    feedback_json: dict = None,
    tariffs_json: dict = None,
    promos_json: dict = None,
    settings_json: dict = None,
    logs_json: list = None,
) -> None:
    """
    Import existing JSON data into the DB.
    Pass only the dicts you want to import; omit the rest.
    Safe to call multiple times — uses upsert semantics.
    """
    init_db()
    if users_json:
        save_users(users_json)
        logger.info("Migrated %d users.", len(users_json))
    if orders_json:
        save_orders(orders_json)
        logger.info("Migrated %d orders.", len(orders_json))
    if feedback_json:
        save_feedback(feedback_json)
        logger.info("Migrated %d feedback entries.", len(feedback_json))
    if tariffs_json:
        save_tariffs_db(tariffs_json)
        logger.info("Migrated %d tariffs.", len(tariffs_json))
    if promos_json:
        save_promos_db(promos_json)
        logger.info("Migrated %d promos.", len(promos_json))
    if settings_json:
        save_settings_db(settings_json)
        logger.info("Migrated %d settings.", len(settings_json))
    if logs_json and isinstance(logs_json, list):
        try:
            with _cursor() as cur:
                for entry in logs_json:
                    cur.execute(
                        "INSERT INTO action_logs (ts, action, uid, details) VALUES (?, ?, ?, ?)",
                        (entry.get("ts", ""), entry.get("action", ""),
                         entry.get("uid"), json.dumps(entry.get("details", {}))),
                    )
            logger.info("Migrated %d log entries.", len(logs_json))
        except Exception as e:
            logger.error("Log migration error: %s", e)
