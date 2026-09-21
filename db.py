"""
db.py — Firebase Cloud Firestore database layer for FunsDiia Bot
================================================================
Replaces SQLite Cloud / JSON file storage with Google Cloud Firestore.
All public functions mirror the old API so bot.py requires minimal changes.

Credentials are read from env: FIREBASE_SERVICE_ACCOUNT (JSON string)
"""

import json
import logging
import os
import threading
from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore

logger = logging.getLogger(__name__)

# ── Initialization ────────────────────────────────────────────────────────────
_db_client = None
_init_lock = threading.Lock()


def _get_db() -> firestore.Client:
    """Return an initialized Firestore client."""
    global _db_client
    if _db_client is not None:
        return _db_client

    with _init_lock:
        if _db_client is None:
            if not firebase_admin._apps:
                service_account_env = os.getenv("FIREBASE_SERVICE_ACCOUNT", "")
                if not service_account_env:
                    raise RuntimeError(
                        "FIREBASE_SERVICE_ACCOUNT env var is missing or empty. "
                        "Pass the raw JSON key string."
                    )
                try:
                    cred_dict = json.loads(service_account_env)
                    cred = credentials.Certificate(cred_dict)
                    firebase_admin.initialize_app(cred)
                except Exception as e:
                    logger.error("Failed to parse FIREBASE_SERVICE_ACCOUNT JSON: %s", e)
                    raise

            _db_client = firestore.client()
            logger.info("✅ Firebase Cloud Firestore initialized successfully.")
    return _db_client


def init_db() -> None:
    """Initialize connection to Firebase. Replaces SQLite schema setup."""
    try:
        _get_db()
    except Exception as e:
        logger.error("❌ DB init error: %s", e)
        raise


# ── Generic collection helpers ────────────────────────────────────────────────

def _collection_load(collection_name: str, default: Any = None) -> dict:
    """Load all documents from a Firestore collection into a dict {doc_id: data}."""
    if default is None:
        default = {}
    try:
        db = _get_db()
        docs = db.collection(collection_name).stream()
        return {doc.id: doc.to_dict() for doc in docs}
    except Exception as e:
        logger.error("DB load error [%s]: %s", collection_name, e)
        return default


def _collection_save(collection_name: str, records: dict) -> bool:
    """Overwrites collection with records. Deletes removed records and sets updated ones."""
    try:
        db = _get_db()
        coll_ref = db.collection(collection_name)

        # 1. Get existing doc IDs to delete missing ones
        existing_docs = {doc.id for doc in coll_ref.select([]).stream()}
        new_ids = set(records.keys())

        batch = db.batch()
        batch_counter = 0

        # Delete docs not present in records
        for doc_id in existing_docs - new_ids:
            batch.delete(coll_ref.document(str(doc_id)))
            batch_counter += 1
            if batch_counter >= 400:
                batch.commit()
                batch = db.batch()
                batch_counter = 0

        # Set / Update current records
        for rec_id, data in records.items():
            doc_ref = coll_ref.document(str(rec_id))
            batch.set(doc_ref, data if isinstance(data, dict) else {"value": data})
            batch_counter += 1
            if batch_counter >= 400:
                batch.commit()
                batch = db.batch()
                batch_counter = 0

        if batch_counter > 0:
            batch.commit()

        return True
    except Exception as e:
        logger.error("DB save error [%s]: %s", collection_name, e)
        return False


def _doc_upsert_one(collection_name: str, doc_id: str, data: dict) -> bool:
    """Upsert a single document into a collection."""
    try:
        db = _get_db()
        db.collection(collection_name).document(str(doc_id)).set(
            data if isinstance(data, dict) else {"value": data}, merge=True
        )
        return True
    except Exception as e:
        logger.error("DB upsert error [%s/%s]: %s", collection_name, doc_id, e)
        return False


def _doc_delete_one(collection_name: str, doc_id: str) -> bool:
    """Delete a single document by ID."""
    try:
        db = _get_db()
        db.collection(collection_name).document(str(doc_id)).delete()
        return True
    except Exception as e:
        logger.error("DB delete error [%s/%s]: %s", collection_name, doc_id, e)
        return False


# ── USERS ─────────────────────────────────────────────────────────────────────

def load_users() -> dict:
    return _collection_load("users", {})


def save_users(users: dict) -> bool:
    return _collection_save("users", users)


def get_user(uid: str) -> dict:
    try:
        db = _get_db()
        doc = db.collection("users").document(str(uid)).get()
        return doc.to_dict() if doc.exists else {}
    except Exception as e:
        logger.error("get_user error [%s]: %s", uid, e)
        return {}


def save_user(uid: str, data: dict) -> bool:
    return _doc_upsert_one("users", str(uid), data)


def delete_user(uid: str) -> bool:
    return _doc_delete_one("users", str(uid))


# ── ORDERS ────────────────────────────────────────────────────────────────────

def load_orders() -> dict:
    return _collection_load("orders", {})


def save_orders(orders: dict) -> bool:
    return _collection_save("orders", orders)


def get_order(order_id: str) -> dict:
    try:
        db = _get_db()
        doc = db.collection("orders").document(str(order_id)).get()
        return doc.to_dict() if doc.exists else {}
    except Exception as e:
        logger.error("get_order error [%s]: %s", order_id, e)
        return {}


def save_order(order_id: str, data: dict) -> bool:
    return _doc_upsert_one("orders", str(order_id), data)


def delete_order(order_id: str) -> bool:
    return _doc_delete_one("orders", str(order_id))


def get_orders_by_user(uid: str) -> dict:
    """Return all orders for a specific user_id using Firestore query."""
    try:
        db = _get_db()
        docs = db.collection("orders").where("user_id", "==", str(uid)).stream()
        return {doc.id: doc.to_dict() for doc in docs}
    except Exception as e:
        logger.error("get_orders_by_user error [%s]: %s", uid, e)
        return {}


def get_orders_by_status(status: str) -> dict:
    """Return all orders with a given status using Firestore query."""
    try:
        db = _get_db()
        docs = db.collection("orders").where("status", "==", status).stream()
        return {doc.id: doc.to_dict() for doc in docs}
    except Exception as e:
        logger.error("get_orders_by_status error [%s]: %s", status, e)
        return {}


# ── FEEDBACK ──────────────────────────────────────────────────────────────────

def load_feedback() -> dict:
    return _collection_load("feedback", {})


def save_feedback(feedbacks: dict) -> bool:
    return _collection_save("feedback", feedbacks)


def save_feedback_one(fid: str, data: dict) -> bool:
    return _doc_upsert_one("feedback", str(fid), data)


# ── TARIFFS ───────────────────────────────────────────────────────────────────

def load_tariffs_db() -> dict:
    return _collection_load("tariffs", {})


def save_tariffs_db(tariffs: dict) -> bool:
    return _collection_save("tariffs", tariffs)


# ── PROMOS ────────────────────────────────────────────────────────────────────

def load_promos_db() -> dict:
    return _collection_load("promos", {})


def save_promos_db(promos: dict) -> bool:
    return _collection_save("promos", promos)


# ── SETTINGS ──────────────────────────────────────────────────────────────────

def load_settings_db() -> dict:
    """Return all settings stored as separate documents or a unified map."""
    try:
        db = _get_db()
        docs = db.collection("settings").stream()
        res = {}
        for doc in docs:
            d = doc.to_dict()
            res[doc.id] = d.get("value") if "value" in d and len(d) == 1 else d
        return res
    except Exception as e:
        logger.error("load_settings_db error: %s", e)
        return {}


def save_settings_db(settings: dict) -> bool:
    """Upsert key/value settings."""
    try:
        db = _get_db()
        batch = db.batch()
        for key, val in settings.items():
            doc_ref = db.collection("settings").document(str(key))
            data = val if isinstance(val, dict) else {"value": val}
            batch.set(doc_ref, data, merge=True)
        batch.commit()
        return True
    except Exception as e:
        logger.error("save_settings_db error: %s", e)
        return False


def get_setting_db(key: str, default: Any = None) -> Any:
    try:
        db = _get_db()
        doc = db.collection("settings").document(str(key)).get()
        if not doc.exists:
            return default
        d = doc.to_dict()
        return d.get("value") if "value" in d and len(d) == 1 else d
    except Exception as e:
        logger.error("get_setting_db error [%s]: %s", key, e)
        return default


def set_setting_db(key: str, value: Any) -> bool:
    try:
        db = _get_db()
        data = value if isinstance(value, dict) else {"value": value}
        db.collection("settings").document(str(key)).set(data, merge=True)
        return True
    except Exception as e:
        logger.error("set_setting_db error [%s]: %s", key, e)
        return False


# ── ACTION LOGS ───────────────────────────────────────────────────────────────
_LOGS_MAX = 500


def log_action_db(ts: str, action: str, uid: str = None, details: dict = None) -> None:
    """Append action log document to Firestore."""
    try:
        db = _get_db()
        log_entry = {
            "ts": ts,
            "action": action,
            "uid": str(uid) if uid else None,
            "details": details or {},
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("action_logs").add(log_entry)

        # Basic pruning fallback if needed
        # (Recommended: set up TTL Policy directly in Firebase Console for action_logs)
    except Exception as e:
        logger.error("log_action_db error: %s", e)


def load_logs_db() -> list:
    """Return action logs as a list of dicts, newest first."""
    try:
        db = _get_db()
        docs = (
            db.collection("action_logs")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(_LOGS_MAX)
            .stream()
        )
        logs = []
        for doc in docs:
            d = doc.to_dict()
            logs.append({
                "ts": d.get("ts", ""),
                "action": d.get("action", ""),
                "uid": d.get("uid"),
                "details": d.get("details", {}),
            })
        return logs
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
    """Import existing JSON data into Firestore."""
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
        for entry in logs_json:
            log_action_db(
                entry.get("ts", ""),
                entry.get("action", ""),
                entry.get("uid"),
                entry.get("details", {}),
            )
        logger.info("Migrated %d log entries.", len(logs_json))
