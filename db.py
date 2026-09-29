"""
db.py — Firebase Cloud Firestore database layer for FunsDiia Bot
================================================================
Replaces SQLite Cloud / JSON file storage with Google Cloud Firestore.
All public functions mirror the old API so bot.py requires minimal changes.

Credentials read from:
 - FIREBASE_SERVICE_ACCOUNT (JSON string) OR
 - FIREBASE_SERVICE_ACCOUNT_FILE (path to json file)
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple, Generator

import firebase_admin
from firebase_admin import credentials, firestore

logger = logging.getLogger(__name__)

# ── Initialization & Threading ───────────────────────────────────────────────
_db_client: Optional[firestore.Client] = None
_init_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=4)

# ── Simple In-Memory Cache for Heavy-Read / Low-Write Collections ───────────
_CACHE_TTL = 60  # seconds
_cache_store: Dict[str, Tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _get_cached(key: str) -> Optional[Any]:
    with _cache_lock:
        if key in _cache_store:
            timestamp, data = _cache_store[key]
            if time.time() - timestamp < _CACHE_TTL:
                return data
            del _cache_store[key]
    return None


def _set_cached(key: str, data: Any) -> None:
    with _cache_lock:
        _cache_store[key] = (time.time(), data)


def _invalidate_cache(key: str) -> None:
    with _cache_lock:
        _cache_store.pop(key, None)


# ── Database Connection Initialization ───────────────────────────────────────

def _get_db() -> firestore.Client:
    """Return an initialized Firestore client (Thread-safe Singleton)."""
    global _db_client
    if _db_client is not None:
        return _db_client

    with _init_lock:
        if _db_client is None:
            if not firebase_admin._apps:
                service_account_env = os.getenv("FIREBASE_SERVICE_ACCOUNT", "").strip()
                service_account_file = os.getenv("FIREBASE_SERVICE_ACCOUNT_FILE", "").strip()

                try:
                    if service_account_env:
                        cred_dict = json.loads(service_account_env)
                        cred = credentials.Certificate(cred_dict)
                    elif service_account_file and os.path.exists(service_account_file):
                        cred = credentials.Certificate(service_account_file)
                    else:
                        raise RuntimeError(
                            "Neither FIREBASE_SERVICE_ACCOUNT nor valid FIREBASE_SERVICE_ACCOUNT_FILE found."
                        )

                    firebase_admin.initialize_app(cred)
                except Exception as e:
                    logger.error("❌ Failed to initialize Firebase Admin SDK: %s", e)
                    raise

            _db_client = firestore.client()
            logger.info("✅ Firebase Cloud Firestore initialized successfully.")
    return _db_client


def init_db() -> None:
    """Initialize connection to Firebase."""
    try:
        _get_db()
    except Exception as e:
        logger.error("❌ DB init error: %s", e)
        raise


# ── Batch Helper ──────────────────────────────────────────────────────────────

def _chunked(iterable: List[Any], size: int = 400) -> Generator[List[Any], None, None]:
    """Yield successive chunks from iterable to strictly stay under 500 batch limit."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


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
    """
    Overwrites collection with records using chunked batches.
    Deletes removed records and sets updated ones safely.
    """
    try:
        db = _get_db()
        coll_ref = db.collection(collection_name)

        # 1. Fetch current IDs
        existing_docs = {doc.id for doc in coll_ref.select([]).stream()}
        new_ids = set(str(k) for k in records.keys())

        # 2. Process deletions in safe batches
        to_delete = list(existing_docs - new_ids)
        for chunk in _chunked(to_delete, 400):
            batch = db.batch()
            for doc_id in chunk:
                batch.delete(coll_ref.document(str(doc_id)))
            batch.commit()

        # 3. Process sets/updates in safe batches
        to_set = list(records.items())
        for chunk in _chunked(to_set, 400):
            batch = db.batch()
            for rec_id, data in chunk:
                doc_ref = coll_ref.document(str(rec_id))
                payload = data if isinstance(data, dict) else {"value": data}
                batch.set(doc_ref, payload)
            batch.commit()

        _invalidate_cache(collection_name)
        return True
    except Exception as e:
        logger.error("DB save error [%s]: %s", collection_name, e)
        return False


def _doc_upsert_one(collection_name: str, doc_id: str, data: dict) -> bool:
    """Upsert a single document into a collection."""
    try:
        db = _get_db()
        payload = data if isinstance(data, dict) else {"value": data}
        db.collection(collection_name).document(str(doc_id)).set(payload, merge=True)
        _invalidate_cache(collection_name)
        return True
    except Exception as e:
        logger.error("DB upsert error [%s/%s]: %s", collection_name, doc_id, e)
        return False


def _doc_delete_one(collection_name: str, doc_id: str) -> bool:
    """Delete a single document by ID."""
    try:
        db = _get_db()
        db.collection(collection_name).document(str(doc_id)).delete()
        _invalidate_cache(collection_name)
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


def update_user_balance(uid: str, amount_delta: float) -> bool:
    """Атомарне оновлення балансу користувача (підтримує плюс та мінус)."""
    try:
        db = _get_db()
        doc_ref = db.collection("users").document(str(uid))
        doc_ref.update({"balance": firestore.Increment(amount_delta)})
        return True
    except Exception as e:
        logger.error("update_user_balance error [%s]: %s", uid, e)
        return False


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
    try:
        db = _get_db()
        docs = db.collection("orders").where("user_id", "==", str(uid)).stream()
        return {doc.id: doc.to_dict() for doc in docs}
    except Exception as e:
        logger.error("get_orders_by_user error [%s]: %s", uid, e)
        return {}


def get_orders_by_status(status: str) -> dict:
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


# ── TARIFFS (With Caching) ───────────────────────────────────────────────────

def load_tariffs_db() -> dict:
    cached = _get_cached("tariffs")
    if cached is not None:
        return cached

    data = _collection_load("tariffs", {})
    _set_cached("tariffs", data)
    return data


def save_tariffs_db(tariffs: dict) -> bool:
    return _collection_save("tariffs", tariffs)


# ── PROMOS (With Caching) ─────────────────────────────────────────────────────

def load_promos_db() -> dict:
    cached = _get_cached("promos")
    if cached is not None:
        return cached

    data = _collection_load("promos", {})
    _set_cached("promos", data)
    return data


def save_promos_db(promos: dict) -> bool:
    return _collection_save("promos", promos)


# ── SETTINGS (With Caching) ───────────────────────────────────────────────────

def load_settings_db() -> dict:
    cached = _get_cached("settings")
    if cached is not None:
        return cached

    try:
        db = _get_db()
        docs = db.collection("settings").stream()
        res = {}
        for doc in docs:
            d = doc.to_dict()
            res[doc.id] = d.get("value") if "value" in d and len(d) == 1 else d
        _set_cached("settings", res)
        return res
    except Exception as e:
        logger.error("load_settings_db error: %s", e)
        return {}


def save_settings_db(settings: dict) -> bool:
    try:
        db = _get_db()
        for chunk in _chunked(list(settings.items()), 400):
            batch = db.batch()
            for key, val in chunk:
                doc_ref = db.collection("settings").document(str(key))
                data = val if isinstance(val, dict) else {"value": val}
                batch.set(doc_ref, data, merge=True)
            batch.commit()

        _invalidate_cache("settings")
        return True
    except Exception as e:
        logger.error("save_settings_db error: %s", e)
        return False


def get_setting_db(key: str, default: Any = None) -> Any:
    settings = load_settings_db()
    return settings.get(str(key), default)


def set_setting_db(key: str, value: Any) -> bool:
    res = _doc_upsert_one("settings", str(key), value)
    _invalidate_cache("settings")
    return res


# ── ACTION LOGS (Async Logging Option) ────────────────────────────────────────

_LOGS_MAX = 500


def _async_log_task(ts: str, action: str, uid: Optional[str], details: dict) -> None:
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
    except Exception as e:
        logger.error("Async log_action_db error: %s", e)


def log_action_db(ts: str, action: str, uid: str = None, details: dict = None, sync: bool = False) -> None:
    """Append action log document to Firestore (Default: Non-blocking async thread execution)."""
    if sync:
        _async_log_task(ts, action, uid, details)
    else:
        _executor.submit(_async_log_task, ts, action, uid, details)


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
    """Import existing JSON data into Firestore safely."""
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
                sync=True
            )
        logger.info("Migrated %d log entries.", len(logs_json))
