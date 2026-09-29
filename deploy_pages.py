"""
db.py — Firebase Cloud Firestore Database Layer for FunsDiia System
===================================================================
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Generator, List, Optional, Tuple

import firebase_admin
from firebase_admin import credentials, firestore

logger = logging.getLogger("db")

_db_client: Optional[firestore.Client] = None
_init_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=4)

_CACHE_TTL = 60
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


def _get_db() -> firestore.Client:
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
                        cred = credentials.Certificate(json.loads(service_account_env))
                    elif service_account_file and os.path.exists(service_account_file):
                        cred = credentials.Certificate(service_account_file)
                    else:
                        raise RuntimeError("Відсутні ключі доступу FIREBASE_SERVICE_ACCOUNT!")
                    firebase_admin.initialize_app(cred)
                except Exception as e:
                    logger.error("Failed to initialize Firebase Admin SDK: %s", e)
                    raise

            _db_client = firestore.client()
            logger.info("✅ Firebase Cloud Firestore ініціалізовано.")
    return _db_client


def init_db() -> None:
    _get_db()


def _chunked(iterable: List[Any], size: int = 400) -> Generator[List[Any], None, None]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _collection_load(collection_name: str) -> dict:
    try:
        db = _get_db()
        docs = db.collection(collection_name).stream()
        return {doc.id: doc.to_dict() for doc in docs}
    except Exception as e:
        logger.error("DB load error [%s]: %s", collection_name, e)
        return {}


def _collection_save(collection_name: str, records: dict) -> bool:
    try:
        db = _get_db()
        coll_ref = db.collection(collection_name)
        existing_docs = {doc.id for doc in coll_ref.select([]).stream()}
        new_ids = set(str(k) for k in records.keys())

        # Видалення застарілих
        for chunk in _chunked(list(existing_docs - new_ids)):
            batch = db.batch()
            for doc_id in chunk:
                batch.delete(coll_ref.document(str(doc_id)))
            batch.commit()

        # Запис нових
        for chunk in _chunked(list(records.items())):
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
    try:
        db = _get_db()
        payload = data if isinstance(data, dict) else {"value": data}
        db.collection(collection_name).document(str(doc_id)).set(payload, merge=True)
        _invalidate_cache(collection_name)
        return True
    except Exception as e:
        logger.error("DB upsert error [%s/%s]: %s", collection_name, doc_id, e)
        return False


# ── USERS & BALANCE ──
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


def update_user_balance(uid: str, amount_delta: float) -> bool:
    try:
        db = _get_db()
        doc_ref = db.collection("users").document(str(uid))
        doc_ref.update({"balance": firestore.Increment(amount_delta)})
        return True
    except Exception as e:
        logger.error("update_user_balance error [%s]: %s", uid, e)
        return False


# ── ORDERS ──
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


def get_orders_by_user(uid: str) -> dict:
    try:
        db = _get_db()
        docs = db.collection("orders").where("user_id", "==", str(uid)).stream()
        return {doc.id: doc.to_dict() for doc in docs}
    except Exception as e:
        logger.error("get_orders_by_user error [%s]: %s", uid, e)
        return {}


# ── LOGS ──
def log_action_db(ts: str, action: str, uid: Optional[str] = None, details: Optional[dict] = None) -> None:
    def _task():
        try:
            db = _get_db()
            db.collection("action_logs").add({
                "ts": ts,
                "action": action,
                "uid": str(uid) if uid else None,
                "details": details or {},
                "created_at": firestore.SERVER_TIMESTAMP,
            })
        except Exception as e:
            logger.error("Async log error: %s", e)

    _executor.submit(_task)
