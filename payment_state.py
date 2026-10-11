"""Pure payment-state transitions shared by bot handlers and tests."""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

PAID_ORDER_STATUSES = frozenset({"approved", "deployed", "completed"})
UNPAID_ORDER_STATUSES = frozenset({"pending", "rejected", "expired", "cancelled"})
VALID_PAYMENT_STATES = frozenset({"paid", "unpaid", "pending"})


def infer_payment_state(order: Dict[str, Any]) -> str:
    """Return the canonical payment state without mutating an order."""
    explicit_paid = order.get("paid")
    if isinstance(explicit_paid, bool):
        return "paid" if explicit_paid else "unpaid"

    explicit_state = str(order.get("payment_status", "")).strip().lower()
    if explicit_state in VALID_PAYMENT_STATES:
        return explicit_state

    status = str(order.get("status", "")).strip().lower()
    if status in PAID_ORDER_STATUSES:
        return "paid"
    if status in UNPAID_ORDER_STATUSES:
        return "unpaid"
    return "pending"


def set_payment_state(
    order: Dict[str, Any],
    payment_state: str,
    actor_id: Optional[str],
    reason: str,
    changed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Apply a validated payment transition and append an audit entry."""
    state = str(payment_state).strip().lower()
    if state not in VALID_PAYMENT_STATES:
        raise ValueError(f"Unsupported payment state: {payment_state}")

    timestamp = changed_at or datetime.now(timezone.utc).isoformat()
    previous_state = infer_payment_state(order)
    order["payment_status"] = state
    order["paid"] = state == "paid"
    order["payment_updated_at"] = timestamp
    if state == "paid":
        order["paid_at"] = timestamp

    history = order.setdefault("payment_history", [])
    if not isinstance(history, list):
        history = []
        order["payment_history"] = history
    history.append({
        "from": previous_state,
        "to": state,
        "actor_id": str(actor_id) if actor_id is not None else None,
        "reason": str(reason or ""),
        "changed_at": timestamp,
    })
    return order
