import unittest

from payment_state import infer_payment_state, set_payment_state


class PaymentStateTests(unittest.TestCase):
    def test_legacy_status_is_inferred(self):
        self.assertEqual(infer_payment_state({"status": "approved"}), "paid")
        self.assertEqual(infer_payment_state({"status": "pending"}), "unpaid")

    def test_explicit_paid_has_priority(self):
        self.assertEqual(infer_payment_state({"paid": True, "status": "pending"}), "paid")
        self.assertEqual(infer_payment_state({"paid": False, "status": "deployed"}), "unpaid")

    def test_transition_is_audited(self):
        order = {"status": "pending"}
        set_payment_state(order, "paid", "admin-1", "receipt approved", "2026-10-11T12:00:00+00:00")
        self.assertTrue(order["paid"])
        self.assertEqual(order["payment_status"], "paid")
        self.assertEqual(order["paid_at"], "2026-10-11T12:00:00+00:00")
        self.assertEqual(order["payment_history"][0]["from"], "unpaid")
        self.assertEqual(order["payment_history"][0]["actor_id"], "admin-1")

    def test_invalid_transition_is_rejected(self):
        with self.assertRaises(ValueError):
            set_payment_state({}, "unknown", "admin-1", "bad")


if __name__ == "__main__":
    unittest.main()
