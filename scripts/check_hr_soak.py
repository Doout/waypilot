"""Check the long-run workload's reproducibility, drift and independent oracle."""

import sys
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "load"))
from hr_soak import PHASES, plan, read_oracle
from hr_soak_peer import VariableHR


class SoakTests(unittest.TestCase):
    def test_plan_is_frozen_and_changes_with_seed(self):
        first = plan(3600, 2, 20261018)
        self.assertEqual(first, plan(3600, 2, 20261018))
        self.assertNotEqual(first, plan(3600, 2, 20261019))
        self.assertLess(len(first), 9000)
        self.assertEqual(len({r["employee"] for r in first}), len(first))
        self.assertEqual({r["phase"] for r in first}, set(PHASES))
        self.assertTrue(all(a["due_seconds"] < b["due_seconds"] for a, b in zip(first, first[1:])))
        by_phase = {
            phase: Counter(r["kind"] for r in first if r["phase"] == phase) for phase in PHASES
        }
        self.assertGreater(
            by_phase["leave-heavy"]["leave"], by_phase["leave-heavy"]["benefits"] * 3
        )
        self.assertGreater(
            by_phase["benefits-drift"]["benefits"], by_phase["benefits-drift"]["leave"] * 3
        )
        self.assertGreater(sum(r["refresh"] for r in first), 100)
        self.assertTrue(any(r["order"] == "parallel" for r in first))
        self.assertTrue(any(max(r["pauses_ms"]) > 5000 for r in first))
        self.assertTrue(any(0 in r["pauses_ms"] for r in first))

    def test_service_time_is_order_independent_and_heterogeneous(self):
        peer = VariableHR(0)
        queries = [{"employee_id": f"E-{i:06d}"} for i in range(1000)]
        first = [peer.read_delay_ms("leave_balance", args) for args in queries]
        second = [peer.read_delay_ms("leave_balance", args) for args in reversed(queries)]
        self.assertEqual(first, list(reversed(second)))
        self.assertLess(min(first), 10)
        self.assertGreater(max(first), 2500)
        self.assertGreater(len(set(first)), 900)

    def test_oracle_checks_values_and_updated_balance(self):
        self.assertEqual(
            read_oracle("leave_balance", "E-100002", 12),
            {
                "employee_id": "E-100002",
                "region": "EU",
                "days": 12,
            },
        )
        self.assertNotEqual(
            read_oracle("leave_balance", "E-100002", 12),
            read_oracle("leave_balance", "E-100002", 8),
        )
        self.assertEqual(read_oracle("leave_policy", "E-100003", 8)["max_days"], 20)


if __name__ == "__main__":
    unittest.main()
