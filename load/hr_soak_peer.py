"""Owned HR peer with repeatable heterogeneous service times for the long soak."""

import argparse
import asyncio
import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents.workbench import main
from hr.workbench import HR, TOOLS


class VariableHR(HR):
    def read_delay_ms(self, name, arguments):
        # Query identity, rather than call order, gives every mode the same service time.
        digest = hashlib.sha256(f"{name}:{arguments['employee_id']}".encode()).digest()
        fraction = int.from_bytes(digest[:4], "big") / (2**32 - 1)
        if digest[4] < 3:
            return 2500 + digest[5] * 2
        if digest[4] < 25:
            return 2 + fraction * 8
        low, high = {
            "employee": (40, 180),
            "leave_balance": (250, 1200),
            "leave_policy": (80, 350),
            "benefit_options": (150, 800),
        }[name]
        return low + fraction * (high - low)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--state-ms", type=int, default=5)
    parser.add_argument("--stdio", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args, VariableHR(args.delay_ms, args.state_ms), TOOLS, "hr-soak"))
