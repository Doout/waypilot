"""Agent-only concurrency fixture: neither independent read can finish before both arrive."""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents import workbench


class RendezvousWorkbench(workbench.Workbench):
    def __init__(self, delay_ms):
        super().__init__(delay_ms)
        self.arrivals = set()
        self.ready = asyncio.Event()

    async def call(self, name, arguments):
        if name in {"get_metrics", "search_runbooks"}:
            self.arrivals.add(name)
            if self.arrivals == {"get_metrics", "search_runbooks"}:
                self.ready.set()
            await asyncio.wait_for(self.ready.wait(), timeout=3)
        return await super().call(name, arguments)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delay-ms", type=int, default=30)
    args = parser.parse_args()
    workbench.Workbench = RendezvousWorkbench
    asyncio.run(workbench.main(argparse.Namespace(delay_ms=args.delay_ms, port=0, stdio=False)))
