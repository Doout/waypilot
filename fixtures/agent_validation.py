"""Controlled batch replies and barriers for gateway validation regressions."""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demos"))
from agents import workbench

Base = workbench.Workbench


class ValidationWorkbench(Base):
    def __init__(self, delay_ms):
        super().__init__(delay_ms)
        self.validation_arrivals = set()
        self.validation_ready = asyncio.Event()
        self.validation_rendezvous = False

    mode = "valid"
    block_batch = False
    batch_pending = 0
    block_note_reply = False
    note_pending = 0

    def control(self, value):
        if "validation_rendezvous" in value:
            if value["validation_rendezvous"] and not self.validation_rendezvous:
                self.validation_arrivals.clear()
                self.validation_ready.clear()
            self.validation_rendezvous = value["validation_rendezvous"]
            if not self.validation_rendezvous:
                self.validation_ready.set()
        self.block_note_reply = value.get("block_note_reply", self.block_note_reply)
        self.mode = value.get("batch_mode", self.mode)
        self.block_batch = value.get("block_batch", self.block_batch)
        return super().control(value)

    def snapshot(self, summary=False):
        return dict(
            super().snapshot(summary),
            batch_pending=self.batch_pending,
            validation_arrivals=sorted(self.validation_arrivals),
            note_pending=self.note_pending,
        )

    async def call(self, name, arguments):
        if self.validation_rendezvous:
            target = (
                "ops"
                if name == "state_batch"
                else "knowledge"
                if name == "state_version" and arguments.get("tool") == "search_runbooks"
                else None
            )
            if target:
                self.validation_arrivals.add(target)
                if len(self.validation_arrivals) == 2:
                    self.validation_ready.set()
                await self.validation_ready.wait()
        if name != "state_batch":
            result = await super().call(name, arguments)
            if name == "append_note":
                self.note_pending += 1
                try:
                    while self.block_note_reply:
                        await asyncio.sleep(0.002)
                finally:
                    self.note_pending -= 1
            return result
        self.batch_pending += 1
        try:
            while self.block_batch:
                await asyncio.sleep(0.002)
            result = await super().call(name, arguments)
            entries = result.get("_meta", {}).get("io.waypilot/state-batch", {}).get("observations")
            if entries:
                if self.mode == "duplicate":
                    entries[-1]["id"] = entries[0]["id"]
                elif self.mode == "missing":
                    entries.pop()
                elif self.mode == "changed":
                    entries[0]["state"]["resources"] = {"wrong-resource": "wrong-version"}
                elif self.mode == "reorder":
                    entries.reverse()
                elif self.mode == "error":
                    result["isError"] = True
            return result
        finally:
            self.batch_pending -= 1


if __name__ == "__main__":
    workbench.Workbench = ValidationWorkbench
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delay-ms", type=int, default=30)
    args = parser.parse_args()
    asyncio.run(workbench.main(argparse.Namespace(delay_ms=args.delay_ms, port=0, stdio=False)))
