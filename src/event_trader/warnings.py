"""Durable, throttled local critical-warning journal."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path


class LocalCriticalWarningSink:
    """Append warning codes to JSONL and stderr without external messaging."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        repeat_after: timedelta = timedelta(minutes=5),
    ) -> None:
        if repeat_after <= timedelta(0):
            raise ValueError("warning repeat interval must be positive")
        self.path = Path(path)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.repeat_after = repeat_after
        self._last_emitted: dict[str, datetime] = {}
        self._lock = threading.RLock()

    async def __call__(self, message: str) -> None:
        if not message.strip():
            raise ValueError("warning message must not be empty")
        await asyncio.to_thread(self._write, message)

    def _write(self, message: str) -> None:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("warning clock must be timezone-aware")
        stamp = now.astimezone(UTC)
        with self._lock:
            previous = self._last_emitted.get(message)
            if previous is not None and stamp - previous < self.repeat_after:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            record = json.dumps(
                {
                    "timestamp": stamp.isoformat(),
                    "severity": "critical",
                    "code": message,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(record + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            print(record, file=sys.stderr, flush=True)
            self._last_emitted[message] = stamp


__all__ = ["LocalCriticalWarningSink"]
