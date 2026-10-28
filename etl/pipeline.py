"""Extract -> validate -> transform -> load, resumable after failure.

Idempotency is the whole point: running the pipeline twice must not duplicate data.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol


class Source(Protocol):
    def fetch(self, cursor: Any) -> Iterable[dict]: ...


@dataclass(slots=True)
class Batch:
    records: list[dict]
    next_cursor: Any = None
    done: bool = False


@dataclass(slots=True)
class Stats:
    extracted: int = 0
    rejected: int = 0
    transformed: int = 0
    loaded: int = 0
    skipped_duplicates: int = 0
    retries: int = 0
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "extracted": self.extracted,
            "rejected": self.rejected,
            "transformed": self.transformed,
            "loaded": self.loaded,
            "skipped_duplicates": self.skipped_duplicates,
            "retries": self.retries,
            "failures": self.failures,
        }


class Checkpoint:
    """Durable cursor so a crash resumes instead of restarting."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.cursor: Any = None
        self.load()

    def load(self) -> None:
        if self.path.exists():
            self.cursor = json.loads(self.path.read_text()).get("cursor")

    def save(self, cursor: Any) -> None:
        self.cursor = cursor
        self.path.write_text(json.dumps({"cursor": cursor}))


def with_retries(fn: Callable[[], Any], *, attempts: int = 3, base_delay: float = 0.1,
                 sleeper: Callable[[float], None] = time.sleep) -> tuple[Any, int]:
    """Returns (result, retries_used). Re-raises the last error when exhausted."""
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn(), attempt
        except Exception as exc:  # noqa: BLE001 - deliberately broad: any failure retries
            last = exc
            if attempt < attempts - 1:
                sleeper(base_delay * 2 ** attempt)
    raise last  # type: ignore[misc]


@dataclass(slots=True)
class Pipeline:
    source: Source
    destination: Any  # anything with add_many(records) -> int
    validator: Callable[[dict], bool]
    transformer: Callable[[dict], dict]
    checkpoint: Checkpoint
    seen: set[Any] = field(default_factory=set)

    def run_once(self, *, attempts: int = 3) -> Stats:
        stats = Stats()
        batch, retries = with_retries(
            lambda: self.source.fetch(self.checkpoint.cursor), attempts=attempts
        )
        stats.retries += retries
        stats.extracted += len(batch.records)

        clean: list[dict] = []
        for record in batch.records:
            if not self.validator(record):
                stats.rejected += 1
                if len(stats.failures) < 5:
                    stats.failures.append(f"rejected record: {record!r}")
                continue
            transformed = self.transformer(record)
            key = transformed.get("id", json.dumps(transformed, sort_keys=True))
            if key in self.seen:
                stats.skipped_duplicates += 1
                continue
            self.seen.add(key)
            clean.append(transformed)

        stats.transformed = len(clean)
        if clean:
            written, _ = with_retries(lambda: self.destination.add_many(clean), attempts=attempts)
            stats.loaded = written
        self.checkpoint.save(batch.next_cursor)
        return stats

    def run(self, *, max_batches: int = 100) -> Stats:
        total = Stats()
        for _ in range(max_batches):
            batch_done = getattr(self.source, "exhausted", False)
            stats = self.run_once()
            total.extracted += stats.extracted
            total.rejected += stats.rejected
            total.transformed += stats.transformed
            total.loaded += stats.loaded
            total.skipped_duplicates += stats.skipped_duplicates
            total.retries += stats.retries
            total.failures.extend(stats.failures)
            if batch_done:
                break
        return total
