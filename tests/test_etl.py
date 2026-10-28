from __future__ import annotations

import pytest

from etl.pipeline import Batch, Checkpoint, Pipeline, Stats, with_retries


class ListSource:
    """Yields a fixed number of batches, tracking how often fetch is called."""

    def __init__(self, batches: list[Batch], fail_times: int = 0) -> None:
        self.batches = batches
        self.fail_times = fail_times
        self.calls = 0
        self.exhausted = False
        self.index = 0

    def fetch(self, cursor):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectionError("source unavailable")
        batch = self.batches[min(self.index, len(self.batches) - 1)]
        self.index += 1
        self.exhausted = self.index >= len(self.batches)
        return batch


class Sink:
    def __init__(self, fail_times: int = 0) -> None:
        self.records: list[dict] = []
        self.fail_times = fail_times

    def add_many(self, records):
        if self.fail_times:
            self.fail_times -= 1
            raise OSError("disk full")
        self.records.extend(records)
        return len(records)


def build(tmp_path, source, sink, validator=lambda r: True, transformer=lambda r: r):
    return Pipeline(
        source=source,
        destination=sink,
        validator=validator,
        transformer=transformer,
        checkpoint=Checkpoint(tmp_path / "cursor.json"),
    )


def test_happy_path(tmp_path):
    sink = Sink()
    pipeline = build(tmp_path, ListSource([Batch([{"id": 1}, {"id": 2}], "c1")]), sink)
    stats = pipeline.run_once()
    assert (stats.extracted, stats.loaded) == (2, 2)
    assert sink.records == [{"id": 1}, {"id": 2}]


def test_checkpoint_is_written(tmp_path):
    pipeline = build(tmp_path, ListSource([Batch([{"id": 1}], "cursor-42")]), Sink())
    pipeline.run_once()
    assert Checkpoint(tmp_path / "cursor.json").cursor == "cursor-42"


def test_validator_rejects_bad_records(tmp_path):
    sink = Sink()
    pipeline = build(
        tmp_path,
        ListSource([Batch([{"id": 1, "ok": True}, {"id": 2, "ok": False}], "c")]),
        sink,
        validator=lambda r: r.get("ok", False),
    )
    stats = pipeline.run_once()
    assert stats.rejected == 1 and stats.loaded == 1
    assert stats.failures


def test_duplicates_are_skipped(tmp_path):
    sink = Sink()
    source = ListSource([Batch([{"id": 1}], "c")])
    pipeline = build(tmp_path, source, sink)
    pipeline.run_once()
    stats = pipeline.run_once()
    assert stats.skipped_duplicates == 1 and stats.loaded == 0


def test_source_retry_is_counted(tmp_path):
    source = ListSource([Batch([{"id": 1}], "c")], fail_times=2)
    stats = build(tmp_path, source, Sink()).run_once(attempts=3)
    assert stats.retries == 2 and stats.loaded == 1


def test_sink_failure_retries(tmp_path):
    sink = Sink(fail_times=1)
    stats = build(tmp_path, ListSource([Batch([{"id": 1}], "c")]), sink).run_once(attempts=3)
    assert stats.loaded == 1


def test_exhausted_retries_raise(tmp_path):
    source = ListSource([Batch([], "c")], fail_times=99)
    with pytest.raises(ConnectionError):
        build(tmp_path, source, Sink()).run_once(attempts=2)


def test_with_retries_uses_exponential_backoff():
    delays: list[float] = []
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("nope")
        return "ok"

    result, retries = with_retries(flaky, attempts=3, base_delay=0.5, sleeper=delays.append)
    assert result == "ok" and retries == 2
    assert delays == [0.5, 1.0]


def test_stats_serialisable():
    assert isinstance(Stats().as_dict()["failures"], list)


def test_transform_applied_before_load(tmp_path):
    sink = Sink()
    pipeline = build(
        tmp_path,
        ListSource([Batch([{"id": 1, "name": " a "}], "c")]),
        sink,
        transformer=lambda r: {**r, "name": r["name"].strip()},
    )
    pipeline.run_once()
    assert sink.records[0]["name"] == "a"
