"""Tests for the storage layer.

The contract we care about:
  1. Events land at the path implied by their event_ts.
  2. Re-writing the same batch is idempotent (no duplication, no errors).
  3. The schema on disk matches what readers expect.
  4. Cross-hour batches split into the right partitions.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.parquet as pq

from finpulse.ingest.base import Event
from finpulse.storage import ParquetSink


def _read_part(path) -> pq.Table:
    """Read a single Parquet file directly, bypassing auto-detected hive
    partitioning (which would collide with our real `source` column)."""
    return pq.ParquetFile(str(path)).read()


def _make_event(event_id: str, ts: datetime, text: str = "hi") -> Event:
    return Event(
        source="test",
        event_id=event_id,
        event_ts=int(ts.timestamp() * 1000),
        ingested_ts=int(ts.timestamp() * 1000),
        author="someone",
        text=text,
        url=None,
        raw={"id": event_id},
    )


def test_write_and_read(tmp_path):
    sink = ParquetSink(root=str(tmp_path), source="test", max_events=10, max_seconds=60)
    ts = datetime(2026, 4, 30, 14, 30, tzinfo=UTC)
    events = [_make_event(str(i), ts, text=f"event {i}") for i in range(5)]
    sink.write(events)
    sink.close()

    expected_dir = tmp_path / "source=test" / "dt=2026-04-30" / "hr=14"
    files = list(expected_dir.glob("*.parquet"))
    assert len(files) == 1, f"expected 1 part file, got {files}"

    table = _read_part(files[0])
    assert table.num_rows == 5
    assert set(table.column_names) == {
        "source",
        "event_id",
        "event_ts",
        "ingested_ts",
        "author",
        "text",
        "url",
    }


def test_cross_hour_split(tmp_path):
    sink = ParquetSink(root=str(tmp_path), source="test", max_events=100, max_seconds=60)
    h1 = datetime(2026, 4, 30, 14, 30, tzinfo=UTC)
    h2 = datetime(2026, 4, 30, 15, 5, tzinfo=UTC)
    events = [_make_event("a", h1), _make_event("b", h2)]
    sink.write(events)
    sink.close()

    assert list((tmp_path / "source=test" / "dt=2026-04-30" / "hr=14").glob("*.parquet"))
    assert list((tmp_path / "source=test" / "dt=2026-04-30" / "hr=15").glob("*.parquet"))


def test_idempotent_rewrite(tmp_path):
    """Re-running an ingest window must not duplicate data on disk.

    Concretely: a fresh sink instance starting at batch_index=0 will overwrite
    the same part-00000.parquet rather than producing part-00001.parquet.
    """
    ts = datetime(2026, 4, 30, 14, 30, tzinfo=UTC)
    events = [_make_event(str(i), ts) for i in range(3)]

    sink1 = ParquetSink(root=str(tmp_path), source="test")
    sink1.write(events)
    sink1.close()

    sink2 = ParquetSink(root=str(tmp_path), source="test")
    sink2.write(events)
    sink2.close()

    files = list(
        (tmp_path / "source=test" / "dt=2026-04-30" / "hr=14").glob("*.parquet")
    )
    assert len(files) == 1, f"expected idempotent overwrite, got {files}"

    table = _read_part(files[0])
    assert table.num_rows == 3
