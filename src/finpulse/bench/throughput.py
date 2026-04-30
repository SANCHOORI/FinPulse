"""Throughput + cost benchmark.

Pumps N synthetic events through ParquetSink and reports:
  - events / sec sustained
  - MB / sec sustained (compressed Parquet, zstd)
  - bytes / event (compressed)

Intentionally a single-process measurement: this is the cost of the *write
path*, isolated from network ingest. Real-world throughput is bounded by
upstream rate limits, not the sink. Adding Kafka or a multi-process pool
would scale this further; the numbers below are the per-process ceiling.
"""
from __future__ import annotations

import os
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from finpulse.ingest.base import Event
from finpulse.storage import ParquetSink


@dataclass(frozen=True)
class ThroughputResult:
    n_events: int
    elapsed_sec: float
    events_per_sec: float
    bytes_written: int
    mb_per_sec: float
    bytes_per_event: float


def _make_event(i: int, base_ts_ms: int) -> Event:
    ts = base_ts_ms + (i // 60) * 60_000  # spread events across minutes
    return Event(
        source="bench",
        event_id=f"e{i}",
        event_ts=ts,
        ingested_ts=ts,
        author=f"user_{i % 100}",
        text=(
            f"Benchmark event {i}: bullish on $AAPL today, "
            "MSFT looking strong, NVDA up on AI news"
        ),
        url=None,
        raw={"i": i},
    )


def run_throughput(n: int = 10_000) -> ThroughputResult:
    base_ts = int(datetime.now(tz=UTC).timestamp() * 1000)
    with tempfile.TemporaryDirectory() as tmp:
        sink = ParquetSink(root=tmp, source="bench", max_events=2_000, max_seconds=600)
        events = (_make_event(i, base_ts) for i in range(n))
        start = time.monotonic()
        sink.write(events)
        sink.close()
        elapsed = time.monotonic() - start

        total_bytes = 0
        for cur_root, _dirs, files in os.walk(tmp):
            for f in files:
                if f.endswith(".parquet"):
                    total_bytes += os.path.getsize(os.path.join(cur_root, f))

        return ThroughputResult(
            n_events=n,
            elapsed_sec=elapsed,
            events_per_sec=n / elapsed if elapsed > 0 else 0.0,
            bytes_written=total_bytes,
            mb_per_sec=(total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0.0,
            bytes_per_event=total_bytes / n if n > 0 else 0.0,
        )
