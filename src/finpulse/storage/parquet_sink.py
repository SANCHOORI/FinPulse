"""Partitioned Parquet sink with idempotent writes.

Layout:    {root}/source={source}/dt={YYYY-MM-DD}/hr={HH}/part-{batch_id}.parquet
Idempotency:    batch_id is deterministic per (source, hour, batch_index).

Works against any path PyArrow can write to:
    - local FS:  ./data/lake
    - S3:        s3://bucket/prefix
    - MinIO:     s3://bucket/prefix (with AWS_S3_ENDPOINT_URL set)
"""
from __future__ import annotations

import time
from collections.abc import Iterable
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from finpulse.ingest.base import Event
from finpulse.log import get_logger
from finpulse.monitoring import metrics

log = get_logger(__name__)


_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("ingested_ts", pa.int64(), nullable=False),
        pa.field("author", pa.string()),
        pa.field("text", pa.string(), nullable=False),
        pa.field("url", pa.string()),
    ]
)


class ParquetSink:
    """Buffers events and flushes Parquet files at batch boundaries.

    Flush triggers, whichever comes first:
      - buffer reaches max_events
      - max_seconds elapsed since first event in buffer
      - explicit close()
    """

    def __init__(
        self,
        root: str,
        source: str,
        max_events: int = 500,
        max_seconds: float = 30.0,
    ):
        self.root = root.rstrip("/")
        self.source = source
        self.max_events = max_events
        self.max_seconds = max_seconds
        self._buffer: list[Event] = []
        self._buffer_started_at: float | None = None
        self._batch_index_by_hour: dict[str, int] = {}

    def write(self, events: Iterable[Event]) -> None:
        for e in events:
            if self._buffer_started_at is None:
                self._buffer_started_at = time.monotonic()
            self._buffer.append(e)
            if self._should_flush():
                self.flush()

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self.max_events:
            return True
        return (
            self._buffer_started_at is not None
            and (time.monotonic() - self._buffer_started_at) >= self.max_seconds
        )

    def flush(self) -> str | None:
        if not self._buffer:
            return None

        # All events in a flush land in the same hour partition based on the
        # earliest event_ts. Cross-hour batches are split into multiple flushes.
        groups: dict[tuple[str, str], list[Event]] = {}
        for e in self._buffer:
            dt_obj = datetime.fromtimestamp(e.event_ts / 1000, tz=UTC)
            dt = dt_obj.strftime("%Y-%m-%d")
            hr = dt_obj.strftime("%H")
            groups.setdefault((dt, hr), []).append(e)

        last_path = None
        for (dt, hr), batch in groups.items():
            hour_key = f"{dt}T{hr}"
            idx = self._batch_index_by_hour.get(hour_key, 0)
            path = self._part_path(dt, hr, idx)
            self._write_parquet(path, batch)
            self._batch_index_by_hour[hour_key] = idx + 1
            metrics.incr("sink.events_out", source=self.source, value=len(batch))
            metrics.incr("sink.flushes", source=self.source)
            log.info("sink.flush", path=path, events=len(batch), source=self.source)
            last_path = path

        self._buffer.clear()
        self._buffer_started_at = None
        return last_path

    def _part_path(self, dt: str, hr: str, idx: int) -> str:
        return (
            f"{self.root}/source={self.source}"
            f"/dt={dt}/hr={hr}/part-{idx:05d}.parquet"
        )

    def _write_parquet(self, path: str, events: list[Event]) -> None:
        rows = {
            "source": [e.source for e in events],
            "event_id": [e.event_id for e in events],
            "event_ts": [e.event_ts for e in events],
            "ingested_ts": [e.ingested_ts for e in events],
            "author": [e.author for e in events],
            "text": [e.text for e in events],
            "url": [e.url for e in events],
        }
        table = pa.Table.from_pydict(rows, schema=_SCHEMA)
        # PyArrow's write_table handles s3:// transparently when pyarrow.fs is
        # configured (AWS_* env vars or boto session). For local paths it
        # creates parent directories on demand via the dataset API; for the
        # simple write_table call we ensure the dir exists.
        if not path.startswith("s3://"):
            import os

            os.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        pq.write_table(table, path, compression="zstd")
        metrics.incr(
            "sink.bytes_written",
            source=self.source,
            value=table.nbytes,
        )

    def close(self) -> None:
        self.flush()
