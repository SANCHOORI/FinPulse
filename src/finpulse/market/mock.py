"""Mock OHLCV generator.

This is a placeholder for a real market-data fetcher (yfinance / Alpaca /
Polygon). Output schema and partition layout match what a real fetcher would
write, so swapping the source is one file's worth of change. The only things
that are fake are the prices themselves.

Why a deterministic mock instead of a real fetch:
  - No external API dependency for reviewers running this locally.
  - Reproducible: same ticker → same price path on every run (seeded by a
    hash of the ticker).
  - Same Parquet schema, same partition layout, same DuckDB query path —
    the rest of the pipeline doesn't know it's synthetic.

Real-fetch swap point: replace `_random_walk` with a function that returns
the same `(ticker, ts, open, high, low, close, volume)` tuples from
yfinance.download() or Alpaca's bars API. Nothing else changes.
"""
from __future__ import annotations

import hashlib
import math
import os
import random
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from finpulse.log import get_logger

log = get_logger(__name__)


_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("ts", pa.int64(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
    ]
)


def _seed_for(ticker: str) -> int:
    return int(hashlib.sha256(ticker.encode()).hexdigest()[:8], 16)


def _random_walk(
    ticker: str,
    start_ts_ms: int,
    minutes: int,
    start_price: float = 100.0,
    sigma: float = 0.005,
) -> list[tuple]:
    """Return `minutes` 1-minute OHLCV bars for `ticker` starting at start_ts_ms."""
    rng = random.Random(_seed_for(ticker))
    close = start_price
    bars: list[tuple] = []
    for i in range(minutes):
        ts = start_ts_ms + i * 60_000
        open_p = close
        log_ret = rng.gauss(0.0, sigma)
        close = open_p * math.exp(log_ret)
        spread_hi = rng.uniform(0.0, sigma)
        spread_lo = rng.uniform(0.0, sigma)
        high = max(open_p, close) * (1 + spread_hi)
        low = min(open_p, close) * (1 - spread_lo)
        volume = max(0, int(rng.gauss(10_000, 3_000)))
        bars.append((ticker, ts, open_p, high, low, close, volume))
    return bars


def generate_mock_market(
    lake_root: str,
    tickers: list[str],
    start_ts_ms: int,
    minutes: int = 120,
) -> int:
    """Generate synthetic OHLCV for the given tickers and write to the lake.

    Output layout:  {root}/derived/market/dt=*/hr=*/part-00000.parquet
    Idempotent: re-running with the same args overwrites the same files.

    Returns the total number of bars written.
    """
    if not tickers:
        return 0

    all_bars: list[tuple] = []
    for t in tickers:
        all_bars.extend(_random_walk(t, start_ts_ms, minutes))

    # Group bars by (dt, hr) of their ts, mirroring the events partition layout.
    groups: dict[tuple[str, str], list[tuple]] = {}
    for bar in all_bars:
        ts_ms = bar[1]
        dt_obj = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        dt = dt_obj.strftime("%Y-%m-%d")
        hr = dt_obj.strftime("%H")
        groups.setdefault((dt, hr), []).append(bar)

    root = lake_root.rstrip("/")
    total = 0
    for (dt, hr), bars in groups.items():
        out_dir = f"{root}/derived/market/dt={dt}/hr={hr}"
        out_path = f"{out_dir}/part-00000.parquet"
        rows = {
            "ticker": [b[0] for b in bars],
            "ts": [b[1] for b in bars],
            "open": [b[2] for b in bars],
            "high": [b[3] for b in bars],
            "low": [b[4] for b in bars],
            "close": [b[5] for b in bars],
            "volume": [b[6] for b in bars],
        }
        table = pa.Table.from_pydict(rows, schema=_SCHEMA)
        if not out_path.startswith("s3://"):
            os.makedirs(out_dir, exist_ok=True)
        pq.write_table(table, out_path, compression="zstd")
        log.info("market.flush", dt=dt, hr=hr, bars=len(bars), path=out_path)
        total += len(bars)

    log.info("market.done", bars=total, tickers=len(tickers))
    return total
