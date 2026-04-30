"""Mock market generator tests."""
from __future__ import annotations

import pyarrow.parquet as pq

from finpulse.market import generate_mock_market


def test_writes_parquet_partitioned_by_dt_hr(tmp_path):
    # Anchor: 2026-04-30 14:30:00 UTC
    start_ts_ms = 1777728600000
    n = generate_mock_market(str(tmp_path), ["AAPL", "MSFT"], start_ts_ms, minutes=10)
    assert n == 20  # 2 tickers * 10 minutes

    files = list((tmp_path / "derived" / "market").rglob("*.parquet"))
    assert len(files) >= 1
    table = pq.ParquetFile(str(files[0])).read()
    assert {"ticker", "ts", "open", "high", "low", "close", "volume"}.issubset(
        set(table.column_names)
    )


def test_deterministic_per_ticker(tmp_path):
    """Same ticker + same start_ts -> same first close (random walk is seeded)."""
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    start_ts_ms = 1777728600000
    generate_mock_market(str(out1), ["AAPL"], start_ts_ms, minutes=5)
    generate_mock_market(str(out2), ["AAPL"], start_ts_ms, minutes=5)
    f1 = next((out1 / "derived" / "market").rglob("*.parquet"))
    f2 = next((out2 / "derived" / "market").rglob("*.parquet"))
    t1 = pq.ParquetFile(str(f1)).read().to_pydict()
    t2 = pq.ParquetFile(str(f2)).read().to_pydict()
    assert t1["close"] == t2["close"]


def test_empty_tickers_noop(tmp_path):
    n = generate_mock_market(str(tmp_path), [], 0, minutes=10)
    assert n == 0
