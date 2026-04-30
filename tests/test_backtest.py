"""Backtest tests."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from finpulse.backtest import run_backtest
from finpulse.features import run_scoring
from finpulse.ingest.base import Event
from finpulse.market import generate_mock_market
from finpulse.storage import ParquetSink


def _ev(eid: str, ts: datetime, text: str) -> Event:
    epoch_ms = int(ts.timestamp() * 1000)
    return Event(
        source="t",
        event_id=eid,
        event_ts=epoch_ms,
        ingested_ts=epoch_ms,
        author="x",
        text=text,
        url=None,
        raw={},
    )


def test_backtest_empty_lake(tmp_path):
    """Backtest on an empty lake returns all zeros, no crash."""
    r = run_backtest(str(tmp_path))
    assert r.n_signals == 0
    assert r.n_trades == 0
    assert r.total_pnl == 0.0


def test_backtest_runs_end_to_end(tmp_path):
    """Populated lake -> backtest produces real metrics with the right shape."""
    sink = ParquetSink(root=str(tmp_path), source="test", max_events=200, max_seconds=60)
    base = datetime(2026, 4, 30, 14, 0, tzinfo=UTC)
    events = [
        _ev(f"a{i}", base + timedelta(seconds=i * 10), f"$AAPL bullish {i}")
        for i in range(60)
    ] + [
        _ev(f"m{i}", base + timedelta(seconds=i * 10), f"MSFT terrible {i}")
        for i in range(60)
    ]
    sink.write(events)
    sink.close()

    run_scoring(str(tmp_path))
    start_ts_ms = int(base.timestamp() * 1000) - 5 * 60_000
    generate_mock_market(str(tmp_path), ["AAPL", "MSFT"], start_ts_ms, minutes=60)

    r = run_backtest(str(tmp_path))
    assert r.n_signals > 0
    # Hit rate is a probability
    assert 0.0 <= r.hit_rate <= 1.0
    # Std non-negative
    assert r.std_pnl >= 0.0
