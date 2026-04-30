"""Signal builder tests."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from finpulse.features import run_scoring
from finpulse.ingest.base import Event
from finpulse.market import generate_mock_market
from finpulse.signals import build_signal
from finpulse.storage import ParquetSink


def _ev(eid: str, ts: datetime, text: str) -> Event:
    epoch_ms = int(ts.timestamp() * 1000)
    return Event(
        source="test",
        event_id=eid,
        event_ts=epoch_ms,
        ingested_ts=epoch_ms,
        author="x",
        text=text,
        url=None,
        raw={},
    )


def test_signal_runs_end_to_end(tmp_path):
    """Write a few events with tickers, score them, generate market data, and
    confirm the signal SQL runs and returns rows with the expected schema.

    With only a few minutes of data the rolling 1h baseline is mostly NaN, so
    we don't assert on signal magnitude — just that the join runs and returns
    structurally-correct rows."""
    sink = ParquetSink(root=str(tmp_path), source="test", max_events=20, max_seconds=60)
    base = datetime(2026, 4, 30, 14, 0, tzinfo=UTC)
    events = []
    for i in range(8):
        events.append(_ev(f"a{i}", base + timedelta(minutes=i), f"Bullish on $AAPL #{i}"))
        events.append(_ev(f"m{i}", base + timedelta(minutes=i), f"MSFT looking weak #{i}"))
    sink.write(events)
    sink.close()

    run_scoring(str(tmp_path))

    start_ts_ms = int(base.timestamp() * 1000) - 5 * 60_000
    generate_mock_market(str(tmp_path), ["AAPL", "MSFT"], start_ts_ms, minutes=30)

    rows = build_signal(str(tmp_path))
    assert len(rows) > 0
    cols = (
        "ticker",
        "bucket",
        "z_mentions",
        "sentiment_shift",
        "composite",
        "position",
        "close_now",
        "close_fwd_5m",
        "fwd_ret_5m",
    )
    # Every row should be a 9-tuple matching the column shape above.
    for r in rows:
        assert len(r) == len(cols)

    # Tickers should be in the expected set.
    tickers = {r[0] for r in rows}
    assert tickers <= {"AAPL", "MSFT"}

    # close_now should be populated for at least some rows (market data was joined).
    assert any(r[6] is not None for r in rows)


def test_signal_empty_lake(tmp_path):
    """No events, no market data — should not crash, returns no rows."""
    rows = build_signal(str(tmp_path))
    assert rows == []
