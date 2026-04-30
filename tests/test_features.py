"""End-to-end test: write raw events, run scoring, query per-ticker view."""
from __future__ import annotations

from datetime import UTC, datetime

from finpulse.features import per_ticker_view, run_scoring, summarize
from finpulse.ingest.base import Event
from finpulse.storage import ParquetSink


def _ev(eid: str, ts: datetime, text: str) -> Event:
    epoch_ms = int(ts.timestamp() * 1000)
    return Event(
        source="test",
        event_id=eid,
        event_ts=epoch_ms,
        ingested_ts=epoch_ms,
        author="someone",
        text=text,
        url=None,
        raw={},
    )


def test_score_and_per_ticker_view(tmp_path):
    sink = ParquetSink(root=str(tmp_path), source="test", max_events=10, max_seconds=60)
    ts = datetime(2026, 4, 30, 14, 30, tzinfo=UTC)
    sink.write(
        [
            _ev("1", ts, "Bullish on $AAPL, great earnings!"),
            _ev("2", ts, "AAPL is doing fantastic, love this stock"),
            _ev("3", ts, "Worried about MSFT, this is terrible news"),
            _ev("4", ts, "just market open today"),
        ]
    )
    sink.close()

    n = run_scoring(str(tmp_path))
    assert n == 4

    # per-minute summary still works
    rows = summarize(str(tmp_path))
    assert any(source == "test" and count == 4 for source, _, count in rows)

    # per-ticker view aggregates correctly
    ticker_rows = per_ticker_view(str(tmp_path))
    by_ticker = {t: (mentions, avg) for t, _bucket, mentions, avg in ticker_rows}
    assert "AAPL" in by_ticker
    assert "MSFT" in by_ticker
    aapl_mentions, aapl_avg = by_ticker["AAPL"]
    msft_mentions, msft_avg = by_ticker["MSFT"]
    assert aapl_mentions == 2
    assert msft_mentions == 1
    assert aapl_avg > 0  # positive sentiment
    assert msft_avg < 0  # negative sentiment


def test_per_ticker_view_empty_lake(tmp_path):
    """Querying before scoring should not crash; should return no rows."""
    rows = per_ticker_view(str(tmp_path))
    assert rows == []
