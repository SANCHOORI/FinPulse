"""FinPulse CLI."""
from __future__ import annotations

import time

import typer

from finpulse.backtest import BacktestResult, run_backtest
from finpulse.bench import ThroughputResult, run_throughput
from finpulse.config import Settings
from finpulse.features import per_ticker_view, run_scoring, summarize
from finpulse.ingest.hackernews import HackerNewsSource
from finpulse.log import configure as configure_logging
from finpulse.log import get_logger
from finpulse.market import generate_mock_market
from finpulse.monitoring import metrics
from finpulse.signals import build_signal
from finpulse.storage import ParquetSink

app = typer.Typer(no_args_is_help=True, add_completion=False)
ingest_app = typer.Typer(no_args_is_help=True)
app.add_typer(ingest_app, name="ingest", help="Run an ingest source")


@ingest_app.command("hackernews")
def ingest_hackernews(
    duration: int = typer.Option(60, help="How long to run, in seconds. 0 = until Ctrl-C."),
):
    """Ingest the HackerNews firehose into the local lake."""
    settings = Settings.from_env()
    configure_logging(settings)
    log = get_logger("cli.ingest.hackernews")

    source = HackerNewsSource(poll_interval=settings.poll_interval)
    sink = ParquetSink(
        root=settings.lake_root,
        source=source.name,
        max_events=settings.batch_max_events,
        max_seconds=settings.batch_max_seconds,
    )
    log.info(
        "ingest.start",
        source=source.name,
        lake_root=settings.lake_root,
        duration=duration,
    )
    deadline = time.monotonic() + duration if duration > 0 else None
    try:
        for event in source.stream():
            sink.write([event])
            if deadline is not None and time.monotonic() >= deadline:
                break
    except KeyboardInterrupt:
        log.info("ingest.interrupt")
    finally:
        sink.close()
        log.info("ingest.done", metrics=metrics.snapshot())


@app.command("query")
def query():
    """Print per-source, per-minute event counts from the lake."""
    settings = Settings.from_env()
    configure_logging(settings)
    rows = summarize(settings.lake_root)
    if not rows:
        typer.echo("(lake is empty)")
        return
    typer.echo(f"{'source':<14} {'minute':<25} {'n':>6}")
    typer.echo("-" * 47)
    for source, minute, n in rows:
        typer.echo(f"{source:<14} {str(minute):<25} {n:>6}")


@app.command("score")
def score():
    """Run sentiment scoring + ticker extraction over the raw lake.

    Reads every raw event, writes one Parquet file per source/dt/hr to
    {lake_root}/derived/sentiment/. Idempotent: re-running overwrites.
    """
    settings = Settings.from_env()
    configure_logging(settings)
    n = run_scoring(settings.lake_root)
    typer.echo(f"scored {n} events")


@app.command("tickers")
def tickers():
    """Print per-ticker, per-minute mention counts + average sentiment."""
    settings = Settings.from_env()
    configure_logging(settings)
    rows = per_ticker_view(settings.lake_root)
    if not rows:
        typer.echo(
            "(no ticker mentions found — run `finpulse score` if you haven't, "
            "otherwise no recent events mention tickers in the whitelist)"
        )
        return
    typer.echo(f"{'ticker':<8} {'bucket':<25} {'mentions':>9} {'avg_compound':>13}")
    typer.echo("-" * 58)
    for ticker, bucket, mentions, avg_compound in rows:
        typer.echo(f"{ticker:<8} {str(bucket):<25} {mentions:>9} {avg_compound:>13}")


_DEFAULT_MOCK_TICKERS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL", "SPY", "QQQ"]


@app.command("market")
def market(
    tickers: str = typer.Option(
        "",
        help="Comma-separated tickers. Default: tickers from scored sentiment "
        "(falls back to a built-in basket if empty).",
    ),
    minutes: int = typer.Option(120, help="Number of 1-minute bars per ticker."),
):
    """Generate synthetic OHLCV market data and write to the lake.

    NOTE: market data is mock-generated for demonstration. Real fetch
    (yfinance / Alpaca) is a one-file swap — see src/finpulse/market/mock.py.
    """
    settings = Settings.from_env()
    configure_logging(settings)

    if tickers.strip():
        ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    else:
        ticker_list = _auto_tickers(settings.lake_root) or _DEFAULT_MOCK_TICKERS

    start_ts_ms = _market_anchor(settings.lake_root, minutes)
    n = generate_mock_market(settings.lake_root, ticker_list, start_ts_ms, minutes)
    typer.echo(
        f"generated {n} synthetic bars for {len(ticker_list)} tickers "
        f"({minutes} min each)"
    )
    typer.echo(
        "NOTE: market data is mock-generated; swap src/finpulse/market/mock.py "
        "for a real fetcher."
    )


def _auto_tickers(lake_root: str) -> list[str]:
    """Read tickers mentioned in the sentiment partition (if any)."""
    from finpulse.features.build import open_lake

    try:
        con = open_lake(lake_root)
        rows = con.execute(
            "SELECT DISTINCT t.ticker FROM sentiment s, UNNEST(s.tickers) AS t(ticker)"
        ).fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []


def _market_anchor(lake_root: str, minutes: int) -> int:
    """Anchor the synthetic market window to overlap with event timestamps so
    the signal can join. Falls back to (now - minutes/2 minutes) if there are
    no events yet."""
    from finpulse.features.build import open_lake

    try:
        con = open_lake(lake_root)
        row = con.execute("SELECT min(event_ts), max(event_ts) FROM events").fetchone()
        if row and row[0] is not None and row[1] is not None:
            min_ts, max_ts = row
            # Center the market window so it covers events on both sides.
            center = (min_ts + max_ts) // 2
            return int(center - (minutes // 2) * 60_000)
    except Exception:
        pass
    import time as _t

    return int(_t.time() * 1000) - (minutes // 2) * 60_000


@app.command("signal")
def signal():
    """Compute the composite signal joined with market data; print the top rows."""
    settings = Settings.from_env()
    configure_logging(settings)
    rows = build_signal(settings.lake_root)
    if not rows:
        typer.echo(
            "(no signal rows — run `finpulse score` and `finpulse market` first, "
            "and ensure events with tickers are in the lake)"
        )
        return
    header = (
        f"{'ticker':<8} {'bucket':<22} {'z_ment':>7} "
        f"{'sent_shift':>10} {'composite':>10} {'pos':<6} "
        f"{'close_now':>10} {'fwd_ret_5m':>11}"
    )
    typer.echo(header)
    typer.echo("-" * len(header))
    for r in rows[:50]:
        ticker, bucket, z, ss, comp, pos, cn, _cf, fr = r
        cn_s = f"{cn:.2f}" if cn is not None else "-"
        fr_s = f"{fr:.5f}" if fr is not None else "-"
        typer.echo(
            f"{ticker:<8} {str(bucket):<22} {z!s:>7} {ss!s:>10} "
            f"{comp!s:>10} {pos:<6} {cn_s:>10} {fr_s:>11}"
        )


@app.command("report")
def report(output: str = typer.Option("BENCHMARKS.md", help="Output markdown path.")):
    """Run backtest + throughput benchmark on synthetic data; write BENCHMARKS.md.

    Self-contained: generates synthetic events with tickers, scores them,
    generates mock OHLCV, builds the signal, computes the backtest, and runs
    the throughput benchmark. Idempotent: re-running overwrites the file.
    """
    import random
    import tempfile
    from datetime import timedelta
    from pathlib import Path

    settings = Settings.from_env()
    configure_logging(settings)

    typer.echo("running throughput benchmark (10k synthetic events)...")
    th = run_throughput(n=10_000)

    typer.echo("running backtest on synthetic events + mock OHLCV...")
    with tempfile.TemporaryDirectory() as tmp:
        # Synthetic events: 1000 mentions of 4 tickers with mixed sentiment,
        # spread over ~2 hours so the rolling 5m / 1h windows have enough data
        # to fire a non-trivial number of trades.
        from datetime import UTC
        from datetime import datetime as _dt

        from finpulse.ingest.base import Event

        sink = ParquetSink(root=tmp, source="synth", max_events=200, max_seconds=600)
        base = _dt(2026, 4, 30, 14, 0, tzinfo=UTC)
        rng = random.Random(42)
        tickers_list = ["AAPL", "MSFT", "NVDA", "TSLA"]
        sentiments = [
            "bullish on",
            "loving",
            "great quarter for",
            "terrible day for",
            "bearish on",
            "mixed feelings about",
            "neutral on",
        ]
        n_events = 1_000
        events = []
        for i in range(n_events):
            t = tickers_list[rng.randint(0, len(tickers_list) - 1)]
            s = sentiments[rng.randint(0, len(sentiments) - 1)]
            events.append(
                Event(
                    source="synth",
                    event_id=f"s{i}",
                    event_ts=int((base + timedelta(seconds=i * 7)).timestamp() * 1000),
                    ingested_ts=int(
                        (base + timedelta(seconds=i * 7)).timestamp() * 1000
                    ),
                    author=f"u{i % 50}",
                    text=f"${t} {s} this week",
                    url=None,
                    raw={},
                )
            )
        sink.write(events)
        sink.close()

        run_scoring(tmp)
        start_ts_ms = int(base.timestamp() * 1000) - 10 * 60_000
        generate_mock_market(tmp, tickers_list, start_ts_ms, minutes=180)

        bt = run_backtest(tmp)

    md = _render_benchmarks(bt, th, n_synthetic_events=n_events)
    Path(output).write_text(md)
    typer.echo(f"wrote {output}")


def _render_benchmarks(
    bt: BacktestResult, th: ThroughputResult, n_synthetic_events: int
) -> str:
    cost_gb_day_100eps = (th.bytes_per_event * 86400 * 100) / (1024**3)
    cost_usd_month = cost_gb_day_100eps * 30 * 0.023  # S3 Standard
    return f"""# FinPulse Benchmarks

Auto-generated by `finpulse report`. Re-run to refresh.

## Backtest

Composite signal (`z_mentions × sentiment_shift`), thresholded long / short /
flat at ±1, joined with mock OHLCV for forward 5-minute returns. Synthetic
inputs: {n_synthetic_events} mentions across 4 tickers (AAPL, MSFT, NVDA, TSLA),
scored with VADER, signal generated over a ~2 hour window.

| Metric | Value |
|---|---|
| Signal rows produced | {bt.n_signals} |
| Trades fired (non-flat) | {bt.n_trades} |
| Total PnL (cumulative) | {bt.total_pnl:+.5f} |
| Mean PnL per trade | {bt.mean_pnl:+.5f} |
| Std PnL per trade | {bt.std_pnl:.5f} |
| Annualised Sharpe (5m holdings) | {bt.sharpe_annual:+.3f} |
| Max drawdown | {bt.max_drawdown:.5f} |
| Hit rate | {bt.hit_rate * 100:.1f}% |

> **Honest caveat:** market prices are synthetic random walks
> (`src/finpulse/market/mock.py`). The random walk has no relation to the
> sentiment signal, so the Sharpe should be **noise around zero** by
> construction. These numbers verify the framework runs end-to-end and
> produces real metrics; they are not financially meaningful. Replace the
> mock with a real OHLCV fetcher (yfinance / Alpaca / Polygon) and re-run for
> meaningful numbers.

## Throughput (single-process write path)

10,000 synthetic events pushed through `ParquetSink` (Hive-partitioned,
zstd-compressed, batch size 2,000). Measures the per-process ceiling on the
sink — separate from network ingest, which is bounded by upstream rate
limits.

| Metric | Value |
|---|---|
| Events processed | {th.n_events:,} |
| Elapsed | {th.elapsed_sec:.2f} sec |
| Events / sec | {th.events_per_sec:,.0f} |
| Bytes written (compressed) | {th.bytes_written:,} ({th.bytes_written / (1024**2):.2f} MB) |
| MB / sec | {th.mb_per_sec:.2f} |
| Bytes / event (compressed) | {th.bytes_per_event:.1f} |

### Cost implication

At a sustained 100 events/sec from a real source, the compressed write rate
implies roughly:

- **Storage growth:** {cost_gb_day_100eps:.2f} GB/day
- **S3 Standard cost:** ~${cost_usd_month:.2f}/month (at $0.023/GB)

For a real production pipeline, lifecycle policies (raw → IA → Glacier) and
a nightly compaction job would lower this further; this is the worst-case
bill for keeping every batch file at full S3 Standard pricing.
"""


@app.command("metrics")
def show_metrics():
    """Print the current metrics snapshot. Useful in dev."""
    import json

    typer.echo(json.dumps(metrics.snapshot(), indent=2, default=str))


if __name__ == "__main__":
    app()
