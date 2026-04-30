"""FinPulse CLI."""
from __future__ import annotations

import time

import typer

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


@app.command("metrics")
def show_metrics():
    """Print the current metrics snapshot. Useful in dev."""
    import json

    typer.echo(json.dumps(metrics.snapshot(), indent=2, default=str))


if __name__ == "__main__":
    app()
