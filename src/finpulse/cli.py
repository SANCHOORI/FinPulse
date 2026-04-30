"""FinPulse CLI."""
from __future__ import annotations

import time

import typer

from finpulse.config import Settings
from finpulse.features import summarize
from finpulse.ingest.hackernews import HackerNewsSource
from finpulse.log import configure as configure_logging
from finpulse.log import get_logger
from finpulse.monitoring import metrics
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


@app.command("metrics")
def show_metrics():
    """Print the current metrics snapshot. Useful in dev."""
    import json

    typer.echo(json.dumps(metrics.snapshot(), indent=2, default=str))


if __name__ == "__main__":
    app()
