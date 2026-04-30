# FinPulse — Architecture

This document explains the *why* behind the design. The README covers *what* is built.

## Goal

Build a data system that turns a live, messy, high-volume external firehose into structured features and trading signals — reliably, cheaply, and at a shape that keeps improving as you scale it.

## The four layers

### 1. Ingest

Each external source is a long-running process implementing one interface:

```python
class IngestSource(Protocol):
    name: str
    def stream(self) -> Iterator[Event]: ...
```

Why a process per source rather than a single ingester:

- **Failure isolation.** A 500 from StockTwits doesn't slow down HackerNews.
- **Independent scaling.** Bluesky firehose at peak is ~10x denser than Reddit; they should not share a thread.
- **Independent backoff.** Each source can implement its own rate-limit handling without leaking concerns.

The current implementation polls the HackerNews Firebase API. This is the lowest-friction source available (no auth, no rate-limit ceiling, stable API) and is good enough to demonstrate the contract.

### 2. Storage

The lake is partitioned Parquet:

```
{lake_root}/source={source}/dt={YYYY-MM-DD}/hr={HH}/part-{batch_id}.parquet
```

Why Parquet on object storage rather than a database:

- **Replayability.** Trading models train on arbitrary historical windows. A column store on S3 is cheaper to scan than a row store and is the standard interchange format for downstream Spark / Ray / training jobs.
- **Schema-on-write.** PyArrow + a Pydantic schema enforce types at write time. Schema drift fails loud rather than silently producing nullable junk.
- **Cheap to keep forever.** S3 standard at $23/TB/month means a small system can keep years of raw events with no operational burden.

Why hour-level partitioning:

- A typical query is "events in the last N minutes / hour / day." Hour-level partitions prune most of the lake without creating tens of thousands of tiny files per day.
- Day-level partitions would force readers to scan whole days. Minute-level would explode file counts and S3 list costs.

#### Idempotency

Each flushed batch gets a deterministic file key (`part-<batch_id>.parquet`) where `batch_id` is derived from `(source, hour_window, batch_index)`. Re-running an ingest window overwrites the same files instead of producing duplicates. This matters because:

- Operators *will* re-run failed windows.
- Backfills are easier when you can re-run without dedup logic downstream.
- Crash-mid-batch is recoverable: the partial file is overwritten on the next attempt.

### 3. Features

Features are DuckDB views over the lake. DuckDB reads Parquet from local FS or S3 with the same SQL — no ETL hop, no separate "feature store" service.

```sql
CREATE VIEW events AS
SELECT * FROM read_parquet('{lake_root}/**/*.parquet', hive_partitioning=1);
```

Why DuckDB for the dev / first-prod feature layer:

- No infrastructure. A single binary, in-process.
- Reads Parquet at memory bandwidth on a laptop.
- Same SQL ports to Spark (`spark.read.parquet`) when you outgrow a single node.

The feature layer is intentionally thin in this scaffold. Real ones (mention-volume per ticker per minute, sentiment delta over rolling windows) are the next step.

### 4. Signal + market data

Implemented in `signals/build.py` as DuckDB SQL:

- Per-ticker, per-minute mentions and average sentiment from the sentiment partition (UNNEST over the `tickers` array).
- 5m and 1h rolling windows over that view.
- `z_mentions = (mentions_5m − mean(mentions over 1h)) / stddev`.
- `sentiment_shift = avg_sentiment_5m − avg_sentiment_1h`.
- `composite = z_mentions × sentiment_shift`, thresholded into `long` / `short` / `flat`.
- LEFT JOINed with market data on `(ticker, minute)` to attach `close_now`, `close_fwd_5m`, and a forward-5m return — useful for visual sanity-checking, not a real PnL.

Market data lives at `{root}/derived/market/dt=*/hr=*/*.parquet` with the same Hive layout as events. The current implementation in `market/mock.py` is a deterministic random walk per ticker (seeded by SHA-256 of the ticker, so re-runs are reproducible). The fetch / write code is identical in shape to a real provider call — `_random_walk()` is the swap point for `yfinance.download()` or Alpaca bars.

A real vectorised backtest with cost + slippage assumptions is the next roadmap item; the signal SQL already exposes everything that backtest would need.

## Cross-cutting concerns

### Observability

Every component emits counters via `monitoring.metrics`. The current sink writes them to structured logs; in production they go to whatever metric backend exists (CloudWatch, Prometheus, Datadog).

The metrics that matter:

- `events_in` (per source) — input rate
- `events_out` (per source) — write rate. Divergence from `events_in` = a bug.
- `parse_errors` — schema or upstream-format failures
- `lag_seconds` — wall clock time minus event timestamp at flush
- `bytes_written` — for cost accounting

### Configuration

Twelve-factor: everything via env vars, with `.env.example` as the documented surface. No YAML. Config is read once at startup into a typed `Settings` object.

### Logging

Structured (`structlog`). One key/value pair per fact. Local dev uses the human renderer; production uses JSON.

## What changes at 100x scale

The current scaffold runs on one machine and writes to a local lake. The shape doesn't change at 100x; only the substrate does:

| Concern | Today | At 100x |
| --- | --- | --- |
| Ingest | one process per source | same, but K8s deployment per source with HPA on lag |
| Inter-component buffer | direct call | Kafka between ingester and sink |
| Storage | local FS | S3 (already supported via `s3://` URIs) |
| File compaction | n/a (writer flush is the file) | nightly compaction job: small batch files → larger hourly files |
| Feature engine | DuckDB | DuckDB for ad-hoc; Spark on EMR for batch training-set builds |
| Schema | Pydantic at write time | Pydantic at write + Iceberg / Delta for evolution + time travel |
| Monitoring | structured logs | metrics → Prometheus, logs → Loki, traces → OTel |
| Cost control | n/a | S3 lifecycle policy (raw → IA after 30d → Glacier after 1yr) |

Critically, *application code does not change*. The sink already takes an `s3://` URI; swapping FS for S3 is one env var. The feature SQL is the same on Parquet regardless of where it lives.

## What this design gets wrong (honest version)

- **No exactly-once semantics across ingest+sink boundary.** If the ingester crashes after fetching events but before the sink flushes, those events are re-fetched on restart. This is fine for an at-least-once social firehose where downstream features are idempotent over duplicates within a window — but is *not* fine for, say, an order book delta stream. Adding Kafka + checkpointing fixes this.
- **No schema evolution story.** Adding a column to an event today means readers that don't know about it are still fine, but removing or renaming a column will break partitions written before the change. Iceberg / Delta solves this; we don't have it.
- **One file per batch is wasteful at small batch sizes.** A 60-second test produces tens of small files. Fine for dev; production wants a compaction job.
- **No cost guardrails.** Nothing enforces "don't write more than X GB / hour" or "fail loud if event rate halves." These are the alerts that actually catch problems.
- **Market data is synthetic.** `market/mock.py` is a placeholder; the price paths it produces are seeded random walks, not real prices. The signal numbers it feeds are structurally correct but financially meaningless. A real fetcher swap is one file.
