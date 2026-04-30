# FinPulse

[![CI](https://github.com/SANCHOORI/FinPulse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SANCHOORI/FinPulse/actions/workflows/ci.yml)

A production-grade, end-to-end data system that ingests live social/financial data, lands it reliably to S3-compatible storage, builds features on top, and produces market signals.

This repo is built as a serious-but-compact demonstration of the data infrastructure that powers a learning trading system: high-throughput ingestion from external providers, durable Parquet-on-S3 storage, structured feature preparation, and observable operation.

---

## What this system does

```
 external sources           durable storage           feature layer            signal layer
 ─────────────────          ───────────────           ─────────────            ────────────
 HackerNews API   ─┐                                                       
 Bluesky firehose  ├─►  partitioned Parquet  ─►  DuckDB / Spark    ─►   sentiment-driven
 StockTwits        │    (S3 / S3-compatible)     feature views          market signal
 Reddit            ─┘                                                  + backtest harness

                          ▲                          ▲
                          │                          │
                  schema + idempotency       monitoring + metrics
```

1. **Ingest** — long-running, restart-safe pollers / streamers per source. Each source is an isolated process behind a common interface, so adding a new provider is one file.
2. **Land** — events land in S3 (or local FS / MinIO for dev) as Parquet, partitioned by `source / dt / hr`. Writes are idempotent: re-running an ingest window will not double-count.
3. **Structure** — a DuckDB-backed feature layer joins social events to a per-ticker, per-minute view. The same SQL runs locally on Parquet and on S3 in production.
4. **Signal** — a simple sentiment-driven strategy (mention-volume + sentiment delta) produces directional bets, evaluated by a vectorised backtest.
5. **Operate** — every component emits counters (events processed, lag, parse errors, write bytes) so failure modes are visible.

---

## Quickstart

```bash
# 1. install (uses uv if available, falls back to pip)
make install

# 2. run the HackerNews ingester for ~60 seconds against local FS
make ingest-hn

# 3. inspect what landed (DuckDB query over Parquet)
make query

# 4. run sentiment scoring + ticker extraction over the lake
finpulse score

# 5. inspect per-ticker mentions and average sentiment
finpulse tickers

# 6. run tests
make test
```

A successful run produces something like:

```
data/lake/source=hackernews/dt=2026-04-30/hr=14/part-00000.parquet
data/lake/source=hackernews/dt=2026-04-30/hr=14/part-00001.parquet
...
```

and `make query` will show counts per minute.

---

## Repo layout

```
src/finpulse/
  ingest/         # one file per source; all implement IngestSource
    base.py
    hackernews.py
  storage/        # parquet sink, partitioning, idempotent writes
    parquet_sink.py
  features/       # DuckDB views, sentiment scoring, ticker extraction
    build.py
    sentiment.py
    tickers.py
  monitoring/     # counters + structured logging
    metrics.py
  config.py
  cli.py          # entrypoints: ingest, query, score, tickers, metrics
docs/
  ARCHITECTURE.md # design choices + what changes at 100x scale
tests/
```

---

## Design choices (the short version — see `docs/ARCHITECTURE.md` for the long version)

- **Parquet on S3, not a database.** A trading model needs to replay arbitrary historical windows. Columnar files in object storage are cheaper, more portable, and easier to backfill than a database.
- **Hive-style partitioning by `source / dt / hr`.** Lets readers prune by time without scanning the whole lake. Hour-level partitioning balances file count vs. query selectivity.
- **Idempotent writes via deterministic file keys.** Each batch writes `part-<batch_id>.parquet`. Re-running the same window overwrites the same files; no duplication.
- **Schema declared up-front.** Every source defines a typed `Event` model. Writers will fail loud on schema drift rather than silently corrupting partitions.
- **DuckDB for features.** Reads Parquet directly from local FS or S3, no ETL hop. Same SQL works in dev and prod. When this stops scaling, the SQL ports to Spark unchanged.
- **One source = one process.** Sources don't share state. A bad day for the StockTwits API doesn't take down HN ingestion.

---

## What's deliberately deferred

This scaffold runs end-to-end with one ingest source. The following are next-step work, not infrastructure decisions:

- **Additional sources** — Bluesky firehose, StockTwits, Reddit. Each is a single `IngestSource` implementation; the storage / feature layers don't change.
- **Sentiment model** — currently a placeholder; swap in VADER for a baseline or a small HF model for something better.
- **Strategy + backtest** — the feature layer is wired, but the signal definition and backtest harness are stubs.
- **Production S3 + Kafka** — local FS is the dev backend; the sink already speaks S3 via `s3://` paths. Kafka between ingest and sink is a one-day change once write rates demand it.

The roadmap section below tracks these.

---

## Roadmap

- [x] Ingest interface + HackerNews implementation
- [x] Partitioned Parquet sink with idempotent writes
- [x] DuckDB-backed feature view
- [x] Counter-based monitoring + structured logging
- [x] Tests for the storage layer
- [x] Sentiment scoring (VADER baseline)
- [x] Ticker extraction (cashtag + whitelist)
- [x] Per-ticker feature view (mention volume, average sentiment)
- [ ] Bluesky firehose ingester (AT Protocol over WebSocket)
- [ ] StockTwits streaming ingester
- [ ] Upgrade sentiment to a small finance-tuned transformer
- [ ] Sentiment delta (rolling window) feature
- [ ] Market data join (Alpaca / yfinance free tier)
- [ ] Signal: abnormal mentions + sentiment shift → directional bet
- [ ] Vectorised backtest with cost / slippage assumptions
- [ ] Throughput + cost report (events/sec, $/GB ingested)
- [ ] Failure-mode chaos test (kill ingest mid-batch, verify no data loss)

---

## External dependencies

- **HackerNews Firebase API** — public, no auth, rate-limit friendly. Used as the default thin-slice source so this repo runs without secrets.
- **Bluesky firehose** *(planned)* — public AT Protocol stream, no auth.
- **Alpaca / yfinance** *(planned)* — for intraday market data joins. Alpaca needs a free API key; yfinance is unauthenticated but limited.

No paid services are required to run the current scaffold.

---

## Notes for reviewers

- This is a thin slice, intentionally. The scaffold covers the parts that are hardest to get right (schema, partitioning, idempotency, observability) and leaves the parts that are easy to bolt on (more sources, a richer signal) as roadmap items.
- The README, the architecture doc, and the commit history are part of the submission. The commit history shows the order things were built and why.
