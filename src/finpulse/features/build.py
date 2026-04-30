"""DuckDB-backed feature views over the Parquet lake.

The same SQL works against local FS or S3; DuckDB autodetects from the URI.

Two layers:
  1. Raw events lake:    {root}/source=*/dt=*/hr=*/*.parquet
  2. Sentiment-scored:   {root}/derived/sentiment/source=*/dt=*/hr=*/*.parquet

`run_scoring` produces (2) from (1). `per_ticker_view` joins them.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from finpulse.features.sentiment import score_text
from finpulse.features.tickers import extract_tickers
from finpulse.log import get_logger
from finpulse.monitoring import metrics

log = get_logger(__name__)


_SENTIMENT_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("compound", pa.float32(), nullable=False),
        pa.field("pos", pa.float32(), nullable=False),
        pa.field("neu", pa.float32(), nullable=False),
        pa.field("neg", pa.float32(), nullable=False),
        pa.field("tickers", pa.list_(pa.string()), nullable=False),
        pa.field("scored_ts", pa.int64(), nullable=False),
    ]
)


def open_lake(lake_root: str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with views over the lake.

    Views:
      - events            (raw)
      - sentiment         (derived; empty view if scoring hasn't run yet)
    """
    con = duckdb.connect(":memory:")
    if lake_root.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")

    root = lake_root.rstrip("/")
    raw_glob = f"{root}/source=*/**/*.parquet"
    sentiment_glob = f"{root}/derived/sentiment/source=*/**/*.parquet"

    if _has_files(root, exclude_subdir="derived"):
        con.execute(
            f"""
            CREATE OR REPLACE VIEW events AS
            SELECT *,
                   make_timestamp(event_ts * 1000) AS event_at
            FROM read_parquet('{raw_glob}', hive_partitioning = 1);
            """
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW events AS
            SELECT
                CAST(NULL AS VARCHAR) AS source,
                CAST(NULL AS VARCHAR) AS event_id,
                CAST(NULL AS BIGINT) AS event_ts,
                CAST(NULL AS BIGINT) AS ingested_ts,
                CAST(NULL AS VARCHAR) AS author,
                CAST(NULL AS VARCHAR) AS text,
                CAST(NULL AS VARCHAR) AS url,
                CAST(NULL AS TIMESTAMP) AS event_at
            WHERE FALSE;
            """
        )

    # sentiment view is conditional: read_parquet errors if no files match.
    if _has_files(f"{root}/derived/sentiment"):
        con.execute(
            f"""
            CREATE OR REPLACE VIEW sentiment AS
            SELECT *,
                   make_timestamp(event_ts * 1000) AS event_at
            FROM read_parquet('{sentiment_glob}', hive_partitioning = 1);
            """
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW sentiment AS
            SELECT
                CAST(NULL AS VARCHAR) AS source,
                CAST(NULL AS VARCHAR) AS event_id,
                CAST(NULL AS BIGINT) AS event_ts,
                CAST(NULL AS REAL) AS compound,
                CAST(NULL AS REAL) AS pos,
                CAST(NULL AS REAL) AS neu,
                CAST(NULL AS REAL) AS neg,
                CAST(NULL AS VARCHAR[]) AS tickers,
                CAST(NULL AS BIGINT) AS scored_ts,
                CAST(NULL AS TIMESTAMP) AS event_at
            WHERE FALSE;
            """
        )
    return con


def _has_files(path: str, exclude_subdir: str | None = None) -> bool:
    """Walk `path` looking for any .parquet file. If `exclude_subdir` is set,
    files under `{path}/{exclude_subdir}/...` are ignored — used to avoid
    treating derived data as raw events when both live under the same root.
    """
    if path.startswith("s3://"):
        return True  # let read_parquet decide; we don't crawl S3 here
    if not os.path.isdir(path):
        return False
    excluded = os.path.join(path, exclude_subdir) if exclude_subdir else None
    for cur_root, _dirs, files in os.walk(path):
        if excluded is not None and (
            cur_root == excluded or cur_root.startswith(excluded + os.sep)
        ):
            continue
        if any(f.endswith(".parquet") for f in files):
            return True
    return False


def summarize(lake_root: str) -> list[tuple]:
    """Per-source, per-minute event counts."""
    con = open_lake(lake_root)
    return con.execute(
        """
        SELECT source,
               date_trunc('minute', event_at) AS minute,
               count(*) AS n
        FROM events
        GROUP BY 1, 2
        ORDER BY 2 DESC, 1
        LIMIT 50;
        """
    ).fetchall()


def per_ticker_view(lake_root: str, window: str = "minute") -> list[tuple]:
    """Per-ticker, per-window mention count + average sentiment.

    `window` is anything date_trunc accepts: 'minute', '5minute', 'hour', etc.
    DuckDB doesn't support '5minute' natively; for non-standard windows the
    caller can compose their own SQL. Defaults to 'minute' for granularity.
    """
    con = open_lake(lake_root)
    return con.execute(
        f"""
        SELECT t.ticker,
               date_trunc('{window}', s.event_at) AS bucket,
               count(*) AS mentions,
               round(avg(s.compound)::DECIMAL(6, 3), 3) AS avg_compound
        FROM sentiment s, UNNEST(s.tickers) AS t(ticker)
        GROUP BY 1, 2
        ORDER BY 2 DESC, 3 DESC, 1
        LIMIT 50;
        """
    ).fetchall()


def run_scoring(lake_root: str) -> int:
    """Read all raw events, score sentiment + extract tickers, write to the
    sentiment partition. Idempotent per (source, dt, hr): re-running rewrites
    the same files.

    Returns the number of events scored.
    """
    con = open_lake(lake_root)
    rows = con.execute(
        """
        SELECT source, event_id, event_ts, text
        FROM events
        ORDER BY event_ts;
        """
    ).fetchall()
    if not rows:
        log.info("score.empty")
        return 0

    # Group by (source, dt, hr) so each group becomes one output partition.
    groups: dict[tuple[str, str, str], list[tuple]] = {}
    for source, event_id, event_ts, text in rows:
        dt_obj = datetime.fromtimestamp(event_ts / 1000, tz=UTC)
        dt = dt_obj.strftime("%Y-%m-%d")
        hr = dt_obj.strftime("%H")
        groups.setdefault((source, dt, hr), []).append((event_id, event_ts, text))

    root = lake_root.rstrip("/")
    scored_ts = int(datetime.now(tz=UTC).timestamp() * 1000)
    total = 0

    for (source, dt, hr), batch in groups.items():
        out_dir = f"{root}/derived/sentiment/source={source}/dt={dt}/hr={hr}"
        out_path = f"{out_dir}/part-00000.parquet"

        scored_event_ids = []
        scored_event_ts = []
        compounds: list[float] = []
        poss: list[float] = []
        neus: list[float] = []
        negs: list[float] = []
        ticker_lists: list[list[str]] = []

        for event_id, event_ts, text in batch:
            s = score_text(text or "")
            ticks = extract_tickers(text or "")
            scored_event_ids.append(event_id)
            scored_event_ts.append(event_ts)
            compounds.append(s.compound)
            poss.append(s.pos)
            neus.append(s.neu)
            negs.append(s.neg)
            ticker_lists.append(ticks)

        n = len(batch)
        table = pa.Table.from_pydict(
            {
                "source": [source] * n,
                "event_id": scored_event_ids,
                "event_ts": scored_event_ts,
                "compound": compounds,
                "pos": poss,
                "neu": neus,
                "neg": negs,
                "tickers": ticker_lists,
                "scored_ts": [scored_ts] * n,
            },
            schema=_SENTIMENT_SCHEMA,
        )

        if not out_path.startswith("s3://"):
            os.makedirs(out_dir, exist_ok=True)
        pq.write_table(table, out_path, compression="zstd")
        log.info("score.flush", source=source, dt=dt, hr=hr, events=n, path=out_path)
        metrics.incr("score.events_out", source=source, value=n)
        total += n

    log.info("score.done", events=total)
    return total
