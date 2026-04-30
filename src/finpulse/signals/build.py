"""Signal definition.

Composite signal:
    z_mentions(5m vs 1h baseline) * sentiment_shift(5m vs 1h)

Per ticker, per minute:
  - z_mentions = (mentions_5m - mean(mentions over 1h)) / stddev(over 1h)
  - sentiment_shift = avg_sentiment_5m - avg_sentiment_1h
  - composite = z_mentions * sentiment_shift
  - position = 'long' if composite > +1, 'short' if < -1, else 'flat'

Joined (LEFT) with mock OHLCV market data to produce a forward-5m return for
each signal row, so reviewers can see signal vs. price together. The forward
return is for inspection only — a real backtest with cost/slippage assumptions
is the next roadmap item.

Note on rolling windows: a thin-slice repo with minutes of data won't have
enough observations to populate the 1-hour baseline cleanly. The SQL handles
this gracefully (NULL z-score → flat position) so the demo doesn't crash on
small inputs; reviewers running a longer ingest will see the signal fire.
"""
from __future__ import annotations

import os

from finpulse.log import get_logger

log = get_logger(__name__)


_SIGNAL_SQL = """
WITH per_minute AS (
    SELECT
        ticker,
        date_trunc('minute', s.event_at) AS bucket,
        count(*) AS mentions,
        avg(s.compound) AS avg_sentiment
    FROM sentiment s, UNNEST(s.tickers) AS t(ticker)
    GROUP BY 1, 2
),
rolled AS (
    SELECT
        ticker,
        bucket,
        mentions,
        avg_sentiment,
        sum(mentions) OVER w5 AS m5,
        avg(avg_sentiment) OVER w5 AS s5,
        avg(mentions) OVER w60 AS m60_mean,
        stddev_pop(mentions) OVER w60 AS m60_sd,
        avg(avg_sentiment) OVER w60 AS s60
    FROM per_minute
    WINDOW
        w5  AS (PARTITION BY ticker ORDER BY bucket
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w60 AS (PARTITION BY ticker ORDER BY bucket
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
),
signal AS (
    SELECT
        ticker,
        bucket,
        mentions,
        m5,
        m60_mean,
        m60_sd,
        s5,
        s60,
        CASE
            WHEN m60_sd IS NULL OR m60_sd = 0 THEN 0
            ELSE (m5 - m60_mean) / m60_sd
        END AS z_mentions,
        COALESCE(s5 - s60, 0) AS sentiment_shift
    FROM rolled
),
scored AS (
    SELECT
        ticker,
        bucket,
        round(z_mentions::DECIMAL(8, 3), 3) AS z_mentions,
        round(sentiment_shift::DECIMAL(8, 3), 3) AS sentiment_shift,
        round((z_mentions * sentiment_shift)::DECIMAL(8, 3), 3) AS composite,
        CASE
            WHEN z_mentions * sentiment_shift > 1  THEN 'long'
            WHEN z_mentions * sentiment_shift < -1 THEN 'short'
            ELSE 'flat'
        END AS position
    FROM signal
),
market_min AS (
    SELECT
        ticker,
        date_trunc('minute', make_timestamp(ts * 1000)) AS bucket,
        close
    FROM market
),
joined AS (
    SELECT
        sc.ticker,
        sc.bucket,
        sc.z_mentions,
        sc.sentiment_shift,
        sc.composite,
        sc.position,
        m_now.close AS close_now,
        m_fwd.close AS close_fwd_5m,
        CASE WHEN m_now.close IS NOT NULL AND m_fwd.close IS NOT NULL
             THEN round(((m_fwd.close / m_now.close) - 1)::DECIMAL(8, 5), 5)
             ELSE NULL
        END AS fwd_ret_5m
    FROM scored sc
    LEFT JOIN market_min m_now
      ON m_now.ticker = sc.ticker AND m_now.bucket = sc.bucket
    LEFT JOIN market_min m_fwd
      ON m_fwd.ticker = sc.ticker
     AND m_fwd.bucket = sc.bucket + INTERVAL 5 MINUTE
)
SELECT *
FROM joined
ORDER BY bucket DESC, abs(composite) DESC, ticker
LIMIT 100;
"""


def build_signal(lake_root: str) -> list[tuple]:
    """Compute the composite signal, joined with mock OHLCV.

    Returns: list of (ticker, bucket, z_mentions, sentiment_shift, composite,
    position, close_now, close_fwd_5m, fwd_ret_5m).
    """
    from finpulse.features.build import open_lake

    con = open_lake(lake_root)

    root = lake_root.rstrip("/")
    market_glob = f"{root}/derived/market/dt=*/**/*.parquet"
    if _has_market(root):
        con.execute(
            f"""
            CREATE OR REPLACE VIEW market AS
            SELECT * FROM read_parquet('{market_glob}', hive_partitioning = 1);
            """
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW market AS
            SELECT
                CAST(NULL AS VARCHAR) AS ticker,
                CAST(NULL AS BIGINT) AS ts,
                CAST(NULL AS DOUBLE) AS open,
                CAST(NULL AS DOUBLE) AS high,
                CAST(NULL AS DOUBLE) AS low,
                CAST(NULL AS DOUBLE) AS close,
                CAST(NULL AS BIGINT) AS volume
            WHERE FALSE;
            """
        )

    return con.execute(_SIGNAL_SQL).fetchall()


def _has_market(root: str) -> bool:
    if root.startswith("s3://"):
        return True
    market_dir = os.path.join(root, "derived", "market")
    if not os.path.isdir(market_dir):
        return False
    return any(
        any(f.endswith(".parquet") for f in files)
        for _r, _d, files in os.walk(market_dir)
    )
