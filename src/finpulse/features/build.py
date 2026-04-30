"""DuckDB-backed feature views over the Parquet lake.

The same SQL works against local FS or S3; DuckDB autodetects from the URI.
"""
from __future__ import annotations

import duckdb


def open_lake(lake_root: str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with an `events` view over the lake."""
    con = duckdb.connect(":memory:")
    if lake_root.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    glob = f"{lake_root.rstrip('/')}/**/*.parquet"
    con.execute(
        f"""
        CREATE OR REPLACE VIEW events AS
        SELECT *,
               make_timestamp(event_ts * 1000) AS event_at
        FROM read_parquet('{glob}', hive_partitioning = 1);
        """
    )
    return con


def summarize(lake_root: str) -> list[tuple]:
    """Return per-source, per-minute event counts. The smallest-possible
    feature: enough to demonstrate the lake is queryable end-to-end."""
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
