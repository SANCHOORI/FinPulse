"""Vectorised pandas backtest over the composite signal.

Position mapping: long -> +1, short -> -1, flat -> 0.
Per-row PnL = position * fwd_ret_5m. Sharpe is annualised assuming 5-minute
holdings traded over a 6.5h x 252d trading year (~19,656 periods/year).

Synthetic-prices caveat: when market data comes from `market/mock.py`
(deterministic random walks), the inputs have no relation to the sentiment
signal, so any computed Sharpe is noise around zero. The framework computes
real metrics; only the prices are fake. Replace `mock.py` with a real
fetcher to get meaningful numbers.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from finpulse.signals import build_signal

PERIODS_PER_YEAR_5M = 252 * 6.5 * 12  # 5-min bars per trading year ~= 19,656


@dataclass(frozen=True)
class BacktestResult:
    n_signals: int
    n_trades: int
    total_pnl: float
    mean_pnl: float
    std_pnl: float
    sharpe_annual: float
    max_drawdown: float
    hit_rate: float


def run_backtest(lake_root: str) -> BacktestResult:
    rows = build_signal(lake_root)
    cols = [
        "ticker",
        "bucket",
        "z_mentions",
        "sentiment_shift",
        "composite",
        "position",
        "close_now",
        "close_fwd_5m",
        "fwd_ret_5m",
    ]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return BacktestResult(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    df = df.dropna(subset=["fwd_ret_5m"])
    pos_map = {"long": 1, "short": -1, "flat": 0}
    df["pos"] = df["position"].map(pos_map).fillna(0).astype(int)
    df["pnl"] = df["pos"] * df["fwd_ret_5m"].astype(float)

    trades = df[df["pos"] != 0]
    n_signals = len(df)
    n_trades = len(trades)

    if n_trades == 0:
        return BacktestResult(n_signals, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    total = float(trades["pnl"].sum())
    mean = float(trades["pnl"].mean())
    std = float(trades["pnl"].std(ddof=0)) or 0.0
    sharpe = (mean / std * math.sqrt(PERIODS_PER_YEAR_5M)) if std > 0 else 0.0

    sorted_trades = trades.sort_values("bucket")
    cum = sorted_trades["pnl"].cumsum()
    drawdown = float((cum - cum.cummax()).min())

    hit_rate = float((trades["pnl"] > 0).mean())

    return BacktestResult(
        n_signals=n_signals,
        n_trades=n_trades,
        total_pnl=total,
        mean_pnl=mean,
        std_pnl=std,
        sharpe_annual=float(sharpe),
        max_drawdown=drawdown,
        hit_rate=hit_rate,
    )
