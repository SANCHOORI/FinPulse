"""Ticker extraction.

Two regex strategies:
  1. Cashtags ($AAPL) — high precision, low recall.
  2. Bare uppercase tokens (AAPL) — higher recall, but only counted if the
     token is in a known-tickers whitelist. Without the whitelist, words
     like "FOR", "ALL", "ONE" produce false positives.

The whitelist is intentionally a small subset (popular US tickers + major
indexes / ETFs) that's good enough for a thin slice. Production would load
this from a vendor universe file (CRSP / Bloomberg / Polygon).
"""
from __future__ import annotations

import re

# ~120 commonly-discussed US tickers + major indexes/ETFs. Not exhaustive.
KNOWN_TICKERS: frozenset[str] = frozenset(
    {
        # Mega-cap tech
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA",
        "AVGO", "ORCL", "CRM", "ADBE", "AMD", "INTC", "CSCO", "QCOM",
        "TXN", "IBM", "NFLX", "PYPL",
        # Banks / fintech
        "JPM", "BAC", "WFC", "GS", "MS", "C", "V", "MA", "AXP", "SCHW",
        "COF", "USB", "BLK", "SQ", "COIN",
        # Retail / consumer
        "WMT", "TGT", "COST", "HD", "LOW", "NKE", "MCD", "SBUX", "DIS",
        "PG", "KO", "PEP", "PM", "MO",
        # Energy / industrials
        "XOM", "CVX", "COP", "OXY", "BA", "GE", "CAT", "HON", "RTX", "LMT",
        # Healthcare / pharma
        "JNJ", "PFE", "MRK", "ABBV", "LLY", "UNH", "CVS", "TMO", "ABT",
        "BMY", "AMGN",
        # EV / mobility / aerospace
        "F", "GM", "RIVN", "LCID", "NIO", "UBER", "LYFT", "DAL", "UAL",
        "AAL",
        # Semiconductors / hardware
        "TSM", "ASML", "MU", "AMAT", "LRCX", "MRVL", "ON", "WDC", "STX",
        # Crypto-adjacent / meme / popular
        "GME", "AMC", "BB", "PLTR", "SOFI", "HOOD", "RBLX", "DKNG", "MARA",
        "RIOT", "MSTR",
        # AI / data
        "SNOW", "DDOG", "NET", "MDB", "CRWD", "PANW", "ZS", "OKTA", "TEAM",
        # Indexes / major ETFs
        "SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "ARKK", "GLD", "SLV",
        "TLT", "TQQQ", "SQQQ", "VIX", "UVXY", "USO",
    }
)

_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")
_BARE_RE = re.compile(r"\b([A-Z]{2,5})\b")


def extract_tickers(text: str) -> list[str]:
    """Return the unique tickers mentioned in `text`, preserving first-seen order.

    Cashtags ($X) are accepted as long as the symbol exists in the whitelist
    (filters $YOLO and similar). Bare uppercase tokens are accepted only if
    they exist in the whitelist.
    """
    if not text:
        return []
    seen: dict[str, None] = {}
    for sym in _CASHTAG_RE.findall(text):
        if sym in KNOWN_TICKERS:
            seen.setdefault(sym, None)
    for sym in _BARE_RE.findall(text):
        if sym in KNOWN_TICKERS:
            seen.setdefault(sym, None)
    return list(seen)
