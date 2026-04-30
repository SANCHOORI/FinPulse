"""Sentiment scoring via VADER.

VADER is a lexicon + heuristics scorer designed for short social text. It
returns four scores: compound (overall, in [-1, 1]), pos, neu, neg (each in
[0, 1] and summing to 1).

Why VADER as a baseline:
  - No model download, no GPU, fast (~10us per text).
  - Calibrated for social-media tone (handles emoticons, intensifiers,
    negation reasonably well).
  - Trivial to swap out: anything implementing `score_text(str) -> Sentiment`
    can replace this.

In production, the next step is a small fine-tuned transformer (e.g. a
distilled finance-tuned BERT) running as a separate batch job. The pipeline
shape doesn't change — same inputs, same outputs.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


@dataclass(frozen=True)
class Sentiment:
    compound: float
    pos: float
    neu: float
    neg: float


@lru_cache(maxsize=1)
def _analyzer() -> SentimentIntensityAnalyzer:
    return SentimentIntensityAnalyzer()


def score_text(text: str) -> Sentiment:
    if not text:
        return Sentiment(0.0, 0.0, 1.0, 0.0)
    s = _analyzer().polarity_scores(text)
    return Sentiment(
        compound=float(s["compound"]),
        pos=float(s["pos"]),
        neu=float(s["neu"]),
        neg=float(s["neg"]),
    )
