"""In-process counters with structured-log emission.

This is intentionally minimal. The interface is:
    metrics.incr("name", value=1, **labels)
    metrics.snapshot() -> dict

In production, swap the backend (CloudWatch / Prometheus / Datadog). The
call sites don't change.
"""
from __future__ import annotations

import threading
from collections import defaultdict

_lock = threading.Lock()
_counters: dict[tuple, int] = defaultdict(int)


def _key(name: str, labels: dict) -> tuple:
    return (name, tuple(sorted(labels.items())))


def incr(name: str, value: int = 1, **labels) -> None:
    with _lock:
        _counters[_key(name, labels)] += value


def snapshot() -> dict:
    with _lock:
        out: dict = {}
        for (name, label_tuple), value in _counters.items():
            out.setdefault(name, []).append({"labels": dict(label_tuple), "value": value})
        return out


def reset() -> None:
    with _lock:
        _counters.clear()
