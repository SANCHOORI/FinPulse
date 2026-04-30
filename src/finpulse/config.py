"""Twelve-factor config: read once from env at startup."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    lake_root: str
    batch_max_events: int
    batch_max_seconds: float
    poll_interval: float
    log_format: str
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            lake_root=os.getenv("FINPULSE_LAKE_ROOT", "./data/lake"),
            batch_max_events=int(os.getenv("FINPULSE_BATCH_MAX_EVENTS", "500")),
            batch_max_seconds=float(os.getenv("FINPULSE_BATCH_MAX_SECONDS", "30")),
            poll_interval=float(os.getenv("FINPULSE_POLL_INTERVAL", "2.0")),
            log_format=os.getenv("FINPULSE_LOG_FORMAT", "text"),
            log_level=os.getenv("FINPULSE_LOG_LEVEL", "INFO"),
        )
