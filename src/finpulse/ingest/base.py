"""The contract every ingest source implements.

A source is a long-running iterable of Events. The runner is responsible for
batching, flushing to storage, and restarting on crash.
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from pydantic import BaseModel, Field


class Event(BaseModel):
    """The canonical event shape after ingest normalization.

    Sources may emit any upstream payload, but everything written to the lake
    flows through this model. New fields require an explicit schema bump.
    """

    source: str = Field(description="Origin source identifier, e.g. 'hackernews'")
    event_id: str = Field(description="Stable upstream identifier, used for dedup")
    event_ts: int = Field(description="Event time in epoch milliseconds (UTC)")
    ingested_ts: int = Field(description="Wall-clock at ingest, epoch milliseconds (UTC)")
    author: str | None = Field(default=None, description="Upstream author/user, if any")
    text: str = Field(description="Free-text payload (title / body / comment)")
    url: str | None = Field(default=None, description="Originating URL, if any")
    raw: dict = Field(default_factory=dict, description="Untouched upstream payload, for replay")


class IngestSource(Protocol):
    """An ingest source. Implementations are typically generators that yield
    events as fast as upstream produces them, sleeping between polls."""

    name: str

    def stream(self) -> Iterator[Event]:
        ...
