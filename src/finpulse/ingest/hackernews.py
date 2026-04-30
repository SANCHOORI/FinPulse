"""HackerNews ingester.

Uses the public Firebase API. Polls /maxitem to discover new IDs and fetches
items individually. No auth, no rate-limit ceiling in practice.

Why HackerNews as the default source: it has no auth requirement, a stable
API, a steady event rate (~10/min), and a meaningful schema. It's the lowest
friction way to demonstrate the contract end-to-end.
"""
from __future__ import annotations

import time
from collections.abc import Iterator

import httpx

from finpulse.ingest.base import Event
from finpulse.log import get_logger
from finpulse.monitoring import metrics

log = get_logger(__name__)

API_BASE = "https://hacker-news.firebaseio.com/v0"


class HackerNewsSource:
    name = "hackernews"

    def __init__(self, poll_interval: float = 2.0, client: httpx.Client | None = None):
        self.poll_interval = poll_interval
        self._client = client or httpx.Client(timeout=10.0)
        self._last_id: int | None = None

    def _max_id(self) -> int:
        r = self._client.get(f"{API_BASE}/maxitem.json")
        r.raise_for_status()
        return int(r.text)

    def _fetch(self, item_id: int) -> dict | None:
        r = self._client.get(f"{API_BASE}/item/{item_id}.json")
        r.raise_for_status()
        if r.text.strip() in ("null", ""):
            return None
        return r.json()

    def _to_event(self, item: dict) -> Event | None:
        # HN items: type in {story, comment, job, poll, pollopt}. Skip deleted/dead.
        if item.get("deleted") or item.get("dead"):
            return None
        text = item.get("title") or item.get("text") or ""
        if not text:
            return None
        return Event(
            source=self.name,
            event_id=str(item["id"]),
            event_ts=int(item.get("time", 0)) * 1000,
            ingested_ts=int(time.time() * 1000),
            author=item.get("by"),
            text=text,
            url=item.get("url"),
            raw=item,
        )

    def stream(self) -> Iterator[Event]:
        if self._last_id is None:
            self._last_id = self._max_id()
            log.info("hn.start", last_id=self._last_id)

        while True:
            try:
                current = self._max_id()
            except httpx.HTTPError as e:
                log.warning("hn.maxitem_error", error=str(e))
                metrics.incr("ingest.upstream_errors", source=self.name)
                time.sleep(self.poll_interval)
                continue

            if current <= self._last_id:
                time.sleep(self.poll_interval)
                continue

            for item_id in range(self._last_id + 1, current + 1):
                metrics.incr("ingest.events_in", source=self.name)
                try:
                    item = self._fetch(item_id)
                except httpx.HTTPError as e:
                    log.warning("hn.item_error", id=item_id, error=str(e))
                    metrics.incr("ingest.upstream_errors", source=self.name)
                    continue
                if item is None:
                    metrics.incr("ingest.events_skipped", source=self.name, reason="null")
                    continue
                event = self._to_event(item)
                if event is None:
                    metrics.incr("ingest.events_skipped", source=self.name, reason="empty")
                    continue
                yield event

            self._last_id = current
