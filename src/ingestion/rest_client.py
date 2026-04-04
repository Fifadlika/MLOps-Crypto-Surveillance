"""
src/ingestion/rest_client.py

Binance REST client for historical kline ingestion and gap backfill.

Responsibilities:
- Fetch historical klines from Binance REST API with pagination.
- Apply request throttling via token bucket (400 requests/minute).
- Fallback to secondary endpoint when primary times out (>10s).
- Retry safely on HTTP 429 with cooldown and bucket reset.
- Enforce two-layer dedup (Redis NX + persistent bloom filter).
- Publish validated closed klines to Redis stream: stream:klines:{symbol}.
- Append raw historical data atomically to data/raw/{symbol}/{YYYY-MM-DD}.jsonl.
- Maintain per-file sidecar metadata in data/raw/{symbol}/{YYYY-MM-DD}.meta.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from pathlib import Path
from typing import Any

import httpx

from src.ingestion.schemas import HistoricalKline
from src.ingestion.writer import AtomicJsonlWriter
from src.utils.bloom import PersistentBloomRegistry
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client

logger = get_logger(__name__)

_DEFAULT_PRIMARY_KLINES_URL = "https://api.binance.com/api/v3/klines"
_DEFAULT_FALLBACK_KLINES_URL = "https://api4.binance.com/api/v3/klines"
_DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
_DEFAULT_RATE_LIMIT_PER_MINUTE = 400
_DEFAULT_RETRY_429_SECONDS = 60
_DEFAULT_KLINE_PAGE_SIZE = 1000
_DEFAULT_STREAM_MAXLEN = 1500


class TokenBucket:
    """Simple async token bucket limiter."""

    def __init__(self, capacity: int, refill_per_second: float) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_per_second <= 0:
            raise ValueError("refill_per_second must be > 0")

        self.capacity = float(capacity)
        self.refill_per_second = float(refill_per_second)
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """Block until enough tokens are available."""
        needed = float(tokens)
        if needed <= 0:
            return

        while True:
            sleep_seconds = 0.0
            async with self._lock:
                self._refill_locked()
                if self._tokens >= needed:
                    self._tokens -= needed
                    return
                deficit = needed - self._tokens
                sleep_seconds = deficit / self.refill_per_second

            await asyncio.sleep(sleep_seconds)

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        self._tokens = self.capacity
        self._last_refill = time.monotonic()

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self._last_refill)
        self._last_refill = now
        self._tokens = min(self.capacity, self._tokens + (elapsed * self.refill_per_second))


class BinanceRESTClient:
    """
    Historical kline client used for bootstrap/gap-fill workflows.

    Primary workflow:
        1. `sync_klines(symbol, start_ms, end_ms)` fetches paginated klines.
        2. Closed klines are validated with `HistoricalKline`.
        3. Valid rows are published to Redis and appended to local raw storage.
    """

    def __init__(
        self,
        *,
        config: Any | None = None,
        redis_client: Any | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config or get_config()
        self._redis = redis_client or get_redis_client()

        streaming_cfg = getattr(self._config, "streaming", None)
        self._primary_klines_url = _DEFAULT_PRIMARY_KLINES_URL
        self._fallback_klines_url = getattr(
            streaming_cfg,
            "rest_url",
            _DEFAULT_FALLBACK_KLINES_URL,
        )
        if not self._fallback_klines_url:
            self._fallback_klines_url = _DEFAULT_FALLBACK_KLINES_URL
        self._fallback_klines_url = f"{str(self._fallback_klines_url).rstrip('/')}/api/v3/klines"
        self._request_timeout_seconds = float(
            getattr(streaming_cfg, "rest_timeout_seconds", _DEFAULT_REQUEST_TIMEOUT_SECONDS)
        )

        data_cfg = getattr(self._config, "data", None)
        raw_path = getattr(data_cfg, "raw_data_path", "data/raw")
        self._raw_data_root = Path(str(raw_path))
        dedup_bloom_dir = (
            Path(str(getattr(data_cfg, "dedup_bloom_dir", "data/raw/.dedup"))) / "rest"
        )

        rate_limit_per_minute = int(
            getattr(streaming_cfg, "rest_rate_limit_per_minute", _DEFAULT_RATE_LIMIT_PER_MINUTE)
        )
        self._retry_429_seconds = int(
            getattr(streaming_cfg, "rest_retry_after_429_seconds", _DEFAULT_RETRY_429_SECONDS)
        )
        self._dedup_ttl_seconds = int(getattr(streaming_cfg, "dedup_ttl_seconds", 3600))

        self._bloom = PersistentBloomRegistry(
            base_dir=dedup_bloom_dir,
            capacity=int(getattr(streaming_cfg, "bloom_capacity", 1_000_000)),
            error_rate=float(getattr(streaming_cfg, "bloom_error_rate", 0.001)),
            autosave_every=500,
        )
        self._writer = AtomicJsonlWriter(
            root_path=self._raw_data_root,
            pipeline_run_id=f"rest-sync-{int(time.time())}",
            lock_timeout_seconds=float(
                getattr(streaming_cfg, "rest_timeout_seconds", _DEFAULT_REQUEST_TIMEOUT_SECONDS)
            ),
        )

        self._rate_limiter = TokenBucket(
            capacity=rate_limit_per_minute,
            refill_per_second=rate_limit_per_minute / 60.0,
        )

        self._http = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None

    async def close(self) -> None:
        """Close owned HTTP client resources."""
        self._bloom.flush_all()
        if self._owns_http_client:
            await self._http.aclose()

    async def sync_klines(  # noqa: C901
        self,
        symbol: str,
        start_ms: int,
        end_ms: int | None = None,
        interval: str = "1m",
    ) -> int:
        """
        Fetch, validate, and publish historical klines for a time range.

        Returns the number of closed klines written to Redis/local raw storage.
        """
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol must not be empty")

        if end_ms is None:
            end_ms = int(time.time() * 1000)

        if start_ms < 0 or end_ms < 0:
            raise ValueError("start_ms and end_ms must be >= 0")
        if start_ms > end_ms:
            raise ValueError("start_ms must be <= end_ms")

        cursor = int(start_ms)
        published_count = 0

        while cursor <= end_ms:
            rows = await self._fetch_page(
                symbol=normalized_symbol,
                start_ms=cursor,
                end_ms=end_ms,
                interval=interval,
                limit=_DEFAULT_KLINE_PAGE_SIZE,
            )
            if not rows:
                break

            last_close_time: int | None = None
            for row in rows:
                kline = HistoricalKline.from_rest_list(normalized_symbol, row)

                # Binance REST can include the currently-forming kline near "now".
                # Closed rows are the only safe inputs for downstream windows.
                if kline.close_time > end_ms:
                    continue

                if await self._is_duplicate_kline(normalized_symbol, kline.open_time):
                    continue

                await self._publish_to_stream(normalized_symbol, kline)
                await self._append_to_raw_file(kline)
                published_count += 1
                last_close_time = kline.close_time

            if last_close_time is None:
                break

            next_start = int(last_close_time) + 1
            if next_start <= cursor:
                logger.warning(
                    "Stopping pagination to avoid infinite loop for %s (cursor=%d next=%d).",
                    normalized_symbol,
                    cursor,
                    next_start,
                )
                break
            cursor = next_start

            if len(rows) < _DEFAULT_KLINE_PAGE_SIZE:
                break

        self._bloom.flush_symbol(normalized_symbol)

        logger.info(
            "REST sync complete for %s (%d -> %d). Published %d closed klines.",
            normalized_symbol,
            start_ms,
            end_ms,
            published_count,
        )
        return published_count

    async def process_gap_event(
        self,
        symbol: str,
        disconnect_time_ms: int,
        reconnect_time_ms: int,
        interval: str = "1m",
    ) -> int:
        """Handle one gap event written by websocket_client.py."""
        return await self.sync_klines(
            symbol=symbol,
            start_ms=disconnect_time_ms,
            end_ms=reconnect_time_ms,
            interval=interval,
        )

    async def _fetch_page(
        self,
        *,
        symbol: str,
        start_ms: int,
        end_ms: int,
        interval: str,
        limit: int,
    ) -> list[list[Any]]:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }

        try:
            return await self._request_page(self._primary_klines_url, params)
        except httpx.TimeoutException:
            logger.warning(
                "Primary REST endpoint timed out for %s. Falling back to %s.",
                symbol,
                self._fallback_klines_url,
            )
            return await self._request_page(self._fallback_klines_url, params)

    async def _request_page(self, url: str, params: dict[str, Any]) -> list[list[Any]]:
        while True:
            await self._rate_limiter.acquire()

            response = await self._http.get(
                url,
                params=params,
                timeout=self._request_timeout_seconds,
            )

            if response.status_code == 429:
                logger.warning(
                    "HTTP 429 received from Binance REST (%s). Sleeping %ds then retrying.",
                    url,
                    self._retry_429_seconds,
                )
                await asyncio.sleep(self._retry_429_seconds)
                self._rate_limiter.reset()
                continue

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(f"Unexpected klines payload type from {url}: {type(payload)}")
            return payload

    async def _publish_to_stream(self, symbol: str, kline: HistoricalKline) -> None:
        stream_key = f"stream:klines:{symbol.lower()}"
        payload = kline.to_stream_dict()

        try:
            await self._redis.xadd(
                stream_key,
                payload,
                maxlen=_DEFAULT_STREAM_MAXLEN,
                approximate=True,
            )
        except TypeError:
            # Current redis wrapper in this repo does not yet expose maxlen args.
            await self._redis.xadd(stream_key, payload)

    async def _append_to_raw_file(self, kline: HistoricalKline) -> None:
        self._writer.append_record(kline.symbol, kline.open_time, kline.model_dump())

    async def _is_duplicate_kline(self, symbol: str, open_time: int) -> bool:
        redis_key = f"dedup:kline:{symbol.lower()}:{open_time}"

        if not await self._set_nx(redis_key):
            return True

        bloom_key = f"{symbol}:{open_time}"
        return self._bloom.check_and_add(symbol, bloom_key)

    async def _set_nx(self, redis_key: str) -> bool:
        set_method = getattr(self._redis, "set", None)
        if set_method is None:
            return True

        try:
            result = set_method(redis_key, "1", ex=self._dedup_ttl_seconds, nx=True)
            if inspect.isawaitable(result):
                result = await result
        except TypeError:
            result = set_method(redis_key, "1")
            if inspect.isawaitable(result):
                result = await result

        if result in (True, "OK", b"OK"):
            return True

        return bool(result)
