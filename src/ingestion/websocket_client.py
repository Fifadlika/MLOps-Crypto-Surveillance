"""
src/ingestion/websocket_client.py

Persistent WebSocket client for Binance multi-stream API.

Responsibilities:
- Connect to Binance combined stream endpoint for trade + kline events
- Route incoming messages to the correct Redis Stream
- Validate raw payloads via Pydantic schemas before writing
- Reconnect automatically with exponential backoff on disconnect/error
- Record disconnection timestamps so rest_client.py can fill gaps later

Data contract output (Bronze layer):
- stream:trades:{symbol}  → trade stream entries (see Data Contract §1)
- stream:klines:{symbol}  → kline stream entries (see Data Contract §1)
- stream:gaps:{symbol}    → gap timestamps (disconnect_time, reconnect_time)
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from src.ingestion.schemas import KlineEvent, TradeEvent
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client

# ── Module-level logger (follows Global Standards §2) ──────────────────────────
logger = get_logger(__name__)

# ── Constants (no magic numbers — all thresholds live in config.yaml) ──────────
# These are only used as fallback defaults; runtime values come from config.
_DEFAULT_MAX_RECONNECT_ATTEMPTS = 10
_DEFAULT_BASE_BACKOFF_SECONDS = 1.0
_DEFAULT_MAX_BACKOFF_SECONDS = 60.0


class BinanceWebSocketClient:
    """
    Manages a persistent connection to the Binance combined stream WebSocket.

    Design decisions:
    - Single combined-stream URL instead of N separate connections keeps
      connection overhead low and simplifies reconnect logic.
    - Pydantic validation happens here (ingestion layer) so that invalid
      events never enter the Bronze layer.
    - Gap timestamps are written to Redis on every disconnect so the REST
      client can fetch missing data without any manual coordination.

    Usage:
        client = BinanceWebSocketClient()
        await client.run()          # runs forever, reconnecting as needed
        await client.stop()         # graceful shutdown
    """

    def __init__(self) -> None:
        self._config = get_config()
        self._redis = get_redis_client()

        # Runtime configuration from config.yaml
        self._symbols: list[str] = [s.upper() for s in self._config.symbols]
        self._ws_base_url: str = self._config.binance.ws_base_url
        self._max_reconnect_attempts: int = getattr(
            self._config.binance, "max_reconnect_attempts", _DEFAULT_MAX_RECONNECT_ATTEMPTS
        )
        self._base_backoff: float = getattr(
            self._config.binance, "base_backoff_seconds", _DEFAULT_BASE_BACKOFF_SECONDS
        )
        self._max_backoff: float = getattr(
            self._config.binance, "max_backoff_seconds", _DEFAULT_MAX_BACKOFF_SECONDS
        )

        # Internal state
        self._running: bool = False
        self._reconnect_count: int = 0
        self._last_disconnect_ts: Optional[int] = None  # Unix ms

    # ── Public API ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Entry point. Loops forever, reconnecting with exponential backoff.

        Think of this as the "supervisor" — it doesn't do any data work itself,
        it just ensures _connect_and_listen() is always running.
        """
        self._running = True
        logger.info("WebSocket client starting. Symbols: %s", self._symbols)

        while self._running:
            if self._reconnect_count >= self._max_reconnect_attempts:
                logger.critical(
                    "Max reconnect attempts (%d) reached. Shutting down WebSocket client.",
                    self._max_reconnect_attempts,
                )
                self._running = False
                break

            try:
                await self._connect_and_listen()
                # If we get here cleanly, reset the backoff counter.
                # A clean exit (stop() called) also lands here.
                if not self._running:
                    break
                self._reconnect_count = 0

            except (ConnectionClosedError, ConnectionClosedOK) as exc:
                self._record_disconnect()
                backoff = self._compute_backoff()
                logger.warning(
                    "WebSocket closed (%s). Reconnecting in %.1fs (attempt %d/%d).",
                    exc,
                    backoff,
                    self._reconnect_count + 1,
                    self._max_reconnect_attempts,
                )
                await asyncio.sleep(backoff)
                self._reconnect_count += 1

            except Exception as exc:  # noqa: BLE001 — log everything, never silently swallow
                self._record_disconnect()
                backoff = self._compute_backoff()
                logger.error(
                    "Unexpected error in WebSocket loop: %s. Reconnecting in %.1fs.",
                    exc,
                    backoff,
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                self._reconnect_count += 1

        logger.info("WebSocket client stopped.")

    async def stop(self) -> None:
        """Signal the run loop to exit after the current connection closes."""
        logger.info("Stop requested — WebSocket client will shut down cleanly.")
        self._running = False

    # ── Private helpers ───────────────────────────────────────────────────────

    def _build_stream_url(self) -> str:
        """
        Build the Binance combined stream URL.

        Format:
            wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1m/...

        Why combined stream?  One TCP connection handles all symbols and event
        types.  Binance routes each event with a "stream" key so we know which
        symbol and type it belongs to.
        """
        streams: list[str] = []
        for symbol in self._symbols:
            s = symbol.lower()
            streams.append(f"{s}@trade")
            streams.append(f"{s}@kline_1m")

        stream_param = "/".join(streams)
        url = f"{self._ws_base_url}/stream?streams={stream_param}"
        logger.debug("Constructed WebSocket URL: %s", url)
        return url

    async def _connect_and_listen(self) -> None:
        """
        Open one WebSocket connection and read messages until it closes.

        This function is intentionally kept narrow: connect → loop → close.
        All business logic lives in _handle_message().
        """
        url = self._build_stream_url()
        logger.info("Connecting to Binance WebSocket: %s", url)

        # websockets.connect is an async context manager — it closes cleanly
        # when we exit, even on exception.
        async with websockets.connect(
            url,
            ping_interval=20,  # Send a ping every 20s to keep connection alive
            ping_timeout=10,  # If no pong within 10s, treat as disconnected
        ) as websocket:
            logger.info(
                "Connected to Binance WebSocket (reconnect count: %d).",
                self._reconnect_count,
            )
            # If we reconnected, publish gap-closed event for rest_client.py
            if self._last_disconnect_ts is not None:
                await self._record_reconnect()

            async for raw_message in websocket:
                if not self._running:
                    # stop() was called — exit the loop gracefully
                    break
                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode("utf-8")
                await self._handle_message(raw_message)

    async def _handle_message(self, raw_message: str) -> None:
        """
        Parse, validate, and route a single incoming WebSocket message.

        Binance combined stream wraps every event in:
            {"stream": "btcusdt@trade", "data": { ...actual event... }}

        Steps:
        1. JSON decode
        2. Identify stream type (trade vs kline)
        3. Validate with Pydantic schema
        4. Write to the correct Redis Stream
        """
        try:
            envelope = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            logger.error("Failed to JSON-decode WebSocket message: %s", exc)
            return

        stream_name: str = envelope.get("stream", "")
        data: dict = envelope.get("data", {})

        if not stream_name or not data:
            logger.warning("Received malformed envelope (missing stream/data): %s", envelope)
            return

        # Route by stream name suffix
        if "@trade" in stream_name:
            await self._handle_trade_event(stream_name, data)
        elif "@kline" in stream_name:
            await self._handle_kline_event(stream_name, data)
        else:
            logger.warning("Unknown stream type received: %s", stream_name)

    async def _handle_trade_event(self, stream_name: str, data: dict) -> None:
        """
        Validate a raw trade event and write it to Redis Stream.

        Data contract output (§1 Bronze layer — Trade Stream Entry):
            trade_id, price, quantity, trade_time, is_buyer_maker, notional
        All values written as strings (Redis Streams requirement).
        """
        try:
            # Pydantic coerces types and raises ValidationError on bad data
            event = TradeEvent(**data)
        except Exception as exc:  # Pydantic ValidationError or unexpected field issues
            logger.error(
                "Trade event validation failed on stream '%s': %s | raw: %s",
                stream_name,
                exc,
                data,
            )
            # Write to Dead Letter Queue (Global Standards §4)
            await self._write_dead_letter(stream_name, data, reason=str(exc))
            return

        symbol = event.symbol
        stream_entry = event.to_stream_dict()

        redis_stream_key = f"stream:trades:{symbol.lower()}"
        await self._redis.xadd(redis_stream_key, stream_entry)

        logger.debug(
            "Trade written → %s | trade_id=%s price=%s qty=%s",
            redis_stream_key,
            event.trade_id,
            event.price,
            event.quantity,
        )

    async def _handle_kline_event(self, stream_name: str, data: dict) -> None:
        """
        Validate a raw kline event and write it to Redis Stream.

        Data contract output (§1 Bronze layer — Kline Stream Entry):
            open_time, open, high, low, close, volume, num_trades, buy_sell_ratio
        All values written as strings.

        Note: We only write the kline when it is *closed* (kline.is_closed == True) to
        avoid feeding partial candles to downstream feature engineering.
        An open candle's values change every tick — using partial data would
        corrupt rolling window calculations.
        """
        try:
            event = KlineEvent(**data)
        except Exception as exc:
            logger.error(
                "Kline event validation failed on stream '%s': %s | raw: %s",
                stream_name,
                exc,
                data,
            )
            await self._write_dead_letter(stream_name, data, reason=str(exc))
            return

        kline = event.kline
        symbol = event.symbol

        # Only write closed candles — this is a deliberate design choice.
        # The feature calculators rely on complete OHLCV data.
        if not kline.is_closed:
            logger.debug("Skipping open kline for %s at %s", symbol, kline.open_time)
            return

        stream_entry = event.to_stream_dict()

        redis_stream_key = f"stream:klines:{symbol.lower()}"
        await self._redis.xadd(redis_stream_key, stream_entry)

        logger.debug(
            "Kline written → %s | open_time=%s close=%s volume=%s",
            redis_stream_key,
            kline.open_time,
            kline.close,
            kline.volume,
        )

    def _compute_backoff(self) -> float:
        """
        Exponential backoff with a cap.

        Formula: min(base * 2^attempt, max_backoff)
        Attempt 0 → 1s, 1 → 2s, 2 → 4s, 3 → 8s, ..., capped at 60s.

        Why exponential backoff?  If Binance is down or rate-limiting us,
        hammering with retries makes things worse.  Backing off gives the
        server time to recover and reduces our chance of being banned.
        """
        delay = min(
            self._base_backoff * (2**self._reconnect_count),
            self._max_backoff,
        )
        return float(delay)

    def _record_disconnect(self) -> None:
        """
        Record the current time as the start of a data gap.

        This timestamp is later paired with the reconnect time by
        _record_reconnect() to produce a complete gap record that
        rest_client.py can use to backfill missing candles.
        """
        self._last_disconnect_ts = int(time.time() * 1000)  # Unix ms
        logger.warning(
            "Disconnect recorded at %s (Unix ms: %d).",
            datetime.fromtimestamp(self._last_disconnect_ts / 1000, tz=timezone.utc).isoformat(),
            self._last_disconnect_ts,
        )

    async def _record_reconnect(self) -> None:
        """
        Write a gap record to Redis for every symbol so rest_client.py
        knows exactly which time window needs backfilling.

        Gap stream key: stream:gaps:{symbol}
        Entry fields: disconnect_time (Unix ms), reconnect_time (Unix ms)
        """

        if self._last_disconnect_ts is None:
            logger.warning("_record_reconnect called but no disconnect timestamp recorded.")
            return

        reconnect_ts = int(time.time() * 1000)

        for symbol in self._symbols:
            gap_entry = {
                "disconnect_time": str(self._last_disconnect_ts),
                "reconnect_time": str(reconnect_ts),
            }
            gap_key = f"stream:gaps:{symbol.lower()}"
            await self._redis.xadd(gap_key, gap_entry)
            logger.info(
                "Gap recorded for %s: %d → %d (%.1f seconds).",
                symbol,
                self._last_disconnect_ts,
                reconnect_ts,
                (reconnect_ts - self._last_disconnect_ts) / 1000,
            )

        # Reset so we don't write duplicate gap records
        self._last_disconnect_ts = None

    async def _write_dead_letter(self, stream_name: str, data: dict, reason: str) -> None:
        """
        Write a failed message to the Dead Letter Queue.

        Per Global Standards §4: messages that fail validation are written
        to stream:dead_letter for later inspection — never silently dropped.
        """
        try:
            dlq_entry = {
                "stream_name": stream_name,
                "raw_data": json.dumps(data),
                "failure_reason": reason,
                "failed_at": str(int(time.time() * 1000)),
            }
            await self._redis.xadd("stream:dead_letter", dlq_entry)
        except Exception as dlq_exc:
            # If even the DLQ write fails, we log CRITICAL — this is a
            # system-level problem, not just a data problem.
            logger.critical("Failed to write to Dead Letter Queue: %s", dlq_exc, exc_info=True)
