"""
src/ingestion/pipeline.py

WebSocket ingestion pipeline entry point.

Responsibilities:
- Bootstrap config, logger, Redis, and MLflow.
- Instantiate and supervise BinanceWebSocketClient.
- Run cleaner stream consumers for trade/kline preprocessing.
- Process gap events with BinanceRESTClient backfill.
- Flush dedup state at midnight UTC for restart-safe idempotency.
- Handle OS signals for graceful shutdown.
- Emit pipeline-level MLflow metrics (start/stop events, uptime).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import mlflow

from src.features.cleaning import DataCleaner
from src.ingestion.rest_client import BinanceRESTClient
from src.ingestion.websocket_client import BinanceWebSocketClient
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client, get_redis_runtime_mode

logger: logging.Logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class WebSocketPipeline:
    """
    Thin orchestration wrapper around BinanceWebSocketClient.

    Lifecycle
    ---------
    1. `start()` – validate dependencies, open MLflow run, launch client.
    2. `stop()`  – signal client to stop, finalise MLflow run.
    """

    def __init__(self) -> None:
        self._config = get_config()
        self._redis = get_redis_client()
        self._client: Optional[BinanceWebSocketClient] = None
        self._rest_client: Optional[BinanceRESTClient] = None
        self._cleaner: Optional[DataCleaner] = None
        self._start_time: Optional[float] = None
        self._mlflow_run: Any | None = None
        self._shutdown_event = asyncio.Event()
        self._auxiliary_tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialise resources and run the WebSocket client until stopped."""
        logger.info("WebSocket pipeline starting.")
        self._start_time = time.monotonic()
        exit_status = "FINISHED"

        await self._ensure_redis_ready()

        self._begin_mlflow_run()

        self._rest_client = BinanceRESTClient(
            config=self._config,
            redis_client=self._redis,
        )
        self._cleaner = DataCleaner(
            config=self._config,
            redis_client=self._redis,
            pipeline_run_id=f"pipeline-{int(time.time())}",
        )
        self._client = BinanceWebSocketClient()

        # Register OS signal handlers so Ctrl-C / SIGTERM trigger clean stop.
        self._register_signal_handlers()

        try:
            self._auxiliary_tasks = [
                asyncio.create_task(self._run_cleaner_loop(), name="cleaner-loop"),
                asyncio.create_task(
                    self._midnight_rotation_watcher(),
                    name="midnight-rotation-watcher",
                ),
            ]
            await self._client.run()
        except asyncio.CancelledError:
            exit_status = "KILLED"
            logger.warning("WebSocket client task cancelled.")
            mlflow.log_param("pipeline.exit_reason", "cancelled")
            raise
        except (RuntimeError, ValueError, ConnectionError, TimeoutError, OSError) as exc:
            exit_status = "FAILED"
            logger.exception("Unhandled exception in WebSocket client: %s", exc)
            mlflow.log_param("pipeline.exit_reason", str(exc))
            raise
        finally:
            await self._teardown(status=exit_status)

    async def stop(self) -> None:
        """Request a graceful shutdown (idempotent)."""
        logger.info("WebSocket pipeline stop requested.")
        self._shutdown_event.set()
        if self._client is not None:
            await self._client.stop()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_experiment_name(self) -> str:
        mlflow_config = getattr(self._config, "mlflow", None)
        experiment_name = getattr(mlflow_config, "experiment_name", None)
        return experiment_name or "ingestion_websocket"

    def _get_symbols(self) -> list[str]:
        symbols = getattr(self._config, "symbols", None)
        if symbols is None:
            data_config = getattr(self._config, "data", None)
            symbols = getattr(data_config, "trading_pairs", [])
        return [str(symbol).upper() for symbol in symbols]

    def _begin_mlflow_run(self) -> None:
        experiment_name = self._get_experiment_name()
        mlflow.set_experiment(experiment_name)
        self._mlflow_run = mlflow.start_run(run_name="websocket_pipeline")

        mlflow.log_params(
            {
                "pipeline.component": "websocket_ingestion",
                "pipeline.symbols": ",".join(self._get_symbols()),
                "pipeline.streams": "trade,kline_1m",
            }
        )
        if self._mlflow_run is not None:
            logger.debug("MLflow run started: %s", self._mlflow_run.info.run_id)

    def _end_mlflow_run(self, status: str = "FINISHED") -> None:
        if self._mlflow_run is None:
            return
        uptime_seconds = time.monotonic() - self._start_time if self._start_time else 0.0
        mlflow.log_metrics(
            {
                "pipeline.uptime_seconds": uptime_seconds,
            }
        )
        mlflow.log_param("pipeline.exit_status", status)
        mlflow.end_run(status=status)
        logger.debug("MLflow run ended (status=%s, uptime=%.1fs).", status, uptime_seconds)

    async def _teardown(self, status: str = "FINISHED") -> None:
        """Release all resources after the client exits."""
        self._shutdown_event.set()

        if self._auxiliary_tasks:
            await asyncio.gather(*self._auxiliary_tasks, return_exceptions=True)
            self._auxiliary_tasks.clear()

        if self._cleaner is not None:
            await self._cleaner.flush_state()

        if self._rest_client is not None:
            await self._rest_client.close()

        redis_close = getattr(self._redis, "close", None)
        if redis_close is not None:
            result = redis_close()
            if inspect.isawaitable(result):
                await result

        self._end_mlflow_run(status=status)
        logger.info("WebSocket pipeline stopped.")

    def _register_signal_handlers(self) -> None:
        """
        Register SIGINT / SIGTERM handlers on the running event loop.

        Uses loop.add_signal_handler (UNIX only); on Windows the asyncio
        loop does not support this but KeyboardInterrupt is still caught.
        """
        loop = asyncio.get_running_loop()

        def _signal_handler(signum: int) -> None:
            sig_name = signal.Signals(signum).name
            logger.info("Received signal %s – initiating graceful shutdown.", sig_name)
            loop.create_task(self.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler, sig)
            except (NotImplementedError, OSError):
                # Windows or restricted environments – fall back silently.
                logger.debug("Could not register signal handler for %s.", sig)

    async def _run_cleaner_loop(self) -> None:  # noqa: C901
        """Consume Redis trade/kline streams and push cleaned records to preprocess storage."""
        poll_seconds = float(
            getattr(getattr(self._config, "streaming", None), "cleaner_poll_interval_seconds", 0.25)
        )
        block_ms = max(int(poll_seconds * 1000), 1)
        stream_offsets = {f"stream:trades:{symbol.lower()}": "$" for symbol in self._get_symbols()}
        stream_offsets.update(
            {f"stream:klines:{symbol.lower()}": "$" for symbol in self._get_symbols()}
        )
        stream_offsets.update(
            {f"stream:gaps:{symbol.lower()}": "$" for symbol in self._get_symbols()}
        )

        while not self._shutdown_event.is_set():
            if self._cleaner is None or self._rest_client is None:
                await asyncio.sleep(poll_seconds)
                continue

            xread_method = getattr(self._redis, "xread", None)
            if xread_method is None:
                await asyncio.sleep(poll_seconds)
                continue

            try:
                response = xread_method(stream_offsets, count=100, block=block_ms)
                if inspect.isawaitable(response):
                    response = await response
                else:
                    await asyncio.sleep(poll_seconds)
                    continue
            except (TypeError, ValueError) as exc:
                logger.warning("Cleaner loop could not read streams: %s", exc)
                await asyncio.sleep(poll_seconds)
                continue

            if not response:
                await asyncio.sleep(poll_seconds)
                continue

            for stream_key, entries in response:
                normalized_stream_key = self._decode_stream_key(stream_key)
                for entry_id, payload in entries:
                    normalized_payload = self._normalize_stream_payload(payload)

                    if normalized_stream_key.startswith("stream:trades:"):
                        symbol = normalized_stream_key.split(":")[-1].upper()
                        await self._cleaner.process_trade(normalized_payload, symbol)
                    elif normalized_stream_key.startswith("stream:klines:"):
                        symbol = normalized_stream_key.split(":")[-1].upper()
                        await self._cleaner.process_kline(normalized_payload, symbol)
                    elif normalized_stream_key.startswith("stream:gaps:"):
                        symbol = normalized_stream_key.split(":")[-1].upper()
                        disconnect_time = int(str(normalized_payload.get("disconnect_time", "0")))
                        reconnect_time = int(str(normalized_payload.get("reconnect_time", "0")))
                        if disconnect_time > 0 and reconnect_time > 0:
                            await self._rest_client.process_gap_event(
                                symbol,
                                disconnect_time,
                                reconnect_time,
                            )

                    stream_offsets[normalized_stream_key] = self._decode_stream_key(entry_id)

    async def _midnight_rotation_watcher(self) -> None:
        """Flush dedup persistence around midnight UTC to preserve day-end state."""
        if self._cleaner is None:
            return

        flush_second = int(
            getattr(getattr(self._config, "streaming", None), "midnight_flush_second", 5)
        )

        while not self._shutdown_event.is_set():
            now = datetime.now(timezone.utc)
            next_day = now.date() + timedelta(days=1)
            target = datetime(
                year=next_day.year,
                month=next_day.month,
                day=next_day.day,
                hour=0,
                minute=0,
                second=flush_second,
                tzinfo=timezone.utc,
            )
            wait_seconds = max((target - now).total_seconds(), 0.5)

            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=wait_seconds)
                return
            except asyncio.TimeoutError:
                await self._cleaner.flush_state()
                logger.info("Midnight rotation flush completed at %s", target.isoformat())

    @staticmethod
    def _decode_stream_key(raw_value: object) -> str:
        if isinstance(raw_value, bytes):
            return raw_value.decode("utf-8")
        return str(raw_value)

    @classmethod
    def _normalize_stream_payload(cls, raw_payload: object) -> dict[str, str]:
        if not isinstance(raw_payload, dict):
            return {}

        normalized: dict[str, str] = {}
        for raw_key, raw_value in raw_payload.items():
            key = cls._decode_stream_key(raw_key)
            value = cls._decode_stream_key(raw_value)
            normalized[key] = value
        return normalized

    async def _ensure_redis_ready(self) -> None:
        runtime_mode = get_redis_runtime_mode(self._config)
        if runtime_mode != "real":
            return

        ping_method = getattr(self._redis, "ping", None)
        if ping_method is None:
            raise ConnectionError("Redis runtime mode is 'real' but ping() is unavailable.")

        result = ping_method()
        if inspect.isawaitable(result):
            result = await result

        if not result:
            raise ConnectionError("Redis ping failed while redis.runtime_mode='real'.")


# ---------------------------------------------------------------------------
# Module entry point
# ---------------------------------------------------------------------------


def run() -> None:
    """Synchronous entry point – used by CLI / container CMD."""
    pipeline = WebSocketPipeline()
    try:
        asyncio.run(pipeline.start())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received – pipeline exiting.")


if __name__ == "__main__":
    run()
