"""CLI worker to consume Redis Streams and write preprocessed records."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import time
from typing import Optional

from src.features.cleaning import DataCleaner
from src.ingestion.rest_client import BinanceRESTClient
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client, get_redis_runtime_mode

logger = get_logger(__name__)


class PreprocessWorker:
    def __init__(self) -> None:
        self._config = get_config()
        self._redis = get_redis_client()
        self._cleaner = DataCleaner(
            config=self._config,
            redis_client=self._redis,
            pipeline_run_id=f"preprocess-{int(time.time())}",
        )
        self._rest_client = BinanceRESTClient(
            config=self._config,
            redis_client=self._redis,
        )
        self._shutdown_event = asyncio.Event()

    async def run(self, duration_seconds: Optional[int] = None) -> None:
        await self._ensure_redis_ready()

        run_task = asyncio.create_task(self._consume_loop(), name="preprocess-consumer-loop")
        try:
            if duration_seconds and duration_seconds > 0:
                await asyncio.sleep(duration_seconds)
                logger.info("Bounded preprocess duration reached (%ss).", duration_seconds)
                self._shutdown_event.set()

            await run_task
        finally:
            self._shutdown_event.set()
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)

            await self._cleaner.flush_state()
            await self._rest_client.close()

            redis_close = getattr(self._redis, "close", None)
            if redis_close is not None:
                close_result = redis_close()
                if inspect.isawaitable(close_result):
                    await close_result

    async def _consume_loop(self) -> None:  # noqa: C901
        poll_seconds = float(
            getattr(getattr(self._config, "streaming", None), "cleaner_poll_interval_seconds", 0.25)
        )
        block_ms = max(int(poll_seconds * 1000), 1)

        symbols = [str(symbol).upper() for symbol in getattr(self._config, "symbols", [])]
        stream_offsets = {f"stream:trades:{symbol.lower()}": "$" for symbol in symbols}
        stream_offsets.update({f"stream:klines:{symbol.lower()}": "$" for symbol in symbols})
        stream_offsets.update({f"stream:gaps:{symbol.lower()}": "$" for symbol in symbols})

        while not self._shutdown_event.is_set():
            xread_method = getattr(self._redis, "xread", None)
            if xread_method is None:
                await asyncio.sleep(poll_seconds)
                continue

            try:
                response = xread_method(stream_offsets, count=100, block=block_ms)
                if inspect.isawaitable(response):
                    response = await response
            except (TypeError, ValueError) as exc:
                logger.warning("Preprocess worker could not read streams: %s", exc)
                await asyncio.sleep(poll_seconds)
                continue

            if not response:
                await asyncio.sleep(poll_seconds)
                continue

            for stream_key, entries in response:
                normalized_stream_key = self._decode(stream_key)
                for entry_id, payload in entries:
                    normalized_payload = self._normalize_payload(payload)

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

                    stream_offsets[normalized_stream_key] = self._decode(entry_id)

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

    @staticmethod
    def _decode(value: object) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    @classmethod
    def _normalize_payload(cls, payload: object) -> dict[str, str]:
        if not isinstance(payload, dict):
            return {}

        normalized: dict[str, str] = {}
        for raw_key, raw_value in payload.items():
            normalized[cls._decode(raw_key)] = cls._decode(raw_value)
        return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run preprocess worker for ingestion streams.")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Optional bounded run duration. Use 0 to run continuously.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    worker = PreprocessWorker()
    try:
        asyncio.run(worker.run(duration_seconds=args.duration_seconds))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Preprocess worker exiting.")


if __name__ == "__main__":
    main()
