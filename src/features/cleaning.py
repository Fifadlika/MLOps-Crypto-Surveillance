"""Cleaning layer implementation for Bronze -> Silver preprocessing."""

from __future__ import annotations

import inspect
import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any

from src.ingestion.writer import AtomicJsonlWriter
from src.utils.bloom import PersistentBloomRegistry
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client

logger = get_logger(__name__)


class DataCleaner:
    """Applies five-stage cleaning in fixed order for trade and kline stream entries."""

    def __init__(
        self,
        *,
        config: Any | None = None,
        redis_client: Any | None = None,
        pipeline_run_id: str = "pipeline-default",
    ) -> None:
        self._config = config or get_config()
        self._redis = redis_client or get_redis_client()

        data_cfg = getattr(self._config, "data", None)
        streaming_cfg = getattr(self._config, "streaming", None)

        preprocess_dir_raw = getattr(data_cfg, "preprocessed_data_path", None) or getattr(
            data_cfg, "processed_data_path", "data/preprocess"
        )
        preprocess_dir = str(preprocess_dir_raw or "data/preprocess")
        dedup_dir = getattr(data_cfg, "dedup_bloom_dir", "data/raw/.dedup")

        lock_timeout = float(getattr(streaming_cfg, "rest_timeout_seconds", 30.0))
        self._writer = AtomicJsonlWriter(
            root_path=Path(preprocess_dir),
            pipeline_run_id=pipeline_run_id,
            lock_timeout_seconds=max(lock_timeout, 5.0),
        )

        self._dedup_ttl_seconds = int(getattr(streaming_cfg, "dedup_ttl_seconds", 3600))
        self._gap_threshold_ms = (
            int(getattr(streaming_cfg, "kline_gap_threshold_seconds", 300)) * 1000
        )

        self._bloom = PersistentBloomRegistry(
            base_dir=(Path(dedup_dir) / "cleaner"),
            capacity=int(getattr(streaming_cfg, "bloom_capacity", 1_000_000)),
            error_rate=float(getattr(streaming_cfg, "bloom_error_rate", 0.001)),
            autosave_every=500,
        )

        self._price_windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=200))
        self._qty_windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=200))
        self._last_kline_open_time: dict[str, int] = {}

    async def process_trade(self, raw_trade: dict[str, str], symbol: str) -> dict[str, Any] | None:
        """Clean one trade entry and return normalized dict or None when rejected."""
        normalized_symbol = symbol.strip().upper()

        # Stage 1: deduplication (must run first).
        try:
            trade_id = int(str(raw_trade["trade_id"]))
        except (KeyError, TypeError, ValueError):
            logger.warning("Trade missing valid trade_id for %s: %s", normalized_symbol, raw_trade)
            return None

        if await self._is_duplicate(namespace="trade", symbol=normalized_symbol, key=str(trade_id)):
            return None

        # Stage 2: type validation/coercion.
        try:
            price = float(str(raw_trade["price"]))
            quantity = float(str(raw_trade["quantity"]))
            trade_time_ms = int(str(raw_trade["trade_time"]))
            is_buyer_maker = self._coerce_bool(raw_trade.get("is_buyer_maker", "False"))
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Trade coercion failed for %s: %s", normalized_symbol, exc)
            return None

        if price <= 0 or quantity <= 0:
            logger.warning(
                "Rejected trade for %s due to non-positive values price=%s quantity=%s",
                normalized_symbol,
                price,
                quantity,
            )
            return None

        # Stage 3: outlier flagging (flag only; do not drop).
        is_price_anomaly = self._is_outlier(price, self._price_windows[normalized_symbol])
        is_volume_anomaly = self._is_outlier(quantity, self._qty_windows[normalized_symbol])

        notional = round(price * quantity, 8)

        # Stage 4: timestamp normalization from Binance event timestamp.
        trade_time_iso = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc).isoformat()

        cleaned = {
            "trade_id": trade_id,
            "symbol": normalized_symbol,
            "price": price,
            "quantity": quantity,
            "notional": notional,
            "trade_time_ms": trade_time_ms,
            "trade_time_iso": trade_time_iso,
            "is_buyer_maker": is_buyer_maker,
            "is_price_anomaly": is_price_anomaly,
            "is_volume_anomaly": is_volume_anomaly,
        }

        self._writer.append_record(normalized_symbol, trade_time_ms, cleaned)
        return cleaned

    async def process_kline(self, raw_kline: dict[str, str], symbol: str) -> dict[str, Any] | None:
        """Clean one kline entry and return normalized dict or None when rejected."""
        normalized_symbol = symbol.strip().upper()

        # Stage 1: deduplication (must run first).
        try:
            open_time = int(str(raw_kline["open_time"]))
        except (KeyError, TypeError, ValueError):
            logger.warning("Kline missing valid open_time for %s: %s", normalized_symbol, raw_kline)
            return None

        if await self._is_duplicate(
            namespace="kline", symbol=normalized_symbol, key=str(open_time)
        ):
            return None

        # Stage 2: type validation/coercion.
        try:
            open_price = float(str(raw_kline["open"]))
            high = float(str(raw_kline["high"]))
            low = float(str(raw_kline["low"]))
            close = float(str(raw_kline["close"]))
            volume = float(str(raw_kline["volume"]))
            num_trades = int(str(raw_kline["num_trades"]))
            buy_sell_ratio = float(str(raw_kline.get("buy_sell_ratio", "0.5")))
            is_closed = self._coerce_bool(raw_kline.get("is_closed", "True"))
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Kline coercion failed for %s: %s", normalized_symbol, exc)
            return None

        # Kline gate: only closed records pass downstream.
        if not is_closed:
            return None

        # Stage 3: outlier on volume (flag-only is represented by no drop behavior).
        _ = self._is_outlier(volume, self._qty_windows[f"{normalized_symbol}:kline_volume"])

        # Stage 4: keep canonical server timestamp in ms.
        event_time_ms = open_time

        # Stage 5: kline-only gap detection.
        await self._detect_kline_gap(normalized_symbol, open_time)

        cleaned = {
            "symbol": normalized_symbol,
            "open_time": open_time,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "num_trades": num_trades,
            "buy_sell_ratio": buy_sell_ratio,
            "is_closed": is_closed,
            "event_time_ms": event_time_ms,
        }

        self._writer.append_record(normalized_symbol, event_time_ms, cleaned)
        return cleaned

    async def flush_state(self) -> None:
        """Persist in-memory dedup state to disk."""
        self._bloom.flush_all()

    async def _is_duplicate(self, *, namespace: str, symbol: str, key: str) -> bool:
        redis_key = f"dedup:{namespace}:{symbol.lower()}:{key}"
        redis_is_new = await self._set_nx(redis_key)

        if not redis_is_new:
            return True

        bloom_key = f"{namespace}:{symbol}:{key}"
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

    async def _detect_kline_gap(self, symbol: str, current_open_time_ms: int) -> None:
        previous_open_time_ms = self._last_kline_open_time.get(symbol)
        self._last_kline_open_time[symbol] = current_open_time_ms

        if previous_open_time_ms is None:
            return

        gap_ms = current_open_time_ms - previous_open_time_ms
        if gap_ms <= self._gap_threshold_ms:
            return

        payload = {
            "symbol": symbol,
            "previous_open_time": previous_open_time_ms,
            "current_open_time": current_open_time_ms,
            "gap_seconds": round(gap_ms / 1000, 3),
        }

        publish_method = getattr(self._redis, "publish", None)
        if publish_method is not None:
            result = publish_method(f"trigger:gap_detected:{symbol.lower()}", json.dumps(payload))
            if inspect.isawaitable(result):
                await result
            return

        xadd_method = getattr(self._redis, "xadd", None)
        if xadd_method is not None:
            gap_entry = {
                "disconnect_time": str(previous_open_time_ms),
                "reconnect_time": str(current_open_time_ms),
            }
            result = xadd_method(f"stream:gaps:{symbol.lower()}", gap_entry)
            if inspect.isawaitable(result):
                await result

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return bool(value)

        text = str(value).strip().lower()
        return text in {"1", "true", "t", "yes", "y"}

    @staticmethod
    def _is_outlier(value: float, history: deque[float], z_threshold: float = 4.0) -> bool:
        if len(history) < 10:
            history.append(value)
            return False

        mean = fmean(history)
        variance = fmean([(item - mean) ** 2 for item in history])
        std_dev = variance**0.5

        history.append(value)
        if std_dev == 0:
            return False

        return bool(abs(value - mean) > (z_threshold * std_dev))
