"""Unit tests for src/features/cleaning.py."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.features.cleaning import DataCleaner


class RecordingRedis:
    def __init__(self) -> None:
        self._kv: dict[str, str] = {}
        self.published: list[tuple[str, str]] = []
        self.stream_calls: list[tuple[str, dict[str, str]]] = []

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        if nx and key in self._kv:
            return False
        self._kv[key] = value
        return True

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def xadd(self, key: str, data: dict[str, str]) -> str:
        self.stream_calls.append((key, data))
        return "0-0"


def _config(tmp_path) -> SimpleNamespace:
    return SimpleNamespace(
        data=SimpleNamespace(
            processed_data_path=str(tmp_path / "preprocess"),
            preprocessed_data_path=str(tmp_path / "preprocess"),
            dedup_bloom_dir=str(tmp_path / ".dedup"),
        ),
        streaming=SimpleNamespace(
            dedup_ttl_seconds=3600,
            bloom_capacity=10000,
            bloom_error_rate=0.001,
            kline_gap_threshold_seconds=300,
            rest_timeout_seconds=10.0,
        ),
    )


def _trade_payload(*, trade_id: int, price: str, quantity: str, trade_time: int) -> dict[str, str]:
    return {
        "trade_id": str(trade_id),
        "price": price,
        "quantity": quantity,
        "trade_time": str(trade_time),
        "is_buyer_maker": "True",
    }


def _kline_payload(*, open_time: int, is_closed: str = "True") -> dict[str, str]:
    return {
        "open_time": str(open_time),
        "open": "100.0",
        "high": "110.0",
        "low": "90.0",
        "close": "105.0",
        "volume": "25.0",
        "num_trades": "15",
        "buy_sell_ratio": "0.5",
        "is_closed": is_closed,
    }


@pytest.mark.asyncio
async def test_clean_trade_rejects_duplicate_trade_id(tmp_path) -> None:
    redis = RecordingRedis()
    cleaner = DataCleaner(config=_config(tmp_path), redis_client=redis, pipeline_run_id="test-run")

    payload = _trade_payload(trade_id=1, price="100.0", quantity="2.0", trade_time=1700000000000)
    first = await cleaner.process_trade(payload, "BTCUSDT")
    second = await cleaner.process_trade(payload, "BTCUSDT")

    assert first is not None
    assert second is None


@pytest.mark.asyncio
async def test_clean_trade_rejects_zero_price(tmp_path) -> None:
    redis = RecordingRedis()
    cleaner = DataCleaner(config=_config(tmp_path), redis_client=redis, pipeline_run_id="test-run")

    payload = _trade_payload(trade_id=2, price="0", quantity="2.0", trade_time=1700000000000)
    result = await cleaner.process_trade(payload, "BTCUSDT")

    assert result is None


@pytest.mark.asyncio
async def test_clean_trade_flags_outlier_but_keeps_record(tmp_path) -> None:
    redis = RecordingRedis()
    cleaner = DataCleaner(config=_config(tmp_path), redis_client=redis, pipeline_run_id="test-run")

    base_time = 1700000000000
    for index in range(20):
        payload = _trade_payload(
            trade_id=100 + index,
            price=str(100.0 + index),
            quantity="2.0",
            trade_time=base_time + (index * 1000),
        )
        assert await cleaner.process_trade(payload, "BTCUSDT") is not None

    outlier = _trade_payload(
        trade_id=999,
        price="1500.0",
        quantity="2.0",
        trade_time=base_time + 30000,
    )
    result = await cleaner.process_trade(outlier, "BTCUSDT")

    assert result is not None
    assert result["is_price_anomaly"] is True


@pytest.mark.asyncio
async def test_clean_kline_rejects_unclosed_record(tmp_path) -> None:
    redis = RecordingRedis()
    cleaner = DataCleaner(config=_config(tmp_path), redis_client=redis, pipeline_run_id="test-run")

    payload = _kline_payload(open_time=1700000000000, is_closed="False")
    result = await cleaner.process_kline(payload, "BTCUSDT")

    assert result is None


@pytest.mark.asyncio
async def test_clean_kline_gap_publishes_event(tmp_path) -> None:
    redis = RecordingRedis()
    cleaner = DataCleaner(config=_config(tmp_path), redis_client=redis, pipeline_run_id="test-run")

    first = _kline_payload(open_time=1700000000000)
    second = _kline_payload(open_time=1700000401000)

    assert await cleaner.process_kline(first, "BTCUSDT") is not None
    assert await cleaner.process_kline(second, "BTCUSDT") is not None

    assert len(redis.published) == 1
    channel, payload = redis.published[0]
    parsed = json.loads(payload)

    assert channel == "trigger:gap_detected:btcusdt"
    assert parsed["symbol"] == "BTCUSDT"
    assert parsed["gap_seconds"] > 300


@pytest.mark.asyncio
async def test_preprocess_sidecar_matches_line_count(tmp_path) -> None:
    redis = RecordingRedis()
    cleaner = DataCleaner(config=_config(tmp_path), redis_client=redis, pipeline_run_id="test-run")

    base_time = 1700000000000
    for index in range(3):
        payload = _trade_payload(
            trade_id=500 + index,
            price="101.0",
            quantity="1.0",
            trade_time=base_time + (index * 1000),
        )
        assert await cleaner.process_trade(payload, "ETHUSDT") is not None

    date_str = datetime.fromtimestamp(base_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    jsonl_path = tmp_path / "preprocess" / "ETHUSDT" / f"{date_str}.jsonl"
    meta_path = tmp_path / "preprocess" / "ETHUSDT" / f"{date_str}.meta"

    lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    assert len(lines) == meta["record_count"]
    assert meta["pipeline_run_id"] == "test-run"
