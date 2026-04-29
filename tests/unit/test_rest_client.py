"""Unit tests for src/ingestion/rest_client.py."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from src.ingestion.rest_client import BinanceRESTClient


class RecordingRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self._kv: dict[str, str] = {}

    async def xadd(self, key: str, data: dict[str, str], **kwargs: Any) -> None:
        self.calls.append((key, data, kwargs))

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        if nx and key in self._kv:
            return False
        self._kv[key] = value
        return True


class FakeHTTPClient:
    def __init__(self, behaviors: dict[str, list[Any]]) -> None:
        self._behaviors = {url: list(items) for url, items in behaviors.items()}
        self.calls: list[tuple[str, dict[str, Any], float]] = []

    async def get(self, url: str, params: dict[str, Any], timeout: float) -> httpx.Response:
        self.calls.append((url, params, timeout))
        queue = self._behaviors.get(url)
        if not queue:
            raise AssertionError(f"No fake behavior queued for URL: {url}")

        result = queue.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def aclose(self) -> None:
        return None


def _config(tmp_path) -> SimpleNamespace:
    return SimpleNamespace(
        data=SimpleNamespace(
            raw_data_path=str(tmp_path / "raw"),
            dedup_bloom_dir=str(tmp_path / ".dedup"),
        ),
        streaming=SimpleNamespace(
            rest_url="https://api4.binance.com",
            rest_timeout_seconds=10.0,
            dedup_ttl_seconds=3600,
            bloom_capacity=10000,
            bloom_error_rate=0.001,
        ),
    )


def _response(url: str, status_code: int, payload: Any) -> httpx.Response:
    request = httpx.Request("GET", url)
    return httpx.Response(status_code=status_code, request=request, json=payload)


def _kline_row(open_time: int, close_time: int) -> list[Any]:
    return [
        open_time,
        "100.0",
        "110.0",
        "95.0",
        "105.0",
        "40.0",
        close_time,
        "0",
        20,
        "10.0",
        "0",
    ]


@pytest.mark.asyncio
async def test_sync_klines_publishes_and_appends_without_overwrite(tmp_path) -> None:
    primary = "https://api.binance.com/api/v3/klines"
    start_ms = 1700000000000
    end_ms = 1700000119999

    first_page = [
        _kline_row(1700000000000, 1700000059999),
        _kline_row(1700000060000, 1700000119999),
        _kline_row(1700000120000, 1700000179999),  # not closed at end_ms -> skipped
    ]
    fake_http = FakeHTTPClient(
        {
            primary: [
                _response(primary, 200, first_page),
                _response(primary, 200, first_page),
            ]
        }
    )
    redis = RecordingRedis()
    client = BinanceRESTClient(config=_config(tmp_path), redis_client=redis, http_client=fake_http)

    first_count = await client.sync_klines("BTCUSDT", start_ms=start_ms, end_ms=end_ms)
    second_count = await client.sync_klines("BTCUSDT", start_ms=start_ms, end_ms=end_ms)

    assert first_count == 2
    assert second_count == 0
    assert len(redis.calls) == 2
    assert all(call[0] == "stream:klines:btcusdt" for call in redis.calls)

    date_str = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    output_file = tmp_path / "raw" / "BTCUSDT" / f"{date_str}.jsonl"
    meta_file = tmp_path / "raw" / "BTCUSDT" / f"{date_str}.meta"
    assert output_file.exists()
    assert meta_file.exists()

    lines = output_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert parsed["symbol"] == "BTCUSDT"
        assert "open_time" in parsed

    sidecar = json.loads(meta_file.read_text(encoding="utf-8"))
    assert sidecar["record_count"] == 2
    assert sidecar["last_ts"] == 1700000060000


@pytest.mark.asyncio
async def test_sync_klines_uses_fallback_when_primary_times_out(tmp_path) -> None:
    primary = "https://api.binance.com/api/v3/klines"
    fallback = "https://api4.binance.com/api/v3/klines"
    timeout_exc = httpx.TimeoutException("primary timeout")

    fake_http = FakeHTTPClient(
        {
            primary: [timeout_exc],
            fallback: [_response(fallback, 200, [_kline_row(1700000000000, 1700000059999)])],
        }
    )
    redis = RecordingRedis()
    client = BinanceRESTClient(config=_config(tmp_path), redis_client=redis, http_client=fake_http)

    count = await client.sync_klines(
        "ETHUSDT",
        start_ms=1700000000000,
        end_ms=1700000060000,
    )

    assert count == 1
    assert [call[0] for call in fake_http.calls] == [primary, fallback]
    assert len(redis.calls) == 1
    assert redis.calls[0][0] == "stream:klines:ethusdt"


@pytest.mark.asyncio
async def test_sync_klines_retries_after_http_429(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary = "https://api.binance.com/api/v3/klines"

    fake_http = FakeHTTPClient(
        {
            primary: [
                _response(primary, 429, {"code": -1003, "msg": "Too many requests"}),
                _response(primary, 200, [_kline_row(1700000000000, 1700000059999)]),
            ]
        }
    )
    redis = RecordingRedis()
    client = BinanceRESTClient(config=_config(tmp_path), redis_client=redis, http_client=fake_http)

    mocked_sleep = AsyncMock()
    monkeypatch.setattr("src.ingestion.rest_client.asyncio.sleep", mocked_sleep)

    count = await client.sync_klines(
        "BNBUSDT",
        start_ms=1700000000000,
        end_ms=1700000060000,
    )

    assert count == 1
    mocked_sleep.assert_awaited_once_with(60)
    assert len(fake_http.calls) == 2
    assert fake_http.calls[0][0] == primary
    assert fake_http.calls[1][0] == primary


@pytest.mark.asyncio
async def test_dedup_persist_across_client_restart(tmp_path) -> None:
    primary = "https://api.binance.com/api/v3/klines"
    start_ms = 1700000000000
    end_ms = 1700000119999
    page = [_kline_row(1700000000000, 1700000059999)]

    first_http = FakeHTTPClient({primary: [_response(primary, 200, page)]})
    first_redis = RecordingRedis()
    first_client = BinanceRESTClient(
        config=_config(tmp_path),
        redis_client=first_redis,
        http_client=first_http,
    )

    second_http = FakeHTTPClient({primary: [_response(primary, 200, page)]})
    second_redis = RecordingRedis()
    second_client = BinanceRESTClient(
        config=_config(tmp_path),
        redis_client=second_redis,
        http_client=second_http,
    )

    first_count = await first_client.sync_klines("BTCUSDT", start_ms=start_ms, end_ms=end_ms)
    await first_client.close()

    second_count = await second_client.sync_klines("BTCUSDT", start_ms=start_ms, end_ms=end_ms)

    assert first_count == 1
    assert second_count == 0
    assert len(first_redis.calls) == 1
    assert len(second_redis.calls) == 0

    date_str = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    output_file = tmp_path / "raw" / "BTCUSDT" / f"{date_str}.jsonl"
    lines = output_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
