import json
from types import SimpleNamespace

import pytest

import src.ingestion.websocket_client as ws_module
from src.ingestion.websocket_client import BinanceWebSocketClient


class RecordingRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def xadd(self, key: str, data: dict) -> None:
        self.calls.append((key, data))


def _make_config() -> SimpleNamespace:
    return SimpleNamespace(
        symbols=["BTCUSDT"],
        binance=SimpleNamespace(
            ws_base_url="wss://stream.binance.com:9443",
            max_reconnect_attempts=10,
            base_backoff_seconds=1.0,
            max_backoff_seconds=60.0,
        ),
    )


def _make_client(monkeypatch: pytest.MonkeyPatch) -> tuple[BinanceWebSocketClient, RecordingRedis]:
    redis = RecordingRedis()
    monkeypatch.setattr(ws_module, "get_config", lambda: _make_config())
    monkeypatch.setattr(ws_module, "get_redis_client", lambda: redis)
    return BinanceWebSocketClient(), redis


@pytest.mark.asyncio
async def test_handle_trade_event_writes_trade_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    client, redis = _make_client(monkeypatch)

    data = {
        "s": "BTCUSDT",
        "t": 101,
        "p": "100.25",
        "q": "2.0",
        "T": 1700000000000,
        "m": True,
    }

    await client._handle_trade_event("btcusdt@trade", data)

    assert len(redis.calls) == 1
    key, payload = redis.calls[0]
    assert key == "stream:trades:btcusdt"
    assert set(payload.keys()) == {
        "trade_id",
        "price",
        "quantity",
        "trade_time",
        "is_buyer_maker",
        "notional",
    }


@pytest.mark.asyncio
async def test_handle_trade_event_validation_failure_writes_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, redis = _make_client(monkeypatch)

    bad_data = {
        "s": "BTCUSDT",
        "t": 102,
        "p": "100.25",
        "q": "0",
        "T": 1700000000000,
        "m": True,
    }

    await client._handle_trade_event("btcusdt@trade", bad_data)

    assert len(redis.calls) == 1
    key, payload = redis.calls[0]
    assert key == "stream:dead_letter"
    assert payload["stream_name"] == "btcusdt@trade"
    assert "failure_reason" in payload
    assert json.loads(payload["raw_data"])["q"] == "0"


@pytest.mark.asyncio
async def test_handle_kline_event_open_candle_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    client, redis = _make_client(monkeypatch)

    open_kline = {
        "s": "BTCUSDT",
        "k": {
            "t": 1700000000000,
            "o": "100.0",
            "h": "110.0",
            "l": "95.0",
            "c": "105.0",
            "v": "40.0",
            "n": 20,
            "V": "10.0",
            "x": False,
        },
    }

    await client._handle_kline_event("btcusdt@kline_1m", open_kline)

    assert redis.calls == []


@pytest.mark.asyncio
async def test_handle_kline_event_closed_candle_writes_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, redis = _make_client(monkeypatch)

    closed_kline = {
        "s": "BTCUSDT",
        "k": {
            "t": 1700000000000,
            "o": "100.0",
            "h": "110.0",
            "l": "95.0",
            "c": "105.0",
            "v": "40.0",
            "n": 20,
            "V": "10.0",
            "x": True,
        },
    }

    await client._handle_kline_event("btcusdt@kline_1m", closed_kline)

    assert len(redis.calls) == 1
    key, payload = redis.calls[0]
    assert key == "stream:klines:btcusdt"
    assert set(payload.keys()) == {
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "num_trades",
        "buy_sell_ratio",
    }
