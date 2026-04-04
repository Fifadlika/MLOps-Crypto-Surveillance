"""Contract tests for ingestion schema stream serializers.

These tests lock the PROJECT_CONTEXT Bronze stream field sets so schema
serializers and ingestion writers do not drift over time.
"""

from src.ingestion.schemas import HistoricalKline, KlineEvent, TradeEvent


def test_trade_event_to_stream_dict_matches_contract() -> None:
    event = TradeEvent.model_validate(
        {
            "s": "BTCUSDT",
            "t": 101,
            "p": "100.25",
            "q": "2.0",
            "T": 1700000000000,
            "m": True,
        }
    )

    payload = event.to_stream_dict()

    assert set(payload.keys()) == {
        "trade_id",
        "price",
        "quantity",
        "trade_time",
        "is_buyer_maker",
        "notional",
    }
    assert all(isinstance(v, str) for v in payload.values())
    assert payload["trade_id"] == "101"
    assert payload["price"] == "100.25"
    assert payload["quantity"] == "2.0"
    assert payload["trade_time"] == "1700000000000"
    assert payload["is_buyer_maker"] == "True"
    assert payload["notional"] == "200.5"


def test_kline_event_to_stream_dict_matches_contract() -> None:
    event = KlineEvent.model_validate(
        {
            "s": "ETHUSDT",
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
    )

    payload = event.to_stream_dict()

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
    assert all(isinstance(v, str) for v in payload.values())
    assert payload["open_time"] == "1700000000000"
    assert payload["open"] == "100.0"
    assert payload["high"] == "110.0"
    assert payload["low"] == "95.0"
    assert payload["close"] == "105.0"
    assert payload["volume"] == "40.0"
    assert payload["num_trades"] == "20"
    assert payload["buy_sell_ratio"] == "0.25"


def test_historical_kline_to_stream_dict_matches_live_contract() -> None:
    row = [
        1700000000000,
        "100.0",
        "110.0",
        "95.0",
        "105.0",
        "40.0",
        1700000059999,
        "0",
        20,
        "10.0",
        "0",
    ]
    event = HistoricalKline.from_rest_list("BNBUSDT", row)

    payload = event.to_stream_dict()

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
    assert all(isinstance(v, str) for v in payload.values())
    assert payload["buy_sell_ratio"] == "0.25"
