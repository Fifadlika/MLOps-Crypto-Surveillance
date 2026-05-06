# mypy: ignore-errors
from src.features.engineering import KlineFeatureCalculator, TradeFeatureCalculator


def test_trade_feature_calculator():
    calc = TradeFeatureCalculator("BTCUSDT")
    res = calc.update(
        {"price": 50000.0, "quantity": 0.5, "trade_time_ms": 123456789, "is_buyer_maker": True}
    )

    assert len(res) == 27
    assert res["symbol"] == "BTCUSDT"
    expected_windows = [50, 200, 1000]
    expected_prefixes = [
        "price_mean",
        "price_std",
        "price_change",
        "vol_mean",
        "vol_std",
        "vol_total",
        "buy_ratio",
        "trade_rate",
    ]
    for w in expected_windows:
        for prefix in expected_prefixes:
            assert f"{prefix}_{w}" in res


def test_kline_feature_calculator():
    calc = KlineFeatureCalculator("ETHUSDT")
    res = calc.update(
        {
            "open": 2000.0,
            "high": 2100.0,
            "low": 1900.0,
            "close": 2050.0,
            "volume": 10.5,
            "open_time": 987654321,
            "is_closed": True,
        }
    )

    assert res is not None
    assert len(res) == 24
    assert res["symbol"] == "ETHUSDT"
    expected_windows = [5, 15, 60]
    expected_prefixes = ["atr", "hl_ratio", "sma", "ema", "momentum", "vwap", "vol_ratio"]
    for w in expected_windows:
        for prefix in expected_prefixes:
            assert f"{prefix}_{w}" in res
