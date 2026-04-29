from src.features.engineering import KlineFeatureCalculator, TradeFeatureCalculator


def test_trade_feature_calculator():
    calc = TradeFeatureCalculator("BTCUSDT")
    res = calc.update({"E": 123456789})
    assert len(res) == 52
    assert res["symbol"] == "BTCUSDT"
    assert res["ts"] == 123456789
    assert res["feature_version"] == "1.0"
    for i in range(1, 50):
        assert f"f{i}" in res


def test_kline_feature_calculator():
    calc = KlineFeatureCalculator("ETHUSDT")
    res = calc.update({"E": 987654321})
    assert len(res) == 52
    assert res["symbol"] == "ETHUSDT"
    assert res["ts"] == 987654321
    assert res["feature_version"] == "1.0"
    for i in range(1, 50):
        assert f"f{i}" in res
