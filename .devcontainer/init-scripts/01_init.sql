CREATE TABLE IF NOT EXISTS features_trade (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL,
    ts          BIGINT NOT NULL,
    feature_version VARCHAR(10),
    price_mean_50   FLOAT, price_mean_200  FLOAT, price_mean_1000 FLOAT,
    price_std_50    FLOAT, price_std_200   FLOAT, price_std_1000  FLOAT,
    price_change_50 FLOAT, price_change_200 FLOAT, price_change_1000 FLOAT,
    vol_mean_50     FLOAT, vol_mean_200    FLOAT, vol_mean_1000   FLOAT,
    vol_std_50      FLOAT, vol_std_200     FLOAT, vol_std_1000    FLOAT,
    vol_total_50    FLOAT, vol_total_200   FLOAT, vol_total_1000  FLOAT,
    buy_ratio_50    FLOAT, buy_ratio_200   FLOAT, buy_ratio_1000  FLOAT,
    trade_rate_50   FLOAT, trade_rate_200  FLOAT, trade_rate_1000 FLOAT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, ts)
);

CREATE TABLE IF NOT EXISTS features_kline (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL,
    ts          BIGINT NOT NULL,
    feature_version VARCHAR(10),
    atr_5       FLOAT, atr_15      FLOAT, atr_60      FLOAT,
    hl_ratio_5  FLOAT, hl_ratio_15 FLOAT, hl_ratio_60 FLOAT,
    sma_5       FLOAT, sma_15      FLOAT, sma_60      FLOAT,
    ema_5       FLOAT, ema_15      FLOAT, ema_60      FLOAT,
    momentum_5  FLOAT, momentum_15 FLOAT, momentum_60 FLOAT,
    vwap_5      FLOAT, vwap_15     FLOAT, vwap_60     FLOAT,
    vol_ratio_5 FLOAT, vol_ratio_15 FLOAT, vol_ratio_60 FLOAT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_features_trade_symbol_ts ON features_trade (symbol, ts);
CREATE INDEX IF NOT EXISTS idx_features_kline_symbol_ts ON features_kline (symbol, ts);