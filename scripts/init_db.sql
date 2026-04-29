CREATE TABLE IF NOT EXISTS features_trade (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    feature_version VARCHAR(20) NOT NULL,

    w50_price_mean FLOAT,
    w200_price_mean FLOAT,
    w1000_price_mean FLOAT,
    w50_price_cv FLOAT,
    w200_price_cv FLOAT,
    w1000_price_cv FLOAT,
    w50_return_skew FLOAT,
    w200_return_skew FLOAT,
    w1000_return_skew FLOAT,

    w50_vwap FLOAT,
    w200_vwap FLOAT,
    w1000_vwap FLOAT,
    w50_vwap_deviation FLOAT,
    w200_vwap_deviation FLOAT,
    w1000_vwap_deviation FLOAT,
    w50_large_trade_ratio FLOAT,
    w200_large_trade_ratio FLOAT,
    w1000_large_trade_ratio FLOAT,

    w50_buy_ratio FLOAT,
    w200_buy_ratio FLOAT,
    w1000_buy_ratio FLOAT,
    w50_trade_count FLOAT,
    w200_trade_count FLOAT,
    w1000_trade_count FLOAT,

    k5_realized_vol FLOAT,
    k15_realized_vol FLOAT,
    k60_realized_vol FLOAT,
    k5_parkinson_vol FLOAT,
    k15_parkinson_vol FLOAT,
    k60_parkinson_vol FLOAT,

    k5_price_position FLOAT,
    k15_price_position FLOAT,
    k60_price_position FLOAT,
    k5_body_ratio FLOAT,
    k15_body_ratio FLOAT,
    k60_body_ratio FLOAT,
    k5_cum_return FLOAT,
    k15_cum_return FLOAT,
    k60_cum_return FLOAT,

    k5_volume_surge FLOAT,
    k15_volume_surge FLOAT,
    k60_volume_surge FLOAT,
    k5_volume_trend FLOAT,
    k15_volume_trend FLOAT,
    k60_volume_trend FLOAT,

    ticker_price_change_pct FLOAT,
    ticker_volume_24h FLOAT,
    ticker_quote_volume_24h FLOAT,
    ticker_number_of_trades_24h FLOAT,

    PRIMARY KEY (symbol, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS features_kline (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    feature_version VARCHAR(20) NOT NULL,

    w50_price_mean FLOAT,
    w200_price_mean FLOAT,
    w1000_price_mean FLOAT,
    w50_price_cv FLOAT,
    w200_price_cv FLOAT,
    w1000_price_cv FLOAT,
    w50_return_skew FLOAT,
    w200_return_skew FLOAT,
    w1000_return_skew FLOAT,

    w50_vwap FLOAT,
    w200_vwap FLOAT,
    w1000_vwap FLOAT,
    w50_vwap_deviation FLOAT,
    w200_vwap_deviation FLOAT,
    w1000_vwap_deviation FLOAT,
    w50_large_trade_ratio FLOAT,
    w200_large_trade_ratio FLOAT,
    w1000_large_trade_ratio FLOAT,

    w50_buy_ratio FLOAT,
    w200_buy_ratio FLOAT,
    w1000_buy_ratio FLOAT,
    w50_trade_count FLOAT,
    w200_trade_count FLOAT,
    w1000_trade_count FLOAT,

    k5_realized_vol FLOAT,
    k15_realized_vol FLOAT,
    k60_realized_vol FLOAT,
    k5_parkinson_vol FLOAT,
    k15_parkinson_vol FLOAT,
    k60_parkinson_vol FLOAT,

    k5_price_position FLOAT,
    k15_price_position FLOAT,
    k60_price_position FLOAT,
    k5_body_ratio FLOAT,
    k15_body_ratio FLOAT,
    k60_body_ratio FLOAT,
    k5_cum_return FLOAT,
    k15_cum_return FLOAT,
    k60_cum_return FLOAT,

    k5_volume_surge FLOAT,
    k15_volume_surge FLOAT,
    k60_volume_surge FLOAT,
    k5_volume_trend FLOAT,
    k15_volume_trend FLOAT,
    k60_volume_trend FLOAT,

    ticker_price_change_pct FLOAT,
    ticker_volume_24h FLOAT,
    ticker_quote_volume_24h FLOAT,
    ticker_number_of_trades_24h FLOAT,

    PRIMARY KEY (symbol, ts)
) PARTITION BY RANGE (ts);

-- Indexes for both tables on (symbol, ts)
CREATE INDEX idx_features_trade_symbol_ts ON features_trade (symbol, ts);
CREATE INDEX idx_features_kline_symbol_ts ON features_kline (symbol, ts);

-- Partitions for April, May, June 2026
CREATE TABLE features_trade_2026_04 PARTITION OF features_trade
    FOR VALUES FROM ('2026-04-01 00:00:00Z') TO ('2026-05-01 00:00:00Z');
CREATE TABLE features_trade_2026_05 PARTITION OF features_trade
    FOR VALUES FROM ('2026-05-01 00:00:00Z') TO ('2026-06-01 00:00:00Z');
CREATE TABLE features_trade_2026_06 PARTITION OF features_trade
    FOR VALUES FROM ('2026-06-01 00:00:00Z') TO ('2026-07-01 00:00:00Z');

CREATE TABLE features_kline_2026_04 PARTITION OF features_kline
    FOR VALUES FROM ('2026-04-01 00:00:00Z') TO ('2026-05-01 00:00:00Z');
CREATE TABLE features_kline_2026_05 PARTITION OF features_kline
    FOR VALUES FROM ('2026-05-01 00:00:00Z') TO ('2026-06-01 00:00:00Z');
CREATE TABLE features_kline_2026_06 PARTITION OF features_kline
    FOR VALUES FROM ('2026-06-01 00:00:00Z') TO ('2026-07-01 00:00:00Z');
