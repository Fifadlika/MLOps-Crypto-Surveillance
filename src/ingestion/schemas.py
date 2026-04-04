"""
src/ingestion/schemas.py

Pydantic v2 models for validating and coercing all incoming Binance data events
before they are written to Redis Streams or PostgreSQL.

Three schemas are defined here, matching the three data sources in the pipeline:
  - TradeEvent      : real-time trade from WebSocket @trade stream
  - KlineEvent      : real-time candlestick from WebSocket @kline stream
  - HistoricalKline : batch candlestick from REST API /api/v3/klines

Design decisions:
  - All numeric fields arrive from Binance as strings → coerced to float/int here.
  - `notional` (price × quantity) is computed here so downstream never has to.
  - `buy_sell_ratio` is guarded against division-by-zero.
  - model_validator runs AFTER field coercion, so validators operate on clean types.
  - Fields that go into Redis Streams must be strings (see to_stream_dict()).

References:
  - Binance WS Trade stream:  https://binance-docs.github.io/apidocs/spot/en/#trade-streams
  - Binance WS Kline stream:  https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
  - Binance REST klines:      https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
  - Data contract: PROJECT_CONTEXT.md § "1. Ingestion → Bronze (Redis Streams)"
"""

import logging
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Module-level logger — follows the project standard:
#   %(asctime)s [%(name)s] %(levelname)s: %(message)s
# The logger name mirrors the module path so log output is traceable.
# ---------------------------------------------------------------------------
logger = logging.getLogger("ingestion.schemas")


# ---------------------------------------------------------------------------
# TradeEvent
# ---------------------------------------------------------------------------
# Binance WebSocket @trade stream payload example:
#
# {
#   "e": "trade",          # Event type
#   "E": 123456789,        # Event time (ms)
#   "s": "BTCUSDT",        # Symbol
#   "t": 12345,            # Trade ID
#   "p": "0.001",          # Price (string!)
#   "q": "100",            # Quantity (string!)
#   "T": 123456785,        # Trade time (ms)
#   "m": true,             # Is buyer the market maker?
# }
#
# After validation this becomes a typed Python object ready for downstream use.
# ---------------------------------------------------------------------------


class TradeEvent(BaseModel):
    """
    Validated, type-coerced representation of a single Binance trade event.

    Incoming Binance JSON uses single-letter keys (e.g., "t", "p", "q").
    We alias them here to readable names. The alias is what Pydantic reads from
    the raw dict; the field name is what the rest of our code uses.

    Field           Binance key   Type after coercion
    --------------- ------------- --------------------
    symbol          s             str
    trade_id        t             int
    price           p             float
    quantity        q             float
    notional        (computed)    float   price x quantity
    trade_time_ms   T             int     Unix milliseconds
    is_buyer_maker  m             bool
    """

    symbol: str = Field(..., alias="s", description="Trading pair, e.g. 'BTCUSDT'")
    trade_id: int = Field(..., alias="t", description="Unique trade ID — used as dedup key")
    price: float = Field(..., alias="p", description="Execution price")
    quantity: float = Field(..., alias="q", description="Executed quantity (base asset)")
    notional: float = Field(
        default=0.0,
        description="price x quantity — computed in model_validator, never supplied by caller",
    )
    trade_time_ms: int = Field(..., alias="T", description="Trade timestamp in Unix milliseconds")
    is_buyer_maker: bool = Field(
        ...,
        alias="m",
        description="True = seller is aggressor (buyer is market maker); False = buyer is aggressor",
    )

    # Allow both alias ("p") and field name ("price") when constructing the model.
    # This matters when we reconstruct from a cleaned dict that already has field names.
    model_config = {"populate_by_name": True}

    # ------------------------------------------------------------------
    # Field-level validators — run individually before model_validator
    # ------------------------------------------------------------------

    @field_validator("symbol")
    @classmethod
    def symbol_must_be_uppercase(cls, v: str) -> str:
        """
        Normalise symbol to uppercase.
        Binance always returns uppercase, but defensive coding costs nothing.
        """
        normalized = v.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def coerce_numeric_string(cls, v: object) -> float:
        """
        Binance returns numeric values as strings (e.g., "67432.15").
        We use Decimal for the initial parse to avoid floating-point rounding
        during the string -> float conversion, then convert to float.

        'mode=before' means this runs on the raw input before Pydantic's own
        type coercion, so both "67432.15" (string) and 67432.15 (float) work.
        """
        try:
            return float(Decimal(str(v)))
        except Exception as exc:
            raise ValueError(f"Cannot coerce '{v}' to a numeric value: {exc}") from exc

    # ------------------------------------------------------------------
    # Model-level validator — runs AFTER all fields are coerced
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def validate_business_rules(self) -> "TradeEvent":
        """
        Cross-field business rules that must all pass before the event
        is considered valid.

        Why here and not in field validators?
        Because these rules depend on multiple fields being valid first.
        model_validator(mode="after") runs once all field coercions are done.
        """
        if self.price <= 0:
            raise ValueError(
                f"price must be > 0, got {self.price} "
                f"(trade_id={self.trade_id}, symbol={self.symbol})"
            )
        if self.quantity <= 0:
            raise ValueError(
                f"quantity must be > 0, got {self.quantity} "
                f"(trade_id={self.trade_id}, symbol={self.symbol})"
            )
        if self.trade_time_ms <= 0:
            raise ValueError(
                f"trade_time_ms must be a positive Unix timestamp, got {self.trade_time_ms}"
            )

        # Compute notional here — single source of truth, no duplication downstream.
        # Use round() to 8 decimal places (standard crypto precision).
        self.notional = round(self.price * self.quantity, 8)

        logger.debug(
            "TradeEvent validated: symbol=%s trade_id=%d price=%.4f qty=%.6f notional=%.4f",
            self.symbol,
            self.trade_id,
            self.price,
            self.quantity,
            self.notional,
        )
        return self

    # ------------------------------------------------------------------
    # Serialisation helper
    # ------------------------------------------------------------------

    def to_stream_dict(self) -> dict[str, str]:
        """
        Serialise to a flat dict of strings for writing to a Redis Stream.

        Redis Streams only store string values. This is the canonical format
        consumed by DataCleaner (src/features/cleaning.py).

        The symbol is intentionally omitted because it is already encoded
        in the Redis stream key: stream:trades:{symbol}.

        Matches the Trade Stream Entry contract in PROJECT_CONTEXT.md §1.
        """
        return {
            "trade_id": str(self.trade_id),
            "price": str(self.price),
            "quantity": str(self.quantity),
            "notional": str(self.notional),
            "trade_time": str(self.trade_time_ms),
            "is_buyer_maker": str(self.is_buyer_maker),
        }


# ---------------------------------------------------------------------------
# KlineData (nested model — embeds inside KlineEvent)
# ---------------------------------------------------------------------------
# Binance packs candlestick data inside a nested "k" object:
#
# {
#   "k": {
#     "t": 123400000,   # Kline open time (ms)
#     "o": "0.0010",    # Open price
#     "h": "0.0025",    # High price
#     "l": "0.0015",    # Low price
#     "c": "0.0020",    # Close price
#     "v": "1000",      # Base asset volume
#     "n": 100,         # Number of trades
#     "V": "500",       # Taker buy base asset volume
#     "x": false,       # Is this kline closed?
#   }
# }
# ---------------------------------------------------------------------------


class KlineData(BaseModel):
    """
    The nested candlestick payload inside a Binance kline WebSocket event.

    We separate this from KlineEvent to keep the outer event clean and
    to make KlineData reusable (e.g., for unit testing kline logic in isolation).
    """

    open_time: int = Field(..., alias="t", description="Kline open time, Unix ms")
    open: float = Field(..., alias="o", description="Open price")
    high: float = Field(..., alias="h", description="High price")
    low: float = Field(..., alias="l", description="Low price")
    close: float = Field(..., alias="c", description="Close price")
    volume: float = Field(..., alias="v", description="Base asset volume")
    num_trades: int = Field(..., alias="n", description="Number of trades in this kline")
    taker_buy_volume: float = Field(
        ...,
        alias="V",
        description="Taker (aggressor buyer) base asset volume",
    )
    is_closed: bool = Field(
        ...,
        alias="x",
        description="True if this kline has closed (i.e., the interval is complete)",
    )
    buy_sell_ratio: float = Field(
        default=0.5,
        description="taker_buy_volume / volume — computed in model_validator",
    )

    model_config = {"populate_by_name": True}

    @field_validator("open", "high", "low", "close", "volume", "taker_buy_volume", mode="before")
    @classmethod
    def coerce_numeric_string(cls, v: object) -> float:
        try:
            return float(Decimal(str(v)))
        except Exception as exc:
            raise ValueError(f"Cannot coerce '{v}' to numeric: {exc}") from exc

    @model_validator(mode="after")
    def validate_ohlcv(self) -> "KlineData":
        """
        OHLCV sanity checks and buy_sell_ratio computation.

        Key rules:
        - high >= low (basic OHLCV integrity)
        - high >= open and high >= close (high is truly the highest)
        - low  <= open and low  <= close (low is truly the lowest)
        - volume >= 0 (zero-volume klines are valid in illiquid periods)
        - taker_buy_volume <= volume (can't buy more than was traded)
        - buy_sell_ratio guarded against division-by-zero
        """
        if self.high < self.low:
            raise ValueError(
                f"high ({self.high}) must be >= low ({self.low}) " f"at open_time={self.open_time}"
            )
        if self.high < self.open or self.high < self.close:
            raise ValueError(
                f"high ({self.high}) must be >= open ({self.open}) and close ({self.close})"
            )
        if self.low > self.open or self.low > self.close:
            raise ValueError(
                f"low ({self.low}) must be <= open ({self.open}) and close ({self.close})"
            )
        if self.volume < 0:
            raise ValueError(f"volume must be >= 0, got {self.volume}")
        if self.taker_buy_volume < 0:
            raise ValueError(f"taker_buy_volume must be >= 0, got {self.taker_buy_volume}")
        if self.taker_buy_volume > self.volume:
            raise ValueError(
                f"taker_buy_volume ({self.taker_buy_volume}) cannot exceed volume ({self.volume})"
            )

        # Guard against division-by-zero on zero-volume klines (illiquid market).
        # A ratio of 0.5 is a neutral/unknown fallback (equal buy/sell pressure).
        if self.volume > 0:
            self.buy_sell_ratio = round(self.taker_buy_volume / self.volume, 6)
        else:
            self.buy_sell_ratio = 0.5
            logger.debug(
                "Zero-volume kline at open_time=%d — buy_sell_ratio set to 0.5",
                self.open_time,
            )

        return self


# ---------------------------------------------------------------------------
# KlineEvent (top-level WebSocket kline event)
# ---------------------------------------------------------------------------
# Full WebSocket @kline payload example:
#
# {
#   "e": "kline",       # Event type
#   "E": 123456789,     # Event time (ms)
#   "s": "BTCUSDT",     # Symbol
#   "k": { ... }        # KlineData nested object (see above)
# }
# ---------------------------------------------------------------------------


class KlineEvent(BaseModel):
    """
    Top-level Binance WebSocket kline event wrapper.

    We only care about 's' (symbol) and 'k' (kline payload) from the outer envelope.
    The inner KlineData model handles its own validation.
    """

    symbol: str = Field(..., alias="s")
    kline: KlineData = Field(..., alias="k")

    model_config = {"populate_by_name": True}

    @field_validator("symbol")
    @classmethod
    def symbol_must_be_uppercase(cls, v: str) -> str:
        normalized = v.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    def to_stream_dict(self) -> dict[str, str]:
        """
        Serialise to a flat string dict for writing to a Redis Stream.

        We flatten the nested KlineData into the top-level dict because
        Redis Streams are flat key-value stores.

        The symbol is encoded in stream:klines:{symbol}, and is_closed is
        enforced by websocket_client.py before writing.

        Matches the Kline Stream Entry contract in PROJECT_CONTEXT.md §1.
        """
        k = self.kline
        return {
            "open_time": str(k.open_time),
            "open": str(k.open),
            "high": str(k.high),
            "low": str(k.low),
            "close": str(k.close),
            "volume": str(k.volume),
            "num_trades": str(k.num_trades),
            "buy_sell_ratio": str(k.buy_sell_ratio),
        }


# ---------------------------------------------------------------------------
# HistoricalKline
# ---------------------------------------------------------------------------
# Binance REST /api/v3/klines returns a LIST of lists (not dicts):
#
# [
#   [
#     1499040000000,        # [0]  Open time (ms)
#     "0.01634790",         # [1]  Open
#     "0.80000000",         # [2]  High
#     "0.01575800",         # [3]  Low
#     "0.01577100",         # [4]  Close
#     "148976.11427815",    # [5]  Volume
#     1499644799999,        # [6]  Close time (ms)
#     "2434.19055334",      # [7]  Quote asset volume
#     308,                  # [8]  Number of trades
#     "1756.87402397",      # [9]  Taker buy base asset volume
#     "28.46694368",        # [10] Taker buy quote asset volume
#     "0"                   # [11] Ignore
#   ],
#   ...
# ]
#
# Because there are no named keys, we parse from positional index via a
# class method factory `from_rest_list()` rather than direct dict construction.
# ---------------------------------------------------------------------------


class HistoricalKline(BaseModel):
    """
    Validated representation of a single candlestick bar from the Binance REST API.

    Used by rest_client.py for:
      1. Initial 30-day bootstrap (scripts/bootstrap.py)
      2. Gap-filling after WebSocket reconnects

    This schema intentionally mirrors KlineData fields so that downstream
    consumers (cleaning.py, feature engineering) can treat them uniformly.
    """

    symbol: str = Field(..., description="Trading pair, e.g. 'BTCUSDT'")
    open_time: int = Field(..., description="Kline open time, Unix ms")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Close price")
    volume: float = Field(..., description="Base asset volume")
    close_time: int = Field(..., description="Kline close time, Unix ms")
    num_trades: int = Field(..., description="Number of trades in this kline")
    taker_buy_volume: float = Field(..., description="Taker buy base asset volume")
    buy_sell_ratio: float = Field(
        default=0.5,
        description="taker_buy_volume / volume — computed in model_validator",
    )

    @model_validator(mode="after")
    def validate_and_compute(self) -> "HistoricalKline":
        """Same OHLCV integrity checks as KlineData, applied to historical bars."""
        if self.high < self.low:
            raise ValueError(
                f"high ({self.high}) must be >= low ({self.low}) at open_time={self.open_time}"
            )
        if self.high < self.open or self.high < self.close:
            raise ValueError(
                f"high ({self.high}) must be >= open ({self.open}) and close ({self.close})"
            )
        if self.low > self.open or self.low > self.close:
            raise ValueError(
                f"low ({self.low}) must be <= open ({self.open}) and close ({self.close})"
            )
        if self.volume < 0:
            raise ValueError(f"volume must be >= 0, got {self.volume}")
        if self.close_time <= self.open_time:
            raise ValueError(
                f"close_time ({self.close_time}) must be > open_time ({self.open_time})"
            )

        if self.volume > 0:
            self.buy_sell_ratio = round(self.taker_buy_volume / self.volume, 6)
        else:
            self.buy_sell_ratio = 0.5

        return self

    @classmethod
    def from_rest_list(cls, symbol: str, row: list) -> "HistoricalKline":
        """
        Factory method: construct a HistoricalKline from a raw Binance REST row.

        Binance returns klines as positional lists, not dicts. This method
        maps each positional index to a named field so the rest of the code
        never has to remember "index 5 is volume".

        Args:
            symbol: Trading pair string (e.g., "BTCUSDT") — not in the row itself.
            row:    Raw list from Binance REST /api/v3/klines response.

        Returns:
            A fully validated HistoricalKline instance.

        Raises:
            ValueError: If the row has fewer than 11 elements (malformed response).
            pydantic.ValidationError: If any field fails validation.

        Example:
            raw_row = [1499040000000, "0.01634790", "0.80000000", ...]
            kline = HistoricalKline.from_rest_list("BTCUSDT", raw_row)
        """
        if len(row) < 11:
            raise ValueError(
                f"Binance kline row must have at least 11 elements, got {len(row)}: {row}"
            )

        # Use Decimal for the initial string->float conversion (same pattern as field validators)
        def to_float(val: object) -> float:
            return float(Decimal(str(val)))

        return cls(
            symbol=symbol.strip().upper(),
            open_time=int(row[0]),
            open=to_float(row[1]),
            high=to_float(row[2]),
            low=to_float(row[3]),
            close=to_float(row[4]),
            volume=to_float(row[5]),
            close_time=int(row[6]),
            num_trades=int(row[8]),
            taker_buy_volume=to_float(row[9]),
        )

    def to_stream_dict(self) -> dict[str, str]:
        """
        Serialise to a flat string dict for writing to a Redis Stream.

        Historical klines are written to the same Redis Stream as live klines
        during gap-fill, so this output must match the live kline contract.
        """
        return {
            "open_time": str(self.open_time),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "num_trades": str(self.num_trades),
            "buy_sell_ratio": str(self.buy_sell_ratio),
        }
