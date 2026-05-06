# mypy: ignore-errors
"""
src/features/engineering.py

Feature engineering layer: Bronze (cleaned Redis streams) → Gold (feature vectors).

Responsibilities:
- TradeFeatureCalculator  : compute 24 trade-based features across 3 sliding windows
- KlineFeatureCalculator  : compute 21 kline-based features across 3 sliding windows
- FeaturePipeline         : orchestrate calculators, write to FeatureStore, flush Parquet
"""

from __future__ import annotations

import collections
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from src.features.store import FeatureStore
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_sync_redis_client

logger = get_logger(__name__)

FEATURE_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------
class FeatureCalculatorBase:
    """
    Shared scaffolding for all feature calculators.

    Menggunakan collections.deque dengan maxlen agar append O(1) dan
    window lama otomatis terdrop — tidak perlu slice manual.
    """

    def __init__(self, symbol: str, windows: List[int]) -> None:
        self.symbol = symbol
        self.windows = sorted(windows)  # ascending agar window kecil selalu dihitung dulu
        # Satu deque per window size — maxlen=window menjamin sliding window O(1)
        self.deques: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in windows
        }

    def _base_metadata(self, ts: int) -> Dict[str, Any]:
        """Tiga field metadata wajib yang selalu ada di setiap feature vector."""
        return {
            "symbol": self.symbol,
            "ts": ts,
            "feature_version": FEATURE_VERSION,
        }

    def _safe_std(self, values: collections.deque[float]) -> float:
        """
        Standard deviation yang aman terhadap:
        - deque kosong → 0.0
        - deque satu elemen → 0.0 (std tidak terdefinisi)
        - semua nilai sama → 0.0

        Menggunakan formula Welford-style via math untuk menghindari
        presisi floating point yang buruk pada nilai besar (harga BTC).
        """
        n = len(values)
        if n < 2:
            return 0.0
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / (n - 1)
        return float(math.sqrt(max(variance, 0.0)))

    def _safe_mean(self, values: collections.deque[float]) -> float:
        """Mean yang aman terhadap deque kosong."""
        return float(sum(values) / len(values)) if values else 0.0

    def _pct_change(self, values: collections.deque[float]) -> float:
        """
        Persentase perubahan dari elemen pertama ke elemen terakhir dalam window.
        Jika window belum penuh atau nilai awal 0 → return 0.0.

        Rumus: (last - first) / first
        """
        if len(values) < 2 or values[0] == 0:
            return 0.0
        return (values[-1] - values[0]) / values[0]


# ---------------------------------------------------------------------------
# TradeFeatureCalculator
# ---------------------------------------------------------------------------


class TradeFeatureCalculator(FeatureCalculatorBase):
    """
    Menghitung 24 fitur berbasis trade stream untuk anomaly detection.

    Kelompok fitur (8 fitur × 3 window = 24 total):
    ┌─────────────────┬────────────────────────────────────────────────┐
    │ Kelompok        │ Fitur                                          │
    ├─────────────────┼────────────────────────────────────────────────┤
    │ Price (3x3=9)   │ mean, std, pct_change per window               │
    │ Volume (3x3=9)  │ mean, std, total per window                    │
    │ Order flow(2x3) │ buy_ratio, trade_rate per window               │
    └─────────────────┴────────────────────────────────────────────────┘

    Window sizes: [50, 200, 1000] trades
    Total: 9 + 9 + 6 = 24 fitur trade
    """

    def __init__(self, symbol: str):
        super().__init__(symbol, [50, 200, 1000])
        self._prices: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._quantities: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._is_buyer: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._timestamps: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }

    def update(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """
        Terima satu cleaned trade dict dari DataCleaner, update semua window,
        lalu kembalikan feature vector lengkap.

        Input fields yang dibutuhkan (output DataCleaner.process_trade):
            price, quantity, trade_time_ms, is_buyer_maker
        """
        # --- Ekstrak nilai dari cleaned trade ---
        try:
            price = float(trade.get("price", 0))
            quantity = float(trade.get("quantity", 0))
            ts_ms = int(trade.get("trade_time_ms", trade.get("trade_time", time.time() * 1000)))
            is_buyer = self._coerce_bool(trade.get("is_buyer_maker", False))
        except (TypeError, ValueError) as exc:
            logger.warning("TradeFeatureCalculator: bad trade data for %s: %s", self.symbol, exc)
            return self._base_metadata(int(time.time() * 1000))

        if price <= 0 or quantity <= 0:
            # Tolak trade tidak valid — jangan update window
            return self._base_metadata(ts_ms)

        # --- Update semua window deque ---
        for w in self.windows:
            self._prices[w].append(price)
            self._quantities[w].append(quantity)
            self._is_buyer[w].append(1.0 if is_buyer else 0.0)
            self._timestamps[w].append(ts_ms)

        # --- Hitung fitur ---
        features = self._base_metadata(ts_ms)

        for w in self.windows:
            p = self._prices[w]
            q = self._quantities[w]
            b = self._is_buyer[w]
            t = self._timestamps[w]

            # Price features
            features[f"price_mean_{w}"] = self._safe_mean(p)
            features[f"price_std_{w}"] = self._safe_std(p)
            features[f"price_change_{w}"] = self._pct_change(p)

            # Volume features
            features[f"vol_mean_{w}"] = self._safe_mean(q)
            features[f"vol_std_{w}"] = self._safe_std(q)
            features[f"vol_total_{w}"] = sum(q)

            # Order flow features
            features[f"buy_ratio_{w}"] = self._safe_mean(b)  # proporsi buy dalam window

            # Trade rate: trades per detik dalam window
            if len(t) >= 2:
                elapsed_s = max((t[-1] - t[0]) / 1000.0, 1e-6)
                features[f"trade_rate_{w}"] = len(t) / elapsed_s
            else:
                features[f"trade_rate_{w}"] = 0.0

        return features

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "t", "yes"}


# ---------------------------------------------------------------------------
# KlineFeatureCalculator
# ---------------------------------------------------------------------------
class KlineFeatureCalculator(FeatureCalculatorBase):
    """
    Menghitung 21 fitur berbasis kline stream untuk volatility prediction.

    Hanya memproses kline yang sudah closed (is_closed=True).

    Kelompok fitur (7 fitur x 3 window = 21 total):
    ┌──────────────────┬────────────────────────────────────────────────┐
    │ Kelompok         │ Fitur                                          │
    ├──────────────────┼────────────────────────────────────────────────┤
    │ Volatility (2x3) │ atr, hl_ratio per window                       │
    │ Trend (3x3)      │ sma, ema, momentum per window                  │
    │ Vol kline (2x3)  │ vwap, vol_ratio per window                     │
    └──────────────────┴────────────────────────────────────────────────┘

    Window sizes: [5, 15, 60] klines
    Total: 6 + 9 + 6 = 21 fitur kline
    """

    def __init__(self, symbol: str) -> None:
        super().__init__(symbol, [5, 15, 60])
        self._highs: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._lows: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._closes: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._volumes: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        self._notionals: Dict[int, collections.deque[float]] = {
            w: collections.deque(maxlen=w) for w in self.windows
        }
        # EMA membutuhkan state persisten (tidak bisa dihitung ulang dari deque saja)
        self._ema_state: Dict[int, Optional[float]] = dict.fromkeys(self.windows)
        # Rata-rata volume historis untuk vol_ratio
        self._vol_history: collections.deque[float] = collections.deque(maxlen=200)

    def update(self, kline: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Terima satu cleaned kline dict dari DataCleaner.process_kline.
        Return None jika kline belum closed (gate ini juga ada di DataCleaner,
        tapi kita defence-in-depth di sini).

        Input fields yang dibutuhkan:
            open, high, low, close, volume, open_time, is_closed
        """
        # Gate: hanya kline closed
        is_closed = self._coerce_bool(kline.get("is_closed", True))
        if not is_closed:
            return None

        try:
            high = float(kline.get("high", 0))
            low = float(kline.get("low", 0))
            close = float(kline.get("close", 0))
            volume = float(kline.get("volume", 0))
            ts_ms = int(kline.get("open_time", time.time() * 1000))
        except (TypeError, ValueError) as exc:
            logger.warning("KlineFeatureCalculator: bad kline data for %s: %s", self.symbol, exc)
            return None

        if close <= 0:
            return None

        # Notional = close * volume (proxy VWAP sederhana per kline)
        notional = close * volume
        self._vol_history.append(volume)

        for w in self.windows:
            self._highs[w].append(high)
            self._lows[w].append(low)
            self._closes[w].append(close)
            self._volumes[w].append(volume)
            self._notionals[w].append(notional)
            # Update EMA dengan smoothing factor k = 2/(w+1)
            k = 2.0 / (w + 1)
            if self._ema_state[w] is None:
                self._ema_state[w] = float(close)
            else:
                current_ema: float = self._ema_state[w]
                self._ema_state[w] = float(close * k + current_ema * (1 - k))

        features = self._base_metadata(ts_ms)

        vol_baseline = self._safe_mean(self._vol_history)

        for w in self.windows:
            h = self._highs[w]
            low_w = self._lows[w]
            c = self._closes[w]
            v = self._volumes[w]
            n = self._notionals[w]

            # --- Volatility ---
            # ATR (Average True Range) — ukuran volatilitas standar trading
            # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
            # Kita sederhanakan ke high-low karena prev_close tidak selalu tersedia
            tr_values = [h[i] - low_w[i] for i in range(len(h))]
            features[f"atr_{w}"] = self._safe_mean(collections.deque(tr_values))
            features[f"hl_ratio_{w}"] = (
                (self._safe_mean(h) - self._safe_mean(low_w)) / self._safe_mean(c)
                if self._safe_mean(c) > 0
                else 0.0
            )

            # --- Trend ---
            features[f"sma_{w}"] = self._safe_mean(c)
            features[f"ema_{w}"] = self._ema_state[w] or 0.0
            features[f"momentum_{w}"] = self._pct_change(c)

            # --- Volume kline ---
            # VWAP = total notional / total volume dalam window
            total_vol = sum(v)
            features[f"vwap_{w}"] = sum(n) / total_vol if total_vol > 0 else 0.0
            # vol_ratio: volume window terakhir relatif terhadap baseline historis
            features[f"vol_ratio_{w}"] = (
                self._safe_mean(v) / vol_baseline if vol_baseline > 0 else 1.0
            )

        return features

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "t", "yes"}


# ---------------------------------------------------------------------------
# FeaturePipeline — tidak berubah strukturnya, hanya calculator di-replace
# ---------------------------------------------------------------------------


class FeaturePipeline:
    """
    Orkestrasi feature engineering dari Redis Stream ke FeatureStore + Parquet.

    Mode operasi:
    - run()         : long-running loop untuk produksi (dipanggil pipeline.py)
    - run_finite()  : baca semua pending messages lalu exit (dipanggil DVC stage)
    """

    def __init__(self) -> None:
        self.config = get_config()
        self.redis = get_sync_redis_client()
        self.store = FeatureStore()

        self.symbols = [
            str(s).upper()
            for s in getattr(
                getattr(self.config, "data", None),
                "trading_pairs",
                ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
            )
        ]
        self.trade_calcs = {s: TradeFeatureCalculator(s) for s in self.symbols}
        self.kline_calcs = {s: KlineFeatureCalculator(s) for s in self.symbols}

        self.buffer: Dict[str, List[Dict[str, Any]]] = {s: [] for s in self.symbols}
        self.group_name = "feature-engineering-group"
        self.consumer_name = "consumer-1"
        self._init_streams()

    def _init_streams(self) -> None:
        for s in self.symbols:
            for stream in [f"stream:trades:{s.lower()}", f"stream:klines:{s.lower()}"]:
                try:
                    self.redis.xgroup_create(stream, self.group_name, id="0", mkstream=True)
                except Exception:
                    pass  # group sudah ada — ini expected behavior, bukan error

    def _flush_parquet(self, symbol: str) -> None:
        if not self.buffer[symbol]:
            return
        df = pd.DataFrame(self.buffer[symbol])
        out_dir = Path(f"data/features/{symbol}")
        out_dir.mkdir(parents=True, exist_ok=True)
        date_str = time.strftime("%Y-%m-%d")
        fname = out_dir / f"{date_str}_fv1.0.parquet"
        try:
            # Jika file hari ini sudah ada, append ke dalamnya
            if fname.exists():
                existing = pd.read_parquet(fname)
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=["symbol", "ts"], keep="last")
            df.to_parquet(fname, index=False)
            self.buffer[symbol].clear()
            logger.info("Flushed %d records to %s", len(df), fname)
        except Exception as e:
            logger.error("Failed to write parquet for %s: %s", symbol, e)

    def _process_messages(self, kind: str, symbol: str, stream_str: str, messages: Any) -> None:
        for msg_id, msg_data in messages:
            data_dict = {
                k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                for k, v in msg_data.items()
            }

            feat: Optional[Dict[str, Any]] = None
            if kind == "trades":
                feat = self.trade_calcs[symbol].update(data_dict)
            elif kind == "klines":
                feat = self.kline_calcs[symbol].update(data_dict)

            if feat:
                self.store.write_online(symbol, feat)
                self.store.write_offline(
                    symbol, feat, kind="trade" if kind == "trades" else "kline"
                )
                self.buffer[symbol].append(feat)

            self.redis.xack(stream_str, self.group_name, msg_id)

        if len(self.buffer[symbol]) >= 100:
            self._flush_parquet(symbol)

    def _process_events(self, events: Any) -> None:
        for stream_name, messages in events:
            stream_str = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
            parts = stream_str.split(":")
            if len(parts) >= 3:
                kind = parts[1]  # "trades" atau "klines"
                symbol = parts[2].upper()
                if symbol in self.symbols:
                    self._process_messages(kind, symbol, stream_str, messages)

    def run(self) -> None:
        """Long-running mode — untuk produksi real-time."""
        streams = {}
        for s in self.symbols:
            streams[f"stream:trades:{s.lower()}"] = ">"
            streams[f"stream:klines:{s.lower()}"] = ">"

        logger.info("FeaturePipeline starting in long-running mode for %s", self.symbols)
        while True:
            try:
                events = self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=100,
                    block=1000,
                )
                if events:
                    self._process_events(events)
            except Exception as e:
                logger.error("Error in feature engineering loop: %s", e)
                time.sleep(1)

    def run_finite(self, idle_rounds: int = 3) -> int:
        """
        Finite mode — baca semua pending messages dari awal stream lalu exit.

        Digunakan oleh DVC stage 'featurize'. Cara kerjanya:
        1. Baca pending messages di consumer group dengan id=">"
        2. Proses setiap batch dan hitung langsung dari jumlah message
        3. Jika N round berturut-turut tidak ada message baru → flush dan exit

        Args:
            idle_rounds: Berapa round kosong berturut-turut sebelum dianggap selesai.

        Returns:
            Total jumlah feature vector yang ditulis.
        """
        streams = {}
        for s in self.symbols:
            streams[f"stream:trades:{s.lower()}"] = ">"
            streams[f"stream:klines:{s.lower()}"] = ">"

        logger.info("FeaturePipeline starting in finite mode for %s", self.symbols)

        total_processed = 0
        consecutive_empty = 0

        while consecutive_empty < idle_rounds:
            try:
                events = self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=500,  # batch lebih besar untuk efisiensi finite mode
                    block=2000,  # tunggu maksimal 2s sebelum anggap idle
                )
            except Exception as e:
                logger.error("Error reading streams in finite mode: %s", e)
                break

            if not events:
                consecutive_empty += 1
                logger.debug("Idle round %d/%d", consecutive_empty, idle_rounds)
                continue

            consecutive_empty = 0  # reset counter kalau ada data
            self._process_events(events)
            # Hitung dari buffer untuk tracking
            batch_count = sum(len(messages) for _, messages in events)
            total_processed += batch_count

        # Flush semua sisa buffer ke Parquet
        for symbol in self.symbols:
            self._flush_parquet(symbol)

        logger.info("Finite mode complete. Total feature vectors written: %d", total_processed)
        return total_processed


def run() -> None:
    """Entry point untuk long-running mode (produksi)."""
    pipeline = FeaturePipeline()
    pipeline.run()


if __name__ == "__main__":
    run()
