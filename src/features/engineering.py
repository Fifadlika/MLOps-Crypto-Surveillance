import collections
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.features.store import FeatureStore
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client

logger = get_logger(__name__)


class FeatureCalculatorBase:
    def __init__(self, symbol: str, windows: List[int]):
        self.symbol = symbol
        self.windows = windows
        self.deques: Dict[int, collections.deque] = {
            w: collections.deque(maxlen=w) for w in windows
        }

    def _create_base_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        d["symbol"] = self.symbol
        d["ts"] = 0
        d["feature_version"] = "1.0"
        for i in range(1, 50):
            d[f"f{i}"] = 0.0
        return d


class TradeFeatureCalculator(FeatureCalculatorBase):
    def __init__(self, symbol: str):
        super().__init__(symbol, [50, 200, 1000])

    def update(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        d = self._create_base_dict()
        d["ts"] = int(trade.get("E", time.time() * 1000))
        return d


class KlineFeatureCalculator(FeatureCalculatorBase):
    def __init__(self, symbol: str):
        super().__init__(symbol, [5, 15, 60])

    def update(self, kline: Dict[str, Any]) -> Dict[str, Any]:
        d = self._create_base_dict()
        d["ts"] = int(kline.get("E", time.time() * 1000))
        return d


class FeaturePipeline:
    def __init__(self) -> None:
        self.config = get_config()
        self.redis = get_redis_client()
        self.store = FeatureStore()

        self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        self.trade_calcs = {s: TradeFeatureCalculator(s) for s in self.symbols}
        self.kline_calcs = {s: KlineFeatureCalculator(s) for s in self.symbols}

        self.buffer: Dict[str, List[Dict[str, Any]]] = {s: [] for s in self.symbols}
        self.group_name = "feature-engineering-group"
        self.consumer_name = "consumer-1"
        self._init_streams()

    def _init_streams(self) -> None:
        for s in self.symbols:
            for stream in [f"stream:trades:{s}", f"stream:klines:{s}"]:
                try:
                    self.redis.xgroup_create(stream, self.group_name, id="0", mkstream=True)
                except Exception:
                    pass

    def _flush_parquet(self, symbol: str) -> None:
        if not self.buffer[symbol]:
            return
        df = pd.DataFrame(self.buffer[symbol])
        out_dir = Path(f"data/features/{symbol}")
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = out_dir / f"{int(time.time() * 1000)}.parquet"
        try:
            df.to_parquet(fname)
            self.buffer[symbol].clear()
        except Exception as e:
            logger.error(f"Failed to write parquet for {symbol}: {e}")

    def run(self) -> None:
        streams = {}
        for s in self.symbols:
            streams[f"stream:trades:{s}"] = ">"
            streams[f"stream:klines:{s}"] = ">"

        while True:
            try:
                events = self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=100,
                    block=1000,
                )

                if not events:
                    continue

                self._process_events(events)
            except Exception as e:
                logger.error(f"Error in feature engineering loop: {e}")
                time.sleep(1)

    def _process_events(self, events: Any) -> None:
        for stream_name, messages in events:
            stream_str = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
            parts = stream_str.split(":")
            if len(parts) >= 3:
                kind = parts[1]
                symbol = parts[2]
                self._process_messages(kind, symbol, stream_str, messages)

    def _process_messages(self, kind: str, symbol: str, stream_str: str, messages: Any) -> None:
        for msg_id, msg_data in messages:
            data_dict = {
                k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                for k, v in msg_data.items()
            }
            if kind == "trades":
                feat = self.trade_calcs[symbol].update(data_dict)
                self.store.write_online(symbol, feat)
                self.store.write_offline(symbol, feat, kind="trade")
                self.buffer[symbol].append(feat)
                self.redis.xack(stream_str, self.group_name, msg_id)
            elif kind == "klines":
                feat = self.kline_calcs[symbol].update(data_dict)
                self.store.write_online(symbol, feat)
                self.store.write_offline(symbol, feat, kind="kline")
                self.buffer[symbol].append(feat)
                self.redis.xack(stream_str, self.group_name, msg_id)

            if len(self.buffer[symbol]) >= 100:
                self._flush_parquet(symbol)


def run() -> None:
    pipeline = FeaturePipeline()
    pipeline.run()


if __name__ == "__main__":
    run()
