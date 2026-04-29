import json
from typing import Optional

import psycopg2

from src.utils.logger import get_logger
from src.utils.redis_client import get_redis_client

logger = get_logger(__name__)


class FeatureStore:
    def __init__(self):
        from src.utils.config import get_config

        self.config = get_config()
        self.redis = get_redis_client()
        self.pg_conn = None
        self._init_pg()

    def _init_pg(self):
        try:
            self.pg_conn = psycopg2.connect(
                host=self.config.postgres.host,
                port=self.config.postgres.port,
                user=self.config.postgres.user,
                password=self.config.postgres.password,
                database=self.config.postgres.db,
            )
            logger.info("Connected to PostgreSQL feature store.")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")

    def write_online(self, symbol: str, feature_vector: dict) -> None:
        try:
            key = f"features:latest:{symbol}"
            mapping = {k: json.dumps(v) for k, v in feature_vector.items()}
            self.redis.hset(key, mapping=mapping)
            self.redis.expire(key, 300)
        except Exception as e:
            logger.error(f"Failed to write online features for {symbol}: {e}")

    def write_offline(self, symbol: str, feature_vector: dict, kind: str) -> None:
        if not self.pg_conn:
            self._init_pg()
        if not self.pg_conn:
            return

        table = f"features_{kind}"
        if kind not in ["trade", "kline"]:
            logger.error(f"Invalid kind for offline features: {kind}")
            return

        columns = list(feature_vector.keys())
        values = list(feature_vector.values())

        # 'symbol' might be already in feature_vector, but let's ensure it is
        # The schema demands symbol, ts, feature_version, and 49 numeric fields

        placeholders = ", ".join(["%s"] * len(values))
        cols_str = ", ".join(columns)

        query = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT (symbol, ts) DO NOTHING"

        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(query, values)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to write offline features for {symbol}: {e}")

    def read_online(self, symbol: str) -> Optional[dict]:
        try:
            key = f"features:latest:{symbol}"
            data = self.redis.hgetall(key)
            if not data:
                return None
            return {
                k.decode("utf-8") if isinstance(k, bytes) else k: json.loads(v)
                for k, v in data.items()
            }
        except Exception as e:
            logger.error(f"Failed to read online features for {symbol}: {e}")
            return None
