from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class DataConfig(BaseModel):
    trading_pairs: list[str] = Field(default_factory=lambda: ["BTCUSDT"])
    raw_data_path: str = "data/raw"
    processed_data_path: str = "data/preprocess"
    preprocessed_data_path: str = "data/preprocess"
    features_path: str = "data/features"
    dedup_bloom_dir: str = "data/raw/.dedup"


class StreamingConfig(BaseModel):
    websocket_url: str = "wss://stream.binance.com:9443"
    rest_url: str = "https://api4.binance.com"
    rest_timeout_seconds: float = 10.0
    rest_rate_limit_per_minute: int = 400
    rest_retry_after_429_seconds: int = 60
    max_reconnect_attempts: int = 10
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    dedup_ttl_seconds: int = 3600
    bloom_capacity: int = 1_000_000
    bloom_error_rate: float = 0.001
    kline_gap_threshold_seconds: int = 300
    midnight_flush_second: int = 5
    cleaner_poll_interval_seconds: float = 0.25


class RedisConfig(BaseSettings):
    runtime_mode: str = Field(default="real", alias="REDIS_RUNTIME_MODE")
    url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    host: str = Field(default="localhost", alias="REDIS_HOST")
    port: int = Field(default=6379, alias="REDIS_PORT")
    db: int = Field(default=0, alias="REDIS_DB")
    password: str | None = Field(default=None, alias="REDIS_PASSWORD")
    decode_responses: bool = Field(default=True, alias="REDIS_DECODE_RESPONSES")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        populate_by_name=True,
    )


class PostgresConfig(BaseSettings):
    host: str = Field(default="localhost", alias="POSTGRES_HOST")
    port: int = Field(default=5432, alias="POSTGRES_PORT")
    db: str = Field(default="crypto_surveillance", alias="POSTGRES_DB")
    user: str = Field(alias="POSTGRES_USER")
    password: str = Field(alias="POSTGRES_PASSWORD")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        populate_by_name=True,
    )


class BinanceCompat(BaseModel):
    ws_base_url: str
    max_reconnect_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float


class MlflowConfig(BaseModel):
    experiment_name: str = "ingestion_websocket"


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        yaml_file=str(Path(__file__).resolve().parents[2] / "config" / "config.yaml"),
        populate_by_name=True,
    )

    data: DataConfig = DataConfig()
    streaming: StreamingConfig = StreamingConfig()
    redis: RedisConfig = RedisConfig()
    postgres: PostgresConfig = PostgresConfig()
    mlflow: MlflowConfig = MlflowConfig()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )

    # Compatibility for current websocket_client usage:
    @property
    def symbols(self) -> list[str]:
        return [s.upper() for s in self.data.trading_pairs]

    @property
    def binance(self) -> BinanceCompat:
        return BinanceCompat(
            ws_base_url=self.streaming.websocket_url,
            max_reconnect_attempts=self.streaming.max_reconnect_attempts,
            base_backoff_seconds=self.streaming.base_backoff_seconds,
            max_backoff_seconds=self.streaming.max_backoff_seconds,
        )


@lru_cache(maxsize=1)
def get_config() -> Config:
    return Config()
