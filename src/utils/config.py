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


class StreamingConfig(BaseModel):
    websocket_url: str = "wss://stream.binance.com:9443"
    max_reconnect_attempts: int = 10
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0


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
    )

    data: DataConfig = DataConfig()
    streaming: StreamingConfig = StreamingConfig()
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
