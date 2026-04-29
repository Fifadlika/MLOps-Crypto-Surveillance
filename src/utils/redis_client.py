from __future__ import annotations

import inspect
import os
from functools import lru_cache
from typing import Any, cast

from redis.asyncio import Redis

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

RedisScalar = bytes | bytearray | memoryview | str | int | float
RedisXAddData = dict[RedisScalar, RedisScalar]
RedisXReadStreams = dict[bytes | str | memoryview, int | bytes | str | memoryview]


class MockRedisClient:
    def __init__(self) -> None:
        self._kv: dict[str, str] = {}

    async def xadd(self, key: str, data: dict, **kwargs: Any):
        return "0-0"

    async def xread(self, streams: dict[str, str], count: int = 10, block: int = 0):
        return []

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self._kv:
            return False
        self._kv[key] = value
        return True

    async def publish(self, channel: str, message: str):
        return 1

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None


class RealRedisClient:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def xadd(self, key: str, data: dict[str, str], **kwargs: Any):
        payload = cast(RedisXAddData, data)
        return await self._redis.xadd(key, payload, **kwargs)

    async def xread(self, streams: dict[str, str], count: int = 10, block: int = 0):
        stream_offsets = cast(RedisXReadStreams, streams)
        return await self._redis.xread(streams=stream_offsets, count=count, block=block)

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        result = self._redis.set(name=key, value=value, ex=ex, nx=nx)
        if inspect.isawaitable(result):
            return bool(await result)
        return bool(result)

    async def publish(self, channel: str, message: str):
        return await self._redis.publish(channel, message)

    async def ping(self) -> bool:
        result = self._redis.ping()
        if inspect.isawaitable(result):
            return bool(await result)
        return bool(result)

    async def close(self) -> None:
        await self._redis.close()


def get_redis_runtime_mode(config: Any | None = None) -> str:
    cfg = config or get_config()
    redis_cfg = getattr(cfg, "redis", None)
    runtime_mode = (
        os.getenv("REDIS_RUNTIME_MODE")
        or os.getenv("REDIS__RUNTIME_MODE")
        or getattr(redis_cfg, "runtime_mode", "mock")
    )
    return str(runtime_mode).strip().lower()


def _resolve_redis_url(redis_cfg: Any) -> str:
    env_url = os.getenv("REDIS_URL") or os.getenv("REDIS__URL")
    if env_url is not None:
        return env_url.strip()

    cfg_url = getattr(redis_cfg, "url", "")
    return str(cfg_url).strip()


def _resolve_redis_host(redis_cfg: Any) -> str:
    return str(
        os.getenv("REDIS_HOST")
        or os.getenv("REDIS__HOST")
        or getattr(redis_cfg, "host", "localhost")
    )


def _resolve_redis_port(redis_cfg: Any) -> int:
    env_port = os.getenv("REDIS_PORT") or os.getenv("REDIS__PORT")
    if env_port is not None:
        return int(env_port)

    return int(getattr(redis_cfg, "port", 6379))


def _resolve_redis_db(redis_cfg: Any) -> int:
    env_db = os.getenv("REDIS_DB") or os.getenv("REDIS__DB")
    if env_db is not None:
        return int(env_db)

    return int(getattr(redis_cfg, "db", 0))


def _resolve_redis_password(redis_cfg: Any) -> str | None:
    env_password = os.getenv("REDIS_PASSWORD") or os.getenv("REDIS__PASSWORD")
    if env_password is not None:
        return env_password

    cfg_password = getattr(redis_cfg, "password", None)
    if cfg_password is None:
        return None
    return str(cfg_password)


def _resolve_decode_responses(redis_cfg: Any) -> bool:
    return bool(getattr(redis_cfg, "decode_responses", True))


@lru_cache(maxsize=1)
def get_redis_client():
    config = get_config()
    redis_cfg = getattr(config, "redis", None)
    runtime_mode = get_redis_runtime_mode(config)

    if runtime_mode != "real":
        logger.info("Using MockRedisClient (redis.runtime_mode=%s)", runtime_mode)
        return MockRedisClient()

    redis_url = _resolve_redis_url(redis_cfg)
    decode_responses = _resolve_decode_responses(redis_cfg)

    if redis_url:
        if decode_responses:
            logger.info("Using RealRedisClient (redis.runtime_mode=real)")
            return RealRedisClient(
                Redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )
            )

        logger.info("Using RealRedisClient (redis.runtime_mode=real)")
        return RealRedisClient(
            Redis.from_url(
                redis_url,
                decode_responses=False,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
        )

    if decode_responses:
        logger.info("Using RealRedisClient (redis.runtime_mode=real)")
        return RealRedisClient(
            Redis(
                host=_resolve_redis_host(redis_cfg),
                port=_resolve_redis_port(redis_cfg),
                db=_resolve_redis_db(redis_cfg),
                password=_resolve_redis_password(redis_cfg),
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
        )

    logger.info("Using RealRedisClient (redis.runtime_mode=real)")
    return RealRedisClient(
        Redis(
            host=_resolve_redis_host(redis_cfg),
            port=_resolve_redis_port(redis_cfg),
            db=_resolve_redis_db(redis_cfg),
            password=_resolve_redis_password(redis_cfg),
            decode_responses=False,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        )
    )
