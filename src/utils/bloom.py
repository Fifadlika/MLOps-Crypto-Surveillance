"""Persistent bloom-filter utilities for restart-safe deduplication."""

from __future__ import annotations

import logging
import os
import pickle
from pathlib import Path
from threading import RLock
from typing import Any

try:
    from pybloom_live import ScalableBloomFilter
except ImportError:  # pragma: no cover - fallback for environments without dependency installed
    ScalableBloomFilter = None

logger = logging.getLogger(__name__)


class _SetBackedBloom:
    """Fallback bloom-like implementation used only when pybloom-live is unavailable."""

    def __init__(self) -> None:
        self._items: set[str] = set()

    def add(self, value: str) -> None:
        self._items.add(value)

    def __contains__(self, value: str) -> bool:
        return value in self._items


class PersistentBloomRegistry:
    """Keeps one persistent bloom filter per symbol in disk-backed .bloom files."""

    def __init__(
        self,
        base_dir: Path,
        *,
        capacity: int = 1_000_000,
        error_rate: float = 0.001,
        autosave_every: int = 1_000,
    ) -> None:
        self._base_dir = Path(base_dir)
        self._capacity = int(capacity)
        self._error_rate = float(error_rate)
        self._autosave_every = max(1, int(autosave_every))

        self._filters: dict[str, Any] = {}
        self._dirty_writes: dict[str, int] = {}
        self._lock = RLock()

        self._base_dir.mkdir(parents=True, exist_ok=True)

    def check_and_add(self, symbol: str, dedup_key: str) -> bool:
        """Return True when key is already present; otherwise add key and return False."""
        normalized_symbol = symbol.strip().lower()
        key = str(dedup_key)

        with self._lock:
            bloom = self._get_or_load(normalized_symbol)
            if key in bloom:
                return True

            bloom.add(key)
            writes = self._dirty_writes.get(normalized_symbol, 0) + 1
            self._dirty_writes[normalized_symbol] = writes

            if writes >= self._autosave_every:
                self._flush_symbol_locked(normalized_symbol)

            return False

    def flush_symbol(self, symbol: str) -> None:
        normalized_symbol = symbol.strip().lower()
        with self._lock:
            self._flush_symbol_locked(normalized_symbol)

    def flush_all(self) -> None:
        with self._lock:
            for symbol in list(self._filters.keys()):
                self._flush_symbol_locked(symbol)

    def _get_or_load(self, symbol: str) -> Any:
        existing = self._filters.get(symbol)
        if existing is not None:
            return existing

        path = self._bloom_path(symbol)
        if path.exists():
            try:
                with path.open("rb") as handle:
                    bloom = pickle.load(handle)
                self._filters[symbol] = bloom
                self._dirty_writes[symbol] = 0
                return bloom
            except (pickle.PickleError, OSError, EOFError) as exc:
                logger.warning("Could not load bloom filter %s: %s", path, exc)

        bloom = self._new_filter()
        self._filters[symbol] = bloom
        self._dirty_writes[symbol] = 0
        return bloom

    def _new_filter(self) -> Any:
        if ScalableBloomFilter is None:
            logger.warning("pybloom-live is unavailable. Falling back to set-backed dedup state.")
            return _SetBackedBloom()

        return ScalableBloomFilter(
            mode=ScalableBloomFilter.SMALL_SET_GROWTH,
            initial_capacity=self._capacity,
            error_rate=self._error_rate,
        )

    def _flush_symbol_locked(self, symbol: str) -> None:
        bloom = self._filters.get(symbol)
        if bloom is None:
            return

        path = self._bloom_path(symbol)
        tmp_path = path.with_suffix(path.suffix + ".tmp")

        try:
            with tmp_path.open("wb") as handle:
                pickle.dump(bloom, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
            self._dirty_writes[symbol] = 0
        except OSError as exc:
            logger.error("Failed to persist bloom filter %s: %s", path, exc)
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

    def _bloom_path(self, symbol: str) -> Path:
        return self._base_dir / f"{symbol}.bloom"
