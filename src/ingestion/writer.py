"""Atomic append-only JSONL writer with sidecar metadata tracking."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import portalocker


class AtomicJsonlWriter:
    """Writes JSONL records safely with file locking and atomic sidecar metadata updates."""

    def __init__(
        self,
        root_path: Path,
        *,
        pipeline_run_id: str,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        self._root_path = Path(root_path)
        self._pipeline_run_id = pipeline_run_id
        self._lock_timeout_seconds = float(lock_timeout_seconds)
        self._root_path.mkdir(parents=True, exist_ok=True)

    def append_record(self, symbol: str, timestamp_ms: int, record: dict[str, Any]) -> Path:
        """Append one JSON record and update the paired .meta sidecar atomically."""
        utc_date = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        symbol_dir = self._root_path / symbol.upper()
        symbol_dir.mkdir(parents=True, exist_ok=True)

        jsonl_path = symbol_dir / f"{utc_date}.jsonl"
        meta_path = symbol_dir / f"{utc_date}.meta"
        lock_path = symbol_dir / f"{utc_date}.lock"

        payload = json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n"

        with portalocker.Lock(str(lock_path), mode="a", timeout=self._lock_timeout_seconds):
            with jsonl_path.open("a", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())

            meta = self._read_meta(meta_path)
            meta["record_count"] = int(meta.get("record_count", 0)) + 1

            trade_id = record.get("trade_id")
            if trade_id is not None:
                meta["last_trade_id"] = int(trade_id)
            elif "last_trade_id" not in meta:
                meta["last_trade_id"] = None

            last_ts = (
                record.get("trade_time_ms")
                or record.get("open_time")
                or record.get("event_time_ms")
                or timestamp_ms
            )
            meta["last_ts"] = int(last_ts)
            meta["pipeline_run_id"] = self._pipeline_run_id
            meta["updated_at"] = datetime.now(tz=timezone.utc).isoformat()

            self._write_meta_atomic(meta_path, meta)

        return jsonl_path

    @staticmethod
    def _read_meta(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            return cast(dict[str, Any], json.load(handle))

    @staticmethod
    def _write_meta_atomic(path: Path, meta: dict[str, Any]) -> None:
        tmp_path = path.with_suffix(path.suffix + ".tmp")

        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(meta, handle, ensure_ascii=True, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, path)
