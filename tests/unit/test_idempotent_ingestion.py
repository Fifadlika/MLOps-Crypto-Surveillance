"""Targeted tests for idempotent ingestion internals."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.ingestion.writer import AtomicJsonlWriter
from src.utils.bloom import PersistentBloomRegistry


def test_dedup_persist(tmp_path) -> None:
    bloom_dir = tmp_path / "dedup"

    first_registry = PersistentBloomRegistry(base_dir=bloom_dir, autosave_every=1)
    assert first_registry.check_and_add("BTCUSDT", "trade:100") is False
    first_registry.flush_all()

    second_registry = PersistentBloomRegistry(base_dir=bloom_dir, autosave_every=1)
    assert second_registry.check_and_add("BTCUSDT", "trade:100") is True


def test_atomic_write(tmp_path) -> None:
    writer = AtomicJsonlWriter(
        root_path=tmp_path / "raw",
        pipeline_run_id="atomic-test-run",
        lock_timeout_seconds=5.0,
    )

    base_ts = 1700000000000
    for index in range(5):
        writer.append_record(
            "BTCUSDT",
            base_ts + (index * 1000),
            {
                "trade_id": 100 + index,
                "trade_time_ms": base_ts + (index * 1000),
                "price": 100.0 + index,
            },
        )

    date_str = datetime.fromtimestamp(base_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    jsonl_path = tmp_path / "raw" / "BTCUSDT" / f"{date_str}.jsonl"
    meta_path = tmp_path / "raw" / "BTCUSDT" / f"{date_str}.meta"

    lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    assert len(lines) == 5
    assert meta["record_count"] == 5
    assert meta["last_trade_id"] == 104
    assert meta["pipeline_run_id"] == "atomic-test-run"
