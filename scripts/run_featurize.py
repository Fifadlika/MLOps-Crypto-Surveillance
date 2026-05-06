"""
scripts/run_featurize.py

One-shot feature engineering untuk DVC stage 'featurize'.

Cara kerja:
1. Inisialisasi FeaturePipeline
2. Panggil run_finite() — baca semua pending messages di Redis Stream lalu exit
3. Feature vector ditulis ke Redis Hash (online) + PostgreSQL + Parquet (offline)
4. Script exit dengan code 0 → DVC menganggap stage selesai

Prasyarat:
- Redis harus running dan stream:trades/klines:{symbol} sudah terisi
  (oleh pull_historical.py atau pipeline.py yang sudah jalan sebelumnya)
- PostgreSQL harus accessible (untuk write_offline)

Usage:
    python scripts/run_featurize.py
    python scripts/run_featurize.py --idle-rounds 5
    python scripts/run_featurize.py --symbols BTCUSDT
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pyarrow.parquet as pq

from src.features.engineering import FeaturePipeline, KlineFeatureCalculator, TradeFeatureCalculator

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run feature engineering in finite mode (DVC-compatible)"
    )
    parser.add_argument(
        "--idle-rounds",
        type=int,
        default=3,
        help=(
            "Berapa round berturut-turut tanpa message baru sebelum dianggap selesai "
            "(default: 3, artinya tunggu 3×2s = 6s tanpa data baru)"
        ),
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="Override simbol. Jika tidak diisi, baca dari config.yaml",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info("run_featurize.py starting (finite mode)")

    try:
        pipeline = FeaturePipeline()

        # Override symbols jika diberikan via CLI
        if args.symbols:
            pipeline.symbols = [s.strip().upper() for s in args.symbols]
            # Re-init calculators untuk symbols yang baru
            pipeline.trade_calcs = {s: TradeFeatureCalculator(s) for s in pipeline.symbols}
            pipeline.kline_calcs = {s: KlineFeatureCalculator(s) for s in pipeline.symbols}
            pipeline.buffer = {s: [] for s in pipeline.symbols}
            pipeline._init_streams()

        pipeline.run_finite(idle_rounds=args.idle_rounds)

    except KeyboardInterrupt:
        logger.info("Featurize interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        logger.exception("Featurize failed: %s", exc)
        sys.exit(1)

    features_path = Path("data/features")
    parquet_files = list(features_path.rglob("*.parquet"))

    if not parquet_files:
        logger.warning(
            "No parquet files found in data/features/. "
            "Pastikan Redis stream terisi: python scripts/pull_historical.py --days 1"
        )
        sys.exit(1)

    total_records = 0
    for f in parquet_files:
        try:
            meta = pq.read_metadata(f)
            total_records += meta.num_rows
        except Exception as e:
            logger.error("Failed to read parquet metadata for %s: %s", f, e)

    print("\n── Featurize Summary ─────────────────")
    print(f"  Parquet files : {len(parquet_files)}")
    print(f"  Total records : {total_records:,}")
    print("  Location      : data/features/{symbol}/{date}_fv1.0.parquet")
    print("──────────────────────────────────────\n")

    logger.info(
        "Featurize complete: %d files, %d total records",
        len(parquet_files),
        total_records,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
