"""
scripts/pull_historical.py

One-shot historical kline ingestion untuk DVC stage 'ingest'.

Cara kerja:
1. Hitung rentang waktu: sekarang mundur N hari
2. Panggil BinanceRESTClient.sync_klines() per symbol
3. Data ditulis ke data/raw/{symbol}/{YYYY-MM-DD}.jsonl oleh AtomicJsonlWriter
4. Script exit dengan code 0 → DVC menganggap stage selesai

Berbeda dengan pipeline.py (WebSocket, long-running), script ini:
- Menggunakan REST API saja (bukan WebSocket)
- Punya titik akhir yang jelas (terminate setelah semua symbol selesai)
- Aman dijalankan berulang (idempotent via dedup Redis + bloom filter)

Usage:
    python scripts/pull_historical.py --days 1
    python scripts/pull_historical.py --days 7 --interval 1m
    python scripts/pull_historical.py --symbols BTCUSDT --days 3
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime, timezone

from src.ingestion.rest_client import BinanceRESTClient
from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull historical klines from Binance REST API (one-shot, DVC-compatible)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Berapa hari ke belakang yang akan ditarik (default: 1)",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="Kline interval: 1m, 5m, 15m, 1h, dll (default: 1m)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=None,
        help="Override simbol dari config. Contoh: --symbols BTCUSDT ETHUSDT",
    )
    return parser.parse_args()


async def pull_all(
    symbols: list[str],
    days: int,
    interval: str,
) -> dict[str, int]:
    """
    Tarik klines untuk semua symbol secara sequential.

    Kenapa sequential dan bukan asyncio.gather() paralel?
    Karena kita menggunakan satu TokenBucket untuk rate limiting.
    Paralel dengan rate limit bersama bisa menyebabkan burst yang
    melanggar limit 400 req/menit Binance.
    """
    # Hitung window waktu
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (days * 24 * 60 * 60 * 1000)

    logger.info(
        "Starting historical pull: %d symbol(s), %d day(s) back, interval=%s",
        len(symbols),
        days,
        interval,
    )
    logger.info(
        "Time window: %s → %s",
        _ms_to_iso(start_ms),
        _ms_to_iso(end_ms),
    )

    client = BinanceRESTClient()
    results: dict[str, int] = {}

    try:
        for symbol in symbols:
            logger.info("Pulling %s ...", symbol)
            count = await client.sync_klines(
                symbol=symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                interval=interval,
            )
            results[symbol] = count
            logger.info("  ✓ %s: %d klines written", symbol, count)
    finally:
        # Selalu close HTTP client meskipun ada exception
        await client.close()

    return results


def _ms_to_iso(ms: int) -> str:
    """Convert Unix milliseconds ke string ISO 8601 untuk logging."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def main() -> None:
    args = parse_args()

    # Ambil simbol dari argumen CLI atau fallback ke config.yaml
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols]
    else:
        config = get_config()
        data_cfg = getattr(config, "data", None)
        symbols = [
            str(s).upper()
            for s in getattr(data_cfg, "trading_pairs", ["BTCUSDT", "ETHUSDT", "BNBUSDT"])
        ]

    logger.info("Symbols to pull: %s", symbols)

    try:
        results = asyncio.run(
            pull_all(
                symbols=symbols,
                days=args.days,
                interval=args.interval,
            )
        )
    except KeyboardInterrupt:
        logger.info("Pull interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        logger.exception("Historical pull failed: %s", exc)
        sys.exit(1)

    # Summary
    total = sum(results.values())
    print("\n── Pull Summary ──────────────────────")
    for sym, count in results.items():
        print(f"  {sym}: {count:,} klines")
    print(f"  Total : {total:,} klines")
    print("─────────────────────────────────────\n")

    if total == 0:
        logger.warning("No klines were written. Check Redis connection and Binance API access.")
        sys.exit(1)  # Exit code 1 → DVC akan menganggap stage gagal

    sys.exit(0)  # Exit code 0 → DVC menganggap stage berhasil


if __name__ == "__main__":
    main()
