"""CLI entrypoint to run live ingestion pipeline."""

from __future__ import annotations

import argparse
import asyncio
from typing import Optional

from src.ingestion.pipeline import WebSocketPipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)


async def run_ingestion(duration_seconds: Optional[int] = None) -> None:
    """Run ingestion indefinitely or for a bounded duration."""
    pipeline = WebSocketPipeline()

    if not duration_seconds or duration_seconds <= 0:
        await pipeline.start()
        return

    run_task = asyncio.create_task(pipeline.start(), name="ingestion-pipeline")
    try:
        await asyncio.sleep(duration_seconds)
        logger.info(
            "Bounded ingestion duration reached (%ss). Stopping pipeline.", duration_seconds
        )
        await pipeline.stop()
        await run_task
    except asyncio.CancelledError:
        await pipeline.stop()
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Binance live ingestion pipeline.")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Optional bounded run duration. Use 0 to run continuously.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run_ingestion(duration_seconds=args.duration_seconds))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Ingestion CLI exiting.")


if __name__ == "__main__":
    main()
