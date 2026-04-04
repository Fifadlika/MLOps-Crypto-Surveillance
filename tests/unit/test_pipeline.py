"""
tests/unit/test_pipeline.py

Unit tests for WebSocketPipeline (src/ingestion/pipeline.py).

Strategy
--------
- All external dependencies (config, redis, mlflow, BinanceWebSocketClient)
  are replaced with lightweight fakes / MagicMock via monkeypatch or patch.
- No network, no Redis, no MLflow server required.
- Every test is fully synchronous from pytest's perspective;
  async coroutines are driven by pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import signal
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


def _make_fake_config(overrides: dict | None = None) -> SimpleNamespace:
    """Return a typed config stub compatible with src.utils.config.Config."""
    defaults = {
        "mlflow": {"experiment_name": "test_experiment"},
        "data": {"trading_pairs": ["BTCUSDT", "ETHUSDT"]},
    }
    merged = {**defaults, **(overrides or {})}
    mlflow_cfg = merged.get("mlflow")
    data_cfg = merged.get("data")
    symbols = merged.get("symbols")

    if symbols is None:
        trading_pairs = data_cfg.get("trading_pairs", []) if data_cfg else []
        symbols = [str(symbol).upper() for symbol in trading_pairs]

    return SimpleNamespace(
        mlflow=SimpleNamespace(**mlflow_cfg) if mlflow_cfg is not None else None,
        data=SimpleNamespace(**data_cfg) if data_cfg is not None else None,
        symbols=symbols,
    )


def _make_fake_mlflow_run(run_id: str = "run-abc123") -> MagicMock:
    run = MagicMock()
    run.info.run_id = run_id
    return run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_config():
    return _make_fake_config()


@pytest.fixture()
def patched_pipeline(fake_config):
    """
    Return a WebSocketPipeline instance with all external deps patched.
    Patches stay active for the lifetime of the test via context managers
    stored on the returned namespace.
    """
    fake_run = _make_fake_mlflow_run()

    with (
        patch("src.ingestion.pipeline.get_config", return_value=fake_config),
        patch("src.ingestion.pipeline.get_redis_client", return_value=MagicMock()),
        patch("src.ingestion.pipeline.get_logger", return_value=MagicMock()),
        patch("src.ingestion.pipeline.mlflow") as mock_mlflow,
        patch("src.ingestion.pipeline.BinanceWebSocketClient") as MockClient,
    ):
        mock_mlflow.start_run.return_value = fake_run

        # Default: client.run() returns immediately (happy path)
        mock_client_instance = MagicMock()
        mock_client_instance.run = AsyncMock(return_value=None)
        mock_client_instance.stop = AsyncMock(return_value=None)
        MockClient.return_value = mock_client_instance

        from src.ingestion.pipeline import WebSocketPipeline  # import inside patch scope

        pipeline = WebSocketPipeline()

        yield pipeline, mock_mlflow, MockClient, mock_client_instance


# ---------------------------------------------------------------------------
# __init__ tests
# ---------------------------------------------------------------------------


class TestWebSocketPipelineInit:
    def test_client_is_none_before_start(self, patched_pipeline):
        pipeline, *_ = patched_pipeline
        assert pipeline._client is None

    def test_start_time_is_none_before_start(self, patched_pipeline):
        pipeline, *_ = patched_pipeline
        assert pipeline._start_time is None

    def test_mlflow_run_is_none_before_start(self, patched_pipeline):
        pipeline, *_ = patched_pipeline
        assert pipeline._mlflow_run is None

    def test_shutdown_event_not_set_before_start(self, patched_pipeline):
        pipeline, *_ = patched_pipeline
        assert not pipeline._shutdown_event.is_set()


# ---------------------------------------------------------------------------
# _begin_mlflow_run tests
# ---------------------------------------------------------------------------


class TestBeginMlflowRun:
    def test_sets_experiment_from_config(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._begin_mlflow_run()
        mock_mlflow.set_experiment.assert_called_once_with("test_experiment")

    def test_starts_run_with_correct_name(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._begin_mlflow_run()
        mock_mlflow.start_run.assert_called_once_with(run_name="websocket_pipeline")

    def test_logs_expected_params(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._begin_mlflow_run()
        logged = mock_mlflow.log_params.call_args[0][0]
        assert logged["pipeline.component"] == "websocket_ingestion"
        assert logged["pipeline.streams"] == "trade,kline_1m"
        assert "BTCUSDT" in logged["pipeline.symbols"]

    def test_symbols_joined_as_comma_string(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._begin_mlflow_run()
        logged = mock_mlflow.log_params.call_args[0][0]
        assert logged["pipeline.symbols"] == "BTCUSDT,ETHUSDT"

    def test_mlflow_run_attribute_set(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._begin_mlflow_run()
        assert pipeline._mlflow_run is not None
        assert pipeline._mlflow_run.info.run_id == "run-abc123"

    def test_uses_default_experiment_when_key_missing(self):
        """Config missing mlflow key → falls back to 'ingestion_websocket'."""
        cfg = _make_fake_config(overrides={"mlflow": None})

        fake_run = _make_fake_mlflow_run()
        with (
            patch("src.ingestion.pipeline.get_config", return_value=cfg),
            patch("src.ingestion.pipeline.get_redis_client", return_value=MagicMock()),
            patch("src.ingestion.pipeline.get_logger", return_value=MagicMock()),
            patch("src.ingestion.pipeline.mlflow") as mock_mlflow,
            patch("src.ingestion.pipeline.BinanceWebSocketClient"),
        ):
            mock_mlflow.start_run.return_value = fake_run
            from src.ingestion.pipeline import WebSocketPipeline

            pipeline = WebSocketPipeline()
            pipeline._start_time = time.monotonic()
            pipeline._begin_mlflow_run()
            mock_mlflow.set_experiment.assert_called_once_with("ingestion_websocket")


# ---------------------------------------------------------------------------
# _end_mlflow_run tests
# ---------------------------------------------------------------------------


class TestEndMlflowRun:
    def test_logs_uptime_metric(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic() - 5.0
        pipeline._mlflow_run = _make_fake_mlflow_run()

        pipeline._end_mlflow_run()

        logged = mock_mlflow.log_metrics.call_args[0][0]
        assert "pipeline.uptime_seconds" in logged
        assert logged["pipeline.uptime_seconds"] >= 5.0

    def test_logs_exit_status_param(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._mlflow_run = _make_fake_mlflow_run()

        pipeline._end_mlflow_run(status="FINISHED")

        mock_mlflow.log_param.assert_called_with("pipeline.exit_status", "FINISHED")

    def test_ends_mlflow_run_with_status(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = time.monotonic()
        pipeline._mlflow_run = _make_fake_mlflow_run()

        pipeline._end_mlflow_run(status="FAILED")

        mock_mlflow.end_run.assert_called_once_with(status="FAILED")

    def test_noop_when_mlflow_run_is_none(self, patched_pipeline):
        """Should not raise or call mlflow if _mlflow_run was never set."""
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._mlflow_run = None

        pipeline._end_mlflow_run()  # must not raise

        mock_mlflow.log_metrics.assert_not_called()
        mock_mlflow.end_run.assert_not_called()

    def test_uptime_zero_when_start_time_missing(self, patched_pipeline):
        pipeline, mock_mlflow, *_ = patched_pipeline
        pipeline._start_time = None
        pipeline._mlflow_run = _make_fake_mlflow_run()

        pipeline._end_mlflow_run()

        logged = mock_mlflow.log_metrics.call_args[0][0]
        assert logged["pipeline.uptime_seconds"] == 0.0


# ---------------------------------------------------------------------------
# start() / stop() lifecycle tests
# ---------------------------------------------------------------------------


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_sets_start_time(self, patched_pipeline):
        pipeline, *_ = patched_pipeline
        before = time.monotonic()
        await pipeline.start()
        assert pipeline._start_time is not None
        assert pipeline._start_time >= before

    @pytest.mark.asyncio
    async def test_start_instantiates_client(self, patched_pipeline):
        pipeline, _, MockClient, _ = patched_pipeline
        await pipeline.start()
        MockClient.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_calls_client_run(self, patched_pipeline):
        pipeline, _, _, mock_client = patched_pipeline
        await pipeline.start()
        mock_client.run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_teardown_called_even_on_exception(self, patched_pipeline):
        """_teardown / end_mlflow_run must be called even when client.run raises."""
        pipeline, mock_mlflow, _, mock_client = patched_pipeline
        pipeline._mlflow_run = _make_fake_mlflow_run()
        mock_client.run = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            await pipeline.start()

        mock_mlflow.end_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_logs_exit_reason(self, patched_pipeline):
        pipeline, mock_mlflow, _, mock_client = patched_pipeline
        mock_client.run = AsyncMock(side_effect=ValueError("bad data"))

        with pytest.raises(ValueError):
            await pipeline.start()

        mock_mlflow.log_param.assert_any_call("pipeline.exit_reason", "bad data")

    @pytest.mark.asyncio
    async def test_stop_calls_client_stop(self, patched_pipeline):
        pipeline, _, _, mock_client = patched_pipeline
        pipeline._client = mock_client
        await pipeline.stop()
        mock_client.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_sets_shutdown_event(self, patched_pipeline):
        pipeline, _, _, mock_client = patched_pipeline
        pipeline._client = mock_client
        await pipeline.stop()
        assert pipeline._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_stop_is_idempotent_when_client_is_none(self, patched_pipeline):
        """stop() before start() must not raise."""
        pipeline, *_ = patched_pipeline
        assert pipeline._client is None
        await pipeline.stop()  # should not raise
        assert pipeline._shutdown_event.is_set()


# ---------------------------------------------------------------------------
# Signal handler tests
# ---------------------------------------------------------------------------


class TestSignalHandlers:
    @pytest.mark.asyncio
    async def test_signal_handler_calls_stop(self, patched_pipeline):
        """
        Simulate the loop calling the registered signal handler,
        then verify stop() is eventually called.
        """
        pipeline, _, _, mock_client = patched_pipeline

        captured_handlers: dict[int, Any] = {}

        loop = asyncio.get_running_loop()

        def fake_add_signal_handler(sig: int, callback: Any, *args: Any) -> None:
            captured_handlers[sig] = (callback, args)

        with patch.object(loop, "add_signal_handler", side_effect=fake_add_signal_handler):
            pipeline._register_signal_handlers()

        assert signal.SIGINT in captured_handlers
        assert signal.SIGTERM in captured_handlers

        # Trigger SIGINT handler — it creates a task calling stop()
        pipeline._client = mock_client
        cb, args = captured_handlers[signal.SIGINT]
        cb(*args)

        # Drain the event loop so the created task runs
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        mock_client.stop.assert_awaited()

    def test_register_signal_handlers_survives_not_implemented(self, patched_pipeline):
        """Should not raise on platforms where add_signal_handler is unsupported."""
        pipeline, *_ = patched_pipeline

        with patch("asyncio.get_running_loop") as mock_loop_fn:
            mock_loop = MagicMock()
            mock_loop.add_signal_handler.side_effect = NotImplementedError
            mock_loop_fn.return_value = mock_loop

            pipeline._register_signal_handlers()  # must not raise


# ---------------------------------------------------------------------------
# run() entry point
# ---------------------------------------------------------------------------


class TestRunEntryPoint:
    def test_run_creates_pipeline_and_calls_asyncio_run(self):
        def _close_and_return(coro):
            coro.close()
            return None

        with (
            patch("src.ingestion.pipeline.get_config", return_value=_make_fake_config()),
            patch("src.ingestion.pipeline.get_redis_client", return_value=MagicMock()),
            patch("src.ingestion.pipeline.get_logger", return_value=MagicMock()),
            patch("src.ingestion.pipeline.mlflow") as mock_mlflow,
            patch("src.ingestion.pipeline.BinanceWebSocketClient") as MockClient,
            patch(
                "src.ingestion.pipeline.asyncio.run", side_effect=_close_and_return
            ) as mock_asyncio_run,
        ):
            fake_run = _make_fake_mlflow_run()
            mock_mlflow.start_run.return_value = fake_run

            mock_client = MagicMock()
            mock_client.run = AsyncMock()
            mock_client.stop = AsyncMock()
            MockClient.return_value = mock_client

            from src.ingestion.pipeline import run

            run()

            mock_asyncio_run.assert_called_once()

    def test_run_handles_keyboard_interrupt_gracefully(self):
        def _close_and_raise_keyboard_interrupt(coro):
            coro.close()
            raise KeyboardInterrupt

        with (
            patch("src.ingestion.pipeline.get_config", return_value=_make_fake_config()),
            patch("src.ingestion.pipeline.get_redis_client", return_value=MagicMock()),
            patch("src.ingestion.pipeline.get_logger", return_value=MagicMock()),
            patch("src.ingestion.pipeline.mlflow"),
            patch("src.ingestion.pipeline.BinanceWebSocketClient"),
            patch(
                "src.ingestion.pipeline.asyncio.run",
                side_effect=_close_and_raise_keyboard_interrupt,
            ),
        ):
            from src.ingestion.pipeline import run

            run()  # must NOT propagate KeyboardInterrupt
