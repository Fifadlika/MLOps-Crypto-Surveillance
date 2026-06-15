# src/training/registry.py
# mypy: ignore-errors
import logging
import os

import mlflow
from mlflow import MlflowClient

from src.training.utils import get_tracking_uri

logger = logging.getLogger(__name__)

MODEL_NAMES = {
    "anomaly": "anomaly-detector-if",
    "volatility": "volatility-predictor-xgb",
}

STAGE_ORDER = ["None", "Staging", "Production"]


def register_model(run_id: str, model_type: str, artifact_path: str) -> str:
    """
    Daftarkan model dari run_id ke MLflow Model Registry.
    Returns: version string
    """
    uri = get_tracking_uri()
    mlflow.set_tracking_uri(uri)
    model_uri = f"runs:/{run_id}/{artifact_path}"
    model_name = MODEL_NAMES[model_type]
    result = mlflow.register_model(model_uri, model_name)
    logger.info(f"Registered: {model_name} v{result.version}")
    return result.version


def transition_stage(model_type: str, version: str, target_stage: str):
    """Pindahkan model ke stage tertentu."""
    uri = get_tracking_uri()
    client = MlflowClient(tracking_uri=uri)
    model_name = MODEL_NAMES[model_type]
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=target_stage,
        archive_existing_versions=(target_stage == "Production"),
    )
    logger.info(f"{model_name} v{version} → {target_stage}")


def load_production_model(model_type: str):
    """Load model dengan stage Production untuk inference."""
    uri = get_tracking_uri()
    mlflow.set_tracking_uri(uri)
    model_name = MODEL_NAMES[model_type]
    model_uri = f"models:/{model_name}/Production"
    model = mlflow.pyfunc.load_model(model_uri)
    logger.info(f"Loaded Production model: {model_name}")
    return model


def get_latest_version(model_type: str, stage: str = "Production") -> str | None:
    uri = get_tracking_uri()
    client = MlflowClient(tracking_uri=uri)
    model_name = MODEL_NAMES[model_type]
    versions = client.get_latest_versions(model_name, stages=[stage])
    return versions[0].version if versions else None
