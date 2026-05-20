# src/training/evaluator.py

import logging

import numpy as np
from sklearn.metrics import f1_score, mean_absolute_error, precision_score, recall_score

logger = logging.getLogger(__name__)

# Threshold dari proposal (konstanta — jangan diubah)
THRESHOLDS = {
    "anomaly_precision": 0.75,
    "anomaly_recall": 0.70,
    "anomaly_f1": 0.72,
    "volatility_mae": 0.15,  # 15% dalam skala relatif
}


def evaluate_anomaly(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Evaluasi model anomaly detection."""
    metrics = {
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
        "n_samples": len(y_true),
        "n_anomalies_detected": int(y_pred.sum()),
    }
    logger.info(f"Anomaly metrics: {metrics}")
    return metrics


def evaluate_volatility(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Evaluasi model volatility prediction."""
    mae = mean_absolute_error(y_true, y_pred)
    # MAE relatif terhadap mean actual (menghindari scale dependency)
    mae_pct = mae / (np.mean(np.abs(y_true)) + 1e-8)
    metrics = {
        "mae": float(mae),
        "mae_pct": float(mae_pct),
        "n_samples": len(y_true),
    }
    logger.info(f"Volatility metrics: {metrics}")
    return metrics


def check_thresholds(metrics: dict, model_type: str) -> tuple[bool, dict]:
    """
    Validasi metrik terhadap success metrics.
    Returns (passed: bool, details: dict)
    """
    results = {}
    if model_type == "anomaly":
        results["precision_ok"] = metrics.get("precision", 0) >= THRESHOLDS["anomaly_precision"]
        results["recall_ok"] = metrics.get("recall", 0) >= THRESHOLDS["anomaly_recall"]
        results["f1_ok"] = metrics.get("f1", 0) >= THRESHOLDS["anomaly_f1"]
    elif model_type == "volatility":
        results["mae_ok"] = metrics.get("mae_pct", 1.0) <= THRESHOLDS["volatility_mae"]

    passed = all(results.values())
    return passed, results
