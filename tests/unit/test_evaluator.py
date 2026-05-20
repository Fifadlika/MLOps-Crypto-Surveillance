# tests/unit/test_evaluator.py

import numpy as np

from src.training.evaluator import check_thresholds, evaluate_anomaly


def test_evaluate_anomaly_perfect():
    y = np.array([1, 0, 1, 0, 1])
    metrics = evaluate_anomaly(y, y)
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0


def test_check_thresholds_pass():
    metrics = {"precision": 0.80, "recall": 0.75, "f1": 0.77}
    passed, details = check_thresholds(metrics, "anomaly")
    assert passed is True


def test_check_thresholds_fail():
    metrics = {"precision": 0.60, "recall": 0.70, "f1": 0.64}
    passed, details = check_thresholds(metrics, "anomaly")
    assert passed is False
    assert details["precision_ok"] is False
