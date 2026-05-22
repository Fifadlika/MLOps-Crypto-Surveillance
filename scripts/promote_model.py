# scripts/promote_model.py
# mypy: ignore-errors
"""
Mensimulasikan alur eksperimen:
  1. Ambil run_id terbaik dari experiment
  2. Register ke MLflow Model Registry
  3. None → Staging → (evaluasi threshold) → Production
"""

import argparse
import sys

import mlflow
from src.training.evaluator import check_thresholds
from src.training.registry import register_model, transition_stage


def get_best_run_id(experiment_name: str, metric: str = "f1") -> tuple[str, dict]:
    mlflow.set_tracking_uri("mlflow/")
    runs = mlflow.search_runs(
        experiment_names=[experiment_name],
        order_by=[f"metrics.{metric} DESC"],
    )
    if runs.empty:
        raise ValueError(f"Tidak ada run di experiment: {experiment_name}")
    best = runs.iloc[0]
    metrics = {
        "precision": best.get("metrics.precision", 0),
        "recall": best.get("metrics.recall", 0),
        "f1": best.get("metrics.f1", 0),
    }
    return best["run_id"], metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["anomaly", "volatility"], default="anomaly")
    args = parser.parse_args()

    artifact_paths = {"anomaly": "isolation-forest", "volatility": "xgboost-volatility"}
    experiment_names = {
        "anomaly": "crypto-anomaly-detection",
        "volatility": "crypto-volatility-prediction",
    }

    print(f"Mencari run terbaik untuk: {args.model}")
    run_id, metrics = get_best_run_id(experiment_names[args.model])
    print(f"Best run_id: {run_id} | Metrics: {metrics}")

    version = register_model(run_id, args.model, artifact_paths[args.model])
    print(f"Registered as version: {version}")

    transition_stage(args.model, version, "Staging")
    print("Stage: None → Staging")

    passed, details = check_thresholds(metrics, args.model)
    if passed:
        transition_stage(args.model, version, "Production")
        print(f"Stage: Staging → Production | Details: {details}")
    else:
        print(f"GAGAL threshold, tidak naik ke Production. Details: {details}")
        sys.exit(1)
