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
from src.training.registry import _get_tracking_uri, register_model, transition_stage

# Metric terbaik per model type — anomaly: f1 DESC, volatility: mae_pct ASC
BEST_METRIC = {
    "anomaly": ("f1", "DESC"),
    "volatility": ("mae_pct", "ASC"),
}

ARTIFACT_PATHS = {
    "anomaly": "isolation-forest",
    "volatility": "xgboost-volatility",
}

EXPERIMENT_NAMES = {
    "anomaly": "crypto-anomaly-detection",
    "volatility": "crypto-volatility-prediction",
}


def get_best_run_id(experiment_name: str, model_type: str) -> tuple[str, dict]:
    mlflow.set_tracking_uri(_get_tracking_uri())  # ← pakai fungsi dari registry.py

    metric, order = BEST_METRIC[model_type]
    runs = mlflow.search_runs(
        experiment_names=[experiment_name],
        order_by=[f"metrics.{metric} {order}"],
    )
    if runs.empty:
        raise ValueError(f"Tidak ada run di experiment: {experiment_name}")

    # Filter run yang metric-nya valid (tidak 0 atau NaN)
    col = f"metrics.{metric}"
    valid_runs = runs[runs[col].notna() & (runs[col] > 0)]
    if valid_runs.empty:
        raise ValueError(
            f"Semua run di '{experiment_name}' punya {metric}=0 atau NaN. "
            "Jalankan ulang train.py terlebih dahulu."
        )

    best = valid_runs.iloc[0]
    metrics = {
        "precision": best.get("metrics.precision", 0),
        "recall": best.get("metrics.recall", 0),
        "f1": best.get("metrics.f1", 0),
        "mae_pct": best.get("metrics.mae_pct", 1.0),
    }
    return best["run_id"], metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["anomaly", "volatility"], default="anomaly")
    parser.add_argument(
        "--force-promote",
        action="store_true",
        help="Skip threshold check, promote langsung ke Production (demo LK-07)",
    )
    args = parser.parse_args()

    print(f"Mencari run terbaik untuk: {args.model}")
    run_id, metrics = get_best_run_id(EXPERIMENT_NAMES[args.model], args.model)
    print(f"Best run_id : {run_id}")
    print(f"Metrics     : {metrics}")

    version = register_model(run_id, args.model, ARTIFACT_PATHS[args.model])
    print(f"Registered as version: {version}")

    transition_stage(args.model, version, "Staging")
    print("Stage: None → Staging")

    passed, details = check_thresholds(metrics, args.model)
    if passed or args.force_promote:
        transition_stage(args.model, version, "Production")
        label = "passed" if passed else "force-promote"
        print(f"Stage: Staging → Production ({label}) | Details: {details}")
    else:
        print(f"GAGAL threshold | Details: {details}")
        print("Tip: gunakan --force-promote untuk demo LK-07")
        sys.exit(1)
