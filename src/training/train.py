# ruff: noqa
import argparse
import logging

import mlflow.sklearn
import mlflow.xgboost
import numpy as np

import mlflow
from src.training.evaluator import check_thresholds, evaluate_anomaly, evaluate_volatility
from src.training.trainer import (
    BaseTrainer,
    IsolationForestTrainer,
    XGBoostVolatilityTrainer,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXPERIMENT_NAMES = {
    "anomaly": "crypto-anomaly-detection",
    "volatility": "crypto-volatility-prediction",
}


def load_features(symbol: str, model_type: str):
    """
    Load dari PostgreSQL features_trade / features_kline.
    Untuk sekarang: generate sintetis agar bisa dijalankan tanpa DB.
    TODO: ganti dengan query nyata di LK-08+.
    """
    np.random.seed(42)
    if model_type == "anomaly":
        X = np.random.randn(500, 49)
        # Simulasi label: 5% anomaly
        y = np.random.choice([0, 1], size=500, p=[0.95, 0.05])
        return X, y
    else:
        X = np.random.randn(500, 49)
        y = np.abs(np.random.randn(500)) * 0.02  # volatility 0-6%
        return X, y


# load_dotenv()

# db_user = os.environ.get("DB__USER")
# db_pass = os.environ.get("DB__PASSWORD")
# db_host = os.environ.get("DB__HOST")
# db_port = os.environ.get("DB__PORT")
# db_name = os.environ.get("DB__NAME")

# tracking_uri = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


def run_experiment(symbol: str, model_type: str, params: dict):
    # mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_tracking_uri("mlflow_data/")
    mlflow.set_experiment(EXPERIMENT_NAMES[model_type])

    with mlflow.start_run(run_name=f"{symbol}_{model_type}"):
        mlflow.log_param("symbol", symbol)
        mlflow.log_param("model_type", model_type)

        X, y = load_features(symbol, model_type)

        trainer: BaseTrainer

        if model_type == "anomaly":
            trainer = IsolationForestTrainer(params=params)
            trainer.fit(X)
            y_pred = trainer.predict(X)
            metrics = evaluate_anomaly(y, y_pred)
            mlflow.log_params(trainer.get_params())
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(trainer.model, name="isolation-forest")
            passed, details = check_thresholds(metrics, "anomaly")

        else:
            trainer = XGBoostVolatilityTrainer(params=params)
            trainer.fit(X, y)
            y_pred = trainer.predict(X)
            metrics = evaluate_volatility(y, y_pred)
            mlflow.log_params(trainer.get_params())
            mlflow.log_metrics(metrics)
            mlflow.xgboost.log_model(trainer.model, name="xgboost-volatility")
            passed, details = check_thresholds(metrics, "volatility")

        mlflow.log_param("threshold_passed", passed)
        logger.info(f"Run selesai. Threshold passed: {passed} | Details: {details}")
        return mlflow.active_run().info.run_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--model", choices=["anomaly", "volatility"], default="anomaly")
    parser.add_argument("--contamination", type=float, default=0.05)
    parser.add_argument("--n_estimators", type=int, default=100)
    parser.add_argument("--max_depth", type=int, default=6)
    args = parser.parse_args()

    params = {}
    if args.model == "anomaly":
        params = {"contamination": args.contamination, "n_estimators": args.n_estimators}
    else:
        params = {"max_depth": args.max_depth}

    run_id = run_experiment(args.symbol, args.model, params)
    print(f"run_id: {run_id}")
