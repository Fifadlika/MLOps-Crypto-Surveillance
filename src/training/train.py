# ruff: noqa
import argparse
from dotenv import load_dotenv
import logging
import os

import mlflow.sklearn
import mlflow.xgboost
import numpy as np
import pandas as pd
import sqlalchemy
from scipy import stats
from sklearn.model_selection import train_test_split

import mlflow
from src.training.evaluator import check_thresholds, evaluate_anomaly, evaluate_volatility
from src.training.trainer import (
    BaseTrainer,
    IsolationForestTrainer,
    XGBoostVolatilityTrainer,
)
from src.training.utils import get_tracking_uri

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXPERIMENT_NAMES = {
    "anomaly": "crypto-anomaly-detection",
    "volatility": "crypto-volatility-prediction",
}

TRADE_FEATURE_COLS = [
    "price_mean_50",
    "price_mean_200",
    "price_mean_1000",
    "price_std_50",
    "price_std_200",
    "price_std_1000",
    "price_change_50",
    "price_change_200",
    "price_change_1000",
    "vol_mean_50",
    "vol_mean_200",
    "vol_mean_1000",
    "vol_std_50",
    "vol_std_200",
    "vol_std_1000",
    "vol_total_50",
    "vol_total_200",
    "vol_total_1000",
    "buy_ratio_50",
    "buy_ratio_200",
    "buy_ratio_1000",
    "trade_rate_50",
    "trade_rate_200",
    "trade_rate_1000",
]

KLINE_FEATURE_COLS = [
    "atr_15",
    "atr_60",
    "hl_ratio_5",
    "hl_ratio_15",
    "hl_ratio_60",
    "sma_5",
    "sma_15",
    "sma_60",
    "ema_5",
    "ema_15",
    "ema_60",
    "momentum_5",
    "momentum_15",
    "momentum_60",
    "vwap_5",
    "vwap_15",
    "vwap_60",
    "vol_ratio_5",
    "vol_ratio_15",
    "vol_ratio_60",
]


def _get_engine():
    db_uri = (
        f"postgresql+psycopg2://"
        f"{os.environ['POSTGRES__USER']}:{os.environ['POSTGRES__PASSWORD']}"
        f"@{os.environ['POSTGRES__HOST']}:{os.environ['POSTGRES__PORT']}"
        f"/{os.environ['POSTGRES__DB']}"
    )

    return sqlalchemy.create_engine(db_uri)


def load_features(symbol: str, model_type: str):
    """
    Load fitur dari PostgreSQL.
    - anomaly   : features_trade — jika kosong, fallback ke sintetis terstruktur
    - volatility: features_kline — query nyata, target = atr_5 (proxy volatility 1h)
    """
    engine = _get_engine()

    if model_type == "anomaly":
        query = f"""
            SELECT {', '.join(TRADE_FEATURE_COLS)}
            FROM features_trade
            WHERE symbol = '{symbol}'
            ORDER BY ts DESC
            LIMIT 50000
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.warning(
                f"features_trade kosong untuk {symbol} — "
                "menggunakan data sintetis terstruktur. "
                "Jalankan WebSocket ingestion untuk data nyata."
            )
            # Sintetis terstruktur: distribusi menyerupai trading normal
            np.random.seed(42)
            n_normal, n_anomaly = 950, 50
            X_normal = np.random.randn(n_normal, len(TRADE_FEATURE_COLS)) * 0.5
            X_anomaly = np.random.randn(n_anomaly, len(TRADE_FEATURE_COLS)) * 4 + 6
            X = np.vstack([X_normal, X_anomaly])
            y = np.array([0] * n_normal + [1] * n_anomaly)
            return X, y

        X = df[TRADE_FEATURE_COLS].fillna(0).values
        # Isolation Forest unsupervised — label dibuat dari IQR outlier sebagai proxy

        z_scores = np.abs(stats.zscore(X, nan_policy="omit"))
        y = (z_scores.max(axis=1) > 3).astype(int)
        logger.info(f"Loaded {len(X)} trade samples | anomaly rate: {y.mean():.2%}")
        return X, y

    else:  # volatility
        query = f"""
            SELECT {', '.join(KLINE_FEATURE_COLS)}, atr_5 as target
            FROM features_kline
            WHERE symbol = '{symbol}'
              AND atr_5 IS NOT NULL
            ORDER BY ts DESC
            LIMIT 10000
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            raise ValueError(
                f"features_kline kosong untuk {symbol}. "
                "Jalankan pull_historical.py terlebih dahulu."
            )

        X = df[KLINE_FEATURE_COLS].fillna(0).values
        y = df["target"].fillna(0).values  # ATR-5 sebagai proxy volatility 1h
        logger.info(f"Loaded {len(X)} kline samples | ATR-5 mean: {y.mean():.6f}")
        return X, y

        """
        Developer Note:
        Tujuan utama dari iterasi ini adalah mencapai MLOps Level 1 (Pipeline Automation).
        Fokus keberhasilan (Metric of Success) diukur dari keandalan pengiriman data (data ingestion latency), stabilitas skema database, 
        dan otomatisasi eksekusi pipeline, bukan pada metrik evaluasi model seperti Precision/Recall. 
        Model heuristik (Z-Score & ATR) digunakan sebagai Baseline & Mock Target untuk menguji end-to-end integration sebelum model ML yang lebih kompleks .

        Penggunaan Z-Score > 3 sebagai proxy label unsupervised dipilih secara sengaja untuk meminimalkan overhead komputasi pada feature store dan database selama fase pengujian pipeline.
        Karena fokus saat ini adalah menguji kemampuan pipeline dalam menangani skema data, mencatat riwayat prediksi ke MLflow Model Registry, dan memicu alert system,
        penggunaan proxy statistik ini menjamin bahwa komponen hilir (downstream components) menerima format data yang valid tanpa dibebani oleh waktu training model yang lama

        Indikator atr_5 dipilih sebagai target volatilitas karena kalkulasinya yang deterministik.
        Dalam fase pengujian keandalan pipeline, target yang deterministik sangat penting untuk mendeteksi data drift atau sistem eror secara isolatif.
        Jika terjadi kegagalan prediksi, kita bisa langsung mengidentifikasi bahwa kesalahan 100% berada pada kendala infrastruktur data (seperti missing value atau keterlambatan data WebSocket),
        bukan karena ketidakstabilan konvergensi model ML."
        """


def run_experiment(symbol: str, model_type: str, params: dict):
    mlflow.set_tracking_uri(get_tracking_uri())
    mlflow.set_experiment(EXPERIMENT_NAMES[model_type])

    with mlflow.start_run(run_name=f"{symbol}_{model_type}"):
        mlflow.log_param("symbol", symbol)
        mlflow.log_param("model_type", model_type)

        X, y = load_features(symbol, model_type)

        trainer: BaseTrainer

        if model_type == "volatility":
            X, y = load_features(symbol, model_type)
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            trainer = XGBoostVolatilityTrainer(params=params)
            trainer.fit(X_train, y_train)
            y_pred = trainer.predict(X_test)
            metrics = evaluate_volatility(y_test, y_pred)
            mlflow.log_params(trainer.get_params())
            mlflow.log_param("test_size", 0.2)
            mlflow.log_metrics(metrics)
            mlflow.xgboost.log_model(trainer.model, name="xgboost-volatility")
            passed, details = check_thresholds(metrics, "volatility")

        else:
            trainer = IsolationForestTrainer(params=params)
            trainer.fit(X)
            y_pred = trainer.predict(X)
            metrics = evaluate_anomaly(y, y_pred)
            mlflow.log_params(trainer.get_params())
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(trainer.model, name="isolation-forest")
            passed, details = check_thresholds(metrics, "anomaly")

        mlflow.log_param("threshold_passed", passed)
        logger.info(f"Run selesai. Threshold passed: {passed} | Details: {details}")
        return mlflow.active_run().info.run_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--model", choices=["anomaly", "volatility"], default="anomaly")
    parser.add_argument("--contamination", type=float, default=0.05)
    parser.add_argument("--learning_rate", type=float, default=0.1)
    parser.add_argument("--n_estimators", type=int, default=100)
    parser.add_argument("--max_depth", type=int, default=6)
    args = parser.parse_args()

    params = {}
    if args.model == "anomaly":
        params = {"contamination": args.contamination, "n_estimators": args.n_estimators}
    else:
        params = {
            "max_depth": args.max_depth,
            "learning_rate": args.learning_rate,
            "n_estimators": args.n_estimators,
        }

    run_id = run_experiment(args.symbol, args.model, params)
    print(f"run_id: {run_id}")
