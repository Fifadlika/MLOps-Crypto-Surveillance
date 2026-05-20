# mypy: ignore-errors
# src/training/trainer.py

import logging
from typing import Optional

import numpy as np
import xgboost as xgb
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class IsolationForestTrainer:
    """Trainer untuk anomaly detection menggunakan Isolation Forest."""

    DEFAULT_PARAMS = {
        "n_estimators": 100,
        "contamination": 0.05,
        "max_samples": "auto",
        "random_state": 42,
    }

    def __init__(self, params: Optional[dict] = None):
        self.params = {**self.DEFAULT_PARAMS, **(params or {})}
        self.model = IsolationForest(**self.params)
        self.scaler = StandardScaler()
        self.is_fitted = False

    def fit(self, X: np.ndarray) -> "IsolationForestTrainer":
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.is_fitted = True
        logger.info("IsolationForest fitted on %d samples", X.shape[0])
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Returns 1 (normal) atau -1 (anomaly) — dikonversi ke 0/1."""
        X_scaled = self.scaler.transform(X)
        raw = self.model.predict(X_scaled)
        return np.where(raw == -1, 1, 0)  # 1 = anomaly, 0 = normal

    def predict_score(self, X: np.ndarray) -> np.ndarray:
        """Anomaly score (lebih negatif = lebih anomalous)."""
        if not self.is_fitted:
            raise RuntimeError(f"{self.__class__.__name__} must be fitted before calling predict()")
        X_scaled = self.scaler.transform(X)
        return self.model.score_samples(X_scaled)

    def get_params(self) -> dict:
        return {
            "model_params": self.params.copy(),
            "is_fitted": self.is_fitted,
        }


class XGBoostVolatilityTrainer:
    """Trainer untuk volatility prediction (regression)."""

    DEFAULT_PARAMS = {
        "n_estimators": 300,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "tree_method": "hist",
    }

    VALID_WINDOWS = {"1h", "4h", "24h"}

    def __init__(self, params: Optional[dict] = None, target_window: str = "1h"):
        self.params = {**self.DEFAULT_PARAMS, **(params or {})}
        if target_window not in self.VALID_WINDOWS:
            raise ValueError(
                f"target_window must be one of {self.VALID_WINDOWS}, got '{target_window}'"
            )
        self.target_window = target_window  # "1h", "4h", "24h"
        self.model = xgb.XGBRegressor(**self.params)
        self.is_fitted = False

    def fit(self, X: np.ndarray, y: np.ndarray, eval_set=None) -> "XGBoostVolatilityTrainer":
        _eval_set = eval_set if eval_set is not None else [(X, y)]
        self.model.fit(X, y, eval_set=_eval_set, verbose=False)
        self.is_fitted = True
        logger.info("XGBoost fitted: %d samples, window=%s", X.shape[0], self.target_window)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        if not self.is_fitted:
            raise RuntimeError(f"{self.__class__.__name__} must be fitted before calling predict()")
        return self.model.predict(X)

    def get_params(self) -> dict:
        return {
            "model_params": self.params.copy(),
            "is_fitted": self.is_fitted,
        }
