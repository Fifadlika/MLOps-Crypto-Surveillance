# tests/unit/test_trainer.py

import numpy as np
import pytest

from src.training.trainer import IsolationForestTrainer, XGBoostVolatilityTrainer


@pytest.fixture
def synthetic_features():
    """52 fitur sesuai feature vector spec."""
    np.random.seed(42)
    return np.random.randn(200, 49)  # 49 fitur (tanpa metadata)


def test_if_trainer_fit_predict(synthetic_features):
    trainer = IsolationForestTrainer()
    trainer.fit(synthetic_features)
    assert trainer.is_fitted
    preds = trainer.predict(synthetic_features)
    assert preds.shape == (200,)
    assert set(preds).issubset({0, 1})


def test_if_trainer_custom_params(synthetic_features):
    trainer = IsolationForestTrainer(params={"contamination": 0.08, "n_estimators": 50})
    trainer.fit(synthetic_features)
    assert trainer.get_params()["contamination"] == 0.08


def test_xgb_trainer_fit_predict(synthetic_features):
    y = np.random.rand(200)  # volatility target
    trainer = XGBoostVolatilityTrainer()
    trainer.fit(synthetic_features, y)
    assert trainer.is_fitted
    preds = trainer.predict(synthetic_features)
    assert preds.shape == (200,)
