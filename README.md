# 🔍 MLOps-Crypto-Surveillance

> Real-time Cryptocurrency Anomaly Detection & Volatility Prediction System based on Continual Learning

![Python](https://img.shields.io/badge/Python-3.11-blue)
![MLflow](https://img.shields.io/badge/MLflow-2.14-orange)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 📌 About

This system is designed for **real-time market surveillance** of cryptocurrency markets with two core capabilities:

1. **Anomaly Detection** — Identifies suspicious trading patterns such as wash trading, pump-and-dump schemes, and market manipulation using Isolation Forest
2. **Volatility Prediction** — Forecasts price volatility across multiple time windows (1h, 4h, 24h) using XGBoost

**Monitored Trading Pairs:** BTCUSDT, ETHUSDT, BNBUSDT

**Data Source:** Binance WebSocket API (real-time) + Binance REST API (`api4.binance.com`)

---

## 🏗️ Directory Structure
```
MLOps-Crypto-Surveillance/
├── .devcontainer/          # GitHub Codespaces configuration
│   └── devcontainer.json
├── config/                 # Project configuration
│   └── config.yaml         # Model, data, and system parameters
├── data/
│   ├── raw/                # Raw data from Binance API
│   ├── processed/          # Preprocessed data
│   ├── features/           # Engineered features
│   └── external/           # External reference data
├── models/
│   ├── artifacts/          # Saved model artifacts
│   └── experiments/        # MLflow experiment results
├── notebooks/              # Jupyter notebooks for EDA & experiments
├── src/
│   ├── ingestion/          # WebSocket & REST API data ingestion
│   ├── features/           # Feature engineering pipeline
│   ├── training/           # Model training & retraining logic
│   ├── serving/            # FastAPI inference service
│   ├── monitoring/         # Prometheus metrics & alerting
│   └── utils/              # Helper functions & shared utilities
├── tests/
│   ├── unit/               # Unit tests per module
│   └── integration/        # End-to-end integration tests
├── scripts/                # Utility scripts (setup, migration, etc.)
├── docker/                 # Docker & docker-compose files
├── .env.example            # Environment variables template
├── requirements.txt        # Python dependencies
└── README.md
```


---

## 🚀 Getting Started with GitHub Codespaces

### Prerequisites
- GitHub account with Codespaces access
- (Optional) Binance API Key for real-time data access

### Step 1: Open Codespaces
```
Click the "Code" button on the repo → "Codespaces" tab → "Create codespace on main"
Environment will be automatically configured in ~2-3 minutes
```

### Step 2: Setup Environment Variables
```bash
# Copy environment variables template
cp .env.example .env

# Edit .env with your credentials
# (Binance API key, database credentials, etc.)
nano .env
```

### Step 3: Sync uv Environment
```bash
# Install runtime + dev dependencies into .venv
uv sync --dev

# Activate the uv-managed virtual environment (optional when using `uv run`)
source .venv/bin/activate
```

### Step 4: Verify Setup
```bash
# Check Python version
uv run python --version
# Expected: Python 3.11.x

# Check installed dependencies
uv run pip list | grep -E "mlflow|xgboost|fastapi|redis"

# Run unit tests
uv run pytest tests/unit -v
```

### Step 5: Run EDA Notebook
```
1. Open notebooks/01_initial_eda.ipynb from the file explorer
2. Select kernel: Python Environments → Python 3.11.x
3. Run All cells
```

---

## 🏛️ System Architecture
```
Binance WebSocket API          Binance REST API
        │                              │
        ▼                              ▼
  WebSocket Client          Batch Sync Service (*/6hr)
        │                              │
        └──────────┬───────────────────┘
                   ▼
            Redis Streams
          (raw_trades, raw_klines, raw_tickers)
                   │
                   ▼
      Feature Engineering Service
      (Rolling windows: 5m, 15m, 1h, 4h)
                   │
                   ▼
           Redis Feature Store
                   │
          ┌────────┴────────┐
          ▼                 ▼
   Isolation Forest      XGBoost
  (Anomaly Detection) (Volatility Pred)
          │                 │
          └────────┬────────┘
                   ▼
            FastAPI Service
         (Champion/Challenger A/B)
                   │
                   ▼
            Redis Stream
          (predictions output)
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
   Alert Service          Dashboard
  (Slack/PagerDuty)    (Grafana/Prometheus)
```

---

### 📊 Tech Stack

| Component | Technology |
|-----------|------------|
| Data Ingestion | WebSocket-client (Python) + Redis Streams |
| Feature Store | Redis (in-memory) + PostgreSQL (persistent) |
| Model Training | Scikit-learn (Isolation Forest) + XGBoost |
| Model Registry | MLflow |
| Model Serving | FastAPI (Python async) |
| Monitoring | Prometheus + Grafana |
| Alerting | Slack API + PagerDuty API |
| Orchestration | Python scripts + Cron |
| Deployment | Docker Compose |

---

## 🎯 Success Metrics

### Model Performance
| Metric | Target |
|--------|--------|
| Anomaly Detection Precision | ≥ 0.75 |
| Anomaly Detection Recall | ≥ 0.70 |
| F1-Score | ≥ 0.72 |
| Volatility Prediction MAE | ≤ 15% |

### System Performance
| Metric | Target |
|--------|--------|
| Inference Latency (p95) | < 100ms |
| Data Freshness | < 5 detik |
| Pipeline Uptime | ≥ 99.5% |
| Retraining Success Rate | ≥ 95% |

---

## 🌿 Branching Strategy (GitHub Flow)
```
main (always stable & deployable)
  │
  ├── feat/     → New features
  ├── fix/      → Bug fixes  
  ├── exp/      → ML experiments
  ├── docs/     → Documentation
  └── refactor/ → Code refactoring
```

**Rules:**
- `main` is only updated via Pull Requests
- Every branch must be reviewed before merging
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) format

---

## 📝 Commit Message Convention
```
feat: add websocket ingestion service
fix: handle reconnection on websocket timeout
exp: tune isolation forest contamination parameter
docs: update README setup instructions
refactor: extract feature engineering to separate module
test: add unit tests for volatility calculator
```
## 📄 License

This project is licensed under the MIT License.
