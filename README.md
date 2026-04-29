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

**Data Source:** Binance WebSocket API (real-time) + Binance REST API (primary `api.binance.com`, fallback `api4.binance.com`)

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
│   ├── preprocess/         # Cleaned/preprocessed ingestion output
│   ├── features/           # Engineered features
│   └── external/           # External reference data
├── models/
│   ├── artifacts/          # Saved model artifacts
│   └── experiments/        # MLflow experiment results
├── notebooks/              # Jupyter notebooks for EDA & experiments
├── src/
│   ├── ingest_data.py      # Live ingestion script entrypoint
│   ├── preprocess.py       # Redis stream preprocessing worker entrypoint
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

## ▶️ Running Ingestion Scripts

### 0) Start Redis for live smoke run
```bash
docker compose -f docker/docker-compose.yml up -d redis
```

If Docker is unavailable, run a local Redis server on `localhost:6379` and keep `REDIS_RUNTIME_MODE=real`.

### 1) Run preprocess worker (`src/preprocess.py`)
```bash
REDIS_RUNTIME_MODE=real uv run python src/preprocess.py --duration-seconds 90
```

What it does:
- Consumes Redis Streams (`stream:trades:*`, `stream:klines:*`, `stream:gaps:*`)
- Runs `DataCleaner` (dedup -> validate -> flag -> normalize -> gap detect)
- Persists cleaned output to `data/preprocess/{SYMBOL}/YYYY-MM-DD.jsonl`

### 2) Run live ingest script (`src/ingest_data.py`)
```bash
REDIS_RUNTIME_MODE=real uv run python src/ingest_data.py --duration-seconds 60
```

What it does:
- Connects to Binance trade + 1m kline websocket streams
- Publishes events to Redis streams for downstream preprocess worker
- Emits reconnect gap events for backfill handling

### 3) Run WebSocket ingestion pipeline (single-command orchestration)
```bash
uv run python -m src.ingestion.pipeline
```

What it does:
- Subscribes to Binance trade + 1m kline streams
- Publishes to Redis streams (`stream:trades:{symbol}` and `stream:klines:{symbol}`)
- Emits gap events to `stream:gaps:{symbol}` when reconnect happens
- Runs cleaner consumer loop and midnight dedup flush watcher internally

### 4) Generate sample raw data in `data/raw/`
Use this when you need to backfill a time range (for example after a reconnect gap):

```bash
REDIS_RUNTIME_MODE=real uv run python - <<'PY'
import asyncio
import time

from src.ingestion.rest_client import BinanceRESTClient


async def main() -> None:
  client = BinanceRESTClient()
  end_ms = int(time.time() * 1000)
  start_ms = end_ms - (15 * 60 * 1000)  # last 15 minutes (quick sample)
  try:
    count = await client.sync_klines("BTCUSDT", start_ms=start_ms, end_ms=end_ms, interval="1m")
    print(f"Published {count} closed klines")
  finally:
    await client.close()


asyncio.run(main())
PY
```

Output behavior:
- Redis stream publish to `stream:klines:{symbol}`
- Local append-only raw storage to `data/raw/{SYMBOL}/YYYY-MM-DD.jsonl`
- Sidecar metadata at `data/raw/{SYMBOL}/YYYY-MM-DD.meta`
- Cleaner output persisted to `data/preprocess/{SYMBOL}/YYYY-MM-DD.jsonl`
- Safe for repeated runs (dedup + append-only + lock)

### 5) Run ingestion unit tests
```bash
uv run pytest tests/unit/test_websocket_handlers.py tests/unit/test_rest_client.py -v
```

### 6) Verify sidecar consistency (raw + preprocess)
```bash
uv run python scripts/verify_raw.py
```

Expected output:
- `VERIFY_RAW:OK` when every `.meta` file matches JSONL line count.

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

## DVC Setup and Commands

To integrate data versioning:
1. `dvc init` - Initialize DVC in the workspace.
2. `dvc repro` - Run data pipeline.
3. `dvc status` - Check status of the pipeline outputs.
4. `dvc diff` - Compare DVC controlled files.
