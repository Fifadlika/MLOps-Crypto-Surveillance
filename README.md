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
│   ├── raw/                # Bronze layer — raw klines from Binance REST API (JSONL)
│   ├── preprocess/         # Silver layer — cleaned/deduplicated output (JSONL)
│   ├── features/           # Gold layer — engineered feature vectors (Parquet)
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
├── scripts/
│   ├── pull_historical.py  # Pulls historical klines from Binance REST API → data/raw/
│   └── run_featurize.py    # Reads Redis Stream, computes feature vectors → data/features/
├── tests/
│   ├── unit/               # Unit tests per module
│   └── integration/        # End-to-end integration tests
├── docker/                 # Docker & docker-compose files
├── dvc.yaml                # DVC pipeline stage definitions
├── dvc.lock                # Auto-generated — dataset version snapshots (do not edit)
├── data/VERSION_REGISTRY.yaml  # Human-readable data version changelog
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
cp .env.example .env
nano .env   # fill in Binance API key, database credentials, etc.
```
 
### Step 3: Sync Environment
```bash
uv sync --dev
source .venv/bin/activate
```
 
### Step 4: Start Redis
```bash
docker compose -f docker/docker-compose.yml up -d redis
```
 
---
 
## 📦 Data Versioning with DVC
 
This project uses [DVC](https://dvc.org) to version datasets separately from source code. Instead of storing raw data in Git, DVC stores an MD5 hash pointer (`dvc.lock`) for each dataset. Every dataset snapshot is therefore tied to a specific Git commit, making any historical state fully reproducible.
 
### How it works
 
```
git commit  ──contains──▶  dvc.lock  ──points to──▶  actual data files
                              │
                              └─ MD5 hash changes whenever data changes
                                 → new commit = new dataset version
```
 
### DVC Pipeline Stages
 
The pipeline is defined in `dvc.yaml` with two stages that map to the data layer architecture:
 
```
┌─────────────────────────────────────────────────┐
│  Stage: ingest                                   │
│  cmd: python scripts/pull_historical.py          │
│       --days 1 --interval 1m                     │
│  out: data/raw/          (Bronze layer)          │
└──────────────────────┬──────────────────────────┘
                       │ dep
                       ▼
┌─────────────────────────────────────────────────┐
│  Stage: featurize                                │
│  cmd: python scripts/run_featurize.py            │
│       --idle-rounds 3                            │
│  out: data/features/     (Gold layer)            │
└─────────────────────────────────────────────────┘
```
 
The `featurize` stage only re-runs when `data/raw` changes — DVC detects this automatically from the MD5 hash, not file timestamps.
 
---
 
### Adding a New Data Version
 
Follow these steps every time new data is ingested (e.g. after each scheduled pipeline run):
 
#### Step 1: Run the pipeline
 
```bash
dvc repro
```
 
This runs both stages in dependency order: `ingest` first, then `featurize`. DVC skips any stage whose inputs have not changed. On completion, `dvc.lock` is updated automatically with the new MD5 hashes.
 
Expected output:
 
```
Running stage 'ingest':
> python scripts/pull_historical.py --days 1 --interval 1m
  ✓ BTCUSDT: 1439 klines written  (data/raw/BTCUSDT/YYYY-MM-DD.jsonl)
  ...
Updating lock file 'dvc.lock'
 
Running stage 'featurize':
> python scripts/run_featurize.py --idle-rounds 3
  Flushed N records to data/features/{symbol}/YYYY-MM-DD_fv1.0.parquet
  ...
Updating lock file 'dvc.lock'
```
 
#### Step 2: Verify what changed
 
```bash
dvc diff
```
 
This compares the current dataset state against the last committed `dvc.lock`. Output is grouped into `Added`, `Deleted`, and `Modified` files. Confirm that:
- New date-stamped files appear under `data/raw/{SYMBOL}/` and `data/features/` (Added)
- No existing date files were overwritten (data is append-only by design)
- `.bloom` filter files changed — this confirms the deduplication index was updated
```bash
dvc diff --show-hash
```
 
Use this to inspect the exact MD5 hash change for each directory. The hash difference between versions is the ground truth for what changed in the dataset.
 
#### Step 3: Update `VERSION_REGISTRY.yaml`
 
Open `data/VERSION_REGISTRY.yaml` and add a new entry. This file is the human-readable counterpart to `dvc.lock` — it records the context behind each version for the team.
 
```yaml
# data/VERSION_REGISTRY.yaml
versions:
  - version: "v1.0.3"
    date: "2026-05-15"
    description: "Daily snapshot — all 3 pairs, appended 2026-05-15 klines"
    changed_fields: []
    model_compatibility:
      isolation_forest: ">=v1.0"
      xgboost: ">=v1.0"
    breaking_change: false
```
 
> **Field guide:**
> - `changed_fields` — list any feature vector fields added, removed, or renamed. Leave empty `[]` if schema is unchanged.
> - `model_compatibility` — minimum model version that is still valid against this dataset. Update this when a breaking change occurs.
> - `breaking_change: true` — set this when feature dimensions change; existing models must be retrained before use.
 
#### Step 4: Commit the new version
 
```bash
git add dvc.lock data/VERSION_REGISTRY.yaml
git commit -m "feat(data): daily snapshot YYYY-MM-DD — 3 pairs"
git tag data-v1.0.3
git push origin main --tags
```
 
Tagging the commit is important — it creates a named checkpoint that can be restored exactly (see [Restoring a Previous Version](#restoring-a-previous-version) below).
 
---
 
### Checking Pipeline Status
 
```bash
# Is dvc.lock consistent with the current data files?
dvc status
```
 
Expected when up-to-date:
```
Data and pipelines are up to date.
```
 
If files have changed since the last `dvc repro`, `dvc status` will list the affected stages. Re-run `dvc repro` to bring the lock file back in sync before committing.
 
---
 
### Restoring a Previous Version
 
Because every `dvc.lock` is committed to Git, any past dataset state can be recovered:
 
```bash
# Switch code and dvc.lock to a previous tag
git checkout data-v1.0.1
 
# Restore the actual data files to match that dvc.lock
dvc checkout
```
 
This is the primary reproducibility guarantee: to retrain a model on the exact data used in a past experiment, check out the corresponding Git commit and run `dvc checkout`. The dataset will be identical to what was used at that point in time.
 
---
 
### Active Dataset Versions
 
| Tag | Commit | Description |
|-----|--------|-------------|
| `data-v1.0.0` | `03bedca` | Initial DVC pipeline + dataset |
| `data-v1.0.1` | `6744bdb` | Daily snapshot 2026-05-06 — 3 pairs |
| `data-v1.0.2` | `dcaa6b3` | Extended to 2026-05-10 |
| `data-v1.0.3` | `17e9e51` | Daily snapshot 2026-05-14 *(current)* |
 
For the full changelog including schema changes and model compatibility notes, see [`data/VERSION_REGISTRY.yaml`](data/VERSION_REGISTRY.yaml).

---

## Running Experiments

```bash
# Single run
python src/training/train.py --symbol BTCUSDT --model anomaly --contamination 0.05

# View MLflow UI
mlflow ui --backend-store-uri mlflow/ --port 5000
# Open http://localhost:5000
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
 
## 📊 Tech Stack
 
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
| Data Freshness | < 5 seconds |
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
feat(data): daily snapshot YYYY-MM-DD — 3 pairs
feat: add websocket ingestion service
fix: handle reconnection on websocket timeout
exp: tune isolation forest contamination parameter
docs: update README data versioning instructions
refactor: extract feature engineering to separate module
test: add unit tests for volatility calculator
```
 
---
 
## 📄 License
 
This project is licensed under the MIT License.
 
