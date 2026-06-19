# load .env before every command
include .env
export

.PHONY: help mlflow mlflow-server train db redis sync

# ── Default ───────────────────────────────────────────────
help:
	@echo ""
	@echo "Available commands:"
	@echo "  make mlflow        Start MLflow UI"
	@echo "  make db            Connect to PostgreSQL shell"
	@echo "  make redis         Connect to Redis shell"
	@echo "  make sync          Sync uv dependencies"
	@echo "  make train         Run training (SYMBOL= MODEL= DEPTH=)"
	@echo ""

# ── MLflow ────────────────────────────────────────────────
mlflow:
	mlflow ui --host 0.0.0.0 --port 5000

mlflow-server:
	mlflow server \
		--backend-store-uri $(MLFLOW_BACKEND_STORE_URI) \
		--default-artifact-root $(MLFLOW_DEFAULT_ARTIFACT_ROOT) \
		--host 0.0.0.0 \
		--port 5000


# ── Database ──────────────────────────────────────────────
db:
	psql -h $(POSTGRES__HOST) -p $(POSTGRES__PORT) -U $(POSTGRES__USER) -d $(POSTGRES__DB)

redis:
	redis-cli -h $(REDIS__HOST) -p $(REDIS__PORT)

# ── Dependencies ──────────────────────────────────────────
sync:
	uv sync --all-groups --all-extras

# ── Training ──────────────────────────────────────────────
train:
	uv run python src/training/train.py \
		--symbol $(SYMBOL) \
		--model $(MODEL) \
		--max_depth $(DEPTH)