#!/bin/bash
set -e

echo "=== Starting PostgreSQL ==="
sudo service postgresql start || true

echo "=== Ensuring database and user exist ==="
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='mlops_user'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE USER mlops_user WITH PASSWORD 'mlops_password';"

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='crypto_surveillance'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE DATABASE crypto_surveillance OWNER mlops_user;"

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE crypto_surveillance TO mlops_user;"

echo "=== Creating tables ==="
sudo -u postgres psql -d crypto_surveillance <<SQL
CREATE TABLE IF NOT EXISTS features_trade (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    ts          BIGINT       NOT NULL,
    feature_version VARCHAR(10),
    data        JSONB        NOT NULL,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS features_kline (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    ts          BIGINT       NOT NULL,
    feature_version VARCHAR(10),
    data        JSONB        NOT NULL,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ft_symbol_ts ON features_trade(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_fk_symbol_ts ON features_kline(symbol, ts);
SQL

echo "=== Starting Redis ==="
sudo service redis-server start 2>/dev/null || \
sudo service redis start 2>/dev/null || \
echo "Redis not installed, skipping."

echo "=== Activating uv venv ==="
export PATH="$HOME/.local/bin:$PATH"
cd "${GITHUB_WORKSPACE:-/workspaces/MLOps-Crypto-Surveillance}" 2>/dev/null || true
uv sync --all-extras 2>/dev/null || true

echo "=== All services ready ==="
pg_isready