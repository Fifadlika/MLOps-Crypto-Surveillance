FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye

# Remove stale apt sources that cause GPG key failures
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /etc/apt/sources.list.d/nodesource.list \
    /usr/share/keyrings/nodesource.gpg \
    /usr/share/keyrings/yarn.gpg 2>/dev/null || true

# Install only the client binaries — no servers, no apt pollution
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*