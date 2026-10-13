#!/usr/bin/env bash
# =============================================================================
# Trading Risk Engine - Ingestion Worker Host Provisioning Script
# =============================================================================
# Standalone provisioning utility for Compute Engine instances or bare-metal
# ingestion workers. Configures OS sysctl parameters, Docker daemon, systemd unit,
# and environment configs.
#
# Usage:
#   sudo ./provision_worker.sh [--validate-only] [--dry-run]
# =============================================================================

set -euo pipefail

VALIDATE_ONLY=0
DRY_RUN=0

for arg in "$@"; do
    case "$arg" in
        --validate-only)
            VALIDATE_ONLY=1
            ;;
        --dry-run)
            DRY_RUN=1
            ;;
        *)
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SYSTEMD_SOURCE="${REPO_DIR}/ingestion/systemd/trading-ingestion.service"

log() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [PROVISION] $*"
}

validate_syntax() {
    log "Validating configuration files and syntax..."

    if [[ ! -f "$SYSTEMD_SOURCE" ]]; then
        echo "Error: Systemd service file not found at $SYSTEMD_SOURCE" >&2
        return 1
    fi

    # Check systemd required sections
    grep -q "\[Unit\]" "$SYSTEMD_SOURCE" || { echo "Missing [Unit] section" >&2; return 1; }
    grep -q "\[Service\]" "$SYSTEMD_SOURCE" || { echo "Missing [Service] section" >&2; return 1; }
    grep -q "\[Install\]" "$SYSTEMD_SOURCE" || { echo "Missing [Install] section" >&2; return 1; }
    grep -q "Restart=always" "$SYSTEMD_SOURCE" || { echo "Missing Restart=always policy" >&2; return 1; }
    grep -q "MemoryMax=" "$SYSTEMD_SOURCE" || { echo "Missing MemoryMax resource boundary" >&2; return 1; }

    log "Validation successful: systemd unit file is structurally sound."
    return 0
}

if [[ "$VALIDATE_ONLY" -eq 1 ]]; then
    validate_syntax
    exit 0
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
    log "Dry-run mode active. Performing pre-flight checks without system changes."
    validate_syntax
    log "Dry-run completed successfully."
    exit 0
fi

# Root check
if [[ "$EUID" -ne 0 ]]; then
    echo "Error: Provisioning must be run as root (or with sudo)." >&2
    exit 1
fi

log "Beginning full host provisioning for trading-risk ingestion worker..."
validate_syntax

# 1. Kernel Network Parameters
log "Applying low-latency kernel network tuning..."
cat << 'EOF' > /etc/sysctl.d/99-trading-risk-network.conf
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 262144
net.core.wmem_default = 262144
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.netdev_max_backlog = 10000
net.core.somaxconn = 4096
fs.file-max = 2097152
EOF
sysctl --system || true

# 2. Docker Installation Check
if ! command -v docker &> /dev/null; then
    log "Installing Docker Engine..."
    curl -fsSL https://get.docker.com | sh
fi

# 3. Docker Daemon Production Configuration
mkdir -p /etc/docker
cat << 'EOF' > /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "50m",
    "max-file": "5"
  },
  "live-restore": true,
  "userland-proxy": false
}
EOF
systemctl daemon-reload
systemctl enable docker
systemctl restart docker

# 4. Service User Setup
SERVICE_USER="riskworker"
if ! id -u "$SERVICE_USER" &>/dev/null; then
    log "Creating service user: $SERVICE_USER..."
    groupadd -g 10001 "$SERVICE_USER" || true
    useradd -u 10001 -g "$SERVICE_USER" -M -s /usr/sbin/nologin "$SERVICE_USER"
fi
usermod -aG docker "$SERVICE_USER" || true

# 5. Environment & Directories
mkdir -p /etc/trading-risk /var/log/trading-risk /opt/trading-risk
if [[ ! -f /etc/trading-risk/ingestion.env ]]; then
    cat << 'EOF' > /etc/trading-risk/ingestion.env
ENVIRONMENT=dev
PROJECT_ID=trading-risk-dev-001
PUBSUB_TOPIC_ID=projects/trading-risk-dev-001/topics/top-trade-orders
WORKER_CONTAINER_IMAGE=gcr.io/trading-risk-dev-001/ingestion-worker:latest
MARKET_FEED_TICKERS=AAPL,GOOGL,MSFT,AMZN,NVDA,TSLA,JPM,GS
METRICS_PORT=8080
LOG_LEVEL=INFO
HEALTH_CHECK_INTERVAL_SEC=15
EOF
fi
chmod 0640 /etc/trading-risk/ingestion.env
chown root:"$SERVICE_USER" /etc/trading-risk/ingestion.env

# 6. Install Systemd Service Unit
cp "$SYSTEMD_SOURCE" /etc/systemd/system/trading-ingestion.service
chmod 0644 /etc/systemd/system/trading-ingestion.service

systemctl daemon-reload
systemctl enable trading-ingestion.service
log "Provisioning complete. Service trading-ingestion enabled on systemd."
