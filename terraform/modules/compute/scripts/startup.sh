#!/usr/bin/env bash
# =============================================================================
# Trading Risk Engine - Ingestion Worker Compute Engine Provisioning Script
# =============================================================================
# Automated bootstrapping script executed via Google Compute Engine metadata.
# Configures:
# 1. Linux OS and Network Socket Buffer Tuning for Ultra-Low Latency Streaming
# 2. Docker Container Engine with Hardened Logging Limits
# 3. Dedicated Service User & Secure Execution Environment
# 4. Systemd Daemon Unit for Continuous Ingestion Worker Process
# =============================================================================

set -euo pipefail

LOG_FILE="/var/log/trading-risk-startup.log"
mkdir -p /var/log /etc/trading-risk /opt/trading-risk

exec > >(tee -a "$LOG_FILE" /dev/tty0) 2>&1

log() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [INGESTION-INIT] $*"
}

log "Starting Trading Risk Ingestion Worker initialization..."

# -----------------------------------------------------------------------------
# 1. LINUX KERNEL NETWORK & SOCKET BUFFER TUNING
# -----------------------------------------------------------------------------
log "Applying kernel network parameters for high-throughput market tick streaming..."

cat << 'EOF' > /etc/sysctl.d/99-trading-risk-network.conf
# Increase max socket receive and send buffer sizes to 16MB
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 262144
net.core.wmem_default = 262144

# Increase TCP read/write buffer allocations
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# Increase network backlog queue for bursty market order spikes
net.core.netdev_max_backlog = 10000
net.core.somaxconn = 4096

# File descriptor ceiling
fs.file-max = 2097152
EOF

sysctl --system || true
log "Kernel parameters successfully applied."

# -----------------------------------------------------------------------------
# 2. DOCKER ENGINE INSTALLATION & CONFIGURATION
# -----------------------------------------------------------------------------
log "Checking Docker installation..."

if ! command -v docker &> /dev/null; then
    log "Docker not found. Installing Docker Engine dependencies..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        jq

    # Add official Docker repository
    install -m 0755 -d /etc/apt/keyrings
    if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
        curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        chmod a+r /etc/apt/keyrings/docker.gpg
    fi

    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin || {
        log "Warning: APT install failed, attempting fallback get.docker.com script..."
        curl -fsSL https://get.docker.com | sh
    }
fi

log "Configuring Docker daemon with production logging limits..."
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
log "Docker Engine successfully configured and running."

# -----------------------------------------------------------------------------
# 3. DEDICATED SERVICE USER CREATION
# -----------------------------------------------------------------------------
SERVICE_USER="riskworker"
SERVICE_GROUP="riskworker"

if ! id -u "$SERVICE_USER" &>/dev/null; then
    log "Creating dedicated non-privileged service user: $SERVICE_USER..."
    groupadd -g 10001 "$SERVICE_GROUP" || true
    useradd -u 10001 -g "$SERVICE_GROUP" -M -s /usr/sbin/nologin "$SERVICE_USER"
fi

# Add service user to docker group to allow daemon execution
usermod -aG docker "$SERVICE_USER" || true

# -----------------------------------------------------------------------------
# 4. ENVIRONMENT FILE CONFIGURATION
# -----------------------------------------------------------------------------
ENV_FILE="/etc/trading-risk/ingestion.env"
log "Rendering environment file at $ENV_FILE..."

cat << EOF > "$ENV_FILE"
# Trading Risk Engine Runtime Configuration
ENVIRONMENT="${environment}"
PROJECT_ID="${project_id}"
PUBSUB_TOPIC_ID="${pubsub_topic_id}"
WORKER_CONTAINER_IMAGE="${worker_container_image}"
MARKET_FEED_TICKERS="${tickers}"
METRICS_PORT=8080
LOG_LEVEL=INFO
HEALTH_CHECK_INTERVAL_SEC=15
EOF

chmod 0640 "$ENV_FILE"
chown root:"$SERVICE_GROUP" "$ENV_FILE"

# -----------------------------------------------------------------------------
# 5. SYSTEMD SERVICE UNIT INSTALLATION
# -----------------------------------------------------------------------------
SYSTEMD_UNIT="/etc/systemd/system/trading-ingestion.service"
log "Installing Systemd service unit at $SYSTEMD_UNIT..."

cat << EOF > "$SYSTEMD_UNIT"
[Unit]
Description=Trading Risk Engine - Market Ingestion Worker Daemon
Documentation=https://github.com/trading-risk-engine
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=docker
Restart=always
RestartSec=5s
TimeoutStopSec=30
KillMode=mixed

# Resource boundaries
MemoryMax=4G
CPUQuota=200%

# Environment variables
EnvironmentFile=-$ENV_FILE

# Container lifecycle commands
ExecStartPre=-/usr/bin/docker stop trading-ingestion-worker
ExecStartPre=-/usr/bin/docker rm trading-ingestion-worker
ExecStart=/usr/bin/docker run \\
    --rm \\
    --name trading-ingestion-worker \\
    --network host \\
    --env-file $ENV_FILE \\
    \${WORKER_CONTAINER_IMAGE}

ExecStop=/usr/bin/docker stop -t 15 trading-ingestion-worker

# Journal logging
StandardOutput=journal+console
StandardError=journal+console
SyslogIdentifier=trading-ingestion

[Install]
WantedBy=multi-user.target
EOF

chmod 0644 "$SYSTEMD_UNIT"

# -----------------------------------------------------------------------------
# 6. SYSTEMD SERVICE REGISTRATION & ACTIVATION
# -----------------------------------------------------------------------------
log "Reloading systemd daemon and enabling trading-ingestion service..."
systemctl daemon-reload
systemctl enable trading-ingestion.service

# Attempt service start (if container image is pre-pulled or available)
if docker pull "${worker_container_image}" &>/dev/null; then
    log "Container image pulled successfully. Starting trading-ingestion service..."
    systemctl start trading-ingestion.service || log "Warning: Failed to start service immediately."
else
    log "Container image '${worker_container_image}' not yet available in registry. Systemd service enabled and ready."
fi

log "Trading Risk Ingestion Worker initialization completed successfully."
