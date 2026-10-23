#!/usr/bin/env bash
# =============================================================================
# Automated Airflow Connections Provisioning Script for Cloud Composer
# =============================================================================
set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROJECT_ID="${GCP_PROJECT:-trading-risk-${ENVIRONMENT}-001}"
COMPOSER_ENV="${COMPOSER_ENV_NAME:-composer-trading-risk-${ENVIRONMENT}}"
LOCATION="${GCP_REGION:-us-central1}"

echo "==================================================================="
echo "Provisioning Airflow Connections for Cloud Composer: ${COMPOSER_ENV}"
echo "Environment: ${ENVIRONMENT} | Project: ${PROJECT_ID} | Region: ${LOCATION}"
echo "==================================================================="

# Export canonical connections using GcpConnectionManager
python3 -c "
from orchestration.airflow.config import AirflowEnvironmentConfig
from orchestration.airflow.connections import GcpConnectionManager

cfg = AirflowEnvironmentConfig(
    environment='${ENVIRONMENT}',
    gcp_project_id='${PROJECT_ID}',
    gcp_region='${LOCATION}',
)
mgr = GcpConnectionManager(cfg)
mgr.export_json('/tmp/airflow_connections.json')
print('Connections JSON exported successfully to /tmp/airflow_connections.json')
"

# Import into Cloud Composer environment via gcloud
if command -v gcloud &> /dev/null; then
    echo "Importing connections into Cloud Composer via gcloud..."
    gcloud composer environments run "${COMPOSER_ENV}" \
        --location "${LOCATION}" \
        --project "${PROJECT_ID}" \
        connections import -- /tmp/airflow_connections.json || echo "Warning: gcloud command exited with non-zero status (check environment availability)"
else
    echo "gcloud CLI not detected. Run manually in Composer terminal."
fi

echo "Connection setup completed."
