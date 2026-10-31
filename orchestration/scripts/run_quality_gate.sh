#!/usr/bin/env bash
# =============================================================================
# Trading Risk Engine: Great Expectations Lakehouse Quality Gate Automation
# Executes data quality checkpoint suites across Silver and Gold Medallion layers.
# =============================================================================

set -euo pipefail

TARGET_DATE="${1:-$(date -u +%Y-%m-%d)}"
ENVIRONMENT="${2:-dev}"
DRY_RUN="${3:-false}"

echo "============================================================================="
echo " Great Expectations Lakehouse Quality Gate: ${ENVIRONMENT} (${TARGET_DATE})"
echo "============================================================================="

SUITES=(
  "silver_market_ticks_suite"
  "silver_trade_orders_suite"
  "silver_rolling_vwap_suite"
  "gold_portfolio_var_suite"
  "gold_risk_features_suite"
)

if [ "${DRY_RUN}" = "true" ] || [ "${DRY_RUN}" = "--dry-run" ]; then
  echo "[DRY RUN] Inspecting registered expectation suites..."
  for suite in "${SUITES[@]}"; do
    python -m data_quality.run_checkpoints --suite "${suite}" --dry-run
  done
  echo "All suites inspected successfully."
  exit 0
fi

echo "Running Great Expectations validation checkpoints..."
for suite in "${SUITES[@]}"; do
  echo "--> Validating suite: ${suite} for target date: ${TARGET_DATE}"
  python -m data_quality.run_checkpoints \
    --suite "${suite}" \
    --target-date "${TARGET_DATE}" \
    --environment "${ENVIRONMENT}" \
    --data-docs-dir "uncommitted/data_docs/${ENVIRONMENT}"
done

echo "============================================================================="
echo " All Great Expectations quality gates PASSED for date: ${TARGET_DATE}"
echo " HTML Data Docs generated under: uncommitted/data_docs/${ENVIRONMENT}/"
echo "============================================================================="
