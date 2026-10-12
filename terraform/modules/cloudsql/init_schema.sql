-- ============================================================================
-- Trading Risk Engine: Operational Risk Alerts DDL (PostgreSQL)
-- Target: Cloud SQL PostgreSQL instance (trading_risk_db)
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ----------------------------------------------------------------------------
-- Table: risk_alerts (Hot operational store written by Cloud Dataflow)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk_alerts (
    alert_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    portfolio_id        VARCHAR(64) NOT NULL,
    ticker              VARCHAR(32) NOT NULL,
    asset_class         VARCHAR(32) NOT NULL,
    alert_type          VARCHAR(64) NOT NULL,
    severity            VARCHAR(16) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    metric_name         VARCHAR(64) NOT NULL,
    threshold_value     NUMERIC(18, 6) NOT NULL,
    observed_value      NUMERIC(18, 6) NOT NULL,
    event_timestamp     TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status              VARCHAR(32) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ACKNOWLEDGED', 'RESOLVED', 'DISMISSED')),
    payload_json        JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Indexing for low-latency operational dashboards & alerts routing
CREATE INDEX IF NOT EXISTS idx_risk_alerts_portfolio_time 
    ON risk_alerts (portfolio_id, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_risk_alerts_ticker 
    ON risk_alerts (ticker, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_risk_alerts_severity_status 
    ON risk_alerts (severity, status) 
    WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_risk_alerts_payload_gin 
    ON risk_alerts USING GIN (payload_json);

-- ----------------------------------------------------------------------------
-- Table: portfolio_limits (Reference table for Dataflow validation)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio_limits (
    limit_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    portfolio_id        VARCHAR(64) NOT NULL UNIQUE,
    asset_class         VARCHAR(32) NOT NULL,
    max_notional_usd    NUMERIC(18, 2) NOT NULL,
    max_drawdown_pct    NUMERIC(6, 4) NOT NULL,
    max_var_99_usd      NUMERIC(18, 2) NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_limits_portfolio 
    ON portfolio_limits (portfolio_id);
