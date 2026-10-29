"""Data Schemas and Quantitative Risk Contracts for Databricks Delta Gold Layer.

Defines:
- Explicit Spark SQL DDL schemas for Delta Gold tables (`gold_portfolio_var`, `gold_risk_feature_store`).
- Matching PyArrow schemas for zero-cost hermetic local Parquet/Delta writing.
- Quantitative data models (`PortfolioVaRRecord`, `RiskFeatureVector`).
- Validation rules ensuring VaR ordering invariants (VaR_99 >= VaR_95 > 0, CVaR >= VaR).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, Optional, Tuple
import pyarrow as pa


# -----------------------------------------------------------------------------
# 1. SPARK SQL DDL SCHEMAS FOR DELTA LAKE GOLD TABLES
# -----------------------------------------------------------------------------

DELTA_GOLD_PORTFOLIO_VAR_DDL = """
    var_id STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    valuation_date STRING NOT NULL,
    valuation_timestamp TIMESTAMP NOT NULL,
    horizon_days INT NOT NULL,
    total_positions INT NOT NULL,
    gross_exposure_usd DOUBLE NOT NULL,
    net_exposure_usd DOUBLE NOT NULL,
    long_exposure_usd DOUBLE NOT NULL,
    short_exposure_usd DOUBLE NOT NULL,
    portfolio_volatility_annualized DOUBLE NOT NULL,
    portfolio_volatility_daily DOUBLE NOT NULL,
    parametric_var_95_usd DOUBLE NOT NULL,
    parametric_var_99_usd DOUBLE NOT NULL,
    historical_var_95_usd DOUBLE NOT NULL,
    historical_var_99_usd DOUBLE NOT NULL,
    monte_carlo_var_95_usd DOUBLE NOT NULL,
    monte_carlo_var_99_usd DOUBLE NOT NULL,
    expected_shortfall_cvar_95_usd DOUBLE NOT NULL,
    expected_shortfall_cvar_99_usd DOUBLE NOT NULL,
    diversification_benefit_pct DOUBLE NOT NULL,
    max_component_var_ticker STRING NOT NULL,
    max_component_var_usd DOUBLE NOT NULL,
    var_limit_usd DOUBLE NOT NULL,
    is_var_breach_95 BOOLEAN NOT NULL,
    is_var_breach_99 BOOLEAN NOT NULL,
    risk_regime STRING NOT NULL,
    monte_carlo_paths INT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    _computed_at TIMESTAMP NOT NULL,
    _source_silver_version INT NOT NULL,
    _feature_hash STRING NOT NULL,
    _batch_id STRING NOT NULL
""".strip()

DELTA_GOLD_RISK_FEATURE_STORE_DDL = """
    feature_key STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    ticker STRING NOT NULL,
    asset_class STRING NOT NULL,
    feature_timestamp TIMESTAMP NOT NULL,
    net_quantity DOUBLE NOT NULL,
    avg_entry_price DOUBLE NOT NULL,
    mark_to_market_price DOUBLE NOT NULL,
    position_notional_usd DOUBLE NOT NULL,
    signed_exposure_usd DOUBLE NOT NULL,
    portfolio_weight_pct DOUBLE NOT NULL,
    unrealized_pnl_usd DOUBLE NOT NULL,
    realized_volatility_daily DOUBLE NOT NULL,
    realized_volatility_annualized DOUBLE NOT NULL,
    vwap_deviation_bps DOUBLE NOT NULL,
    avg_spread_bps DOUBLE NOT NULL,
    liquidity_score DOUBLE NOT NULL,
    order_imbalance_ratio DOUBLE NOT NULL,
    aggressive_order_ratio DOUBLE NOT NULL,
    standalone_var_95_usd DOUBLE NOT NULL,
    standalone_var_99_usd DOUBLE NOT NULL,
    component_var_95_usd DOUBLE NOT NULL,
    beta_to_portfolio DOUBLE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    _computed_at TIMESTAMP NOT NULL,
    _feature_hash STRING NOT NULL,
    _batch_id STRING NOT NULL
""".strip()


# -----------------------------------------------------------------------------
# 2. PYARROW SCHEMAS FOR ZERO-COST HERMETIC LOCAL PROCESSING
# -----------------------------------------------------------------------------

ARROW_DELTA_GOLD_PORTFOLIO_VAR_SCHEMA = pa.schema([
    ("var_id", pa.string()),
    ("portfolio_id", pa.string()),
    ("valuation_date", pa.string()),
    ("valuation_timestamp", pa.timestamp("us", tz="UTC")),
    ("horizon_days", pa.int32()),
    ("total_positions", pa.int32()),
    ("gross_exposure_usd", pa.float64()),
    ("net_exposure_usd", pa.float64()),
    ("long_exposure_usd", pa.float64()),
    ("short_exposure_usd", pa.float64()),
    ("portfolio_volatility_annualized", pa.float64()),
    ("portfolio_volatility_daily", pa.float64()),
    ("parametric_var_95_usd", pa.float64()),
    ("parametric_var_99_usd", pa.float64()),
    ("historical_var_95_usd", pa.float64()),
    ("historical_var_99_usd", pa.float64()),
    ("monte_carlo_var_95_usd", pa.float64()),
    ("monte_carlo_var_99_usd", pa.float64()),
    ("expected_shortfall_cvar_95_usd", pa.float64()),
    ("expected_shortfall_cvar_99_usd", pa.float64()),
    ("diversification_benefit_pct", pa.float64()),
    ("max_component_var_ticker", pa.string()),
    ("max_component_var_usd", pa.float64()),
    ("var_limit_usd", pa.float64()),
    ("is_var_breach_95", pa.bool_()),
    ("is_var_breach_99", pa.bool_()),
    ("risk_regime", pa.string()),
    ("monte_carlo_paths", pa.int32()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("_computed_at", pa.timestamp("us", tz="UTC")),
    ("_source_silver_version", pa.int32()),
    ("_feature_hash", pa.string()),
    ("_batch_id", pa.string()),
])

ARROW_DELTA_GOLD_RISK_FEATURE_STORE_SCHEMA = pa.schema([
    ("feature_key", pa.string()),
    ("portfolio_id", pa.string()),
    ("ticker", pa.string()),
    ("asset_class", pa.string()),
    ("feature_timestamp", pa.timestamp("us", tz="UTC")),
    ("net_quantity", pa.float64()),
    ("avg_entry_price", pa.float64()),
    ("mark_to_market_price", pa.float64()),
    ("position_notional_usd", pa.float64()),
    ("signed_exposure_usd", pa.float64()),
    ("portfolio_weight_pct", pa.float64()),
    ("unrealized_pnl_usd", pa.float64()),
    ("realized_volatility_daily", pa.float64()),
    ("realized_volatility_annualized", pa.float64()),
    ("vwap_deviation_bps", pa.float64()),
    ("avg_spread_bps", pa.float64()),
    ("liquidity_score", pa.float64()),
    ("order_imbalance_ratio", pa.float64()),
    ("aggressive_order_ratio", pa.float64()),
    ("standalone_var_95_usd", pa.float64()),
    ("standalone_var_99_usd", pa.float64()),
    ("component_var_95_usd", pa.float64()),
    ("beta_to_portfolio", pa.float64()),
    ("year", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("_computed_at", pa.timestamp("us", tz="UTC")),
    ("_feature_hash", pa.string()),
    ("_batch_id", pa.string()),
])


# -----------------------------------------------------------------------------
# 3. GOLD DATA MODELS & VALIDATORS
# -----------------------------------------------------------------------------

def compute_gold_hash(record: Dict[str, Any]) -> str:
    """Compute deterministic SHA-256 hash across curated Gold risk metrics."""
    excluded = {
        "_computed_at",
        "_source_silver_version",
        "_feature_hash",
        "_batch_id",
        "year",
        "month",
        "day",
    }
    payload = {
        k: v
        for k, v in sorted(record.items())
        if k not in excluded and not k.startswith("_")
    }
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass
class PortfolioVaRRecord:
    """Strongly typed representation of a Gold Portfolio Value-at-Risk record."""

    var_id: str
    portfolio_id: str
    valuation_date: str
    valuation_timestamp: datetime
    horizon_days: int
    total_positions: int
    gross_exposure_usd: float
    net_exposure_usd: float
    long_exposure_usd: float
    short_exposure_usd: float
    portfolio_volatility_annualized: float
    portfolio_volatility_daily: float
    parametric_var_95_usd: float
    parametric_var_99_usd: float
    historical_var_95_usd: float
    historical_var_99_usd: float
    monte_carlo_var_95_usd: float
    monte_carlo_var_99_usd: float
    expected_shortfall_cvar_95_usd: float
    expected_shortfall_cvar_99_usd: float
    diversification_benefit_pct: float
    max_component_var_ticker: str
    max_component_var_usd: float
    var_limit_usd: float
    is_var_breach_95: bool
    is_var_breach_99: bool
    risk_regime: str
    monte_carlo_paths: int
    year: int
    month: int
    day: int
    _computed_at: datetime
    _source_silver_version: int
    _feature_hash: str
    _batch_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize record into dictionary compatible with PyArrow schema."""
        return {
            "var_id": self.var_id,
            "portfolio_id": self.portfolio_id,
            "valuation_date": self.valuation_date,
            "valuation_timestamp": self.valuation_timestamp,
            "horizon_days": self.horizon_days,
            "total_positions": self.total_positions,
            "gross_exposure_usd": self.gross_exposure_usd,
            "net_exposure_usd": self.net_exposure_usd,
            "long_exposure_usd": self.long_exposure_usd,
            "short_exposure_usd": self.short_exposure_usd,
            "portfolio_volatility_annualized": self.portfolio_volatility_annualized,
            "portfolio_volatility_daily": self.portfolio_volatility_daily,
            "parametric_var_95_usd": self.parametric_var_95_usd,
            "parametric_var_99_usd": self.parametric_var_99_usd,
            "historical_var_95_usd": self.historical_var_95_usd,
            "historical_var_99_usd": self.historical_var_99_usd,
            "monte_carlo_var_95_usd": self.monte_carlo_var_95_usd,
            "monte_carlo_var_99_usd": self.monte_carlo_var_99_usd,
            "expected_shortfall_cvar_95_usd": self.expected_shortfall_cvar_95_usd,
            "expected_shortfall_cvar_99_usd": self.expected_shortfall_cvar_99_usd,
            "diversification_benefit_pct": self.diversification_benefit_pct,
            "max_component_var_ticker": self.max_component_var_ticker,
            "max_component_var_usd": self.max_component_var_usd,
            "var_limit_usd": self.var_limit_usd,
            "is_var_breach_95": self.is_var_breach_95,
            "is_var_breach_99": self.is_var_breach_99,
            "risk_regime": self.risk_regime,
            "monte_carlo_paths": self.monte_carlo_paths,
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "_computed_at": self._computed_at,
            "_source_silver_version": self._source_silver_version,
            "_feature_hash": self._feature_hash,
            "_batch_id": self._batch_id,
        }


@dataclass
class RiskFeatureVector:
    """Strongly typed representation of a Gold Risk Feature Store vector per (portfolio_id, ticker)."""

    feature_key: str
    portfolio_id: str
    ticker: str
    asset_class: str
    feature_timestamp: datetime
    net_quantity: float
    avg_entry_price: float
    mark_to_market_price: float
    position_notional_usd: float
    signed_exposure_usd: float
    portfolio_weight_pct: float
    unrealized_pnl_usd: float
    realized_volatility_daily: float
    realized_volatility_annualized: float
    vwap_deviation_bps: float
    avg_spread_bps: float
    liquidity_score: float
    order_imbalance_ratio: float
    aggressive_order_ratio: float
    standalone_var_95_usd: float
    standalone_var_99_usd: float
    component_var_95_usd: float
    beta_to_portfolio: float
    year: int
    month: int
    day: int
    _computed_at: datetime
    _feature_hash: str
    _batch_id: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize feature vector into dictionary compatible with PyArrow schema."""
        return {
            "feature_key": self.feature_key,
            "portfolio_id": self.portfolio_id,
            "ticker": self.ticker,
            "asset_class": self.asset_class,
            "feature_timestamp": self.feature_timestamp,
            "net_quantity": self.net_quantity,
            "avg_entry_price": self.avg_entry_price,
            "mark_to_market_price": self.mark_to_market_price,
            "position_notional_usd": self.position_notional_usd,
            "signed_exposure_usd": self.signed_exposure_usd,
            "portfolio_weight_pct": self.portfolio_weight_pct,
            "unrealized_pnl_usd": self.unrealized_pnl_usd,
            "realized_volatility_daily": self.realized_volatility_daily,
            "realized_volatility_annualized": self.realized_volatility_annualized,
            "vwap_deviation_bps": self.vwap_deviation_bps,
            "avg_spread_bps": self.avg_spread_bps,
            "liquidity_score": self.liquidity_score,
            "order_imbalance_ratio": self.order_imbalance_ratio,
            "aggressive_order_ratio": self.aggressive_order_ratio,
            "standalone_var_95_usd": self.standalone_var_95_usd,
            "standalone_var_99_usd": self.standalone_var_99_usd,
            "component_var_95_usd": self.component_var_95_usd,
            "beta_to_portfolio": self.beta_to_portfolio,
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "_computed_at": self._computed_at,
            "_feature_hash": self._feature_hash,
            "_batch_id": self._batch_id,
        }


def validate_gold_var_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate quantitative mathematical invariants on a Gold Portfolio VaR record."""
    required = [
        "var_id",
        "portfolio_id",
        "valuation_date",
        "horizon_days",
        "gross_exposure_usd",
        "parametric_var_95_usd",
        "parametric_var_99_usd",
        "monte_carlo_var_95_usd",
        "monte_carlo_var_99_usd",
        "expected_shortfall_cvar_95_usd",
        "expected_shortfall_cvar_99_usd",
    ]
    for field_name in required:
        if record.get(field_name) is None:
            return False, "MISSING_REQUIRED_FIELD", f"Missing required field: '{field_name}'"

    if int(record["horizon_days"]) <= 0:
        return False, "INVALID_HORIZON", f"horizon_days must be > 0, got {record['horizon_days']}"

    gross = float(record["gross_exposure_usd"])
    if gross < 0.0:
        return False, "NEGATIVE_GROSS_EXPOSURE", f"gross_exposure_usd cannot be negative: {gross}"

    p95 = float(record["parametric_var_95_usd"])
    p99 = float(record["parametric_var_99_usd"])
    mc95 = float(record["monte_carlo_var_95_usd"])
    mc99 = float(record["monte_carlo_var_99_usd"])
    cvar95 = float(record["expected_shortfall_cvar_95_usd"])
    cvar99 = float(record["expected_shortfall_cvar_99_usd"])

    if p95 < 0.0 or p99 < 0.0 or mc95 < 0.0 or mc99 < 0.0:
        return False, "NEGATIVE_VAR", "VaR metrics must be non-negative"

    if p99 + 1e-6 < p95:
        return False, "VAR_CONFIDENCE_INVARIANT_VIOLATION", f"Parametric VaR 99% ({p99}) < VaR 95% ({p95})"

    if mc99 + 1e-6 < mc95:
        return False, "VAR_CONFIDENCE_INVARIANT_VIOLATION", f"Monte Carlo VaR 99% ({mc99}) < VaR 95% ({mc95})"

    if cvar95 + 1e-6 < mc95:
        return False, "CVAR_SUBADDITIVITY_VIOLATION", f"Expected Shortfall 95% ({cvar95}) < Monte Carlo VaR 95% ({mc95})"

    if cvar99 + 1e-6 < mc99:
        return False, "CVAR_SUBADDITIVITY_VIOLATION", f"Expected Shortfall 99% ({cvar99}) < Monte Carlo VaR 99% ({mc99})"

    return True, None, None


def validate_gold_feature_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate quantitative invariants on a Gold Risk Feature Store record."""
    required = [
        "feature_key",
        "portfolio_id",
        "ticker",
        "asset_class",
        "mark_to_market_price",
        "position_notional_usd",
        "realized_volatility_daily",
        "liquidity_score",
    ]
    for field_name in required:
        if record.get(field_name) is None:
            return False, "MISSING_REQUIRED_FIELD", f"Missing required field: '{field_name}'"

    mtm = float(record["mark_to_market_price"])
    if mtm <= 0.0:
        return False, "NON_POSITIVE_MTM_PRICE", f"mark_to_market_price must be > 0, got {mtm}"

    notional = float(record["position_notional_usd"])
    if notional < 0.0:
        return False, "NEGATIVE_POSITION_NOTIONAL", f"position_notional_usd must be >= 0, got {notional}"

    vol = float(record["realized_volatility_daily"])
    if vol < 0.0:
        return False, "NEGATIVE_VOLATILITY", f"realized_volatility_daily must be >= 0, got {vol}"

    liq = float(record["liquidity_score"])
    if not (0.0 <= liq <= 100.0):
        return False, "LIQUIDITY_SCORE_OUT_OF_BOUNDS", f"liquidity_score must be in [0, 100], got {liq}"

    return True, None, None
