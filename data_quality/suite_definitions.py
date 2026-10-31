"""Great Expectations Suite Definitions and Data Quality Contracts.

Defines standardized, production-grade Expectation Suites for the Trading Risk Lakehouse:
1. silver_market_ticks_suite: Validates Silver microstructure ticks (positive prices, bid<=ask non-crossed, spreads).
2. silver_trade_orders_suite: Validates Silver cleansed orders (uniqueness, BUY/SELL side, valid order types, notional).
3. silver_rolling_vwap_suite: Validates rolling VWAP aggregations (Bollinger bands bounds, price stddev, regime).
4. gold_portfolio_var_suite: Validates Value-at-Risk matrices (VaR_99 >= VaR_95 >= 0, CVaR >= VaR, risk regimes).
5. gold_risk_features_suite: Validates point-in-time features (liquidity score in [0, 1], order imbalance in [-1, 1]).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


@dataclass
class ExpectationConfig:
    """Strongly typed representation of a Great Expectations expectation configuration."""
    expectation_type: str
    kwargs: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "expectation_type": self.expectation_type,
            "kwargs": dict(self.kwargs),
        }
        if self.meta:
            result["meta"] = dict(self.meta)
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ExpectationConfig:
        return cls(
            expectation_type=data["expectation_type"],
            kwargs=data.get("kwargs", {}),
            meta=data.get("meta", {}),
        )


@dataclass
class ExpectationSuite:
    """Strongly typed representation of a Great Expectations suite."""
    expectation_suite_name: str
    expectations: List[ExpectationConfig] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    data_asset_type: Optional[str] = None

    def add_expectation(self, expectation: ExpectationConfig) -> None:
        self.expectations.append(expectation)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data_asset_type": self.data_asset_type,
            "expectation_suite_name": self.expectation_suite_name,
            "expectations": [exp.to_dict() for exp in self.expectations],
            "meta": dict(self.meta),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ExpectationSuite:
        return cls(
            expectation_suite_name=data["expectation_suite_name"],
            expectations=[
                ExpectationConfig.from_dict(exp)
                for exp in data.get("expectations", [])
            ],
            meta=data.get("meta", {}),
            data_asset_type=data.get("data_asset_type"),
        )

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, json_str: str) -> ExpectationSuite:
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> ExpectationSuite:
        p = Path(path)
        with open(p, "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    def save(self, path: Union[str, Path]) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)


# =============================================================================
# PROGRAMMATIC SUITE FACTORIES
# =============================================================================

def build_silver_market_ticks_suite() -> ExpectationSuite:
    """Construct production Great Expectations suite for Silver market ticks."""
    suite = ExpectationSuite(
        expectation_suite_name="silver_market_ticks_suite",
        meta={
            "great_expectations_version": "0.18.12",
            "layer": "silver",
            "table": "silver_market_ticks",
        },
    )
    # Row count
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1},
        meta={"description": "Ensure Silver market tick partition contains at least 1 record"},
    ))
    # Columns existence
    for col in ["ticker", "asset_class", "bid", "ask", "last_price", "volume"]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_to_exist",
            kwargs={"column": col},
        ))
    # Non-null values
    for col in ["ticker", "asset_class", "bid", "ask", "last_price", "volume", "timestamp", "spread", "mid_price"]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col},
        ))
    # Domain constraints
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "asset_class", "value_set": ["EQUITY", "FX", "COMMODITY", "CRYPTO", "ETF"]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "bid", "min_value": 0.0001},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "ask", "min_value": 0.0001},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "volume", "min_value": 0.0},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "spread", "min_value": 0.0},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "relative_spread_bps", "min_value": 0.0, "max_value": 2000.0},
    ))
    # Non-crossed quote
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "ask", "column_B": "bid"},
        meta={"description": "Non-crossed quote enforcement: ask price must be >= bid price"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "quote_condition", "value_set": ["TIGHT_SPREAD", "REGULAR", "WIDE_SPREAD"]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "is_crossed_quote", "value_set": [False]},
    ))
    return suite


def build_silver_trade_orders_suite() -> ExpectationSuite:
    """Construct production Great Expectations suite for Silver trade orders."""
    suite = ExpectationSuite(
        expectation_suite_name="silver_trade_orders_suite",
        meta={
            "great_expectations_version": "0.18.12",
            "layer": "silver",
            "table": "silver_trade_orders",
        },
    )
    # Row count
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1},
        meta={"description": "Ensure Silver trade orders partition contains at least 1 record"},
    ))
    # Column existence & non-null
    for col in ["order_id", "portfolio_id", "ticker", "side", "order_type", "quantity", "price", "notional_usd"]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_to_exist",
            kwargs={"column": col},
        ))
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col},
        ))
    # Primary key uniqueness
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "order_id"},
        meta={"description": "Primary key uniqueness enforcement across cleansed Silver orders"},
    ))
    # Value sets
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "side", "value_set": ["BUY", "SELL"]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "order_type", "value_set": ["MARKET", "LIMIT", "STOP_LOSS", "STOP_LIMIT"]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "order_category", "value_set": [
            "EQUITY_ORDER", "CRYPTO_ORDER", "FX_ORDER", "ETF_ORDER", "COMMODITY_ORDER"
        ]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "notional_tier", "value_set": ["MICRO", "STANDARD", "LARGE", "BLOCK"]},
    ))
    # Numerical bounds
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "price", "min_value": 0.0001},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "quantity", "min_value": 0.000001},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "notional_usd", "min_value": 0.01},
    ))
    return suite


def build_silver_rolling_vwap_suite() -> ExpectationSuite:
    """Construct production Great Expectations suite for Silver rolling VWAP."""
    suite = ExpectationSuite(
        expectation_suite_name="silver_rolling_vwap_suite",
        meta={
            "great_expectations_version": "0.18.12",
            "layer": "silver",
            "table": "silver_rolling_vwap",
        },
    )
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1},
        meta={"description": "Ensure Silver rolling VWAP partition contains at least 1 record"},
    ))
    for col in ["ticker", "asset_class", "vwap", "upper_band", "lower_band", "price_stddev"]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_to_exist",
            kwargs={"column": col},
        ))
    for col in ["ticker", "asset_class", "window_start", "window_end", "vwap", "upper_band", "lower_band", "price_stddev"]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col},
        ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "vwap", "min_value": 0.0001},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "price_stddev", "min_value": 0.0},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "bandwidth_bps", "min_value": 0.0},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "upper_band", "column_B": "vwap"},
        meta={"description": "Bollinger Upper Band must be >= VWAP"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "vwap", "column_B": "lower_band"},
        meta={"description": "VWAP must be >= Bollinger Lower Band"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "volatility_regime", "value_set": ["LOW", "NORMAL", "HIGH", "EXTREME"]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "trend", "value_set": ["BULLISH", "BEARISH", "NEUTRAL"]},
    ))
    return suite


def build_gold_portfolio_var_suite() -> ExpectationSuite:
    """Construct production Great Expectations suite for Gold portfolio Value-at-Risk."""
    suite = ExpectationSuite(
        expectation_suite_name="gold_portfolio_var_suite",
        meta={
            "great_expectations_version": "0.18.12",
            "layer": "gold",
            "table": "gold_portfolio_var",
        },
    )
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1},
        meta={"description": "Ensure Gold portfolio VaR partition contains at least 1 record"},
    ))
    for col in [
        "var_id", "portfolio_id", "valuation_date", "gross_exposure_usd",
        "parametric_var_95_usd", "parametric_var_99_usd",
        "monte_carlo_var_95_usd", "monte_carlo_var_99_usd",
        "expected_shortfall_cvar_95_usd", "expected_shortfall_cvar_99_usd",
    ]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_to_exist",
            kwargs={"column": col},
        ))
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col},
        ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "portfolio_id"},
        meta={"description": "Ensure single partition valuation row per portfolio per day"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "gross_exposure_usd", "min_value": 0.0},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "parametric_var_95_usd", "min_value": 0.0},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "parametric_var_99_usd", "min_value": 0.0},
    ))
    # Mathematical Ordering Invariants
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "parametric_var_99_usd", "column_B": "parametric_var_95_usd"},
        meta={"description": "Parametric VaR confidence monotonicity: VaR_99 must be >= VaR_95"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "monte_carlo_var_99_usd", "column_B": "monte_carlo_var_95_usd"},
        meta={"description": "Monte Carlo VaR confidence monotonicity: MC VaR_99 must be >= MC VaR_95"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "expected_shortfall_cvar_95_usd", "column_B": "monte_carlo_var_95_usd"},
        meta={"description": "Coherent tail risk: CVaR_95 must be >= MC VaR_95"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_or_equal_to_b",
        kwargs={"column_A": "expected_shortfall_cvar_99_usd", "column_B": "monte_carlo_var_99_usd"},
        meta={"description": "Coherent tail risk: CVaR_99 must be >= MC VaR_99"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "risk_regime", "value_set": [
            "LOW_RISK", "MODERATE_RISK", "ELEVATED_RISK", "HIGH_RISK", "CRITICAL_BREACH"
        ]},
    ))
    return suite


def build_gold_risk_features_suite() -> ExpectationSuite:
    """Construct production Great Expectations suite for Gold risk feature store."""
    suite = ExpectationSuite(
        expectation_suite_name="gold_risk_features_suite",
        meta={
            "great_expectations_version": "0.18.12",
            "layer": "gold",
            "table": "gold_risk_feature_store",
        },
    )
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1},
        meta={"description": "Ensure Gold risk features partition contains at least 1 record"},
    ))
    for col in [
        "feature_key", "portfolio_id", "ticker", "asset_class",
        "mark_to_market_price", "liquidity_score", "order_imbalance_ratio",
    ]:
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_to_exist",
            kwargs={"column": col},
        ))
        suite.add_expectation(ExpectationConfig(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col},
        ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "feature_key"},
        meta={"description": "Ensure uniqueness of feature key per position per valuation window"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "asset_class", "value_set": ["EQUITY", "FX", "COMMODITY", "CRYPTO", "ETF"]},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "mark_to_market_price", "min_value": 0.0001},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "liquidity_score", "min_value": 0.0, "max_value": 1.0},
        meta={"description": "Liquidity score normalized between [0, 1]"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "order_imbalance_ratio", "min_value": -1.0, "max_value": 1.0},
        meta={"description": "Order flow imbalance bounded between [-1, 1]"},
    ))
    suite.add_expectation(ExpectationConfig(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "aggressive_order_ratio", "min_value": 0.0, "max_value": 1.0},
    ))
    return suite


# Suite Catalog Mapping
SUITE_BUILDERS = {
    "silver_market_ticks_suite": build_silver_market_ticks_suite,
    "silver_trade_orders_suite": build_silver_trade_orders_suite,
    "silver_rolling_vwap_suite": build_silver_rolling_vwap_suite,
    "gold_portfolio_var_suite": build_gold_portfolio_var_suite,
    "gold_risk_features_suite": build_gold_risk_features_suite,
}


def get_expectation_suite(
    suite_name: str,
    expectations_dir: Optional[Union[str, Path]] = None,
) -> ExpectationSuite:
    """Retrieve an ExpectationSuite by name from disk or from programmatic factory."""
    if expectations_dir:
        candidate_file = Path(expectations_dir) / f"{suite_name}.json"
        if candidate_file.exists():
            return ExpectationSuite.from_file(candidate_file)

    # Check default repository path
    default_dir = Path(__file__).parent / "great_expectations" / "expectations"
    default_file = default_dir / f"{suite_name}.json"
    if default_file.exists():
        return ExpectationSuite.from_file(default_file)

    # Fallback to programmatic builder
    if suite_name in SUITE_BUILDERS:
        return SUITE_BUILDERS[suite_name]()

    raise KeyError(f"Unknown expectation suite: '{suite_name}'. Available: {list(SUITE_BUILDERS.keys())}")
