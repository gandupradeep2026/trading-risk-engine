"""Databricks Gold Layer: Portfolio Value-at-Risk (VaR) & Risk Feature Store Tests (Day 18).

Validates:
1. Terraform Databricks Gold Module:
   - Gold schema resource (`databricks_schema.gold_schema`) bound to GCS Gold external location.
   - Gold VaR & Feature Store job (`databricks_job.gold_var_job`) pointing to `03_gold_var_feature_store_runner`.
   - Gold outputs in module `outputs.tf` and root `terraform/outputs.tf`.
2. Gold Configurations & Table Properties:
   - `GOLD_PORTFOLIO_VAR_CONFIG` and `GOLD_RISK_FEATURE_STORE_CONFIG` partitions, Z-Order keys, and CDF properties.
3. Gold Schemas & Quantitative Contracts:
   - Exact field parity between Spark SQL DDL schemas and PyArrow schemas.
   - Mathematical invariant validators (`validate_gold_var_record`, `validate_gold_feature_record`).
4. Quantitative Value-at-Risk (VaR) & Expected Shortfall Invariants:
   - Confidence ordering: VaR_99 > VaR_95 > 0 across Parametric, Historical, and Monte Carlo methods.
   - Coherent tail risk: Expected Shortfall (CVaR_95 >= MC_VaR_95, CVaR_99 >= MC_VaR_99).
   - Square-root-of-time horizon scaling: 10-day Parametric VaR == sqrt(10) * 1-day Parametric VaR.
   - Multi-asset diversification benefit (> 0% for multi-asset portfolios).
   - Euler Homogeneous Function Theorem: sum of Component VaR_95 equals Portfolio Parametric VaR_95.
   - VaR limit breach detection and risk regime classification (`LOW_RISK` vs `CRITICAL_BREACH`).
5. Risk Feature Store & Point-in-Time Retrieval APIs:
   - Position notional, unrealized PnL, order flow imbalance, aggressive order ratio, liquidity score, VWAP deviation bps.
   - Online lookups via `get_portfolio_features`, `get_feature_vector`, and `get_latest_portfolio_var`.
6. Delta Lake ACID MERGE Upserts & Table Maintenance:
   - Initial `WRITE` and subsequent `MERGE` commits on `gold_portfolio_var` and `gold_risk_feature_store`.
   - `OPTIMIZE` (Z-Ordering) and `VACUUM` transaction log commits.
7. End-to-End Medallion Pipeline (Bronze -> Silver -> Gold):
   - Full data flow from raw ticks/orders/VWAP through Bronze and Silver into Gold VaR & Feature Store.
8. Databricks Gold Notebook Runner CLI & Telemetry:
   - CLI argument parsing, `--dry-run`, full execution, and Prometheus metrics exposition.
"""

from datetime import date, datetime, timezone
import json
import math
from pathlib import Path
import re
import tempfile
from typing import Any, Dict, List
import pytest

from batch.databricks import (
    ARROW_DELTA_GOLD_PORTFOLIO_VAR_SCHEMA,
    ARROW_DELTA_GOLD_RISK_FEATURE_STORE_SCHEMA,
    DELTA_GOLD_PORTFOLIO_VAR_DDL,
    DELTA_GOLD_RISK_FEATURE_STORE_DDL,
    DeltaBronzeIngestionEngine,
    DeltaGoldVaREngine,
    DeltaLogManager,
    DeltaSilverCleansingEngine,
    GOLD_PORTFOLIO_VAR_CONFIG,
    GOLD_RISK_FEATURE_STORE_CONFIG,
    GoldVaRTelemetry,
    compute_gold_hash,
    get_pairwise_correlation,
    infer_asset_class,
    validate_gold_feature_record,
    validate_gold_var_record,
)
from batch.databricks.notebooks.gold_var_runner import (
    parse_args,
    run_gold_var_pipeline,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_DATABRICKS_DIR = REPO_ROOT / "terraform" / "modules" / "databricks"
TF_ROOT_DIR = REPO_ROOT / "terraform"


# =============================================================================
# 1. TERRAFORM DATABRICKS GOLD MODULE VALIDATION
# =============================================================================

class TestTerraformGoldModule:
    """Validate Terraform Databricks resources and outputs for Gold layer."""

    def test_gold_schema_and_job_defined_in_main_tf(self) -> None:
        """Verify databricks_schema.gold_schema and databricks_job.gold_var_job exist."""
        main_tf = (TF_DATABRICKS_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'resource "databricks_schema" "gold_schema"' in main_tf
        assert 'name         = "gold"' in main_tf
        assert "${var.gcs_gold_bucket_url}/delta/gold" in main_tf
        assert "databricks_external_location.gold_external_location" in main_tf

        assert 'resource "databricks_job" "gold_var_job"' in main_tf
        assert "03_gold_var_feature_store_runner" in main_tf
        assert "gold_schema_name        = databricks_schema.gold_schema.name" in main_tf

    def test_gold_outputs_in_module_and_root(self) -> None:
        """Verify gold schema, job, and external location outputs exist in module and root."""
        mod_outputs = (TF_DATABRICKS_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert 'output "gold_schema_name"' in mod_outputs
        assert 'output "gold_job_id"' in mod_outputs
        assert 'output "gold_external_location_id"' in mod_outputs

        root_outputs = (TF_ROOT_DIR / "outputs.tf").read_text(encoding="utf-8")
        assert 'output "databricks_gold_schema"' in root_outputs
        assert 'output "databricks_gold_job_id"' in root_outputs
        assert 'output "databricks_gold_external_location_id"' in root_outputs

    def test_balanced_braces_in_terraform(self) -> None:
        """Verify balanced HCL braces across all Terraform files."""
        for tf_path in list(TF_DATABRICKS_DIR.glob("*.tf")) + list(TF_ROOT_DIR.glob("*.tf")):
            content = tf_path.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_path.name}"


# =============================================================================
# 2. GOLD CONFIGURATIONS & INVARIANTS
# =============================================================================

class TestGoldConfigurations:
    """Validate Gold DeltaTableConfig specifications."""

    def test_gold_table_configs_partitions_and_zorder(self) -> None:
        """Verify GOLD_PORTFOLIO_VAR_CONFIG and GOLD_RISK_FEATURE_STORE_CONFIG."""
        assert GOLD_PORTFOLIO_VAR_CONFIG.table_name == "gold_portfolio_var"
        assert GOLD_PORTFOLIO_VAR_CONFIG.schema == "gold"
        assert GOLD_PORTFOLIO_VAR_CONFIG.partition_columns == ["year", "month", "day"]
        assert GOLD_PORTFOLIO_VAR_CONFIG.z_order_columns == ["portfolio_id", "valuation_timestamp"]
        assert GOLD_PORTFOLIO_VAR_CONFIG.enable_change_data_feed is True
        assert GOLD_PORTFOLIO_VAR_CONFIG.full_table_identifier == "trading_risk.gold.gold_portfolio_var"

        assert GOLD_RISK_FEATURE_STORE_CONFIG.table_name == "gold_risk_feature_store"
        assert GOLD_RISK_FEATURE_STORE_CONFIG.schema == "gold"
        assert GOLD_RISK_FEATURE_STORE_CONFIG.partition_columns == ["year", "month", "day", "asset_class"]
        assert GOLD_RISK_FEATURE_STORE_CONFIG.z_order_columns == ["portfolio_id", "ticker", "feature_timestamp"]
        assert GOLD_RISK_FEATURE_STORE_CONFIG.enable_change_data_feed is True
        assert GOLD_RISK_FEATURE_STORE_CONFIG.full_table_identifier == "trading_risk.gold.gold_risk_feature_store"


# =============================================================================
# 3. GOLD SCHEMAS & QUANTITATIVE CONTRACTS
# =============================================================================

class TestGoldSchemasAndContracts:
    """Validate Spark SQL DDL schemas, PyArrow schemas, and mathematical validators."""

    def test_ddl_and_arrow_schema_consistency(self) -> None:
        """Verify Gold DDL column names strictly match PyArrow schema field names."""
        def extract_ddl_names(ddl: str) -> List[str]:
            return [
                line.strip().rstrip(",").split()[0]
                for line in ddl.strip().splitlines()
                if line.strip()
            ]

        assert extract_ddl_names(DELTA_GOLD_PORTFOLIO_VAR_DDL) == ARROW_DELTA_GOLD_PORTFOLIO_VAR_SCHEMA.names
        assert extract_ddl_names(DELTA_GOLD_RISK_FEATURE_STORE_DDL) == ARROW_DELTA_GOLD_RISK_FEATURE_STORE_SCHEMA.names

    def test_validate_gold_var_record_invariants(self) -> None:
        """Verify VaR validator catches confidence inversions, CVaR violations, and invalid horizons."""
        valid = {
            "var_id": "PORT-1#2026-10-29#H1",
            "portfolio_id": "PORT-1",
            "valuation_date": "2026-10-29",
            "horizon_days": 1,
            "gross_exposure_usd": 500000.0,
            "parametric_var_95_usd": 12000.0,
            "parametric_var_99_usd": 17000.0,
            "monte_carlo_var_95_usd": 12200.0,
            "monte_carlo_var_99_usd": 17300.0,
            "expected_shortfall_cvar_95_usd": 15000.0,
            "expected_shortfall_cvar_99_usd": 20000.0,
        }
        ok, cat, _ = validate_gold_var_record(valid)
        assert ok is True
        assert cat is None

        # Confidence inversion (VaR 99 < VaR 95)
        inverted = dict(valid, parametric_var_99_usd=10000.0)
        ok, cat, _ = validate_gold_var_record(inverted)
        assert ok is False
        assert cat == "VAR_CONFIDENCE_INVARIANT_VIOLATION"

        # Expected Shortfall < VaR violation
        bad_cvar = dict(valid, expected_shortfall_cvar_99_usd=15000.0)
        ok, cat, _ = validate_gold_var_record(bad_cvar)
        assert ok is False
        assert cat == "CVAR_SUBADDITIVITY_VIOLATION"

        # Invalid horizon
        bad_horizon = dict(valid, horizon_days=0)
        ok, cat, _ = validate_gold_var_record(bad_horizon)
        assert ok is False
        assert cat == "INVALID_HORIZON"

    def test_validate_gold_feature_record_invariants(self) -> None:
        """Verify feature record validator catches invalid prices, volatilities, and liquidity bounds."""
        valid_feat = {
            "feature_key": "PORT-1#AAPL#2026-10-29",
            "portfolio_id": "PORT-1",
            "ticker": "AAPL",
            "asset_class": "EQUITY",
            "mark_to_market_price": 155.0,
            "position_notional_usd": 155000.0,
            "realized_volatility_daily": 0.018,
            "liquidity_score": 92.5,
        }
        ok, cat, _ = validate_gold_feature_record(valid_feat)
        assert ok is True

        bad_liq = dict(valid_feat, liquidity_score=120.0)
        ok, cat, _ = validate_gold_feature_record(bad_liq)
        assert ok is False
        assert cat == "LIQUIDITY_SCORE_OUT_OF_BOUNDS"

        bad_mtm = dict(valid_feat, mark_to_market_price=0.0)
        ok, cat, _ = validate_gold_feature_record(bad_mtm)
        assert ok is False
        assert cat == "NON_POSITIVE_MTM_PRICE"


# =============================================================================
# 4. QUANTITATIVE VALUE-AT-RISK & EXPECTED SHORTFALL INVARIANTS
# =============================================================================

class TestQuantitativeVaREngine:
    """Validate mathematical properties of Parametric, Historical, and Monte Carlo VaR."""

    @staticmethod
    def _sample_multi_asset_orders() -> List[Dict[str, Any]]:
        return [
            {
                "order_id": "ORD-1",
                "portfolio_id": "PORT-MACRO",
                "ticker": "AAPL",
                "side": "BUY",
                "quantity": 1000.0,
                "price": 150.0,
                "order_type": "LIMIT",
                "execution_status": "FILLED",
                "is_aggressive": False,
            },
            {
                "order_id": "ORD-2",
                "portfolio_id": "PORT-MACRO",
                "ticker": "SPY",
                "side": "BUY",
                "quantity": 500.0,
                "price": 500.0,
                "order_type": "MARKET",
                "execution_status": "FILLED",
                "is_aggressive": True,
            },
            {
                "order_id": "ORD-3",
                "portfolio_id": "PORT-MACRO",
                "ticker": "BTCUSD",
                "side": "BUY",
                "quantity": 2.0,
                "price": 60000.0,
                "order_type": "MARKET",
                "execution_status": "FILLED",
                "is_aggressive": True,
            },
            {
                "order_id": "ORD-4",
                "portfolio_id": "PORT-MACRO",
                "ticker": "EURUSD",
                "side": "BUY",
                "quantity": 100000.0,
                "price": 1.10,
                "order_type": "LIMIT",
                "execution_status": "FILLED",
                "is_aggressive": False,
            },
        ]

    @staticmethod
    def _sample_market_ticks() -> List[Dict[str, Any]]:
        return [
            {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 155.0, "relative_spread_bps": 2.5, "volume": 1000, "sequence_number": 1},
            {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 158.0, "relative_spread_bps": 2.5, "volume": 1200, "sequence_number": 2},
            {"ticker": "SPY", "asset_class": "ETF", "last_price": 505.0, "relative_spread_bps": 1.5, "volume": 5000, "sequence_number": 1},
            {"ticker": "BTCUSD", "asset_class": "CRYPTO", "last_price": 62000.0, "relative_spread_bps": 12.0, "volume": 15, "sequence_number": 1},
            {"ticker": "EURUSD", "asset_class": "FX", "last_price": 1.12, "relative_spread_bps": 1.0, "volume": 50000, "sequence_number": 1},
        ]

    def test_var_confidence_ordering_cvar_and_euler_decomposition(self) -> None:
        """Verify VaR_99 > VaR_95 > 0, CVaR >= VaR, Diversification > 0%, and Euler Component VaR sum."""
        engine = DeltaGoldVaREngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            var_path = Path(tmp_dir) / "gold_portfolio_var"
            feat_path = Path(tmp_dir) / "gold_risk_feature_store"

            res = engine.compute_var_and_feature_store(
                orders_source=self._sample_multi_asset_orders(),
                ticks_source=self._sample_market_ticks(),
                var_target_path=var_path,
                feature_store_target_path=feat_path,
                valuation_date=date(2026, 10, 29),
                horizon_days=1,
                monte_carlo_paths=3000,
                var_limit_usd=500000.0,
            )

            assert res.portfolios_evaluated == 1
            assert res.positions_evaluated == 4
            assert res.var_breaches_detected == 0

            port_var = engine.get_latest_portfolio_var(var_path, "PORT-MACRO")
            assert port_var is not None
            assert port_var["total_positions"] == 4
            assert port_var["parametric_var_99_usd"] > port_var["parametric_var_95_usd"] > 0.0
            assert port_var["historical_var_99_usd"] > port_var["historical_var_95_usd"] > 0.0
            assert port_var["monte_carlo_var_99_usd"] > port_var["monte_carlo_var_95_usd"] > 0.0
            assert port_var["expected_shortfall_cvar_95_usd"] >= port_var["monte_carlo_var_95_usd"]
            assert port_var["expected_shortfall_cvar_99_usd"] >= port_var["monte_carlo_var_99_usd"]
            assert port_var["diversification_benefit_pct"] > 0.0
            assert port_var["risk_regime"] == "LOW_RISK"

            # Verify Euler Component VaR theorem: sum(component_var_95) == parametric_var_95
            features = engine.get_portfolio_features(feat_path, "PORT-MACRO")
            assert len(features) == 4
            comp_var_sum = sum(f["component_var_95_usd"] for f in features)
            assert comp_var_sum == pytest.approx(port_var["parametric_var_95_usd"], rel=1e-3)

            # Verify portfolio weights sum to 100%
            weight_sum = sum(f["portfolio_weight_pct"] for f in features)
            assert weight_sum == pytest.approx(100.0, rel=1e-4)

    def test_horizon_square_root_of_time_scaling(self) -> None:
        """Verify 10-day Parametric VaR scales by sqrt(10) relative to 1-day Parametric VaR."""
        engine = DeltaGoldVaREngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            var_1d_path = Path(tmp_dir) / "var_1d"
            var_10d_path = Path(tmp_dir) / "var_10d"
            feat_path = Path(tmp_dir) / "feat"

            engine.compute_var_and_feature_store(
                orders_source=self._sample_multi_asset_orders(),
                ticks_source=self._sample_market_ticks(),
                var_target_path=var_1d_path,
                feature_store_target_path=feat_path,
                horizon_days=1,
            )
            engine.compute_var_and_feature_store(
                orders_source=self._sample_multi_asset_orders(),
                ticks_source=self._sample_market_ticks(),
                var_target_path=var_10d_path,
                feature_store_target_path=feat_path,
                horizon_days=10,
            )

            v1 = engine.get_latest_portfolio_var(var_1d_path, "PORT-MACRO")
            v10 = engine.get_latest_portfolio_var(var_10d_path, "PORT-MACRO")
            assert v1 is not None and v10 is not None
            assert v10["parametric_var_95_usd"] == pytest.approx(
                v1["parametric_var_95_usd"] * math.sqrt(10.0), rel=1e-4
            )
            assert v10["parametric_var_99_usd"] == pytest.approx(
                v1["parametric_var_99_usd"] * math.sqrt(10.0), rel=1e-4
            )

    def test_var_limit_breach_and_critical_regime(self) -> None:
        """Verify VaR limit breach triggers CRITICAL_BREACH regime and increments breach counter."""
        engine = DeltaGoldVaREngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            var_path = Path(tmp_dir) / "var_breach"
            feat_path = Path(tmp_dir) / "feat_breach"

            res = engine.compute_var_and_feature_store(
                orders_source=self._sample_multi_asset_orders(),
                ticks_source=self._sample_market_ticks(),
                var_target_path=var_path,
                feature_store_target_path=feat_path,
                var_limit_usd=1000.0,  # Tight limit guaranteed to breach
            )
            assert res.var_breaches_detected == 1
            port_var = engine.get_latest_portfolio_var(var_path, "PORT-MACRO")
            assert port_var is not None
            assert port_var["is_var_breach_99"] is True
            assert port_var["risk_regime"] == "CRITICAL_BREACH"


# =============================================================================
# 5. FEATURE STORE POINT-IN-TIME LOOKUP & DELTA MERGE UPSERTS
# =============================================================================

class TestFeatureStoreAndDeltaMerge:
    """Validate Feature Store lookups, Delta MERGE INTO upserts, and OPTIMIZE/VACUUM."""

    def test_feature_vector_lookup_and_delta_merge_upsert(self) -> None:
        """Verify first run writes version 0 (WRITE) and second run merges version 1 (MERGE)."""
        engine = DeltaGoldVaREngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            var_path = Path(tmp_dir) / "gold_var"
            feat_path = Path(tmp_dir) / "gold_features"

            orders = [
                {
                    "order_id": "O-1",
                    "portfolio_id": "PORT-EQ",
                    "ticker": "AAPL",
                    "side": "BUY",
                    "quantity": 200.0,
                    "price": 150.0,
                    "order_type": "MARKET",
                    "execution_status": "FILLED",
                    "is_aggressive": True,
                }
            ]
            ticks = [
                {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 160.0, "relative_spread_bps": 4.0, "volume": 500}
            ]

            # Run 1: Initial WRITE
            r1 = engine.compute_var_and_feature_store(
                orders_source=orders,
                ticks_source=ticks,
                var_target_path=var_path,
                feature_store_target_path=feat_path,
                valuation_date=date(2026, 10, 29),
            )
            assert r1.var_commit_version == 0
            assert r1.var_operation_type == "WRITE"
            assert r1.feature_store_commit_version == 0
            assert r1.feature_operation_type == "WRITE"

            aapl_feat = engine.get_feature_vector(feat_path, "PORT-EQ", "AAPL")
            assert aapl_feat is not None
            assert aapl_feat["net_quantity"] == 200.0
            assert aapl_feat["mark_to_market_price"] == 160.0
            assert aapl_feat["unrealized_pnl_usd"] == 2000.0  # 200 * (160 - 150)
            assert aapl_feat["aggressive_order_ratio"] == 1.0
            assert aapl_feat["order_imbalance_ratio"] == 1.0

            # Run 2: Upsert with updated price (MERGE)
            ticks_updated = [
                {"ticker": "AAPL", "asset_class": "EQUITY", "last_price": 170.0, "relative_spread_bps": 3.0, "volume": 800}
            ]
            r2 = engine.compute_var_and_feature_store(
                orders_source=orders,
                ticks_source=ticks_updated,
                var_target_path=var_path,
                feature_store_target_path=feat_path,
                valuation_date=date(2026, 10, 29),
            )
            assert r2.var_commit_version == 1
            assert r2.var_operation_type == "MERGE"
            assert r2.feature_store_commit_version == 1
            assert r2.feature_operation_type == "MERGE"

            aapl_feat_v2 = engine.get_feature_vector(feat_path, "PORT-EQ", "AAPL")
            assert aapl_feat_v2 is not None
            assert aapl_feat_v2["mark_to_market_price"] == 170.0
            assert aapl_feat_v2["unrealized_pnl_usd"] == 4000.0

            # Optimize & Vacuum
            opt = engine.optimize_table(var_path, z_order_columns=["portfolio_id", "valuation_timestamp"])
            assert opt["operation"] == "OPTIMIZE"
            assert opt["version"] == 2

            vac = engine.vacuum_table(var_path, retention_hours=168)
            assert vac["operation"] == "VACUUM"
            assert vac["version"] == 3


# =============================================================================
# 6. END-TO-END MEDALLION PIPELINE (BRONZE -> SILVER -> GOLD)
# =============================================================================

class TestEndToEndMedallionPipeline:
    """Validate complete Bronze -> Silver -> Gold Lakehouse execution."""

    def test_full_bronze_to_silver_to_gold_pipeline(self) -> None:
        """Ingest raw orders/ticks/vwap into Bronze, cleanse into Silver, and compute Gold VaR."""
        bronze = DeltaBronzeIngestionEngine()
        silver = DeltaSilverCleansingEngine()
        gold = DeltaGoldVaREngine()

        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            b_orders, b_ticks, b_vwap = base / "b_orders", base / "b_ticks", base / "b_vwap"
            s_orders, s_ticks, s_vwap = base / "s_orders", base / "s_ticks", base / "s_vwap"
            g_var, g_feat = base / "g_var", base / "g_feat"

            raw_orders = [
                {
                    "order_id": "ORD-100",
                    "portfolio_id": "PORT-INST",
                    "ticker": "NVDA",
                    "side": "BUY",
                    "quantity": 400.0,
                    "price": 120.0,
                    "order_type": "LIMIT",
                    "timestamp": "2026-10-29T10:00:00Z",
                    "notional_usd": 48000.0,
                },
                {
                    "order_id": "ORD-101",
                    "portfolio_id": "PORT-INST",
                    "ticker": "MSFT",
                    "side": "BUY",
                    "quantity": 200.0,
                    "price": 410.0,
                    "order_type": "MARKET",
                    "timestamp": "2026-10-29T10:01:00Z",
                    "notional_usd": 82000.0,
                },
            ]
            raw_ticks = [
                {
                    "ticker": "NVDA",
                    "bid": 124.9,
                    "ask": 125.1,
                    "last_price": 125.0,
                    "volume": 2000.0,
                    "timestamp": "2026-10-29T10:02:00Z",
                    "asset_class": "EQUITY",
                    "sequence_number": 1,
                },
                {
                    "ticker": "MSFT",
                    "bid": 414.8,
                    "ask": 415.2,
                    "last_price": 415.0,
                    "volume": 1500.0,
                    "timestamp": "2026-10-29T10:02:00Z",
                    "asset_class": "EQUITY",
                    "sequence_number": 1,
                },
            ]
            raw_vwap = [
                {
                    "ticker": "NVDA",
                    "asset_class": "EQUITY",
                    "window_start": "2026-10-29T10:00:00Z",
                    "window_end": "2026-10-29T10:05:00Z",
                    "tick_count": 50,
                    "open_price": 122.0,
                    "high_price": 126.0,
                    "low_price": 121.5,
                    "close_price": 125.0,
                    "total_volume": 10000.0,
                    "total_notional": 1240000.0,
                    "vwap": 124.0,
                    "price_stddev": 1.2,
                    "price_variance": 1.44,
                    "volatility_multiplier": 2.0,
                    "upper_band": 126.4,
                    "lower_band": 121.6,
                    "bandwidth_bps": 387.1,
                    "percent_b": 0.708,
                    "is_upper_breach": False,
                    "is_lower_breach": False,
                    "volatility_spike": False,
                }
            ]

            # Step 1: Bronze
            bronze.ingest_trade_orders(raw_orders, b_orders)
            bronze.ingest_market_ticks(raw_ticks, b_ticks)
            bronze.ingest_rolling_vwap(raw_vwap, b_vwap)

            # Step 2: Silver
            silver.cleanse_trade_orders(b_orders, s_orders)
            silver.cleanse_market_ticks(b_ticks, s_ticks)
            silver.cleanse_rolling_vwap(b_vwap, s_vwap)

            # Step 3: Gold
            g_res = gold.compute_var_and_feature_store(
                orders_source=s_orders,
                ticks_source=s_ticks,
                vwap_source=s_vwap,
                var_target_path=g_var,
                feature_store_target_path=g_feat,
                valuation_date=date(2026, 10, 29),
            )
            assert g_res.portfolios_evaluated == 1
            assert g_res.positions_evaluated == 2

            nvda_feat = gold.get_feature_vector(g_feat, "PORT-INST", "NVDA")
            assert nvda_feat is not None
            # MTM = 125.0, VWAP = 124.0 -> vwap_deviation_bps = ((125 - 124) / 124) * 10000 = 80.6452 bps
            assert nvda_feat["vwap_deviation_bps"] == pytest.approx(80.6452, abs=0.01)


# =============================================================================
# 7. GOLD NOTEBOOK RUNNER CLI & TELEMETRY
# =============================================================================

class TestGoldNotebookRunnerAndTelemetry:
    """Validate CLI runner arguments, dry-run mode, full execution, and Prometheus metrics."""

    def test_cli_parse_args_and_dry_run(self) -> None:
        """Verify CLI flag parsing and dry-run plan output."""
        args = parse_args([
            "--environment", "prod",
            "--valuation-date", "2026-10-29",
            "--horizon-days", "5",
            "--monte-carlo-simulations", "5000",
            "--var-limit-usd", "1000000.0",
            "--dry-run",
        ])
        assert args.environment == "prod"
        assert args.horizon_days == 5
        assert args.monte_carlo_simulations == 5000
        assert args.dry_run is True

        plan = run_gold_var_pipeline(args)
        assert plan["status"] == "DRY_RUN_SUCCESS"
        assert plan["horizon_days"] == 5

    def test_cli_full_execution_and_prometheus_telemetry(self) -> None:
        """Verify full CLI runner execution writes Gold Delta tables and emits Prometheus metrics."""
        silver = DeltaSilverCleansingEngine()
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            s_orders = base / "silver_trade_orders"
            s_ticks = base / "silver_market_ticks"
            g_var = base / "gold_portfolio_var"
            g_feat = base / "gold_risk_feature_store"

            silver.cleanse_trade_orders(
                source=[
                    {
                        "order_id": "ORD-CLI-1",
                        "portfolio_id": "PORT-CLI",
                        "ticker": "AAPL",
                        "side": "BUY",
                        "quantity": 100.0,
                        "price": 150.0,
                        "order_type": "LIMIT",
                        "timestamp": "2026-10-29T10:00:00Z",
                    }
                ],
                target_path=s_orders,
            )
            silver.cleanse_market_ticks(
                source=[
                    {
                        "ticker": "AAPL",
                        "asset_class": "EQUITY",
                        "bid": 151.9,
                        "ask": 152.1,
                        "last_price": 152.0,
                        "volume": 500.0,
                        "timestamp": "2026-10-29T10:01:00Z",
                        "sequence_number": 1,
                    }
                ],
                target_path=s_ticks,
            )

            args = parse_args([
                "--environment", "dev",
                "--valuation-date", "2026-10-29",
                "--orders-source-path", str(s_orders),
                "--ticks-source-path", str(s_ticks),
                "--var-target-path", str(g_var),
                "--feature-store-target-path", str(g_feat),
                "--monte-carlo-simulations", "500",
                "--run-optimize", "true",
                "--run-vacuum", "true",
            ])
            out = run_gold_var_pipeline(args)
            assert out["status"] == "SUCCESS"
            assert out["result"]["portfolios_evaluated"] == 1
            assert "databricks_gold_portfolios_evaluated_total 1" in out["telemetry_metrics"]
            assert "databricks_gold_feature_vectors_written_total 1" in out["telemetry_metrics"]
