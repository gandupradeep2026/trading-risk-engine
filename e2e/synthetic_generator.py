"""Trading Risk Engine: Synthetic Data Generation Engine for End-to-End Testing.

Generates correlated, multi-asset synthetic streams of market ticks, trade order executions,
quantitative market microstructure anomalies (flash crash, fat finger), and corrupted
records destined for Dead-Letter Queues (DLQ), alongside deterministic reconciliation baselines.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import random
from typing import Any, Dict, List, Optional, Tuple

from e2e.config import E2ERunnerConfig, ExecutionScenario
from ingestion.market_tick import MarketTick
from ingestion.pubsub_client import TradeOrder


@dataclass
class SyntheticE2EDataBatch:
    """Container for all synthetic records and expected reconciliation invariants."""

    target_date: str
    market_ticks: List[MarketTick]
    corrupted_ticks: List[Dict[str, Any]]
    trade_orders: List[TradeOrder]
    corrupted_orders: List[Dict[str, Any]]
    flash_crash_orders: List[TradeOrder]
    fat_finger_orders: List[TradeOrder]
    portfolio_limits: List[Dict[str, Any]]

    # Deterministic ground-truth reconciliation invariants
    expected_valid_orders_count: int = 0
    expected_dlq_orders_count: int = 0
    expected_valid_ticks_count: int = 0
    expected_dlq_ticks_count: int = 0
    expected_total_notional_usd: float = 0.0
    expected_flash_crash_alerts_count: int = 0
    expected_fat_finger_alerts_count: int = 0

    @property
    def all_orders(self) -> List[TradeOrder]:
        """All valid trade orders including normal flow, flash crash, and fat finger."""
        return self.trade_orders + self.flash_crash_orders + self.fat_finger_orders

    @property
    def total_injected_orders(self) -> int:
        return len(self.all_orders) + len(self.corrupted_orders)

    @property
    def total_injected_ticks(self) -> int:
        return len(self.market_ticks) + len(self.corrupted_ticks)


class SyntheticE2EGenerator:
    """Generates deterministic, correlated multi-tier synthetic test data."""

    BASE_INSTRUMENTS = [
        ("AAPL", "EQUITY", 200.0, 0.02, 100),
        ("MSFT", "EQUITY", 420.0, 0.03, 100),
        ("GOOGL", "EQUITY", 175.0, 0.02, 100),
        ("NVDA", "EQUITY", 130.0, 0.02, 100),
        ("SPY", "ETF", 550.0, 0.01, 100),
        ("EURUSD", "FX", 1.0850, 0.0001, 100000),
        ("BTCUSD", "CRYPTO", 65000.0, 5.0, 1),
        ("XAUUSD", "COMMODITY", 2650.0, 0.20, 100),
    ]

    PORTFOLIOS = ["PORTFOLIO_ALPHA", "PORTFOLIO_BETA", "PORTFOLIO_GAMMA", "PORTFOLIO_OMEGA"]
    TRADERS = ["TRADER_ALEX", "TRADER_SARAH", "TRADER_MICHAEL", "TRADER_ELENA", "TRADER_DAVID"]

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self._rng = random.Random(seed)

    def generate(self, config: Optional[E2ERunnerConfig] = None) -> SyntheticE2EDataBatch:
        """Generate comprehensive synthetic data batch based on runner configuration."""
        cfg = config or E2ERunnerConfig()
        scale = max(0.1, cfg.scale_factor)
        t_date = cfg.target_date

        self._rng.seed(self.seed)

        base_time = datetime.strptime(f"{t_date}T09:30:00", "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

        # 1. Generate Valid Market Ticks
        ticks_per_inst = max(5, int(15 * scale))
        market_ticks: List[MarketTick] = []
        seq = 1000

        for ticker, asset_class, base_price, spread_base, default_lot in self.BASE_INSTRUMENTS:
            price = base_price
            for i in range(ticks_per_inst):
                drift = (self._rng.random() - 0.49) * 0.002 * price
                price = max(0.01, round(price + drift, 4 if asset_class == "FX" else 2))
                spread = max(0.0001, round(spread_base * (1.0 + self._rng.random() * 0.2), 4 if asset_class == "FX" else 2))
                bid = max(0.01, round(price - spread / 2.0, 4 if asset_class == "FX" else 2))
                ask = max(bid + 0.0001, round(price + spread / 2.0, 4 if asset_class == "FX" else 2))
                size = int(default_lot * (1 + self._rng.randint(1, 5)))
                tick_time = base_time + timedelta(seconds=i * 10 + self._rng.randint(0, 5))

                seq += 1
                tick = MarketTick.create(
                    ticker=ticker,
                    asset_class=asset_class,
                    bid=bid,
                    ask=ask,
                    last_price=price,
                    volume=float(size),
                    sequence_number=seq,
                    timestamp=tick_time.isoformat(),
                )
                market_ticks.append(tick)

        # 2. Generate Valid Trade Orders
        num_clean_orders = max(10, int(30 * scale))
        trade_orders: List[TradeOrder] = []
        total_clean_notional = 0.0

        for i in range(num_clean_orders):
            inst = self._rng.choice(self.BASE_INSTRUMENTS)
            ticker, asset_class, base_price, _, default_lot = inst
            portfolio = self._rng.choice(self.PORTFOLIOS)
            trader = self._rng.choice(self.TRADERS)
            side = self._rng.choice(["BUY", "SELL"])
            order_type = self._rng.choice(["MARKET", "LIMIT"])

            qty = default_lot * self._rng.randint(1, 10)
            price = round(base_price * (1.0 + (self._rng.random() - 0.5) * 0.01), 2)
            notional = round(price * qty, 2)
            total_clean_notional += notional

            order_time = base_time + timedelta(seconds=i * 20 + self._rng.randint(1, 15))
            seq += 1
            order = TradeOrder.create(
                order_id=f"ORD-E2E-{seq}",
                trader_id=trader,
                portfolio_id=portfolio,
                ticker=ticker,
                asset_class=asset_class,
                side=side,
                order_type=order_type,
                price=price,
                quantity=qty,
                sequence_number=seq,
                timestamp=order_time.isoformat(),
            )
            trade_orders.append(order)

        # 3. Anomaly Scenarios: Flash Crash & Fat Finger
        flash_crash_orders: List[TradeOrder] = []
        expected_flash_crash = 0
        if cfg.inject_flash_crash and cfg.scenario in (ExecutionScenario.ANOMALY_INJECTION, ExecutionScenario.FULL_E2E):
            expected_flash_crash = 1
            # 5 consecutive falling orders on AAPL starting from $200 down to $185 (7.5% drop)
            crash_prices = [200.0, 196.0, 192.0, 188.0, 185.0]
            crash_time = base_time + timedelta(minutes=15)
            for j, p in enumerate(crash_prices):
                seq += 1
                qty = 500
                notional = round(p * qty, 2)
                total_clean_notional += notional
                fo = TradeOrder.create(
                    order_id=f"ORD-CRASH-{seq}",
                    trader_id="TRADER_ALEX",
                    portfolio_id="PORTFOLIO_ALPHA",
                    ticker="AAPL",
                    asset_class="EQUITY",
                    side="SELL",
                    order_type="MARKET",
                    price=p,
                    quantity=qty,
                    sequence_number=seq,
                    timestamp=(crash_time + timedelta(seconds=j * 10)).isoformat(),
                )
                flash_crash_orders.append(fo)

        fat_finger_orders: List[TradeOrder] = []
        expected_fat_finger = 0
        if cfg.inject_fat_finger and cfg.scenario in (ExecutionScenario.ANOMALY_INJECTION, ExecutionScenario.FULL_E2E):
            expected_fat_finger = 1
            # Single huge order: NVDA price=500.0, qty=12500 -> $6,250,000 (> $5,000,000 limit)
            seq += 1
            ff_price = 500.0
            ff_qty = 12500
            ff_notional = round(ff_price * ff_qty, 2)
            total_clean_notional += ff_notional
            ff_order = TradeOrder.create(
                order_id=f"ORD-FATFINGER-{seq}",
                trader_id="TRADER_DAVID",
                portfolio_id="PORTFOLIO_BETA",
                ticker="NVDA",
                asset_class="EQUITY",
                side="BUY",
                order_type="LIMIT",
                price=ff_price,
                quantity=ff_qty,
                sequence_number=seq,
                timestamp=(base_time + timedelta(minutes=25)).isoformat(),
            )
            fat_finger_orders.append(ff_order)

        # 4. Corrupted Injections for Dead-Letter Queues (DLQ)
        corrupted_orders: List[Dict[str, Any]] = []
        if cfg.inject_corrupted_orders and cfg.scenario in (ExecutionScenario.CORRUPTED_DLQ, ExecutionScenario.FULL_E2E):
            corrupted_orders = [
                {
                    "order_id": "ORD-CORRUPT-001",
                    "portfolio_id": "PORTFOLIO_ALPHA",
                    "price": -150.0,  # Negative price
                    "quantity": 100,
                    "side": "BUY",
                    "ticker": "AAPL",
                    "error_type": "NON_POSITIVE_PRICE",
                },
                {
                    "order_id": "ORD-CORRUPT-002",
                    "portfolio_id": "PORTFOLIO_BETA",
                    "price": 100.0,
                    "quantity": -50,  # Negative quantity
                    "side": "BUY",
                    "ticker": "MSFT",
                    "error_type": "NON_POSITIVE_QUANTITY",
                },
                {
                    "order_id": "ORD-CORRUPT-003",
                    "portfolio_id": "PORTFOLIO_GAMMA",
                    "price": 250.0,
                    "quantity": 100,
                    "side": "HOLD",  # Invalid side
                    "ticker": "TSLA",
                    "error_type": "INVALID_SIDE",
                },
                {
                    "raw_corrupted_payload": "{invalid_json_missing_quotes: true",
                    "error_type": "JSON_DECODE_ERROR",
                },
            ]

        corrupted_ticks: List[Dict[str, Any]] = []
        if cfg.inject_corrupted_ticks and cfg.scenario in (ExecutionScenario.CORRUPTED_DLQ, ExecutionScenario.FULL_E2E):
            corrupted_ticks = [
                {
                    "ticker": "AAPL",
                    "bid_price": 210.0,
                    "ask_price": 200.0,  # Crossed quote: ask < bid
                    "last_volume": 100,
                    "error_type": "CROSSED_QUOTE",
                },
                {
                    "ticker": "MSFT",
                    "bid_price": -420.0,  # Negative price
                    "ask_price": 420.0,
                    "last_volume": 100,
                    "error_type": "NEGATIVE_PRICE",
                },
                {
                    "ticker": "GOOGL",
                    "bid_price": 175.0,
                    "ask_price": 175.5,
                    "last_volume": -500,  # Negative volume
                    "error_type": "NEGATIVE_VOLUME",
                },
            ]

        # 5. Portfolio Mandate Limits for Cloud SQL replication
        portfolio_limits = [
            {
                "limit_id": "LIM-ALPHA-01",
                "portfolio_id": "PORTFOLIO_ALPHA",
                "asset_class": "EQUITY",
                "max_notional_usd": 10_000_000.0,
                "max_drawdown_pct": 5.0,
                "max_var_99_usd": 500_000.0,
                "is_active": True,
                "updated_at": f"{t_date}T00:00:00",
            },
            {
                "limit_id": "LIM-BETA-01",
                "portfolio_id": "PORTFOLIO_BETA",
                "asset_class": "EQUITY",
                "max_notional_usd": 15_000_000.0,
                "max_drawdown_pct": 4.0,
                "max_var_99_usd": 750_000.0,
                "is_active": True,
                "updated_at": f"{t_date}T00:00:00",
            },
            {
                "limit_id": "LIM-GAMMA-01",
                "portfolio_id": "PORTFOLIO_GAMMA",
                "asset_class": "MULTI_ASSET",
                "max_notional_usd": 25_000_000.0,
                "max_drawdown_pct": 6.0,
                "max_var_99_usd": 1_200_000.0,
                "is_active": True,
                "updated_at": f"{t_date}T00:00:00",
            },
        ]

        expected_valid_orders = len(trade_orders) + len(flash_crash_orders) + len(fat_finger_orders)

        return SyntheticE2EDataBatch(
            target_date=t_date,
            market_ticks=market_ticks,
            corrupted_ticks=corrupted_ticks,
            trade_orders=trade_orders,
            corrupted_orders=corrupted_orders,
            flash_crash_orders=flash_crash_orders,
            fat_finger_orders=fat_finger_orders,
            portfolio_limits=portfolio_limits,
            expected_valid_orders_count=expected_valid_orders,
            expected_dlq_orders_count=len(corrupted_orders),
            expected_valid_ticks_count=len(market_ticks),
            expected_dlq_ticks_count=len(corrupted_ticks),
            expected_total_notional_usd=round(total_clean_notional, 2),
            expected_flash_crash_alerts_count=expected_flash_crash,
            expected_fat_finger_alerts_count=expected_fat_finger,
        )
