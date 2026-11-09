"""Trading Risk Engine: Executive Portfolio Case Study Configuration.

Defines configuration models, portfolio book specifications, quantitative thresholds,
incident injection scenarios, and containment action policies for the portfolio case study.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class IncidentType(str, Enum):
    """Types of market and operational risk incidents simulated in the case study."""

    FLASH_CRASH_DRAWDOWN = "FLASH_CRASH_DRAWDOWN"
    SINGLE_ORDER_FAT_FINGER = "SINGLE_ORDER_FAT_FINGER"
    POISON_PILL_CORRUPTION = "POISON_PILL_CORRUPTION"
    CONCENTRATED_SELL_PRESSURE = "CONCENTRATED_SELL_PRESSURE"
    VAR_LIMIT_BREACH = "VAR_LIMIT_BREACH"


class ContainmentStatus(str, Enum):
    """Operational status of automated incident containment."""

    PENDING = "PENDING"
    TRIGGERED = "TRIGGERED"
    CONTAINED = "CONTAINED"
    ESCALATED = "ESCALATED"
    RESOLVED = "RESOLVED"


@dataclass
class PortfolioBookSpec:
    """Specification of an institutional trading book / mandate."""

    book_id: str
    desk_name: str
    mandate_currency: str = "USD"
    initial_nav_usd: float = 125_000_000.0
    var_95_limit_usd: float = 3_750_000.0  # 3.0% of NAV
    var_99_limit_usd: float = 6_250_000.0  # 5.0% of NAV
    max_single_order_notional_usd: float = 5_000_000.0
    max_rolling_drawdown_pct: float = 5.0  # 5% max 5-min drop
    primary_tickers: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "book_id": self.book_id,
            "desk_name": self.desk_name,
            "mandate_currency": self.mandate_currency,
            "initial_nav_usd": self.initial_nav_usd,
            "var_95_limit_usd": self.var_95_limit_usd,
            "var_99_limit_usd": self.var_99_limit_usd,
            "max_single_order_notional_usd": self.max_single_order_notional_usd,
            "max_rolling_drawdown_pct": self.max_rolling_drawdown_pct,
            "primary_tickers": list(self.primary_tickers),
        }


# Standard institutional portfolio book catalog ($500M total AUM across 4 trading desks)
DEFAULT_PORTFOLIO_BOOKS: List[PortfolioBookSpec] = [
    PortfolioBookSpec(
        book_id="PORTFOLIO_ALPHA",
        desk_name="Tech & High-Beta Quantitative Equities",
        initial_nav_usd=150_000_000.0,
        var_95_limit_usd=4_500_000.0,
        var_99_limit_usd=7_500_000.0,
        max_single_order_notional_usd=5_000_000.0,
        max_rolling_drawdown_pct=5.0,
        primary_tickers=["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA"],
    ),
    PortfolioBookSpec(
        book_id="PORTFOLIO_BETA",
        desk_name="Macro Liquid Arbitrage & Currencies",
        initial_nav_usd=150_000_000.0,
        var_95_limit_usd=3_000_000.0,
        var_99_limit_usd=5_000_000.0,
        max_single_order_notional_usd=10_000_000.0,
        max_rolling_drawdown_pct=3.0,
        primary_tickers=["SPY", "QQQ", "EURUSD", "GBPUSD", "USDJPY"],
    ),
    PortfolioBookSpec(
        book_id="PORTFOLIO_GAMMA",
        desk_name="Global Commodities & Energy Arbitrage",
        initial_nav_usd=100_000_000.0,
        var_95_limit_usd=3_000_000.0,
        var_99_limit_usd=5_000_000.0,
        max_single_order_notional_usd=5_000_000.0,
        max_rolling_drawdown_pct=4.0,
        primary_tickers=["XAUUSD", "BRENT"],
    ),
    PortfolioBookSpec(
        book_id="PORTFOLIO_OMEGA",
        desk_name="Digital Assets & Crypto Systematic",
        initial_nav_usd=100_000_000.0,
        var_95_limit_usd=5_000_000.0,
        var_99_limit_usd=8_000_000.0,
        max_single_order_notional_usd=2_500_000.0,
        max_rolling_drawdown_pct=8.0,
        primary_tickers=["BTCUSD", "ETHUSD"],
    ),
]


@dataclass
class IncidentScenarioSpec:
    """Specification of a risk incident scenario to be simulated."""

    incident_id: str
    incident_type: IncidentType
    book_id: str
    ticker: str
    severity: str  # "CRITICAL", "HIGH", "MEDIUM"
    description: str
    simulated_at_offset_sec: float
    trigger_threshold: float
    observed_value: float
    expected_containment_sla_ms: float = 250.0  # Max acceptable SLA for automated containment

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "incident_type": self.incident_type.value,
            "book_id": self.book_id,
            "ticker": self.ticker,
            "severity": self.severity,
            "description": self.description,
            "simulated_at_offset_sec": self.simulated_at_offset_sec,
            "trigger_threshold": self.trigger_threshold,
            "observed_value": self.observed_value,
            "expected_containment_sla_ms": self.expected_containment_sla_ms,
        }


@dataclass
class CaseStudyConfig:
    """Master configuration for the portfolio case study simulation."""

    scenario_name: str = "Multi-Asset Flash Crash & Rogue Trader Incident"
    books: List[PortfolioBookSpec] = field(default_factory=lambda: list(DEFAULT_PORTFOLIO_BOOKS))
    flash_crash_ticker: str = "NVDA"
    flash_crash_drop_pct: float = 7.5
    fat_finger_notional_usd: float = 6_250_000.0
    fat_finger_ticker: str = "AAPL"
    fat_finger_trader_id: str = "TRADER_ALEX"
    poison_pill_count: int = 5
    seed: int = 42
    output_dir: Optional[str] = None

    @property
    def total_initial_nav_usd(self) -> float:
        return sum(b.initial_nav_usd for b in self.books)

    def get_book(self, book_id: str) -> Optional[PortfolioBookSpec]:
        for b in self.books:
            if b.book_id == book_id:
                return b
        return None
