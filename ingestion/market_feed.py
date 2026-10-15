"""Trading Risk Engine - Multi-Asset Market Data Feed & Simulation Generator.

Provides:
- Realistic quantitative price simulation via Geometric Brownian Motion with
  Ornstein-Uhlenbeck mean-reversion drift and bid-ask spread dynamics.
- Comprehensive multi-asset universe (Equities, ETFs, FX, Crypto, Commodities).
- Thread-safe per-ticker state management with strictly monotonic sequence numbers.
- Controlled synthetic anomaly/corruption generation for Dead-Letter Queue (DLQ) validation.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import random
import threading
from typing import Any, Dict, List, Optional, Set

from ingestion.market_tick import ALLOWED_ASSET_CLASSES, MarketTick


@dataclass(frozen=True)
class TickerProfile:
    """Quantitative parameters defining a financial instrument's price dynamics."""

    ticker: str
    asset_class: str
    base_price: float
    volatility: float  # Annualized volatility parameter (sigma)
    mean_reversion: float  # Mean-reversion rate toward base_price (theta)
    spread_bps: float  # Typical bid-ask spread in basis points (1 bp = 0.0001)
    min_tick_size: float = 0.01
    avg_volume: float = 100.0
    price_decimals: int = 2

    def __post_init__(self) -> None:
        if self.asset_class not in ALLOWED_ASSET_CLASSES:
            raise ValueError(
                f"Invalid asset_class '{self.asset_class}'. Allowed: {sorted(ALLOWED_ASSET_CLASSES)}"
            )
        if self.base_price <= 0:
            raise ValueError(f"base_price must be > 0, got {self.base_price}")
        if self.volatility < 0:
            raise ValueError(f"volatility must be non-negative, got {self.volatility}")
        if self.spread_bps <= 0:
            raise ValueError(f"spread_bps must be > 0, got {self.spread_bps}")
        if self.min_tick_size <= 0:
            raise ValueError(f"min_tick_size must be > 0, got {self.min_tick_size}")


# Multi-Asset Production Universe Reference Catalog
DEFAULT_MARKET_UNIVERSE: Dict[str, TickerProfile] = {
    # Equities (US Large Cap)
    "AAPL": TickerProfile("AAPL", "EQUITY", base_price=185.00, volatility=0.22, mean_reversion=0.15, spread_bps=1.5, min_tick_size=0.01, avg_volume=250.0, price_decimals=2),
    "MSFT": TickerProfile("MSFT", "EQUITY", base_price=420.00, volatility=0.20, mean_reversion=0.15, spread_bps=1.5, min_tick_size=0.01, avg_volume=200.0, price_decimals=2),
    "GOOGL": TickerProfile("GOOGL", "EQUITY", base_price=175.00, volatility=0.24, mean_reversion=0.18, spread_bps=2.0, min_tick_size=0.01, avg_volume=180.0, price_decimals=2),
    "AMZN": TickerProfile("AMZN", "EQUITY", base_price=180.00, volatility=0.25, mean_reversion=0.18, spread_bps=2.0, min_tick_size=0.01, avg_volume=190.0, price_decimals=2),
    "NVDA": TickerProfile("NVDA", "EQUITY", base_price=125.00, volatility=0.45, mean_reversion=0.25, spread_bps=2.5, min_tick_size=0.01, avg_volume=450.0, price_decimals=2),
    "TSLA": TickerProfile("TSLA", "EQUITY", base_price=240.00, volatility=0.48, mean_reversion=0.30, spread_bps=3.0, min_tick_size=0.01, avg_volume=300.0, price_decimals=2),
    "JPM": TickerProfile("JPM", "EQUITY", base_price=200.00, volatility=0.18, mean_reversion=0.12, spread_bps=1.8, min_tick_size=0.01, avg_volume=150.0, price_decimals=2),
    "GS": TickerProfile("GS", "EQUITY", base_price=460.00, volatility=0.20, mean_reversion=0.12, spread_bps=2.0, min_tick_size=0.01, avg_volume=120.0, price_decimals=2),
    # Index ETFs
    "SPY": TickerProfile("SPY", "ETF", base_price=550.00, volatility=0.14, mean_reversion=0.10, spread_bps=0.8, min_tick_size=0.01, avg_volume=500.0, price_decimals=2),
    "QQQ": TickerProfile("QQQ", "ETF", base_price=480.00, volatility=0.18, mean_reversion=0.12, spread_bps=1.0, min_tick_size=0.01, avg_volume=400.0, price_decimals=2),
    # Foreign Exchange (G10 FX)
    "EURUSD": TickerProfile("EURUSD", "FX", base_price=1.0850, volatility=0.08, mean_reversion=0.10, spread_bps=0.6, min_tick_size=0.0001, avg_volume=1000.0, price_decimals=4),
    "GBPUSD": TickerProfile("GBPUSD", "FX", base_price=1.2950, volatility=0.09, mean_reversion=0.10, spread_bps=0.8, min_tick_size=0.0001, avg_volume=800.0, price_decimals=4),
    "USDJPY": TickerProfile("USDJPY", "FX", base_price=152.30, volatility=0.10, mean_reversion=0.10, spread_bps=0.8, min_tick_size=0.01, avg_volume=900.0, price_decimals=2),
    # Digital Assets / Crypto
    "BTCUSD": TickerProfile("BTCUSD", "CRYPTO", base_price=67500.00, volatility=0.60, mean_reversion=0.20, spread_bps=4.0, min_tick_size=0.10, avg_volume=50.0, price_decimals=2),
    "ETHUSD": TickerProfile("ETHUSD", "CRYPTO", base_price=3500.00, volatility=0.65, mean_reversion=0.22, spread_bps=5.0, min_tick_size=0.05, avg_volume=100.0, price_decimals=2),
    # Commodities
    "XAUUSD": TickerProfile("XAUUSD", "COMMODITY", base_price=2650.00, volatility=0.16, mean_reversion=0.10, spread_bps=1.2, min_tick_size=0.10, avg_volume=80.0, price_decimals=2),
    "BRENT": TickerProfile("BRENT", "COMMODITY", base_price=78.50, volatility=0.30, mean_reversion=0.15, spread_bps=2.5, min_tick_size=0.01, avg_volume=200.0, price_decimals=2),
}


class TickerSimulator:
    """Thread-safe stateful market price simulator for a single financial ticker."""

    def __init__(self, profile: TickerProfile, seed: Optional[int] = None) -> None:
        self.profile = profile
        self._rng = random.Random(seed)
        self._lock = threading.Lock()
        self.current_price: float = profile.base_price
        self.sequence_number: int = 0
        self.last_tick_time: Optional[datetime] = None

    def generate_tick(
        self,
        dt_seconds: float = 0.1,
        timestamp: Optional[str] = None,
    ) -> MarketTick:
        """Simulate next market tick using Geometric Brownian Motion with mean-reversion drift."""
        with self._lock:
            self.sequence_number += 1
            seq = self.sequence_number

            # dt in annual terms (assuming 252 trading days, 6.5 hours/day)
            trading_seconds_per_year = 252.0 * 6.5 * 3600.0
            dt = max(1e-6, dt_seconds / trading_seconds_per_year)

            # Mean-reverting drift: theta * (base_price - S) / base_price
            drift = self.profile.mean_reversion * (
                (self.profile.base_price - self.current_price) / self.profile.base_price
            )

            # Standard normal random draw
            z = self._rng.gauss(0.0, 1.0)
            vol = self.profile.volatility

            # Geometric Brownian Motion step
            ret = (drift - 0.5 * vol * vol) * dt + vol * math.sqrt(dt) * z
            new_price = self.current_price * math.exp(ret)

            # Keep within reasonable bounds (e.g. within 20% to 500% of base)
            min_allowed = max(self.profile.min_tick_size, self.profile.base_price * 0.2)
            max_allowed = self.profile.base_price * 5.0
            new_price = max(min_allowed, min(max_allowed, new_price))
            self.current_price = new_price

            decimals = self.profile.price_decimals
            mid = round(new_price, decimals)

            # Calculate spread in currency units based on spread_bps
            spread_raw = max(
                self.profile.min_tick_size,
                new_price * (self.profile.spread_bps * 1e-4),
            )
            spread = max(self.profile.min_tick_size, round(spread_raw, decimals))
            half_spread = round(spread / 2.0, decimals)
            if half_spread <= 0:
                half_spread = self.profile.min_tick_size

            bid = round(mid - half_spread, decimals)
            # Ensure bid is strictly positive
            if bid <= 0:
                bid = self.profile.min_tick_size
            ask = round(bid + spread, decimals)
            if ask <= bid:
                ask = round(bid + self.profile.min_tick_size, decimals)

            # Last traded price falls within the spread
            trade_offset = self._rng.uniform(0.0, 1.0)
            last_price = round(bid + trade_offset * (ask - bid), decimals)
            # Guarantee last_price is within [bid, ask] and > 0
            last_price = max(bid, min(ask, last_price))

            # Realistic trade volume (log-normal distribution)
            vol_mean = math.log(max(1.0, self.profile.avg_volume))
            volume_raw = self._rng.lognormvariate(vol_mean, 0.5)
            volume = max(1.0, round(volume_raw, 2))

            ts = timestamp or datetime.now(timezone.utc).isoformat()
            self.last_tick_time = datetime.now(timezone.utc)

            return MarketTick.create(
                ticker=self.profile.ticker,
                asset_class=self.profile.asset_class,
                bid=bid,
                ask=ask,
                last_price=last_price,
                volume=volume,
                timestamp=ts,
                sequence_number=seq,
            )

    def generate_corrupt_tick(
        self,
        corruption_type: str = "crossed",
        timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Generate intentionally invalid tick payload for testing dead-letter queue behavior."""
        with self._lock:
            self.sequence_number += 1
            seq = self.sequence_number

        ts = timestamp or datetime.now(timezone.utc).isoformat()
        base_dict = {
            "trade_id": f"corrupt-{seq}",
            "ticker": self.profile.ticker,
            "asset_class": self.profile.asset_class,
            "bid": round(self.profile.base_price, 2),
            "ask": round(self.profile.base_price + 0.10, 2),
            "last_price": round(self.profile.base_price + 0.05, 2),
            "volume": 100.0,
            "timestamp": ts,
            "sequence_number": seq,
        }

        if corruption_type == "crossed":
            # Crossed quote: bid strictly greater than ask
            base_dict["bid"] = round(self.profile.base_price + 10.0, 2)
            base_dict["ask"] = round(self.profile.base_price, 2)
        elif corruption_type == "negative_price":
            base_dict["bid"] = -10.0
            base_dict["last_price"] = -5.0
        elif corruption_type == "negative_volume":
            base_dict["volume"] = -50.0
        elif corruption_type == "unknown_asset":
            base_dict["asset_class"] = "DERIVATIVE_UNKNOWN"
        elif corruption_type == "malformed_json":
            # Returns a raw string representation of invalid JSON
            return {"raw_malformed_string": '{"ticker": "' + self.profile.ticker + '", invalid_json}}'}
        else:
            base_dict["bid"] = 0.0

        return base_dict


class MarketFeedGenerator:
    """Manages multi-ticker simulators and generates continuous multi-asset feeds."""

    def __init__(
        self,
        tickers: Optional[List[str]] = None,
        custom_profiles: Optional[Dict[str, TickerProfile]] = None,
        seed: Optional[int] = None,
    ) -> None:
        self.profiles: Dict[str, TickerProfile] = dict(DEFAULT_MARKET_UNIVERSE)
        if custom_profiles:
            self.profiles.update(custom_profiles)

        selected_tickers = tickers or list(self.profiles.keys())
        self.simulators: Dict[str, TickerSimulator] = {}
        for idx, sym in enumerate(selected_tickers):
            sym_upper = sym.strip().upper()
            if sym_upper not in self.profiles:
                # Default fallback profile as EQUITY
                self.profiles[sym_upper] = TickerProfile(
                    ticker=sym_upper,
                    asset_class="EQUITY",
                    base_price=100.0,
                    volatility=0.25,
                    mean_reversion=0.15,
                    spread_bps=2.0,
                )
            sub_seed = (seed + idx) if seed is not None else None
            self.simulators[sym_upper] = TickerSimulator(
                self.profiles[sym_upper], seed=sub_seed
            )

        self._ticker_list: List[str] = sorted(self.simulators.keys())
        self._rng = random.Random(seed)
        self._lock = threading.Lock()

    @property
    def tickers(self) -> List[str]:
        """List of active tickers configured in this feed."""
        return list(self._ticker_list)

    def get_profile(self, ticker: str) -> Optional[TickerProfile]:
        """Retrieve profile specification for a ticker."""
        return self.profiles.get(ticker.strip().upper())

    def simulate_tick(self, ticker: Optional[str] = None) -> MarketTick:
        """Generate next simulated tick for a specific ticker or uniformly random ticker."""
        if ticker:
            sym = ticker.strip().upper()
            if sym not in self.simulators:
                raise KeyError(f"Ticker '{ticker}' not found in active feed universe.")
            return self.simulators[sym].generate_tick()

        with self._lock:
            sym = self._rng.choice(self._ticker_list)
        return self.simulators[sym].generate_tick()

    def simulate_corrupt_tick(
        self, ticker: Optional[str] = None, corruption_type: str = "crossed"
    ) -> Dict[str, Any]:
        """Generate intentional corrupt tick for DLQ verification."""
        if ticker:
            sym = ticker.strip().upper()
            if sym not in self.simulators:
                raise KeyError(f"Ticker '{ticker}' not found in active feed universe.")
            return self.simulators[sym].generate_corrupt_tick(corruption_type=corruption_type)

        with self._lock:
            sym = self._rng.choice(self._ticker_list)
        return self.simulators[sym].generate_corrupt_tick(corruption_type=corruption_type)
