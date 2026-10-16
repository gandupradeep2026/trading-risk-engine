"""Trading Risk Engine - Institutional Multi-Asset Trade Order Execution Feed.

Provides:
- Realistic institutional trade order flow generator simulating electronic execution gateways.
- Multi-asset coverage (Equities, ETFs, FX, Crypto, Commodities).
- Realistic pricing models, lot sizes, order types (MARKET, LIMIT, STOP_LOSS).
- Thread-safe strictly monotonic sequence numbering per (portfolio_id, ticker).
- Synthetic anomaly and corruption injection for Dead-Letter Queue (DLQ) testing.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import random
import threading
from typing import Dict, List, Optional, Tuple
import uuid

from ingestion.pubsub_client import TradeOrder


@dataclass
class AssetProfile:
    """Pricing and volume boundaries for a given financial ticker."""

    ticker: str
    asset_class: str
    base_price: float
    min_qty: float
    max_qty: float
    price_decimals: int
    lot_step: float


DEFAULT_ORDER_UNIVERSE: Dict[str, AssetProfile] = {
    # Equities
    "AAPL": AssetProfile("AAPL", "EQUITY", 185.50, 10.0, 500.0, 2, 1.0),
    "MSFT": AssetProfile("MSFT", "EQUITY", 420.25, 5.0, 250.0, 2, 1.0),
    "GOOGL": AssetProfile("GOOGL", "EQUITY", 175.80, 10.0, 400.0, 2, 1.0),
    "NVDA": AssetProfile("NVDA", "EQUITY", 880.00, 5.0, 300.0, 2, 1.0),
    "AMZN": AssetProfile("AMZN", "EQUITY", 180.10, 10.0, 400.0, 2, 1.0),
    "TSLA": AssetProfile("TSLA", "EQUITY", 170.40, 10.0, 600.0, 2, 1.0),
    "JPM": AssetProfile("JPM", "EQUITY", 195.60, 15.0, 450.0, 2, 1.0),
    "GS": AssetProfile("GS", "EQUITY", 440.30, 5.0, 200.0, 2, 1.0),
    # ETFs
    "SPY": AssetProfile("SPY", "ETF", 515.20, 20.0, 1000.0, 2, 1.0),
    "QQQ": AssetProfile("QQQ", "ETF", 442.80, 20.0, 800.0, 2, 1.0),
    # FX
    "EURUSD": AssetProfile("EURUSD", "FX", 1.0850, 25000.0, 500000.0, 5, 1000.0),
    "GBPUSD": AssetProfile("GBPUSD", "FX", 1.2820, 20000.0, 400000.0, 5, 1000.0),
    "USDJPY": AssetProfile("USDJPY", "FX", 155.40, 25000.0, 500000.0, 3, 1000.0),
    # Crypto
    "BTCUSD": AssetProfile("BTCUSD", "CRYPTO", 64500.00, 0.05, 3.5, 2, 0.01),
    "ETHUSD": AssetProfile("ETHUSD", "CRYPTO", 3450.00, 0.5, 25.0, 2, 0.1),
    # Commodities
    "XAUUSD": AssetProfile("XAUUSD", "COMMODITY", 2380.50, 5.0, 100.0, 2, 1.0),
    "BRENT": AssetProfile("BRENT", "COMMODITY", 84.75, 50.0, 1500.0, 2, 10.0),
}

DEFAULT_PORTFOLIOS = [
    "PORT-EQUITY-ALPHA",
    "PORT-GLOBAL-MACRO",
    "PORT-VOL-ARB",
    "PORT-FX-FLOW",
    "PORT-TECH-GROWTH",
]

DEFAULT_TRADERS = [
    "ALGO-MOMENTUM-01",
    "ALGO-VWAP-02",
    "ALGO-STATARB-03",
    "DESK-EXEC-01",
    "DESK-FLOW-02",
]


class OrderFeedGenerator:
    """Thread-safe multi-asset trade order generator."""

    def __init__(
        self,
        universe: Optional[Dict[str, AssetProfile]] = None,
        portfolios: Optional[List[str]] = None,
        traders: Optional[List[str]] = None,
        seed: Optional[int] = None,
    ) -> None:
        self.universe = universe or DEFAULT_ORDER_UNIVERSE
        self.portfolios = portfolios or DEFAULT_PORTFOLIOS
        self.traders = traders or DEFAULT_TRADERS
        self._rnd = random.Random(seed)
        self._lock = threading.Lock()
        self._sequence_map: Dict[Tuple[str, str], int] = {}

    def _next_sequence(self, portfolio_id: str, ticker: str) -> int:
        """Atomically increment and return the monotonic sequence number."""
        with self._lock:
            key = (portfolio_id, ticker)
            curr = self._sequence_map.get(key, 0) + 1
            self._sequence_map[key] = curr
            return curr

    def generate_order(
        self,
        ticker: Optional[str] = None,
        portfolio_id: Optional[str] = None,
        side: Optional[str] = None,
        order_type: Optional[str] = None,
        corrupt: bool = False,
    ) -> TradeOrder:
        """Generate a realistic trade order execution record."""
        target_ticker = ticker or self._rnd.choice(list(self.universe.keys()))
        profile = self.universe.get(target_ticker)
        if not profile:
            raise KeyError(f"Unknown ticker: {target_ticker}")

        target_portfolio = portfolio_id or self._rnd.choice(self.portfolios)
        target_side = side or self._rnd.choice(["BUY", "SELL"])

        # Weighted order type distribution
        if not order_type:
            roll = self._rnd.random()
            if roll < 0.65:
                target_order_type = "MARKET"
            elif roll < 0.95:
                target_order_type = "LIMIT"
            else:
                target_order_type = "STOP_LOSS"
        else:
            target_order_type = order_type

        # Generate realistic price with +/- 1.5% jitter around base price
        price_jitter = self._rnd.uniform(-0.015, 0.015)
        price = round(profile.base_price * (1.0 + price_jitter), profile.price_decimals)

        # Generate realistic quantity aligned with lot step
        raw_qty = self._rnd.uniform(profile.min_qty, profile.max_qty)
        qty_steps = round(raw_qty / profile.lot_step)
        quantity = max(profile.min_qty, round(qty_steps * profile.lot_step, 4))

        trader_id = self._rnd.choice(self.traders)
        seq = self._next_sequence(target_portfolio, target_ticker)

        if corrupt:
            # Inject synthetic corruption for DLQ routing test paths
            corruption_type = self._rnd.choice(["negative_price", "negative_qty", "invalid_side", "invalid_type"])
            if corruption_type == "negative_price":
                price = -abs(price)
            elif corruption_type == "negative_qty":
                quantity = -abs(quantity)
            elif corruption_type == "invalid_side":
                target_side = "CORRUPT_SIDE"
            else:
                target_order_type = "UNKNOWN_TYPE"

            # Create unvalidated instance directly
            return TradeOrder(
                order_id=str(uuid.uuid4()),
                portfolio_id=target_portfolio,
                ticker=target_ticker,
                asset_class=profile.asset_class,
                side=target_side,
                order_type=target_order_type,
                quantity=quantity,
                price=price,
                notional_usd=round(quantity * price, 2),
                timestamp=datetime.now(timezone.utc).isoformat(),
                trader_id=trader_id,
                execution_status="REJECTED",
                sequence_number=seq,
            )

        return TradeOrder.create(
            portfolio_id=target_portfolio,
            ticker=target_ticker,
            asset_class=profile.asset_class,
            side=target_side,
            order_type=target_order_type,
            quantity=quantity,
            price=price,
            trader_id=trader_id,
            execution_status="FILLED",
            sequence_number=seq,
        )

    def generate_batch(self, size: int, corrupt_ratio: float = 0.0) -> List[TradeOrder]:
        """Generate a batch of orders with an optional proportion of corrupt orders."""
        orders: List[TradeOrder] = []
        for _ in range(size):
            is_corrupt = (self._rnd.random() < corrupt_ratio) if corrupt_ratio > 0 else False
            orders.append(self.generate_order(corrupt=is_corrupt))
        return orders
