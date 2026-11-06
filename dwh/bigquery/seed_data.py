"""Deterministic Financial Seed Data Generator for BigQuery Star Schema (Day 22).

Generates high-fidelity master dimension records (tickers, portfolios, traders,
calendar dates) and realistic synthetic fact records (trades, ticks, risk stats, alerts)
for local testing, CI validation, and DWH verification.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
import decimal
import hashlib
import json
from typing import Any, Dict, List, Optional
import uuid


def get_dim_tickers_records() -> List[Dict[str, Any]]:
    """Master financial instrument dimension seed dataset."""
    now = datetime(2026, 1, 1, 0, 0, 0)
    now_iso = now.isoformat()

    instruments = [
        # Equities
        ("AAPL", "Apple Inc.", "EQUITY", "NASDAQ", "USD", "Technology", "Consumer Electronics", 1, decimal.Decimal("0.01")),
        ("MSFT", "Microsoft Corp.", "EQUITY", "NASDAQ", "USD", "Technology", "Software Infrastructure", 1, decimal.Decimal("0.01")),
        ("GOOGL", "Alphabet Inc.", "EQUITY", "NASDAQ", "USD", "Technology", "Internet Content & Information", 1, decimal.Decimal("0.01")),
        ("AMZN", "Amazon.com Inc.", "EQUITY", "NASDAQ", "USD", "Consumer Cyclical", "Internet Retail", 1, decimal.Decimal("0.01")),
        ("NVDA", "NVIDIA Corp.", "EQUITY", "NASDAQ", "USD", "Technology", "Semiconductors", 1, decimal.Decimal("0.01")),
        ("TSLA", "Tesla Inc.", "EQUITY", "NASDAQ", "USD", "Consumer Cyclical", "Auto Manufacturers", 1, decimal.Decimal("0.01")),
        ("JPM", "JPMorgan Chase & Co.", "EQUITY", "NYSE", "USD", "Financials", "Diversified Banks", 1, decimal.Decimal("0.01")),
        ("GS", "Goldman Sachs Group", "EQUITY", "NYSE", "USD", "Financials", "Capital Markets", 1, decimal.Decimal("0.01")),
        # ETFs
        ("SPY", "SPDR S&P 500 ETF Trust", "ETF", "NYSE", "USD", "Broad Market", "Large Cap Index", 1, decimal.Decimal("0.01")),
        ("QQQ", "Invesco QQQ Trust Series 1", "ETF", "NASDAQ", "USD", "Technology", "Nasdaq 100 Index", 1, decimal.Decimal("0.01")),
        # FX Spot
        ("EURUSD", "Euro / US Dollar", "FX", "FOREX", "USD", "Currencies", "Major Pairs", 100000, decimal.Decimal("0.0001")),
        ("GBPUSD", "British Pound / US Dollar", "FX", "FOREX", "USD", "Currencies", "Major Pairs", 100000, decimal.Decimal("0.0001")),
        ("USDJPY", "US Dollar / Japanese Yen", "FX", "FOREX", "JPY", "Currencies", "Major Pairs", 100000, decimal.Decimal("0.01")),
        # Crypto
        ("BTCUSD", "Bitcoin / US Dollar", "CRYPTO", "BINANCE", "USD", "Digital Assets", "Layer 1 Currency", 1, decimal.Decimal("0.50")),
        ("ETHUSD", "Ethereum / US Dollar", "CRYPTO", "BINANCE", "USD", "Digital Assets", "Smart Contract Platform", 1, decimal.Decimal("0.10")),
        # Commodities
        ("XAUUSD", "Gold Ounce / US Dollar", "COMMODITY", "CME", "USD", "Precious Metals", "Spot Physical", 100, decimal.Decimal("0.10")),
        ("BRENT", "Brent Crude Oil", "COMMODITY", "CME", "USD", "Energy", "Petroleum Futures", 1000, decimal.Decimal("0.01")),
    ]

    records = []
    for ticker, name, aclass, exchange, ccy, sector, industry, lot, tick in instruments:
        records.append({
            "ticker_key": f"{aclass}#{ticker}",
            "ticker": ticker,
            "ticker_name": name,
            "asset_class": aclass,
            "exchange": exchange,
            "currency": ccy,
            "sector": sector,
            "industry": industry,
            "lot_size": lot,
            "tick_size": float(tick),
            "is_active": True,
            "valid_from": now_iso,
            "valid_to": None,
            "created_at": now_iso,
            "updated_at": now_iso,
        })
    return records


def get_dim_portfolios_records() -> List[Dict[str, Any]]:
    """Master portfolio dimension seed dataset."""
    now = datetime(2026, 1, 1, 0, 0, 0).isoformat()

    portfolios = [
        ("PORTFOLIO_ALPHA", "Global Equity Quantitative Alpha", "EQUITY_DERIVATIVES", "Dr. Marcus Vance", decimal.Decimal("10000000.00"), decimal.Decimal("500000.00"), decimal.Decimal("1000000.00"), "USD", "STAT_ARB"),
        ("PORTFOLIO_BETA", "Macro Commodities & Energy Trend", "COMMODITIES_MACRO", "Sarah Connor", decimal.Decimal("8000000.00"), decimal.Decimal("400000.00"), decimal.Decimal("800000.00"), "USD", "TREND_FOLLOWING"),
        ("PORTFOLIO_GAMMA", "G10 FX Spot & Liquid Arbitrage", "FX_SPOT", "Michael Chang", decimal.Decimal("15000000.00"), decimal.Decimal("750000.00"), decimal.Decimal("1500000.00"), "USD", "MARKET_MAKING"),
        ("PORTFOLIO_DELTA", "Digital Asset High-Frequency Arbitrage", "CRYPTO_ARB", "Elena Rostova", decimal.Decimal("5000000.00"), decimal.Decimal("250000.00"), decimal.Decimal("500000.00"), "USD", "STAT_ARB"),
        ("PORTFOLIO_OMEGA", "Multi-Asset Strategic Value Preservation", "EQUITY_DERIVATIVES", "David Vance", decimal.Decimal("20000000.00"), decimal.Decimal("1000000.00"), decimal.Decimal("2000000.00"), "USD", "MACRO_VALUE"),
    ]

    records = []
    for pid, name, desk, pm, rlimit, dlimit, vlimit, ccy, strat in portfolios:
        records.append({
            "portfolio_key": f"PORTFOLIO#{pid}",
            "portfolio_id": pid,
            "portfolio_name": name,
            "desk": desk,
            "portfolio_manager": pm,
            "risk_limit_usd": float(rlimit),
            "daily_loss_limit_usd": float(dlimit),
            "max_var_limit_usd": float(vlimit),
            "base_currency": ccy,
            "strategy_type": strat,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        })
    return records


def get_dim_traders_records() -> List[Dict[str, Any]]:
    """Master trading personnel dimension seed dataset."""
    now = datetime(2026, 1, 1, 0, 0, 0).isoformat()

    traders = [
        ("TRADER_ALEX", "Alex Mercer", "EQUITY_DERIVATIVES", "SENIOR_TRADER", decimal.Decimal("2000000.00"), "alex.mercer@tradingrisk.internal"),
        ("TRADER_SARAH", "Sarah Connor", "COMMODITIES_MACRO", "PORTFOLIO_MANAGER", decimal.Decimal("3000000.00"), "sarah.connor@tradingrisk.internal"),
        ("TRADER_MICHAEL", "Michael Chang", "FX_SPOT", "SENIOR_TRADER", decimal.Decimal("4000000.00"), "michael.chang@tradingrisk.internal"),
        ("TRADER_ELENA", "Elena Rostova", "CRYPTO_ARB", "QUANT_RESEARCHER", decimal.Decimal("1000000.00"), "elena.rostova@tradingrisk.internal"),
        ("TRADER_DAVID", "David Vance", "EQUITY_DERIVATIVES", "EXECUTION_ALGO", decimal.Decimal("5000000.00"), "algo.execution@tradingrisk.internal"),
    ]

    records = []
    for tid, name, desk, role, max_order, email in traders:
        records.append({
            "trader_key": f"TRADER#{tid}",
            "trader_id": tid,
            "trader_name": name,
            "desk": desk,
            "role": role,
            "max_single_order_usd": float(max_order),
            "email": email,
            "is_active": True,
            "created_at": now,
        })
    return records


def get_dim_date_records(
    start_date: date = date(2026, 1, 1), end_date: date = date(2026, 12, 31)
) -> List[Dict[str, Any]]:
    """Financial trading calendar dimension records for the year."""
    # US Federal & Exchange holidays 2026
    us_holidays = {
        date(2026, 1, 1),   # New Year's Day
        date(2026, 1, 19),  # MLK Day
        date(2026, 2, 16),  # Presidents Day
        date(2026, 4, 3),   # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 6, 19),  # Juneteenth
        date(2026, 7, 3),   # Independence Day observed
        date(2026, 9, 7),   # Labor Day
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),  # Christmas
    }

    records = []
    curr = start_date
    while curr <= end_date:
        dkey = int(curr.strftime("%Y%m%d"))
        is_wknd = curr.weekday() in (5, 6)
        is_hol = curr in us_holidays
        is_trading = not (is_wknd or is_hol)
        quarter = (curr.month - 1) // 3 + 1

        records.append({
            "date_key": dkey,
            "calendar_date": curr.isoformat(),
            "year": curr.year,
            "quarter": quarter,
            "month": curr.month,
            "month_name": curr.strftime("%B"),
            "day_of_month": curr.day,
            "day_of_week": curr.isoweekday(),
            "day_name": curr.strftime("%A"),
            "week_of_year": curr.isocalendar()[1],
            "is_weekend": is_wknd,
            "is_trading_day": is_trading,
            "is_us_holiday": is_hol,
            "fiscal_year": curr.year,
            "fiscal_quarter": quarter,
        })
        curr += timedelta(days=1)
    return records


def generate_mock_trade_orders(
    count: int = 50, trade_date: str = "2026-11-02"
) -> List[Dict[str, Any]]:
    """Generate realistic financial trade orders fact records for testing."""
    tickers = ["AAPL", "MSFT", "GOOGL", "NVDA", "SPY", "QQQ", "EURUSD", "BTCUSD", "XAUUSD"]
    portfolios = ["PORTFOLIO_ALPHA", "PORTFOLIO_BETA", "PORTFOLIO_GAMMA", "PORTFOLIO_DELTA"]
    traders = ["TRADER_ALEX", "TRADER_SARAH", "TRADER_MICHAEL", "TRADER_ELENA", "TRADER_DAVID"]
    sides = ["BUY", "SELL"]
    order_types = ["MARKET", "LIMIT"]

    prices = {
        "AAPL": 225.50, "MSFT": 420.00, "GOOGL": 175.25, "NVDA": 135.00,
        "SPY": 570.00, "QQQ": 490.00, "EURUSD": 1.0850, "BTCUSD": 68500.00, "XAUUSD": 2720.00
    }
    asset_classes = {
        "AAPL": "EQUITY", "MSFT": "EQUITY", "GOOGL": "EQUITY", "NVDA": "EQUITY",
        "SPY": "ETF", "QQQ": "ETF", "EURUSD": "FX", "BTCUSD": "CRYPTO", "XAUUSD": "COMMODITY"
    }

    base_date = date.fromisoformat(trade_date)
    dkey = int(base_date.strftime("%Y%m%d"))
    records = []

    for i in range(count):
        sym = tickers[i % len(tickers)]
        base_price = prices[sym]
        qty = (i + 1) * 10 if sym != "BTCUSD" else (i + 1) * 0.5
        price = round(base_price * (1.0 + (i % 7 - 3) * 0.002), 4)
        notional = round(qty * price, 2)
        side = sides[i % 2]
        exec_status = "FILLED" if i % 10 != 0 else "PARTIALLY_FILLED"

        order_id = f"ORD-{trade_date.replace('-', '')}-{i+1:05d}"
        exec_time = datetime(base_date.year, base_date.month, base_date.day, 9 + (i // 10) % 7, (i * 7) % 60, (i * 13) % 60)

        record_hash = hashlib.sha256(f"{order_id}#{sym}#{notional}".encode()).hexdigest()

        records.append({
            "trade_order_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, order_id)),
            "order_id": order_id,
            "trade_date": trade_date,
            "execution_timestamp": exec_time.isoformat(),
            "date_key": dkey,
            "ticker": sym,
            "portfolio_id": portfolios[i % len(portfolios)],
            "trader_id": traders[i % len(traders)],
            "asset_class": asset_classes[sym],
            "side": side,
            "order_type": order_types[i % len(order_types)],
            "execution_status": exec_status,
            "quantity": float(qty),
            "price": float(price),
            "notional_usd": float(notional),
            "fee_usd": round(float(notional) * 0.0002, 2),
            "slippage_bps": round(float((i % 5) * 0.8), 2),
            "is_aggressive": (i % 2 == 0),
            "sequence_number": i + 1,
            "_cleansed_record_hash": record_hash,
            "_ingested_at": exec_time.isoformat(),
        })

    return records


def generate_mock_market_ticks(
    count: int = 100, tick_date: str = "2026-11-02"
) -> List[Dict[str, Any]]:
    """Generate granular market tick fact records."""
    tickers = ["AAPL", "MSFT", "GOOGL", "SPY", "EURUSD", "BTCUSD"]
    asset_classes = {
        "AAPL": "EQUITY", "MSFT": "EQUITY", "GOOGL": "EQUITY",
        "SPY": "ETF", "EURUSD": "FX", "BTCUSD": "CRYPTO"
    }
    base_prices = {"AAPL": 225.0, "MSFT": 420.0, "GOOGL": 175.0, "SPY": 570.0, "EURUSD": 1.085, "BTCUSD": 68500.0}

    base_d = date.fromisoformat(tick_date)
    dkey = int(base_d.strftime("%Y%m%d"))
    records = []

    for i in range(count):
        sym = tickers[i % len(tickers)]
        mid = base_prices[sym] + (i % 11 - 5) * 0.1
        spread = round(mid * 0.0004, 4)
        bid = round(mid - spread / 2, 4)
        ask = round(mid + spread / 2, 4)
        vol = 100.0 if sym != "BTCUSD" else 1.5
        notional = round(vol * mid, 2)
        spread_bps = round((spread / mid) * 10000, 2)

        tick_time = datetime(base_d.year, base_d.month, base_d.day, 10, (i // 2) % 60, (i * 17) % 60)
        tick_id = f"TICK-{tick_date.replace('-', '')}-{i+1:06d}"
        raw_hash = hashlib.sha256(f"{tick_id}#{sym}#{mid}".encode()).hexdigest()

        records.append({
            "tick_id": tick_id,
            "tick_date": tick_date,
            "event_timestamp": tick_time.isoformat(),
            "date_key": dkey,
            "ticker": sym,
            "asset_class": asset_classes[sym],
            "bid_price": float(bid),
            "ask_price": float(ask),
            "mid_price": float(mid),
            "spread": float(spread),
            "relative_spread_bps": float(spread_bps),
            "last_price": float(mid),
            "volume": float(vol),
            "notional_usd": float(notional),
            "sequence_number": i + 1,
            "is_crossed_quote": False,
            "_raw_record_hash": raw_hash,
            "_ingested_at": tick_time.isoformat(),
        })

    return records


def generate_mock_risk_stats(
    valuation_date: str = "2026-11-02"
) -> List[Dict[str, Any]]:
    """Generate portfolio risk stats fact records across confidence levels."""
    portfolios = [
        ("PORTFOLIO_ALPHA", 10000000.0, 1000000.0, "LOW_RISK", 420000.0, 410000.0, 425000.0, 510000.0),
        ("PORTFOLIO_BETA", 8000000.0, 800000.0, "MODERATE_RISK", 380000.0, 395000.0, 390000.0, 470000.0),
        ("PORTFOLIO_GAMMA", 15000000.0, 1500000.0, "LOW_RISK", 520000.0, 510000.0, 530000.0, 640000.0),
        ("PORTFOLIO_DELTA", 5000000.0, 500000.0, "ELEVATED_RISK", 390000.0, 410000.0, 400000.0, 490000.0),
        ("PORTFOLIO_OMEGA", 20000000.0, 2000000.0, "LOW_RISK", 750000.0, 740000.0, 760000.0, 920000.0),
    ]

    base_d = date.fromisoformat(valuation_date)
    dkey = int(base_d.strftime("%Y%m%d"))
    vtime = datetime(base_d.year, base_d.month, base_d.day, 17, 0, 0).isoformat()
    records = []

    for idx, (pid, gross_limit, var_limit, regime, pvar, hvar, mcvar, cvar) in enumerate(portfolios):
        for conf in [0.95, 0.99]:
            mult = 1.0 if conf == 0.95 else 1.414
            pvar_adj = round(pvar * mult, 2)
            hvar_adj = round(hvar * mult, 2)
            mcvar_adj = round(mcvar * mult, 2)
            cvar_adj = round(cvar * mult, 2)
            utilization = round((pvar_adj / var_limit) * 100, 2)

            records.append({
                "risk_stat_id": f"RISK-{valuation_date.replace('-', '')}-{pid}-{int(conf*100)}",
                "valuation_date": valuation_date,
                "valuation_timestamp": vtime,
                "date_key": dkey,
                "portfolio_id": pid,
                "risk_regime": regime,
                "confidence_level": conf,
                "horizon_days": 1,
                "parametric_var_usd": pvar_adj,
                "historical_var_usd": hvar_adj,
                "monte_carlo_var_usd": mcvar_adj,
                "cvar_usd": cvar_adj,
                "portfolio_limit_usd": gross_limit,
                "var_limit_usd": var_limit,
                "limit_utilization_pct": utilization,
                "is_limit_breached": (utilization > 100.0),
                "gross_exposure_usd": round(gross_limit * 0.65, 2),
                "net_exposure_usd": round(gross_limit * 0.12, 2),
                "unrealized_pnl_usd": round(35000.0 * (idx - 2), 2),
                "diversification_benefit_pct": 18.5,
                "hhi_concentration": 0.145,
                "num_positions": 24,
                "_source_batch_id": f"batch-gold-{valuation_date}",
                "_calculated_at": vtime,
            })

    return records


def generate_mock_risk_alerts(
    count: int = 10, alert_date: str = "2026-11-02"
) -> List[Dict[str, Any]]:
    """Generate operational streaming risk alerts fact records."""
    portfolios = ["PORTFOLIO_ALPHA", "PORTFOLIO_BETA", "PORTFOLIO_DELTA"]
    tickers = ["AAPL", "NVDA", "BTCUSD", "XAUUSD"]
    alert_types = [
        ("FLASH_CRASH_DRAWDOWN", "CRITICAL", "rolling_5min_drawdown_pct", 5.0, 6.2),
        ("SINGLE_ORDER_FAT_FINGER", "WARNING", "single_order_notional_usd", 2500000.0, 3100000.0),
        ("NOTIONAL_EXPOSURE_BREACH", "CRITICAL", "cumulative_5min_notional_usd", 5000000.0, 5800000.0),
        ("CONCENTRATED_SELL_PRESSURE", "WARNING", "sell_side_volume_concentration_pct", 85.0, 91.5),
    ]

    base_d = date.fromisoformat(alert_date)
    dkey = int(base_d.strftime("%Y%m%d"))
    records = []

    for i in range(count):
        atype, sev, metric, thresh, obs = alert_types[i % len(alert_types)]
        pid = portfolios[i % len(portfolios)]
        sym = tickers[i % len(tickers)]
        atime = datetime(base_d.year, base_d.month, base_d.day, 11, (i * 5) % 60, (i * 11) % 60).isoformat()
        alert_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{alert_date}-{i}-{atype}"))

        payload = json.dumps({
            "alert_id": alert_id,
            "portfolio_id": pid,
            "ticker": sym,
            "observed_value": obs,
            "threshold_value": thresh,
            "timestamp": atime,
        })

        records.append({
            "alert_id": alert_id,
            "alert_date": alert_date,
            "detected_at": atime,
            "event_timestamp": atime,
            "date_key": dkey,
            "portfolio_id": pid,
            "ticker": sym,
            "asset_class": "EQUITY" if sym in ("AAPL", "NVDA") else ("CRYPTO" if sym == "BTCUSD" else "COMMODITY"),
            "alert_type": atype,
            "severity": sev,
            "metric_name": metric,
            "threshold_value": float(thresh),
            "observed_value": float(obs),
            "payload_json": payload,
            "_ingested_at": atime,
        })

    return records
