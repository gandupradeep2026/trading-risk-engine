"""Dead-Letter Queue (DLQ) Forensic Analysis & Alert Monitor.

Scans rejected financial payloads from Pub/Sub DLQ topics, Kafka dead-letter partitions,
or GCS cold-tier quarantine storage, performs automated error categorization, and
triggers Slack Block Kit alerts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from monitoring.config import PoisonPillCategory
from monitoring.slack_alerts import SlackDlqAlertDispatcher


@dataclass
class DlqForensicRecord:
    """Forensic breakdown of an individual dead-letter message."""

    record_id: str
    source_topic: str
    timestamp: str
    category: PoisonPillCategory
    ticker: Optional[str]
    portfolio_id: Optional[str]
    error_message: str
    raw_payload_preview: str
    delivery_attempts: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "source_topic": self.source_topic,
            "timestamp": self.timestamp,
            "category": self.category.value,
            "ticker": self.ticker,
            "portfolio_id": self.portfolio_id,
            "error_message": self.error_message,
            "raw_payload_preview": self.raw_payload_preview,
            "delivery_attempts": self.delivery_attempts,
        }


@dataclass
class DlqForensicReport:
    """Aggregated forensic analysis report across a batch of dead letters."""

    timestamp: str
    total_dead_letters: int
    categories_breakdown: Dict[str, int]
    affected_tickers: List[str]
    affected_portfolios: List[str]
    records: List[DlqForensicRecord]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "total_dead_letters": self.total_dead_letters,
            "categories_breakdown": self.categories_breakdown,
            "affected_tickers": self.affected_tickers,
            "affected_portfolios": self.affected_portfolios,
            "records": [r.to_dict() for r in self.records],
        }

    def to_markdown(self) -> str:
        """Render readable markdown summary of the DLQ analysis."""
        lines = [
            "# Dead-Letter Queue Forensic Incident Report",
            f"- **Report Timestamp**: {self.timestamp}",
            f"- **Total Dead Letters**: {self.total_dead_letters}",
            f"- **Affected Tickers**: {', '.join(self.affected_tickers) if self.affected_tickers else 'None'}",
            f"- **Affected Portfolios**: {', '.join(self.affected_portfolios) if self.affected_portfolios else 'None'}",
            "",
            "## Failure Categorization Breakdown",
            "| Category | Count | Percentage |",
            "| :--- | :--- | :--- |",
        ]
        total = max(1, self.total_dead_letters)
        for cat, cnt in sorted(self.categories_breakdown.items(), key=lambda x: x[1], reverse=True):
            pct = (cnt / total) * 100.0
            lines.append(f"| `{cat}` | {cnt} | {pct:.1f}% |")

        if self.records:
            lines.extend(
                [
                    "",
                    "## Recent Dead Letters (Sample)",
                    "| Record ID | Ticker | Portfolio | Category | Error |",
                    "| :--- | :--- | :--- | :--- | :--- |",
                ]
            )
            for r in self.records[:10]:
                lines.append(
                    f"| `{r.record_id[:8]}` | `{r.ticker or 'N/A'}` | `{r.portfolio_id or 'N/A'}` | `{r.category.value}` | {r.error_message[:40]} |"
                )

        return "\n".join(lines)


class DeadLetterQueueMonitor:
    """Monitors Dead-Letter Queues and coordinates forensic classification and alerting."""

    def __init__(self, default_dlq_topic: str = "top-trade-orders-dlq-dev") -> None:
        self.default_dlq_topic = default_dlq_topic

    @staticmethod
    def classify_payload(raw_data: Any, error_hint: str = "") -> Tuple[PoisonPillCategory, Dict[str, Any]]:
        """Classify corrupted or poison-pill payload and extract metadata."""
        parsed_payload: Dict[str, Any] = {}
        str_content = ""

        # Normalize raw payload to string/dict
        if isinstance(raw_data, bytes):
            try:
                str_content = raw_data.decode("utf-8")
                parsed_payload = json.loads(str_content)
            except UnicodeDecodeError:
                return PoisonPillCategory.JSON_DECODE_ERROR, {"raw": "<binary undecodable>"}
            except json.JSONDecodeError:
                return PoisonPillCategory.JSON_DECODE_ERROR, {"raw": str_content[:200]}
        elif isinstance(raw_data, str):
            str_content = raw_data
            try:
                parsed_payload = json.loads(raw_data)
            except json.JSONDecodeError:
                return PoisonPillCategory.JSON_DECODE_ERROR, {"raw": raw_data[:200]}
        elif isinstance(raw_data, dict):
            parsed_payload = raw_data
            str_content = json.dumps(raw_data)

        # Inspect error hint and payload for specific domain rule breaches
        err_lower = error_hint.lower()
        if "json" in err_lower or "decode" in err_lower or "syntax" in err_lower:
            return PoisonPillCategory.JSON_DECODE_ERROR, parsed_payload

        if "cross" in err_lower or (
            parsed_payload.get("bid") is not None
            and parsed_payload.get("ask") is not None
            and float(parsed_payload["bid"]) > float(parsed_payload["ask"])
        ):
            return PoisonPillCategory.CROSSED_QUOTE_ERROR, parsed_payload

        price = parsed_payload.get("price")
        if price is not None:
            try:
                if float(price) <= 0:
                    return PoisonPillCategory.NON_POSITIVE_PRICE_ERROR, parsed_payload
            except (ValueError, TypeError):
                pass
        if "price" in err_lower and ("positive" in err_lower or "<=" in err_lower or "negative" in err_lower):
            return PoisonPillCategory.NON_POSITIVE_PRICE_ERROR, parsed_payload

        quantity = parsed_payload.get("quantity") or parsed_payload.get("volume")
        if quantity is not None:
            try:
                if float(quantity) <= 0:
                    return PoisonPillCategory.NON_POSITIVE_QUANTITY_ERROR, parsed_payload
            except (ValueError, TypeError):
                pass
        if "quantity" in err_lower and ("positive" in err_lower or "<=" in err_lower or "negative" in err_lower):
            return PoisonPillCategory.NON_POSITIVE_QUANTITY_ERROR, parsed_payload

        if "schema" in err_lower or "required" in err_lower or "missing" in err_lower or "field" in err_lower:
            return PoisonPillCategory.SCHEMA_VALIDATION_ERROR, parsed_payload

        if "retry" in err_lower or "exhausted" in err_lower:
            return PoisonPillCategory.MAX_RETRIES_EXHAUSTED, parsed_payload

        return PoisonPillCategory.UNKNOWN_ERROR, parsed_payload

    def inspect_records(
        self,
        raw_records: List[Dict[str, Any]],
        source_topic: Optional[str] = None,
    ) -> DlqForensicReport:
        """Analyze a batch of dead-letter records and generate a structured forensic report."""
        topic = source_topic or self.default_dlq_topic
        now_str = datetime.now(timezone.utc).isoformat()
        categories_breakdown: Dict[str, int] = {}
        affected_tickers: set[str] = set()
        affected_portfolios: set[str] = set()
        forensic_records: List[DlqForensicRecord] = []

        for idx, rec in enumerate(raw_records):
            rec_id = str(rec.get("order_id") or rec.get("id") or rec.get("message_id") or f"dlq-{idx}")
            raw_payload = rec.get("raw_payload") or rec.get("payload") or rec
            err_msg = str(rec.get("error_message") or rec.get("error") or "Unspecified processing error")
            attempts = int(rec.get("delivery_attempts") or rec.get("attempts") or 5)

            category, extracted_data = self.classify_payload(raw_payload, err_msg)
            ticker = extracted_data.get("ticker") or rec.get("ticker")
            portfolio = extracted_data.get("portfolio_id") or rec.get("portfolio_id")

            if ticker:
                affected_tickers.add(str(ticker))
            if portfolio:
                affected_portfolios.add(str(portfolio))

            categories_breakdown[category.value] = categories_breakdown.get(category.value, 0) + 1

            # Format sanitized payload preview
            if isinstance(raw_payload, (dict, list)):
                preview = json.dumps(raw_payload)
            else:
                preview = str(raw_payload)

            forensic_records.append(
                DlqForensicRecord(
                    record_id=rec_id,
                    source_topic=topic,
                    timestamp=rec.get("timestamp") or now_str,
                    category=category,
                    ticker=str(ticker) if ticker else None,
                    portfolio_id=str(portfolio) if portfolio else None,
                    error_message=err_msg,
                    raw_payload_preview=preview[:300],
                    delivery_attempts=attempts,
                )
            )

        return DlqForensicReport(
            timestamp=now_str,
            total_dead_letters=len(raw_records),
            categories_breakdown=categories_breakdown,
            affected_tickers=sorted(list(affected_tickers)),
            affected_portfolios=sorted(list(affected_portfolios)),
            records=forensic_records,
        )

    def scan_and_alert(
        self,
        raw_records: List[Dict[str, Any]],
        dispatcher: SlackDlqAlertDispatcher,
        source_topic: Optional[str] = None,
        bypass_cooldown: bool = False,
    ) -> int:
        """Analyze dead letters and dispatch deduplicated alerts to Slack."""
        report = self.inspect_records(raw_records, source_topic=source_topic)
        alerts_sent = 0

        # Group by category and ticker for consolidated alerting
        grouped: Dict[Tuple[PoisonPillCategory, Optional[str]], List[DlqForensicRecord]] = {}
        for r in report.records:
            key = (r.category, r.ticker)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(r)

        for (cat, ticker), rec_list in grouped.items():
            first_rec = rec_list[0]
            err = first_rec.error_message
            if len(rec_list) > 1:
                err = f"{err} (and {len(rec_list) - 1} similar errors in burst)"

            success = dispatcher.dispatch_dlq_alert(
                topic_name=source_topic or self.default_dlq_topic,
                category=cat,
                error_message=err,
                ticker=ticker,
                portfolio_id=first_rec.portfolio_id,
                delivery_attempts=first_rec.delivery_attempts,
                payload_preview=first_rec.raw_payload_preview,
                bypass_cooldown=bypass_cooldown,
            )
            if success:
                alerts_sent += 1

        return alerts_sent
