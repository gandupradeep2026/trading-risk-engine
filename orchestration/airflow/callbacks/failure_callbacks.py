"""Airflow Task Callbacks for Resilient Alerting & Escalation.

Provides standard Airflow callback functions (`on_failure_callback`, `on_retry_callback`,
`on_success_callback`, and `sla_miss_callback`) that extract runtime task context,
format actionable error diagnostics, and dispatch to Slack and PagerDuty.
"""

from __future__ import annotations

from datetime import datetime
import logging
import traceback
from typing import Any, Callable, Dict, List, Optional

from orchestration.airflow.hooks.notification_dispatcher import NotificationDispatcher
from orchestration.airflow.hooks.slack_webhook_hook import AlertSeverity

logger = logging.getLogger("trading_risk.airflow.callbacks")


def _extract_context_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
    """Defensively extract metadata from Airflow task execution context."""
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    task = context.get("task")

    dag_id = getattr(dag, "dag_id", None) or (getattr(ti, "dag_id", None) if ti else "unknown_dag")
    task_id = getattr(task, "task_id", None) or (getattr(ti, "task_id", None) if ti else "unknown_task")

    exec_date = context.get("execution_date")
    if isinstance(exec_date, datetime):
        exec_date_str = exec_date.isoformat() + "Z"
    else:
        exec_date_str = str(exec_date) if exec_date else datetime.utcnow().isoformat() + "Z"

    try_number = getattr(ti, "try_number", 1) if ti else 1
    max_tries = getattr(ti, "max_tries", 4) if ti else 4
    log_url = getattr(ti, "log_url", None) if ti else None
    duration = getattr(ti, "duration", None) if ti else None

    # Extract exception or error message
    exc = context.get("exception")
    error_message: Optional[str] = None
    if exc:
        if isinstance(exc, BaseException):
            error_message = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        else:
            error_message = str(exc)

    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": exec_date_str,
        "try_number": try_number,
        "max_tries": max_tries,
        "log_url": log_url,
        "duration_sec": duration,
        "error_message": error_message,
    }


def create_failure_callback(
    dispatcher: Optional[NotificationDispatcher] = None,
) -> Callable[[Dict[str, Any]], None]:
    """Factory returning an `on_failure_callback` function for Airflow DAGs/Tasks."""
    active_dispatcher = dispatcher or NotificationDispatcher()

    def on_failure_callback(context: Dict[str, Any]) -> None:
        meta = _extract_context_metadata(context)
        is_final_failure = meta["try_number"] >= meta["max_tries"]
        severity = AlertSeverity.CRITICAL if is_final_failure else AlertSeverity.WARNING

        title = (
            f"Task Execution Failed (Retries Exhausted)"
            if is_final_failure
            else f"Task Execution Failed (Attempt {meta['try_number']}/{meta['max_tries']})"
        )

        logger.error(
            f"Airflow Task Failure: dag={meta['dag_id']}, task={meta['task_id']}, "
            f"attempt={meta['try_number']}/{meta['max_tries']}, severity={severity.value}"
        )

        active_dispatcher.dispatch(
            severity=severity,
            title=title,
            dag_id=meta["dag_id"],
            task_id=meta["task_id"],
            execution_date=meta["execution_date"],
            try_number=meta["try_number"],
            max_tries=meta["max_tries"],
            error_message=meta["error_message"] or "Task failed without an explicit exception trace.",
            log_url=meta["log_url"],
            duration_sec=meta["duration_sec"],
        )

    return on_failure_callback


def create_retry_callback(
    dispatcher: Optional[NotificationDispatcher] = None,
) -> Callable[[Dict[str, Any]], None]:
    """Factory returning an `on_retry_callback` function for Airflow DAGs/Tasks."""
    active_dispatcher = dispatcher or NotificationDispatcher()

    def on_retry_callback(context: Dict[str, Any]) -> None:
        meta = _extract_context_metadata(context)
        logger.warning(
            f"Airflow Task Retrying: dag={meta['dag_id']}, task={meta['task_id']}, "
            f"next_attempt={meta['try_number'] + 1}/{meta['max_tries']}"
        )

        active_dispatcher.dispatch(
            severity=AlertSeverity.WARNING,
            title=f"Task Failed, Entering Exponential Backoff Retry (Attempt {meta['try_number']}/{meta['max_tries']})",
            dag_id=meta["dag_id"],
            task_id=meta["task_id"],
            execution_date=meta["execution_date"],
            try_number=meta["try_number"],
            max_tries=meta["max_tries"],
            error_message=meta["error_message"] or "Task failed and is retrying.",
            log_url=meta["log_url"],
            duration_sec=meta["duration_sec"],
        )

    return on_retry_callback


def create_success_callback(
    dispatcher: Optional[NotificationDispatcher] = None,
) -> Callable[[Dict[str, Any]], None]:
    """Factory returning an `on_success_callback` function for Airflow milestones."""
    active_dispatcher = dispatcher or NotificationDispatcher()

    def on_success_callback(context: Dict[str, Any]) -> None:
        meta = _extract_context_metadata(context)
        logger.info(f"Airflow Task Succeeded: dag={meta['dag_id']}, task={meta['task_id']}")

        active_dispatcher.dispatch(
            severity=AlertSeverity.SUCCESS,
            title="Batch Lakehouse Milestone Completed Successfully",
            dag_id=meta["dag_id"],
            task_id=meta["task_id"],
            execution_date=meta["execution_date"],
            try_number=meta["try_number"],
            max_tries=meta["max_tries"],
            log_url=meta["log_url"],
            duration_sec=meta["duration_sec"],
        )

    return on_success_callback


def create_sla_miss_callback(
    dispatcher: Optional[NotificationDispatcher] = None,
) -> Callable[..., None]:
    """Factory returning an `sla_miss_callback` function for Airflow DAGs."""
    active_dispatcher = dispatcher or NotificationDispatcher()

    def sla_miss_callback(
        dag: Any,
        task_list: str,
        blocking_task_list: str,
        slas: List[Any],
        blocking_tis: List[Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        dag_id = getattr(dag, "dag_id", "unknown_dag") if dag else "unknown_dag"
        exec_date_str = datetime.utcnow().isoformat() + "Z"
        error_msg = (
            f"Pipeline SLA Missed!\n"
            f"Missed Tasks: {task_list}\n"
            f"Blocking Tasks: {blocking_task_list}\n"
            f"SLA Definitions: {slas}\n"
            f"Blocking Task Instances: {blocking_tis}"
        )

        logger.critical(f"Airflow SLA Miss in DAG {dag_id}: {error_msg}")

        active_dispatcher.dispatch(
            severity=AlertSeverity.CRITICAL,
            title="CRITICAL: Batch Risk Pipeline Missed SLA Deadline",
            dag_id=dag_id,
            task_id="sla_enforcement",
            execution_date=exec_date_str,
            try_number=1,
            max_tries=1,
            error_message=error_msg,
            dedup_key=f"trading-risk/{active_dispatcher.config.environment}/{dag_id}/sla_miss",
        )

    return sla_miss_callback
