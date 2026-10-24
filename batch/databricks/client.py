"""Databricks REST API Client and Zero-Cost Hermetic Mock.

Provides:
- Production `DatabricksClient` interacting with Databricks REST API 2.1 on GCP
  (Jobs API, Clusters API, Unity Catalog) with exponential backoff retry.
- Zero-cost in-memory `MockDatabricksClient` for hermetic testing in CI and local
  environments without requiring paid cloud infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import logging
import time
from typing import Any, Dict, List, Optional
import uuid

import requests

from batch.databricks.config import DatabricksClusterConfig, DatabricksWorkspaceConfig

logger = logging.getLogger(__name__)


class RunLifeCycleState(str, Enum):
    """Databricks Job Run lifecycle states."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    SKIPPED = "SKIPPED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class RunResultState(str, Enum):
    """Databricks Job Run final outcome states."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEDOUT = "TIMEDOUT"
    CANCELED = "CANCELED"
    MAXIMUM_CONCURRENT_RUNS_REACHED = "MAXIMUM_CONCURRENT_RUNS_REACHED"


class DatabricksClientException(Exception):
    """Raised when a Databricks API request fails."""


class DatabricksClient:
    """Production Databricks REST API 2.1 client with retries and backoff."""

    def __init__(
        self,
        workspace_config: Optional[DatabricksWorkspaceConfig] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        timeout_seconds: int = 30,
    ):
        self.config = workspace_config or DatabricksWorkspaceConfig.from_env()
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        if self.config.token:
            self.session.headers.update({"Authorization": f"Bearer {self.config.token}"})
        self.session.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        """Construct full REST API endpoint URL."""
        return f"{self.config.workspace_url.rstrip('/')}/api/2.1/{path.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute HTTP request with exponential backoff on transient errors."""
        url = self._url(path)
        attempt = 0

        while attempt <= self.max_retries:
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    json=json_data,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    attempt += 1
                    if attempt > self.max_retries:
                        resp.raise_for_status()
                    sleep_time = self.backoff_factor * (2 ** (attempt - 1))
                    logger.warning(
                        f"Databricks API returned HTTP {resp.status_code}, retrying in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                    continue

                resp.raise_for_status()
                return resp.json() if resp.content else {}
            except requests.RequestException as err:
                attempt += 1
                if attempt > self.max_retries:
                    raise DatabricksClientException(
                        f"Databricks request to {url} failed after {self.max_retries} retries: {err}"
                    ) from err
                sleep_time = self.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_time)

        raise DatabricksClientException(f"Failed to execute request to {url}")

    # -------------------------------------------------------------------------
    # Jobs API Methods
    # -------------------------------------------------------------------------
    def create_job(self, job_spec: Dict[str, Any]) -> int:
        """Create a new Databricks job."""
        res = self._request("POST", "jobs/create", json_data=job_spec)
        return int(res["job_id"])

    def run_job_now(
        self,
        job_id: int,
        parameters: Optional[Dict[str, str]] = None,
    ) -> int:
        """Trigger an immediate run of an existing Databricks job."""
        payload: Dict[str, Any] = {"job_id": job_id}
        if parameters:
            payload["notebook_params"] = parameters
        res = self._request("POST", "jobs/run-now", json_data=payload)
        return int(res["run_id"])

    def get_run(self, run_id: int) -> Dict[str, Any]:
        """Retrieve run execution metadata and state."""
        return self._request("GET", "jobs/runs/get", params={"run_id": run_id})

    def get_run_output(self, run_id: int) -> Dict[str, Any]:
        """Retrieve the output of a completed job run."""
        return self._request("GET", "jobs/runs/get-output", params={"run_id": run_id})

    def cancel_run(self, run_id: int) -> None:
        """Cancel an active Databricks job run."""
        self._request("POST", "jobs/runs/cancel", json_data={"run_id": run_id})

    # -------------------------------------------------------------------------
    # Clusters API Methods
    # -------------------------------------------------------------------------
    def create_cluster(self, cluster_config: DatabricksClusterConfig) -> str:
        """Create a new Databricks cluster."""
        payload = cluster_config.to_api_payload()
        res = self._request("POST", "../2.0/clusters/create", json_data=payload)
        return str(res["cluster_id"])

    def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """Retrieve cluster status and runtime info."""
        return self._request("GET", "../2.0/clusters/get", params={"cluster_id": cluster_id})

    def terminate_cluster(self, cluster_id: str) -> None:
        """Terminate a running cluster."""
        self._request("POST", "../2.0/clusters/delete", json_data={"cluster_id": cluster_id})


# -----------------------------------------------------------------------------
# 5. ZERO-COST HERMETIC IN-MEMORY MOCK CLIENT
# -----------------------------------------------------------------------------

@dataclass
class MockJobRun:
    """In-memory representation of a Databricks Job Run."""

    run_id: int
    job_id: int
    parameters: Dict[str, str] = field(default_factory=dict)
    life_cycle_state: RunLifeCycleState = RunLifeCycleState.PENDING
    result_state: Optional[RunResultState] = None
    output_notebook: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    created_time: int = field(default_factory=lambda: int(time.time() * 1000))


class MockDatabricksClient(DatabricksClient):
    """Zero-cost in-memory mock of DatabricksClient for hermetic testing."""

    def __init__(
        self,
        workspace_config: Optional[DatabricksWorkspaceConfig] = None,
        auto_succeed_runs: bool = True,
    ):
        super().__init__(workspace_config=workspace_config)
        self.auto_succeed_runs = auto_succeed_runs
        self._jobs: Dict[int, Dict[str, Any]] = {}
        self._runs: Dict[int, MockJobRun] = {}
        self._clusters: Dict[str, Dict[str, Any]] = {}
        self._next_job_id = 1001
        self._next_run_id = 5001

    def create_job(self, job_spec: Dict[str, Any]) -> int:
        job_id = self._next_job_id
        self._next_job_id += 1
        self._jobs[job_id] = dict(job_spec)
        return job_id

    def run_job_now(
        self,
        job_id: int,
        parameters: Optional[Dict[str, str]] = None,
    ) -> int:
        if job_id not in self._jobs:
            raise DatabricksClientException(f"Job ID {job_id} not found in workspace")

        run_id = self._next_run_id
        self._next_run_id += 1

        run = MockJobRun(
            run_id=run_id,
            job_id=job_id,
            parameters=parameters or {},
            life_cycle_state=RunLifeCycleState.TERMINATED if self.auto_succeed_runs else RunLifeCycleState.RUNNING,
            result_state=RunResultState.SUCCESS if self.auto_succeed_runs else None,
            output_notebook={"result": "SUCCESS", "records_ingested": 1000},
        )
        self._runs[run_id] = run
        return run_id

    def get_run(self, run_id: int) -> Dict[str, Any]:
        if run_id not in self._runs:
            raise DatabricksClientException(f"Run ID {run_id} not found")
        run = self._runs[run_id]
        return {
            "run_id": run.run_id,
            "job_id": run.job_id,
            "state": {
                "life_cycle_state": run.life_cycle_state.value,
                "result_state": run.result_state.value if run.result_state else None,
                "state_message": run.error_message or "Execution completed successfully",
            },
            "overriding_parameters": {"notebook_params": run.parameters},
            "start_time": run.created_time,
        }

    def get_run_output(self, run_id: int) -> Dict[str, Any]:
        if run_id not in self._runs:
            raise DatabricksClientException(f"Run ID {run_id} not found")
        run = self._runs[run_id]
        return {
            "notebook_output": {"result": json.dumps(run.output_notebook)},
            "error": run.error_message,
        }

    def cancel_run(self, run_id: int) -> None:
        if run_id not in self._runs:
            raise DatabricksClientException(f"Run ID {run_id} not found")
        self._runs[run_id].life_cycle_state = RunLifeCycleState.TERMINATED
        self._runs[run_id].result_state = RunResultState.CANCELED

    def create_cluster(self, cluster_config: DatabricksClusterConfig) -> str:
        cluster_id = f"mock-cluster-{uuid.uuid4().hex[:8]}"
        self._clusters[cluster_id] = {
            "cluster_id": cluster_id,
            "state": "RUNNING",
            "cluster_name": cluster_config.cluster_name,
            "spark_version": cluster_config.spark_version,
            "node_type_id": cluster_config.node_type_id,
            "autoscale": {
                "min_workers": cluster_config.min_workers,
                "max_workers": cluster_config.max_workers,
            },
        }
        return cluster_id

    def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        if cluster_id not in self._clusters:
            raise DatabricksClientException(f"Cluster ID {cluster_id} not found")
        return self._clusters[cluster_id]

    def terminate_cluster(self, cluster_id: str) -> None:
        if cluster_id in self._clusters:
            self._clusters[cluster_id]["state"] = "TERMINATED"
