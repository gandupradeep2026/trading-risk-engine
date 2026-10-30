"""Airflow Compatibility Layer & Hermetic Simulation Engine.

Provides seamless compatibility with native Apache Airflow 2.x when running
inside Google Cloud Composer, and provides a zero-cost, type-annotated, pure-Python
emulation layer when running in local development or lean CI environments without
Apache Airflow installed.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
import threading
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union

# Thread-local storage for active DAG and TaskGroup context managers
_context_local = threading.local()


def _get_active_dag() -> Optional[DAG]:
    return getattr(_context_local, "active_dag", None)


def _set_active_dag(dag: Optional[DAG]) -> None:
    _context_local.active_dag = dag


def _get_active_task_group() -> Optional[TaskGroup]:
    stack = getattr(_context_local, "task_group_stack", None)
    if stack and len(stack) > 0:
        return stack[-1]
    return None


def _push_task_group(tg: TaskGroup) -> None:
    if not hasattr(_context_local, "task_group_stack"):
        _context_local.task_group_stack = []
    _context_local.task_group_stack.append(tg)


def _pop_task_group() -> Optional[TaskGroup]:
    stack = getattr(_context_local, "task_group_stack", None)
    if stack and len(stack) > 0:
        return stack.pop()
    return None


class AirflowException(Exception):
    """Base exception for Airflow errors."""
    pass


class AirflowDagCycleException(AirflowException):
    """Raised when a cycle is detected in the DAG topology."""
    pass


class AirflowTaskTimeout(AirflowException):
    """Raised when a task exceeds its execution_timeout."""
    pass


class TriggerRule:
    """Airflow TriggerRule constants controlling task scheduling conditions."""
    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    NONE_FAILED = "none_failed"
    NONE_SKIPPED = "none_skipped"
    DUMMY = "dummy"


class MockTaskInstance:
    """Mock Airflow TaskInstance representation for callback contexts."""

    def __init__(
        self,
        task: BaseOperator,
        execution_date: datetime,
        try_number: int = 1,
        state: str = "failed",
        log_url: Optional[str] = None,
    ) -> None:
        self.task = task
        self.task_id = task.task_id
        self.dag_id = task.dag.dag_id if task.dag else "unknown_dag"
        self.execution_date = execution_date
        self.try_number = try_number
        self.max_tries = (task.retries or 0) + 1
        self.state = state
        self.duration: float = 42.5
        self.log_url = log_url or (
            f"https://composer.cloud.google.com/airflow/log?"
            f"dag_id={self.dag_id}&task_id={self.task_id}&execution_date={execution_date.isoformat()}"
        )

    def __repr__(self) -> str:
        return f"<TaskInstance: {self.dag_id}.{self.task_id} {self.execution_date.isoformat()} [{self.state}]>"


class BaseOperator:
    """Pure-Python BaseOperator modeling Apache Airflow's BaseOperator."""

    def __init__(
        self,
        task_id: str,
        owner: str = "airflow",
        retries: int = 0,
        retry_delay: timedelta = timedelta(seconds=300),
        retry_exponential_backoff: bool = False,
        max_retry_delay: Optional[timedelta] = None,
        execution_timeout: Optional[timedelta] = None,
        on_failure_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_retry_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_success_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        sla: Optional[timedelta] = None,
        dag: Optional[DAG] = None,
        task_group: Optional[TaskGroup] = None,
        params: Optional[Dict[str, Any]] = None,
        doc_md: Optional[str] = None,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs: Any,
    ) -> None:
        self.task_id = task_id
        self.owner = owner
        self.retries = retries
        self.retry_delay = retry_delay
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = max_retry_delay
        self.execution_timeout = execution_timeout
        self.on_failure_callback = on_failure_callback
        self.on_retry_callback = on_retry_callback
        self.on_success_callback = on_success_callback
        self.sla = sla
        self.params = params or {}
        self.doc_md = doc_md
        self.trigger_rule = trigger_rule
        self.extra_kwargs = kwargs

        self._upstream_tasks: Set[BaseOperator] = set()
        self._downstream_tasks: Set[BaseOperator] = set()

        # Resolve active TaskGroup
        active_tg = task_group or _get_active_task_group()
        self.task_group = active_tg
        if active_tg:
            active_tg.add_child(self)

        # Resolve active DAG
        target_dag = dag or _get_active_dag()
        self.dag: Optional[DAG] = target_dag
        if target_dag:
            target_dag.add_task(self)

    @property
    def upstream_task_ids(self) -> Set[str]:
        return {t.task_id for t in self._upstream_tasks}

    @property
    def downstream_task_ids(self) -> Set[str]:
        return {t.task_id for t in self._downstream_tasks}

    @property
    def upstream_list(self) -> List[BaseOperator]:
        return sorted(list(self._upstream_tasks), key=lambda t: t.task_id)

    @property
    def downstream_list(self) -> List[BaseOperator]:
        return sorted(list(self._downstream_tasks), key=lambda t: t.task_id)

    def has_dag(self) -> bool:
        return self.dag is not None

    def set_downstream(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> None:
        """Set other as downstream of this task."""
        if isinstance(other, list):
            for item in other:
                self.set_downstream(item)
            return

        if isinstance(other, TaskGroup):
            # Connect to all roots of the TaskGroup
            for root_task in other.roots:
                self.set_downstream(root_task)
            return

        if not isinstance(other, BaseOperator):
            raise TypeError(f"Cannot set downstream for type: {type(other)}")

        self._downstream_tasks.add(other)
        other._upstream_tasks.add(self)

    def set_upstream(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> None:
        """Set other as upstream of this task."""
        if isinstance(other, list):
            for item in other:
                self.set_upstream(item)
            return

        if isinstance(other, TaskGroup):
            # Connect from all leaves of the TaskGroup
            for leaf_task in other.leaves:
                self.set_upstream(leaf_task)
            return

        if not isinstance(other, BaseOperator):
            raise TypeError(f"Cannot set upstream for type: {type(other)}")

        self._upstream_tasks.add(other)
        other._downstream_tasks.add(self)

    def __rshift__(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]:
        self.set_downstream(other)
        return other

    def __lshift__(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]:
        self.set_upstream(other)
        return other

    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the operator logic in production."""
        return None

    def __repr__(self) -> str:
        return f"<Task({self.__class__.__name__}): {self.task_id}>"


class EmptyOperator(BaseOperator):
    """No-op milestone operator, identical to airflow.operators.empty.EmptyOperator."""

    def execute(self, context: Dict[str, Any]) -> None:
        pass


# Alias DummyOperator to EmptyOperator for backward compatibility
DummyOperator = EmptyOperator


def _render_jinja_string(template: str, context: Optional[Dict[str, Any]]) -> str:
    """Helper to render basic Airflow Jinja template parameters (ds, ds_nodash, try_number)."""
    if not isinstance(template, str) or "{{" not in template:
        return template
    if not context:
        return template
    res = template
    exec_date = context.get("execution_date") or datetime.utcnow()
    ti = context.get("task_instance") or context.get("ti")
    try_num = getattr(ti, "try_number", 1) if ti else 1
    ds = exec_date.strftime("%Y-%m-%d")
    ds_nodash = exec_date.strftime("%Y%m%d")

    res = res.replace("{{ ds }}", ds)
    res = res.replace("{{ds}}", ds)
    res = res.replace("{{ ds_nodash }}", ds_nodash)
    res = res.replace("{{ds_nodash}}", ds_nodash)
    res = res.replace("{{ task_instance.try_number }}", str(try_num))
    res = res.replace("{{task_instance.try_number}}", str(try_num))
    res = res.replace("{{ ti.try_number }}", str(try_num))
    res = res.replace("{{ti.try_number}}", str(try_num))
    return res


def _render_templates_in_obj(obj: Any, context: Optional[Dict[str, Any]]) -> Any:
    """Recursively render Jinja templates in nested dicts/lists/strings."""
    if isinstance(obj, str):
        return _render_jinja_string(obj, context)
    elif isinstance(obj, list):
        return [_render_templates_in_obj(item, context) for item in obj]
    elif isinstance(obj, dict):
        return {k: _render_templates_in_obj(v, context) for k, v in obj.items()}
    return obj


class DataprocClusterStateRegistry:
    """Thread-safe registry simulating Google Cloud Dataproc cluster and job state in memory."""

    _lock = threading.Lock()
    _clusters: Dict[str, Dict[str, Any]] = {}
    _jobs: Dict[str, Dict[str, Any]] = {}
    _injected_failures: Dict[str, Exception] = {}

    @classmethod
    def reset(cls) -> None:
        """Clear all registered clusters, jobs, and failure injections."""
        with cls._lock:
            cls._clusters.clear()
            cls._jobs.clear()
            cls._injected_failures.clear()

    @classmethod
    def inject_failure(cls, target: str, exception: Exception) -> None:
        """Inject simulated failure for testing (target can be 'create_cluster', 'submit_job', 'delete_cluster')."""
        with cls._lock:
            cls._injected_failures[target] = exception

    @classmethod
    def clear_injected_failures(cls) -> None:
        """Clear all injected failures."""
        with cls._lock:
            cls._injected_failures.clear()

    @classmethod
    def create_cluster(
        cls,
        project_id: str,
        region: str,
        cluster_name: str,
        cluster_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        with cls._lock:
            if "create_cluster" in cls._injected_failures:
                raise cls._injected_failures["create_cluster"]

            if cluster_name in cls._clusters and cls._clusters[cluster_name]["status"] == "RUNNING":
                return cls._clusters[cluster_name]

            cluster_entry = {
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "cluster_config": cluster_config,
                "status": "RUNNING",
                "created_at": datetime.utcnow().isoformat(),
                "deleted_at": None,
            }
            cls._clusters[cluster_name] = cluster_entry
            return cluster_entry

    @classmethod
    def get_cluster(cls, cluster_name: str) -> Optional[Dict[str, Any]]:
        with cls._lock:
            return cls._clusters.get(cluster_name)

    @classmethod
    def list_active_clusters(cls) -> List[str]:
        with cls._lock:
            return [name for name, c in cls._clusters.items() if c["status"] == "RUNNING"]

    @classmethod
    def delete_cluster(cls, cluster_name: str, cluster_not_found_ok: bool = True) -> bool:
        with cls._lock:
            if "delete_cluster" in cls._injected_failures:
                raise cls._injected_failures["delete_cluster"]

            if cluster_name not in cls._clusters:
                if not cluster_not_found_ok:
                    raise AirflowException(f"Dataproc cluster '{cluster_name}' not found for deletion")
                return False

            cls._clusters[cluster_name]["status"] = "DELETED"
            cls._clusters[cluster_name]["deleted_at"] = datetime.utcnow().isoformat()
            return True

    @classmethod
    def submit_job(
        cls,
        project_id: str,
        region: str,
        job: Dict[str, Any],
    ) -> Dict[str, Any]:
        with cls._lock:
            if "submit_job" in cls._injected_failures:
                raise cls._injected_failures["submit_job"]

            cluster_name = (
                job.get("placement", {}).get("cluster_name")
                or job.get("cluster_name")
            )
            if not cluster_name:
                raise AirflowException("Job specification missing placement.cluster_name")

            cluster = cls._clusters.get(cluster_name)
            if not cluster or cluster["status"] != "RUNNING":
                raise AirflowException(
                    f"Dataproc cluster '{cluster_name}' is not running (status: {cluster.get('status') if cluster else 'NOT_FOUND'})"
                )

            job_id = (
                job.get("reference", {}).get("job_id")
                or f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
            )

            job_entry = {
                "project_id": project_id,
                "region": region,
                "job_id": job_id,
                "cluster_name": cluster_name,
                "job_spec": job,
                "status": "SUCCESS",
                "submitted_at": datetime.utcnow().isoformat(),
            }
            cls._jobs[job_id] = job_entry
            return job_entry

    @classmethod
    def get_job(cls, job_id: str) -> Optional[Dict[str, Any]]:
        with cls._lock:
            return cls._jobs.get(job_id)


class DataprocCreateClusterOperator(BaseOperator):
    """Creates a Google Cloud Dataproc cluster.

    Compatible with airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator.
    """

    def __init__(
        self,
        task_id: str,
        project_id: str,
        cluster_name: str,
        cluster_config: Dict[str, Any],
        region: str = "us-central1",
        gcp_conn_id: str = "google_cloud_default",
        delete_on_error: bool = True,
        use_if_exists: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.project_id = project_id
        self.cluster_name = cluster_name
        self.cluster_config = cluster_config
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.delete_on_error = delete_on_error
        self.use_if_exists = use_if_exists

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        rendered_name = _render_jinja_string(self.cluster_name, context)
        if not isinstance(self.cluster_config, dict):
            raise AirflowException("cluster_config must be a dictionary")
        if "master_config" not in self.cluster_config:
            raise AirflowException("cluster_config missing required 'master_config'")

        return DataprocClusterStateRegistry.create_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=rendered_name,
            cluster_config=self.cluster_config,
        )


class DataprocSubmitJobOperator(BaseOperator):
    """Submits a PySpark or batch job to a Google Cloud Dataproc cluster.

    Compatible with airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator.
    """

    def __init__(
        self,
        task_id: str,
        project_id: str,
        region: str,
        job: Dict[str, Any],
        gcp_conn_id: str = "google_cloud_default",
        asynchronous: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.project_id = project_id
        self.region = region
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.asynchronous = asynchronous

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        rendered_job = _render_templates_in_obj(self.job, context)
        return DataprocClusterStateRegistry.submit_job(
            project_id=self.project_id,
            region=self.region,
            job=rendered_job,
        )


class DataprocDeleteClusterOperator(BaseOperator):
    """Deletes a Google Cloud Dataproc cluster to ensure zero idle compute cost.

    Compatible with airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator.
    Defaults to trigger_rule=TriggerRule.ALL_DONE so clusters are reliably deleted even on failure.
    """

    def __init__(
        self,
        task_id: str,
        project_id: str,
        cluster_name: str,
        region: str = "us-central1",
        gcp_conn_id: str = "google_cloud_default",
        trigger_rule: str = TriggerRule.ALL_DONE,
        cluster_not_found_ok: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, trigger_rule=trigger_rule, **kwargs)
        self.project_id = project_id
        self.cluster_name = cluster_name
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.cluster_not_found_ok = cluster_not_found_ok

    def execute(self, context: Optional[Dict[str, Any]] = None) -> bool:
        rendered_name = _render_jinja_string(self.cluster_name, context)
        return DataprocClusterStateRegistry.delete_cluster(
            cluster_name=rendered_name,
            cluster_not_found_ok=self.cluster_not_found_ok,
        )


# =============================================================================
# DATABRICKS OPERATORS & IN-MEMORY STATE REGISTRY (Day 20)
# =============================================================================

class DatabricksJobStateRegistry:
    """Thread-safe registry simulating Databricks Runs and Jobs REST API state in memory.

    Provides zero-cost hermetic simulation for Databricks job runs, tracking states,
    run outputs, task execution attempt counts, and configurable transient or permanent
    failure injections to rigorously test automated retry logic and alerting.
    """

    _lock = threading.Lock()
    _runs: Dict[int, Dict[str, Any]] = {}
    _jobs: Dict[Union[int, str], Dict[str, Any]] = {}
    _injected_failures: Dict[str, Dict[str, Any]] = {}
    _task_attempt_counts: Dict[str, int] = {}
    _next_run_id: int = 7001
    _next_job_id: int = 1001

    @classmethod
    def reset(cls) -> None:
        """Clear all registered runs, jobs, failures, and attempt counters."""
        with cls._lock:
            cls._runs.clear()
            cls._jobs.clear()
            cls._injected_failures.clear()
            cls._task_attempt_counts.clear()
            cls._next_run_id = 7001
            cls._next_job_id = 1001

    @classmethod
    def inject_failure(
        cls,
        key: str,
        exception: Exception,
        fail_count: int = 1,
    ) -> None:
        """Inject a transient or permanent failure for a task, job, or endpoint.

        Args:
            key: Task ID or job identifier to fail.
            exception: The Exception to raise upon execution.
            fail_count: Number of consecutive attempts to fail before succeeding.
                        Use a large number (e.g. 9999) for permanent failure.
        """
        with cls._lock:
            cls._injected_failures[key] = {
                "exception": exception,
                "remaining_fails": fail_count,
            }

    @classmethod
    def get_attempt_count(cls, task_id: str) -> int:
        """Return the number of execution attempts recorded for a given task ID."""
        with cls._lock:
            return cls._task_attempt_counts.get(task_id, 0)

    @classmethod
    def register_job(cls, job_id: Union[int, str], job_spec: Dict[str, Any]) -> None:
        """Register a pre-existing job in the workspace mock."""
        with cls._lock:
            cls._jobs[job_id] = dict(job_spec)

    @classmethod
    def submit_run(
        cls,
        run_spec: Dict[str, Any],
        caller_task_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Simulate Databricks Jobs API `runs/submit` endpoint with retry simulation."""
        with cls._lock:
            if caller_task_id:
                cls._task_attempt_counts[caller_task_id] = (
                    cls._task_attempt_counts.get(caller_task_id, 0) + 1
                )

            # Check for failure injection
            failure_key = caller_task_id or run_spec.get("run_name") or "submit_run"
            if failure_key in cls._injected_failures:
                fail_info = cls._injected_failures[failure_key]
                if fail_info["remaining_fails"] > 0:
                    fail_info["remaining_fails"] -= 1
                    raise fail_info["exception"]

            run_id = cls._next_run_id
            cls._next_run_id += 1

            run_entry = {
                "run_id": run_id,
                "run_name": run_spec.get("run_name", f"run_{run_id}"),
                "spec": run_spec,
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                    "state_message": "Run completed successfully",
                },
                "tasks": run_spec.get("tasks", []),
                "start_time": datetime.utcnow().isoformat(),
                "end_time": datetime.utcnow().isoformat(),
                "run_page_url": f"https://trading-risk.cloud.databricks.com/#job/run/{run_id}",
            }
            cls._runs[run_id] = run_entry
            return run_entry

    @classmethod
    def run_now(
        cls,
        job_id: Union[int, str],
        notebook_params: Optional[Dict[str, str]] = None,
        caller_task_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Simulate Databricks Jobs API `jobs/run-now` endpoint with retry simulation."""
        with cls._lock:
            if caller_task_id:
                cls._task_attempt_counts[caller_task_id] = (
                    cls._task_attempt_counts.get(caller_task_id, 0) + 1
                )

            failure_key = caller_task_id or str(job_id) or "run_now"
            if failure_key in cls._injected_failures:
                fail_info = cls._injected_failures[failure_key]
                if fail_info["remaining_fails"] > 0:
                    fail_info["remaining_fails"] -= 1
                    raise fail_info["exception"]

            run_id = cls._next_run_id
            cls._next_run_id += 1

            run_entry = {
                "run_id": run_id,
                "job_id": job_id,
                "parameters": notebook_params or {},
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                    "state_message": "Run-now completed successfully",
                },
                "start_time": datetime.utcnow().isoformat(),
                "end_time": datetime.utcnow().isoformat(),
                "run_page_url": f"https://trading-risk.cloud.databricks.com/#job/{job_id}/run/{run_id}",
            }
            cls._runs[run_id] = run_entry
            return run_entry

    @classmethod
    def get_run(cls, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve execution state of a specific run."""
        with cls._lock:
            return cls._runs.get(run_id)

    @classmethod
    def cancel_run(cls, run_id: int) -> None:
        """Cancel an in-flight run."""
        with cls._lock:
            if run_id in cls._runs:
                cls._runs[run_id]["state"]["life_cycle_state"] = "TERMINATED"
                cls._runs[run_id]["state"]["result_state"] = "CANCELED"

    @classmethod
    def get_all_runs(cls) -> List[Dict[str, Any]]:
        """Retrieve all recorded runs."""
        with cls._lock:
            return list(cls._runs.values())


class DatabricksSubmitRunOperator(BaseOperator):
    """Submits a one-off run to Databricks.

    Compatible with airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator.
    Supports Jinja template rendering across JSON specifications, notebook tasks,
    and cluster definitions, with automated retry handling.
    """

    template_fields = (
        "json",
        "tasks",
        "notebook_task",
        "spark_python_task",
        "new_cluster",
        "existing_cluster_id",
    )

    def __init__(
        self,
        task_id: str,
        json: Optional[Dict[str, Any]] = None,
        tasks: Optional[List[Dict[str, Any]]] = None,
        spark_jar_task: Optional[Dict[str, str]] = None,
        notebook_task: Optional[Dict[str, str]] = None,
        spark_python_task: Optional[Dict[str, Any]] = None,
        spark_submit_task: Optional[Dict[str, str]] = None,
        new_cluster: Optional[Dict[str, Any]] = None,
        existing_cluster_id: Optional[str] = None,
        databricks_conn_id: str = "databricks_default",
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 60,
        polling_period_seconds: int = 30,
        do_xcom_push: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.json = json or {}
        self.tasks = tasks
        self.spark_jar_task = spark_jar_task
        self.notebook_task = notebook_task
        self.spark_python_task = spark_python_task
        self.spark_submit_task = spark_submit_task
        self.new_cluster = new_cluster
        self.existing_cluster_id = existing_cluster_id
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.polling_period_seconds = polling_period_seconds
        self.do_xcom_push = do_xcom_push

    def execute(self, context: Optional[Dict[str, Any]] = None) -> int:
        rendered_json = _render_templates_in_obj(self.json, context)
        run_spec = dict(rendered_json) if isinstance(rendered_json, dict) else {}

        if self.tasks:
            run_spec["tasks"] = _render_templates_in_obj(self.tasks, context)
        if self.notebook_task:
            run_spec["notebook_task"] = _render_templates_in_obj(self.notebook_task, context)
        if self.spark_python_task:
            run_spec["spark_python_task"] = _render_templates_in_obj(self.spark_python_task, context)
        if self.spark_jar_task:
            run_spec["spark_jar_task"] = _render_templates_in_obj(self.spark_jar_task, context)
        if self.spark_submit_task:
            run_spec["spark_submit_task"] = _render_templates_in_obj(self.spark_submit_task, context)
        if self.new_cluster:
            run_spec["new_cluster"] = _render_templates_in_obj(self.new_cluster, context)
        if self.existing_cluster_id:
            run_spec["existing_cluster_id"] = _render_jinja_string(self.existing_cluster_id, context)

        run_res = DatabricksJobStateRegistry.submit_run(
            run_spec=run_spec,
            caller_task_id=self.task_id,
            context=context,
        )
        run_id = int(run_res["run_id"])
        return run_id


class DatabricksRunNowOperator(BaseOperator):
    """Triggers an immediate run of an existing Databricks Job.

    Compatible with airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator.
    Supports Jinja template rendering of notebook parameters, automated retry handling,
    and returns run_id.
    """

    template_fields = (
        "job_id",
        "notebook_params",
        "python_params",
        "python_named_params",
        "spark_submit_params",
    )

    def __init__(
        self,
        task_id: str,
        job_id: Union[int, str, None] = None,
        job_name: Optional[str] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        python_named_params: Optional[Dict[str, str]] = None,
        spark_submit_params: Optional[List[str]] = None,
        databricks_conn_id: str = "databricks_default",
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 60,
        polling_period_seconds: int = 30,
        do_xcom_push: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.job_id = job_id
        self.job_name = job_name
        self.notebook_params = notebook_params or {}
        self.python_params = python_params
        self.python_named_params = python_named_params
        self.spark_submit_params = spark_submit_params
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.polling_period_seconds = polling_period_seconds
        self.do_xcom_push = do_xcom_push

    def execute(self, context: Optional[Dict[str, Any]] = None) -> int:
        rendered_params = _render_templates_in_obj(self.notebook_params, context)
        rendered_job_id = self.job_id
        if isinstance(self.job_id, str):
            rendered_job_id = _render_jinja_string(self.job_id, context)

        if not rendered_job_id and not self.job_name:
            raise AirflowException("Either job_id or job_name must be provided to DatabricksRunNowOperator")

        run_res = DatabricksJobStateRegistry.run_now(
            job_id=rendered_job_id or self.job_name,
            notebook_params=rendered_params,
            caller_task_id=self.task_id,
            context=context,
        )
        return int(run_res["run_id"])


class TaskGroup:
    """Pure-Python TaskGroup modeling Apache Airflow 2.x TaskGroup."""

    def __init__(
        self,
        group_id: str,
        prefix_group_id: bool = True,
        dag: Optional[DAG] = None,
        parent_group: Optional[TaskGroup] = None,
        tooltip: str = "",
    ) -> None:
        self.group_id = group_id
        self.prefix_group_id = prefix_group_id
        self.tooltip = tooltip
        self.children: Dict[str, Union[BaseOperator, TaskGroup]] = {}
        self.parent_group = parent_group or _get_active_task_group()
        self.dag = dag or _get_active_dag()

        if self.parent_group:
            self.parent_group.add_child(self)

    def add_child(self, child: Union[BaseOperator, TaskGroup]) -> None:
        key = child.task_id if isinstance(child, BaseOperator) else child.group_id
        self.children[key] = child

    @property
    def roots(self) -> List[BaseOperator]:
        """Return all root tasks within this task group."""
        group_tasks = self.get_all_tasks()
        task_ids = {t.task_id for t in group_tasks}
        roots = []
        for t in group_tasks:
            internal_upstreams = t.upstream_task_ids.intersection(task_ids)
            if not internal_upstreams:
                roots.append(t)
        return roots or group_tasks

    @property
    def leaves(self) -> List[BaseOperator]:
        """Return all leaf tasks within this task group."""
        group_tasks = self.get_all_tasks()
        task_ids = {t.task_id for t in group_tasks}
        leaves = []
        for t in group_tasks:
            internal_downstreams = t.downstream_task_ids.intersection(task_ids)
            if not internal_downstreams:
                leaves.append(t)
        return leaves or group_tasks

    def get_all_tasks(self) -> List[BaseOperator]:
        """Recursively collect all BaseOperator instances in this group."""
        tasks: List[BaseOperator] = []
        for child in self.children.values():
            if isinstance(child, BaseOperator):
                tasks.append(child)
            elif isinstance(child, TaskGroup):
                tasks.extend(child.get_all_tasks())
        return tasks

    def __enter__(self) -> TaskGroup:
        _push_task_group(self)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _pop_task_group()

    def set_downstream(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> None:
        for leaf in self.leaves:
            leaf.set_downstream(other)

    def set_upstream(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> None:
        for root in self.roots:
            root.set_upstream(other)

    def __rshift__(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]:
        self.set_downstream(other)
        return other

    def __lshift__(
        self, other: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]
    ) -> Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]:
        self.set_upstream(other)
        return other

    def __repr__(self) -> str:
        return f"<TaskGroup: {self.group_id}>"


def chain(*tasks_or_task_lists: Union[BaseOperator, TaskGroup, List[Union[BaseOperator, TaskGroup]]]) -> None:
    """Connect a sequence of tasks or lists of tasks in linear order."""
    for i in range(len(tasks_or_task_lists) - 1):
        upstream = tasks_or_task_lists[i]
        downstream = tasks_or_task_lists[i + 1]
        if isinstance(upstream, list):
            for u in upstream:
                u.set_downstream(downstream)
        else:
            upstream.set_downstream(downstream)


class DAG:
    """Pure-Python DAG modeling Apache Airflow's DAG class."""

    def __init__(
        self,
        dag_id: str,
        description: str = "",
        schedule_interval: Optional[str] = "@daily",
        start_date: Optional[datetime] = None,
        catchup: bool = False,
        max_active_runs: int = 1,
        default_args: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        doc_md: Optional[str] = None,
        sla: Optional[timedelta] = None,
        sla_miss_callback: Optional[Callable[..., None]] = None,
        on_failure_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_success_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        **kwargs: Any,
    ) -> None:
        self.dag_id = dag_id
        self.description = description
        self.schedule_interval = schedule_interval
        self.start_date = start_date or datetime(2026, 1, 1)
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.default_args = default_args or {}
        self.tags = tags or []
        self.doc_md = doc_md
        self.sla = sla
        self.sla_miss_callback = sla_miss_callback
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.extra_kwargs = kwargs

        self._tasks: Dict[str, BaseOperator] = {}

    @property
    def tasks(self) -> List[BaseOperator]:
        return list(self._tasks.values())

    @property
    def task_dict(self) -> Dict[str, BaseOperator]:
        return dict(self._tasks)

    @property
    def task_ids(self) -> List[str]:
        return list(self._tasks.keys())

    def add_task(self, task: BaseOperator) -> None:
        if task.task_id in self._tasks and self._tasks[task.task_id] is not task:
            raise AirflowException(f"Duplicate task id: {task.task_id} in dag {self.dag_id}")
        self._tasks[task.task_id] = task
        task.dag = self

        # Apply default args if not set on the operator
        if self.default_args:
            if task.retries == 0 and "retries" in self.default_args:
                task.retries = self.default_args["retries"]
            if "retry_delay" in self.default_args and task.retry_delay == timedelta(seconds=300):
                task.retry_delay = self.default_args["retry_delay"]
            if "retry_exponential_backoff" in self.default_args and not task.retry_exponential_backoff:
                task.retry_exponential_backoff = self.default_args["retry_exponential_backoff"]
            if "max_retry_delay" in self.default_args and task.max_retry_delay is None:
                task.max_retry_delay = self.default_args["max_retry_delay"]
            if "execution_timeout" in self.default_args and task.execution_timeout is None:
                task.execution_timeout = self.default_args["execution_timeout"]
            if "on_failure_callback" in self.default_args and task.on_failure_callback is None:
                task.on_failure_callback = self.default_args["on_failure_callback"]
            if "on_retry_callback" in self.default_args and task.on_retry_callback is None:
                task.on_retry_callback = self.default_args["on_retry_callback"]
            if "owner" in self.default_args and task.owner == "airflow":
                task.owner = self.default_args["owner"]

    def get_task(self, task_id: str) -> BaseOperator:
        if task_id not in self._tasks:
            raise KeyError(f"Task '{task_id}' not found in DAG '{self.dag_id}'")
        return self._tasks[task_id]

    def has_task(self, task_id: str) -> bool:
        return task_id in self._tasks

    @property
    def roots(self) -> List[BaseOperator]:
        """Tasks with no upstream dependencies."""
        return [t for t in self.tasks if len(t.upstream_task_ids) == 0]

    @property
    def leaves(self) -> List[BaseOperator]:
        """Tasks with no downstream dependencies."""
        return [t for t in self.tasks if len(t.downstream_task_ids) == 0]

    def topological_sort(self) -> List[BaseOperator]:
        """Sort tasks in topological order; raise AirflowDagCycleException if cycles exist."""
        in_degree: Dict[str, int] = {t.task_id: len(t.upstream_task_ids) for t in self.tasks}
        queue: deque[str] = deque([tid for tid, deg in in_degree.items() if deg == 0])
        ordered: List[BaseOperator] = []

        while queue:
            tid = queue.popleft()
            task = self._tasks[tid]
            ordered.append(task)
            for down_tid in task.downstream_task_ids:
                in_degree[down_tid] -= 1
                if in_degree[down_tid] == 0:
                    queue.append(down_tid)

        if len(ordered) != len(self.tasks):
            cycle_tasks = [tid for tid, deg in in_degree.items() if deg > 0]
            raise AirflowDagCycleException(
                f"Cycle detected in DAG '{self.dag_id}' involving tasks: {cycle_tasks}"
            )
        return ordered

    def validate(self) -> None:
        """Validate DAG structural integrity and ensure acyclic graph."""
        self.topological_sort()

    def __enter__(self) -> DAG:
        _set_active_dag(self)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _set_active_dag(None)
        if exc_type is None:
            self.validate()

    def __repr__(self) -> str:
        return f"<DAG: {self.dag_id} tasks={len(self._tasks)}>"


def create_mock_task_context(
    dag: DAG,
    task: BaseOperator,
    execution_date: Optional[datetime] = None,
    exception: Optional[Exception] = None,
    try_number: int = 1,
    state: str = "failed",
    duration_sec: float = 45.2,
) -> Dict[str, Any]:
    """Generate an authentic Airflow execution context dictionary for testing callbacks."""
    now = execution_date or datetime.utcnow()
    ti = MockTaskInstance(
        task=task,
        execution_date=now,
        try_number=try_number,
        state=state,
    )
    ti.duration = duration_sec

    return {
        "dag": dag,
        "task": task,
        "task_instance": ti,
        "ti": ti,
        "execution_date": now,
        "ts": now.isoformat(),
        "exception": exception,
        "params": task.params or {},
        "run_id": f"scheduled__{now.isoformat()}",
        "try_number": try_number,
    }
