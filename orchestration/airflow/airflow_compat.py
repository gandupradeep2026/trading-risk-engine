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
