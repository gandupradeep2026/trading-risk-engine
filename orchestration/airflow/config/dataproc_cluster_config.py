"""Dataproc Ephemeral Cluster & PySpark Batch Job Configuration.

Provides strongly typed configuration for ephemeral Dataproc PySpark clusters
and batch analytical job submissions on Google Cloud Platform.
Enforces security, network segmentation (PGA, internal IPs), cost optimization
(Preemptible / Spot secondary workers), and automatic lifecycle teardown (idle TTL).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import os
import re
from typing import Any, Dict, List, Optional


@dataclass
class DataprocEphemeralClusterConfig:
    """Strongly typed configuration for ephemeral Dataproc PySpark clusters."""

    project_id: str = "trading-risk-dev-001"
    region: str = "us-central1"
    zone: str = "us-central1-a"
    subnetwork: str = "sb-data-services"
    cluster_name_prefix: str = "tre-dp-batch"
    service_account: str = ""
    internal_ip_only: bool = True

    # Master node configuration
    master_machine_type: str = "n2-standard-4"
    master_disk_type: str = "pd-ssd"
    master_disk_size_gb: int = 100
    master_num_instances: int = 1

    # Primary worker nodes configuration
    worker_machine_type: str = "n2-standard-4"
    worker_disk_type: str = "pd-standard"
    worker_disk_size_gb: int = 100
    worker_num_instances: int = 2

    # Secondary / Spot preemptible workers for aggressive cloud cost optimization
    secondary_worker_num_instances: int = 2
    secondary_worker_preemptible: bool = True

    # Software & Image configuration
    image_version: str = "2.2-debian12"
    spark_properties: Dict[str, str] = field(
        default_factory=lambda: {
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.dynamicAllocation.enabled": "true",
            "spark:spark.dynamicAllocation.minExecutors": "1",
            "spark:spark.dynamicAllocation.maxExecutors": "8",
            "spark:spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark:spark.sql.parquet.output.committer.class": "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
            "dataproc:dataproc.conscrypt.provider.enable": "false",
        }
    )

    # Automated cluster lifecycle safety nets (guarantees zero lingering idle compute costs)
    idle_delete_ttl_sec: int = 1200  # Auto-teardown if idle for 20 minutes
    auto_delete_ttl_sec: int = 7200  # Hard kill after 2 hours maximum lifetime

    # Firewall and network tagging
    tags: List[str] = field(
        default_factory=lambda: ["trading-risk", "dataproc-ephemeral", "batch"]
    )

    def __post_init__(self) -> None:
        if not self.service_account:
            self.service_account = f"sa-composer-dev@{self.project_id}.iam.gserviceaccount.com"
        self.validate()

    def validate(self) -> None:
        """Validate configuration invariants."""
        if not self.project_id:
            raise ValueError("project_id cannot be empty")
        if not self.region:
            raise ValueError("region cannot be empty")
        if not self.subnetwork:
            raise ValueError("subnetwork cannot be empty")
        if self.master_num_instances < 1:
            raise ValueError("master_num_instances must be at least 1")
        if self.worker_num_instances < 0:
            raise ValueError("worker_num_instances cannot be negative")
        if self.secondary_worker_num_instances < 0:
            raise ValueError("secondary_worker_num_instances cannot be negative")
        if self.master_disk_size_gb < 20:
            raise ValueError("master_disk_size_gb must be at least 20 GB")
        if self.worker_disk_size_gb < 20:
            raise ValueError("worker_disk_size_gb must be at least 20 GB")
        if self.idle_delete_ttl_sec <= 0:
            raise ValueError("idle_delete_ttl_sec must be positive")
        if self.auto_delete_ttl_sec < self.idle_delete_ttl_sec:
            raise ValueError("auto_delete_ttl_sec must be >= idle_delete_ttl_sec")

    def generate_cluster_name(self, execution_date_nodash: str, try_number: int = 1) -> str:
        """Generate canonical RFC 1035 compliant cluster name.

        Must be lowercase, start with a letter, <= 51 characters, alphanumeric + hyphens.
        """
        raw_name = f"{self.cluster_name_prefix}-{execution_date_nodash}-{try_number}".lower()
        # Truncate if exceeds 51 characters while preserving validity
        cluster_name = raw_name[:51].rstrip("-")
        pattern = r"^[a-z][a-z0-9-]{0,49}[a-z0-9]$"
        if not re.match(pattern, cluster_name):
            raise ValueError(f"Generated cluster name '{cluster_name}' violates Dataproc naming standard: {pattern}")
        return cluster_name

    def to_cluster_config_dict(self) -> Dict[str, Any]:
        """Produce the standard Google Cloud Dataproc ClusterConfig dictionary for Airflow."""
        subnetwork_uri = (
            self.subnetwork
            if self.subnetwork.startswith("projects/")
            else f"projects/{self.project_id}/regions/{self.region}/subnetworks/{self.subnetwork}"
        )

        config: Dict[str, Any] = {
            "gce_cluster_config": {
                "subnetwork_uri": subnetwork_uri,
                "internal_ip_only": self.internal_ip_only,
                "service_account": self.service_account,
                "service_account_scopes": [
                    "https://www.googleapis.com/auth/cloud-platform"
                ],
                "tags": list(self.tags),
                "metadata": {
                    "enable-oslogin": "TRUE",
                },
            },
            "master_config": {
                "num_instances": self.master_num_instances,
                "machine_type_uri": self.master_machine_type,
                "disk_config": {
                    "boot_disk_type": self.master_disk_type,
                    "boot_disk_size_gb": self.master_disk_size_gb,
                },
            },
            "worker_config": {
                "num_instances": self.worker_num_instances,
                "machine_type_uri": self.worker_machine_type,
                "disk_config": {
                    "boot_disk_type": self.worker_disk_type,
                    "boot_disk_size_gb": self.worker_disk_size_gb,
                },
            },
            "software_config": {
                "image_version": self.image_version,
                "properties": dict(self.spark_properties),
            },
            "lifecycle_config": {
                "idle_delete_ttl": f"{self.idle_delete_ttl_sec}s",
                "auto_delete_ttl": f"{self.auto_delete_ttl_sec}s",
            },
        }

        # Add secondary spot/preemptible worker nodes if configured
        if self.secondary_worker_num_instances > 0:
            config["secondary_worker_config"] = {
                "num_instances": self.secondary_worker_num_instances,
                "is_preemptible": self.secondary_worker_preemptible,
                "preemptibility": "PREEMPTIBLE" if self.secondary_worker_preemptible else "NON_PREEMPTIBLE",
            }

        return config

    @classmethod
    def from_env(cls, environment: Optional[str] = None) -> DataprocEphemeralClusterConfig:
        """Construct configuration from environment variables."""
        env = (environment or os.getenv("ENVIRONMENT", "dev")).lower()
        project_id = os.getenv("GCP_PROJECT_ID", f"trading-risk-{env}-001")
        region = os.getenv("GCP_REGION", "us-central1")

        return cls(
            project_id=project_id,
            region=region,
            zone=os.getenv("DATAPROC_ZONE", f"{region}-a"),
            subnetwork=os.getenv("DATAPROC_SUBNETWORK", "sb-data-services"),
            cluster_name_prefix=os.getenv("DATAPROC_CLUSTER_PREFIX", f"tre-dp-batch-{env}"),
            service_account=os.getenv(
                "DATAPROC_SERVICE_ACCOUNT",
                f"sa-composer-{env}@{project_id}.iam.gserviceaccount.com",
            ),
            internal_ip_only=os.getenv("DATAPROC_INTERNAL_IP_ONLY", "true").lower() == "true",
            master_machine_type=os.getenv("DATAPROC_MASTER_MACHINE", "n2-standard-4"),
            worker_machine_type=os.getenv("DATAPROC_WORKER_MACHINE", "n2-standard-4"),
            num_workers=int(os.getenv("DATAPROC_NUM_WORKERS", "2")),
            secondary_worker_num_instances=int(os.getenv("DATAPROC_NUM_SECONDARY_WORKERS", "2")),
            secondary_worker_preemptible=os.getenv("DATAPROC_SECONDARY_PREEMPTIBLE", "true").lower() == "true",
            idle_delete_ttl_sec=int(os.getenv("DATAPROC_IDLE_DELETE_TTL_SEC", "1200")),
            auto_delete_ttl_sec=int(os.getenv("DATAPROC_AUTO_DELETE_TTL_SEC", "7200")),
        )


@dataclass
class DataprocBatchJobConfig:
    """Strongly typed configuration for submitting PySpark batch jobs to Dataproc."""

    project_id: str = "trading-risk-dev-001"
    region: str = "us-central1"
    main_python_file_uri: str = "gs://trading-risk-bronze-dev/scripts/dataproc/daily_tick_aggregation.py"
    args: List[str] = field(default_factory=list)
    jar_file_uris: List[str] = field(default_factory=list)
    python_file_uris: List[str] = field(default_factory=list)
    archive_uris: List[str] = field(default_factory=list)
    properties: Dict[str, str] = field(
        default_factory=lambda: {
            "spark.submit.deployMode": "cluster",
            "spark.sql.shuffle.partitions": "16",
        }
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate job configuration invariants."""
        if not self.project_id:
            raise ValueError("project_id cannot be empty")
        if not self.region:
            raise ValueError("region cannot be empty")
        if not self.main_python_file_uri:
            raise ValueError("main_python_file_uri cannot be empty")

    def to_pyspark_job_dict(self, cluster_name: str, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate standard Dataproc PySpark job dictionary for Airflow DataprocSubmitJobOperator."""
        job_dict: Dict[str, Any] = {
            "placement": {
                "cluster_name": cluster_name,
            },
            "pyspark_job": {
                "main_python_file_uri": self.main_python_file_uri,
                "args": list(self.args),
                "properties": dict(self.properties),
            },
        }

        if self.jar_file_uris:
            job_dict["pyspark_job"]["jar_file_uris"] = list(self.jar_file_uris)
        if self.python_file_uris:
            job_dict["pyspark_job"]["python_file_uris"] = list(self.python_file_uris)
        if self.archive_uris:
            job_dict["pyspark_job"]["archive_uris"] = list(self.archive_uris)

        if job_id:
            job_dict["reference"] = {
                "project_id": self.project_id,
                "job_id": job_id,
            }

        return job_dict
