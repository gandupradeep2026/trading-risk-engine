"""Airflow Connections Manager & GCP Service Integrations.

Manages declarative Airflow connections for Google Cloud Platform, Dataproc,
Cloud SQL PostgreSQL, Databricks Lakehouse, Slack Webhooks, and PagerDuty Events API v2.
Supports URI generation, Airflow environment variable exports, and connections JSON.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse

from orchestration.airflow.config.airflow_config import AirflowEnvironmentConfig


@dataclass
class AirflowConnection:
    """Represents a standardized Apache Airflow Connection definition."""

    conn_id: str
    conn_type: str
    host: Optional[str] = None
    schema: Optional[str] = None
    login: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None

    def to_uri(self) -> str:
        """Serialize connection to canonical Airflow connection URI."""
        netloc_parts: List[str] = []
        userinfo = ""
        if self.login or self.password:
            l = urllib.parse.quote(self.login or "", safe="")
            p = urllib.parse.quote(self.password or "", safe="")
            userinfo = f"{l}:{p}@" if self.password else f"{l}@"

        host_str = self.host or ""
        # Strip scheme if accidentally in host
        if host_str.startswith("http://"):
            host_str = host_str[7:]
        elif host_str.startswith("https://"):
            host_str = host_str[8:]

        netloc = f"{userinfo}{host_str}"
        if self.port:
            netloc = f"{netloc}:{self.port}"

        path = f"/{urllib.parse.quote(self.schema, safe='')}" if self.schema else ""

        query_params: Dict[str, str] = {}
        if self.extra:
            query_params["extra"] = json.dumps(self.extra, sort_keys=True)

        query = urllib.parse.urlencode(query_params) if query_params else ""
        query_suffix = f"?{query}" if query else ""

        return f"{self.conn_type}://{netloc}{path}{query_suffix}"


    def to_env_var(self) -> Tuple[str, str]:
        """Generate Airflow environment variable key and URI value."""
        env_key = f"AIRFLOW_CONN_{self.conn_id.upper()}"
        return env_key, self.to_uri()

    def to_cli_command(self) -> str:
        """Generate `airflow connections add` CLI invocation."""
        uri = self.to_uri()
        desc = f" --conn-description '{self.description}'" if self.description else ""
        return f"airflow connections add '{self.conn_id}' --conn-uri '{uri}'{desc}"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary matching `airflow connections import` JSON format."""
        d: Dict[str, Any] = {
            "conn_type": self.conn_type,
        }
        if self.host is not None:
            d["host"] = self.host
        if self.schema is not None:
            d["schema"] = self.schema
        if self.login is not None:
            d["login"] = self.login
        if self.password is not None:
            d["password"] = self.password
        if self.port is not None:
            d["port"] = self.port
        if self.extra:
            d["extra"] = self.extra
        if self.description is not None:
            d["description"] = self.description
        return d


class GcpConnectionManager:
    """Declarative manager for Trading Risk GCP and enterprise Airflow connections."""

    def __init__(self, config: Optional[AirflowEnvironmentConfig] = None) -> None:
        self.config = config or AirflowEnvironmentConfig()
        self._connections: Dict[str, AirflowConnection] = {}
        self._initialize_canonical_connections()

    def _initialize_canonical_connections(self) -> None:
        """Build the 6 canonical connections required by the Trading Risk platform."""
        cfg = self.config

        # 1. Google Cloud Platform Default (Application Default Credentials / Service Account)
        self.register(
            AirflowConnection(
                conn_id=cfg.gcp_conn_id,
                conn_type="google-cloud-platform",
                description="Default GCP connection using attached IAM Service Account and ADC",
                extra={
                    "extra__google_cloud_platform__project": cfg.gcp_project_id,
                    "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
                },
            )
        )

        # 2. Dataproc Cluster Connection
        self.register(
            AirflowConnection(
                conn_id=cfg.dataproc_conn_id,
                conn_type="google-cloud-platform",
                description="Dataproc batch execution connection bound to regional cluster",
                extra={
                    "extra__google_cloud_platform__project": cfg.gcp_project_id,
                    "extra__google_cloud_platform__region": cfg.gcp_region,
                    "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
                },
            )
        )

        # 3. Cloud SQL PostgreSQL Private IP Connection
        self.register(
            AirflowConnection(
                conn_id=cfg.cloudsql_conn_id,
                conn_type="postgres",
                host="10.10.30.5",  # Data services subnet internal RFC 1918 IP
                schema=cfg.cloudsql_database,
                login=cfg.cloudsql_user,
                port=5432,
                description="Cloud SQL PostgreSQL connection for operational risk alerts and audit sink",
                extra={
                    "sslmode": "require",
                    "cloudsql_instance": cfg.cloudsql_instance,
                },
            )
        )

        # 4. Databricks Lakehouse Workspace Connection
        self.register(
            AirflowConnection(
                conn_id=cfg.databricks_conn_id,
                conn_type="databricks",
                host=f"https://trading-risk-{cfg.environment}.cloud.databricks.com",
                description="Databricks REST API connection for Silver cleansing and Gold VaR pipelines",
                extra={
                    "token": "{{ var.value.databricks_token }}",
                },
            )
        )

        # 5. Slack Webhook Alerts Connection
        self.register(
            AirflowConnection(
                conn_id=cfg.slack_conn_id,
                conn_type="http",
                host="hooks.slack.com",
                schema="https",
                description="Slack incoming webhook connection for operational pipeline notifications",
                extra={
                    "channel": cfg.slack_channel,
                    "webhook_token": "{{ var.value.slack_webhook_token }}",
                },
            )
        )

        # 6. PagerDuty Events API v2 Connection
        self.register(
            AirflowConnection(
                conn_id=cfg.pagerduty_conn_id,
                conn_type="http",
                host="events.pagerduty.com",
                schema="https",
                description="PagerDuty Events API v2 integration for critical SLA breaches and task failures",
                extra={
                    "routing_key": "{{ var.value.pagerduty_routing_key }}",
                },
            )
        )

    def register(self, conn: AirflowConnection) -> None:
        """Register or override an Airflow connection."""
        self._connections[conn.conn_id] = conn

    def get_connection(self, conn_id: str) -> AirflowConnection:
        """Retrieve connection by identifier."""
        if conn_id not in self._connections:
            raise KeyError(f"Connection '{conn_id}' is not registered.")
        return self._connections[conn_id]

    def get_all_connections(self) -> List[AirflowConnection]:
        """Retrieve all registered connections sorted by identifier."""
        return sorted(list(self._connections.values()), key=lambda c: c.conn_id)

    def generate_env_vars(self) -> Dict[str, str]:
        """Generate complete dictionary of `AIRFLOW_CONN_*` environment variables."""
        env_map: Dict[str, str] = {}
        for conn in self._connections.values():
            key, val = conn.to_env_var()
            env_map[key] = val
        return env_map

    def export_json(self, filepath: Optional[Path | str] = None) -> str:
        """Export all connections in Airflow 2.x connections JSON import format."""
        export_payload = {
            conn.conn_id: conn.to_dict()
            for conn in self.get_all_connections()
        }
        json_str = json.dumps(export_payload, indent=2, sort_keys=True)
        if filepath:
            p = Path(filepath)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json_str, encoding="utf-8")
        return json_str

    def export_cli_script(self) -> str:
        """Generate bash script invoking `airflow connections add` for all connections."""
        lines = [
            "#!/usr/bin/env bash",
            "# Automated Airflow Connection Provisioning Script for Trading Risk Engine",
            "set -euo pipefail",
            "",
        ]
        for conn in self.get_all_connections():
            lines.append(f"# {conn.description or conn.conn_id}")
            lines.append(conn.to_cli_command())
            lines.append("")
        return "\n".join(lines)

    def validate_all(self) -> None:
        """Validate that all connections contain valid required fields and parseable URIs."""
        for conn in self._connections.values():
            if not conn.conn_id:
                raise ValueError("Connection conn_id cannot be empty")
            if not conn.conn_type:
                raise ValueError(f"Connection '{conn.conn_id}' missing conn_type")
            uri = conn.to_uri()
            parsed = urllib.parse.urlsplit(uri)
            if not parsed.scheme:
                raise ValueError(f"Generated URI for '{conn.conn_id}' missing scheme: {uri}")
