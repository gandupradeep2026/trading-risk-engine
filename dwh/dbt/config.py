"""dbt Project Configuration & Environment Model (Day 23).

Defines strongly-typed configuration models for dbt Core project execution,
BigQuery and DuckDB profile targets, model selection, and environment resolution.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class DbtConfig(BaseModel):
    """Configuration model for dbt project runtime execution."""

    project_name: str = Field(default="trading_risk_dbt", description="dbt project name")
    project_dir: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parent,
        description="Root directory of the dbt project containing dbt_project.yml",
    )
    profile: str = Field(default="trading_risk", description="Profile name matching profiles.yml")
    target: str = Field(default="dev", description="Target profile environment: dev, prod, duckdb")
    environment: str = Field(default="dev", description="Platform deployment environment: dev or prod")
    gcp_project_id: str = Field(
        default="trading-risk-dev-001",
        description="Google Cloud Platform Project ID",
    )
    dataset_id: str = Field(
        default="trading_risk_dwh_dev",
        description="BigQuery destination dataset ID",
    )
    threads: int = Field(default=4, ge=1, le=32, description="Concurrent execution threads")
    models: List[str] = Field(
        default_factory=lambda: ["staging", "intermediate"],
        description="Model selectors or layer names to compile/execute",
    )
    vars: Dict[str, Any] = Field(
        default_factory=dict,
        description="Global dbt runtime variables dictionary",
    )

    @field_validator("project_dir", mode="before")
    @classmethod
    def resolve_path(cls, v: Any) -> Path:
        return Path(v).resolve() if v else Path(__file__).resolve().parent

    @classmethod
    def from_env(cls, project_dir: Optional[Path] = None) -> DbtConfig:
        """Create configuration from environment variables with sensible production defaults."""
        env = os.environ.get("ENVIRONMENT", "dev").lower()
        project_id = os.environ.get("GCP_PROJECT_ID", f"trading-risk-{env}-001")
        dataset = os.environ.get("BIGQUERY_DATASET", f"trading_risk_dwh_{env}")
        target = os.environ.get("DBT_TARGET", env)
        p_dir = project_dir or Path(os.environ.get("DBT_PROJECT_DIR", str(Path(__file__).resolve().parent)))

        return cls(
            project_dir=p_dir,
            environment=env,
            target=target,
            gcp_project_id=project_id,
            dataset_id=dataset,
            vars={
                "environment": env,
                "gcp_project_id": project_id,
                "dataset_id": dataset,
            },
        )
