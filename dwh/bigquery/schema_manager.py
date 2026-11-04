"""BigQuery Schema & DDL Management Service (Day 22).

Loads, validates, and renders BigQuery JSON schemas and GoogleSQL DDL scripts,
verifying structural integrity, data type compatibility, and alignment with
the Star-Schema table catalog.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

import pyarrow as pa

from dwh.bigquery.config import (
    FEDERATED_VIEWS_CATALOG,
    MATERIALIZED_VIEWS_CATALOG,
    STAR_SCHEMA_CATALOG,
    TablePartitionClusteringSpec,
)

ALLOWED_BIGQUERY_TYPES = {
    "STRING",
    "BYTES",
    "INTEGER",
    "INT64",
    "FLOAT",
    "FLOAT64",
    "NUMERIC",
    "BIGNUMERIC",
    "BOOLEAN",
    "BOOL",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "DATETIME",
    "GEOGRAPHY",
    "JSON",
    "RECORD",
    "STRUCT",
}

ALLOWED_MODES = {"REQUIRED", "NULLABLE", "REPEATED"}


class SchemaValidationError(Exception):
    """Raised when BigQuery JSON schema or DDL violates validation constraints."""
    pass


class BigQuerySchemaManager:
    """Manages BigQuery table schemas, DDL scripts, and catalog synchronization."""

    def __init__(
        self,
        schemas_dir: Optional[Path] = None,
        ddl_dir: Optional[Path] = None,
    ) -> None:
        module_dir = Path(__file__).resolve().parent
        self.schemas_dir = schemas_dir or (module_dir / "schemas")
        self.ddl_dir = ddl_dir or (module_dir / "ddl")

    def load_json_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Load and parse the JSON schema definition for a specified table."""
        schema_path = self.schemas_dir / f"{table_name}.json"
        if not schema_path.is_file():
            raise FileNotFoundError(f"BigQuery JSON schema not found: {schema_path}")

        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            raise SchemaValidationError(f"Invalid JSON in schema file {schema_path}: {e}") from e

        if not isinstance(data, list):
            raise SchemaValidationError(
                f"Schema {schema_path} must be a JSON array of column objects, got {type(data).__name__}"
            )

        self.validate_json_schema_fields(table_name, data)
        return data

    def validate_json_schema_fields(
        self, table_name: str, fields: List[Dict[str, Any]]
    ) -> None:
        """Validate that all fields adhere to Google BigQuery schema specification."""
        if not fields:
            raise SchemaValidationError(f"Schema for '{table_name}' must contain at least one field.")

        seen_names = set()
        for field in fields:
            name = field.get("name")
            ftype = field.get("type")
            mode = field.get("mode", "NULLABLE")

            if not name or not isinstance(name, str):
                raise SchemaValidationError(f"Table '{table_name}': field name must be a non-empty string.")
            if name in seen_names:
                raise SchemaValidationError(f"Table '{table_name}': duplicate column name '{name}'.")
            seen_names.add(name)

            if not ftype or ftype.upper() not in ALLOWED_BIGQUERY_TYPES:
                raise SchemaValidationError(
                    f"Table '{table_name}', column '{name}': invalid type '{ftype}'. "
                    f"Allowed: {sorted(ALLOWED_BIGQUERY_TYPES)}"
                )

            if mode.upper() not in ALLOWED_MODES:
                raise SchemaValidationError(
                    f"Table '{table_name}', column '{name}': invalid mode '{mode}'. "
                    f"Allowed: {sorted(ALLOWED_MODES)}"
                )

    def load_raw_ddl(self, table_name: str) -> str:
        """Load the raw SQL DDL file for a specified table."""
        ddl_path = self.ddl_dir / f"{table_name}.sql"
        if not ddl_path.is_file():
            raise FileNotFoundError(f"BigQuery DDL file not found: {ddl_path}")

        return ddl_path.read_text(encoding="utf-8")

    def render_ddl(
        self,
        table_name: str,
        project_id: str,
        dataset_id: str,
        region: str = "us-central1",
        connection_id: str = "conn_cloudsql_risk_db_dev",
    ) -> str:
        """Render table DDL with project_id, dataset_id, region, and connection_id interpolation."""
        raw_ddl = self.load_raw_ddl(table_name)
        rendered = raw_ddl.replace("${project_id}", project_id)
        rendered = rendered.replace("${dataset_id}", dataset_id)
        rendered = rendered.replace("${region}", region)
        rendered = rendered.replace("${connection_id}", connection_id)
        return rendered

    def render_master_ddl(self, project_id: str, dataset_id: str) -> str:
        """Render master DDL (all_star_schema.sql) executing all tables in order."""
        raw_ddl = self.load_raw_ddl("all_star_schema")
        rendered = raw_ddl.replace("${project_id}", project_id)
        rendered = rendered.replace("${dataset_id}", dataset_id)
        return rendered

    def render_federated_views_ddl(
        self,
        project_id: str,
        dataset_id: str,
        region: str = "us-central1",
        connection_id: str = "conn_cloudsql_risk_db_dev",
    ) -> str:
        """Render master federated views DDL (all_federated_views.sql)."""
        raw_ddl = self.load_raw_ddl("all_federated_views")
        rendered = raw_ddl.replace("${project_id}", project_id)
        rendered = rendered.replace("${dataset_id}", dataset_id)
        rendered = rendered.replace("${region}", region)
        rendered = rendered.replace("${connection_id}", connection_id)
        return rendered


    def validate_ddl_and_schema_parity(self, table_name: str) -> None:
        """Verify that column definitions match between JSON schema and DDL file."""
        json_fields = self.load_json_schema(table_name)
        json_col_names = [f["name"] for f in json_fields]

        ddl = self.load_raw_ddl(table_name)
        # Extract column names from DDL (lines inside CREATE TABLE (...) block)
        ddl_clean = re.sub(r"--.*", "", ddl)
        match = re.search(r"CREATE\s+OR\s+REPLACE\s+TABLE[^(]+\((.*?)\)\s*(?:PARTITION\s+BY|CLUSTER\s+BY|OPTIONS)", ddl_clean, re.DOTALL | re.IGNORECASE)
        if not match:
            # Fallback if without options
            match = re.search(r"CREATE\s+OR\s+REPLACE\s+TABLE[^(]+\((.*?)\)", ddl_clean, re.DOTALL | re.IGNORECASE)

        if not match:
            raise SchemaValidationError(f"Could not parse CREATE TABLE columns in DDL for '{table_name}'.")

        col_block = match.group(1)
        ddl_cols = []
        for line in col_block.splitlines():
            line = line.strip().rstrip(",")
            if line:
                tokens = line.split()
                if tokens:
                    ddl_cols.append(tokens[0])

        if json_col_names != ddl_cols:
            raise SchemaValidationError(
                f"Column mismatch for table '{table_name}':\n"
                f"JSON schema: {json_col_names}\n"
                f"DDL columns: {ddl_cols}"
            )

    def validate_catalog(self) -> None:
        """Validate all tables in the star schema catalog against schema files and DDLs."""
        for table_name, spec in STAR_SCHEMA_CATALOG.items():
            self.load_json_schema(table_name)
            self.load_raw_ddl(table_name)
            self.validate_ddl_and_schema_parity(table_name)

            # Check clustering columns exist in schema
            fields = {f["name"] for f in self.load_json_schema(table_name)}
            for cluster_col in spec.clustering_columns:
                if cluster_col not in fields:
                    raise SchemaValidationError(
                        f"Clustering column '{cluster_col}' in spec not found in table '{table_name}' fields: {fields}"
                    )
            if spec.partition_column and spec.partition_column not in fields:
                raise SchemaValidationError(
                    f"Partition column '{spec.partition_column}' in spec not found in table '{table_name}' fields: {fields}"
                )

    def validate_materialized_views_catalog(self) -> None:
        """Validate all materialized views against JSON schema files, DDLs, and specs."""
        for mv_name, spec in MATERIALIZED_VIEWS_CATALOG.items():
            fields = {f["name"] for f in self.load_json_schema(mv_name)}
            self.load_raw_ddl(mv_name)
            for cluster_col in spec.clustering_columns:
                if cluster_col not in fields:
                    raise SchemaValidationError(
                        f"Clustering column '{cluster_col}' in spec not found in materialized view '{mv_name}' fields: {fields}"
                    )
            if spec.partition_column and spec.partition_column not in fields:
                raise SchemaValidationError(
                    f"Partition column '{spec.partition_column}' in spec not found in materialized view '{mv_name}' fields: {fields}"
                )

    def validate_federated_views_catalog(self) -> None:
        """Validate all federated views against JSON schema files, DDLs, and catalog specs."""
        for view_name, spec in FEDERATED_VIEWS_CATALOG.items():
            self.load_json_schema(view_name)
            self.load_raw_ddl(view_name)


    def to_arrow_schema(self, table_name: str) -> pa.Schema:
        """Convert BigQuery JSON schema into an Apache PyArrow Schema."""
        fields = self.load_json_schema(table_name)
        arrow_fields = []
        for f in fields:
            name = f["name"]
            bq_type = f.get("type", "STRING").upper()
            mode = f.get("mode", "NULLABLE").upper()
            nullable = (mode != "REQUIRED")

            if bq_type in ("STRING", "JSON"):
                pa_type = pa.string()
            elif bq_type in ("INT64", "INTEGER"):
                pa_type = pa.int64()
            elif bq_type in ("FLOAT64", "FLOAT"):
                pa_type = pa.float64()
            elif bq_type == "NUMERIC":
                pa_type = pa.decimal128(18, 4)
            elif bq_type == "BIGNUMERIC":
                pa_type = pa.decimal128(38, 9)
            elif bq_type in ("BOOL", "BOOLEAN"):
                pa_type = pa.bool_()
            elif bq_type == "TIMESTAMP":
                pa_type = pa.timestamp("us", tz="UTC")
            elif bq_type == "DATE":
                pa_type = pa.date32()
            elif bq_type == "BYTES":
                pa_type = pa.binary()
            else:
                pa_type = pa.string()

            arrow_fields.append(pa.field(name, pa_type, nullable=nullable))
        return pa.schema(arrow_fields)
