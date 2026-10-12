"""Storage and Cloud SQL Configuration Validation Tests (Day 02).

Validates:
- GCS Storage Module: Bucket definitions, Uniform Bucket-Level Access, versioning,
  and tiering lifecycle policies (Standard -> Nearline -> Coldline -> Expiration).
- Cloud SQL Module: Private IP enforcement, zero public IP, deletion protection,
  PITR automated backup window, and database flags.
- Operational DDL: Syntax, indices, and constraints for hot risk alerts.
"""

from pathlib import Path
import re
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_STORAGE_DIR = REPO_ROOT / "terraform" / "modules" / "storage"
TF_CLOUDSQL_DIR = REPO_ROOT / "terraform" / "modules" / "cloudsql"
TF_ROOT_DIR = REPO_ROOT / "terraform"


class TestTerraformStorageModule:
    """Validate GCS data lake storage module configuration."""

    def test_storage_module_files_exist(self) -> None:
        """Verify all critical storage module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for filename in required:
            file_path = TF_STORAGE_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_all_buckets_enforce_uniform_access(self) -> None:
        """Verify all defined buckets enforce uniform bucket-level access."""
        main_tf = (TF_STORAGE_DIR / "main.tf").read_text(encoding="utf-8")
        bucket_count = main_tf.count('resource "google_storage_bucket"')
        uniform_count = main_tf.count("uniform_bucket_level_access = true")
        assert bucket_count == 4, f"Expected 4 buckets (bronze, silver, gold, staging), got {bucket_count}"
        assert uniform_count == bucket_count, "All storage buckets must enforce uniform_bucket_level_access = true"

    def test_versioning_and_lifecycle_on_bronze(self) -> None:
        """Verify bronze bucket has versioning and lifecycle rules configured."""
        main_tf = (TF_STORAGE_DIR / "main.tf").read_text(encoding="utf-8")
        assert 'google_storage_bucket" "bronze_raw"' in main_tf
        assert 'storage_class = "NEARLINE"' in main_tf
        assert 'storage_class = "COLDLINE"' in main_tf
        assert 'type = "Delete"' in main_tf
        assert 'AbortIncompleteMultipartUpload' in main_tf

    def test_balanced_braces_in_storage_module(self) -> None:
        """Verify balanced HCL block braces in storage module."""
        for tf_file in TF_STORAGE_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"


class TestTerraformCloudSQLModule:
    """Validate Cloud SQL PostgreSQL module configuration."""

    def test_cloudsql_module_files_exist(self) -> None:
        """Verify all critical Cloud SQL module files exist."""
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf", "init_schema.sql"]
        for filename in required:
            file_path = TF_CLOUDSQL_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_zero_public_ip_enforcement(self) -> None:
        """Ensure public IPv4 is explicitly disabled and private network is bound."""
        main_tf = (TF_CLOUDSQL_DIR / "main.tf").read_text(encoding="utf-8")
        assert "ipv4_enabled                                  = false" in main_tf
        assert "private_network                               = var.network_id" in main_tf

    def test_backup_and_pitr_configuration(self) -> None:
        """Ensure automated backups and point-in-time-recovery are enabled."""
        main_tf = (TF_CLOUDSQL_DIR / "main.tf").read_text(encoding="utf-8")
        assert "enabled                        = true" in main_tf
        assert "point_in_time_recovery_enabled = true" in main_tf
        assert "transaction_log_retention_days = 7" in main_tf

    def test_query_insights_and_flags(self) -> None:
        """Verify operational query insights and performance logging flags."""
        main_tf = (TF_CLOUDSQL_DIR / "main.tf").read_text(encoding="utf-8")
        assert "query_insights_enabled  = true" in main_tf
        assert "log_min_duration_statement" in main_tf
        assert "max_connections" in main_tf

    def test_balanced_braces_in_cloudsql_module(self) -> None:
        """Verify balanced HCL block braces in Cloud SQL module."""
        for tf_file in TF_CLOUDSQL_DIR.glob("*.tf"):
            content = tf_file.read_text(encoding="utf-8")
            cleaned = re.sub(r"#.*", "", content)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            assert cleaned.count("{") == cleaned.count("}"), f"Unbalanced braces in {tf_file.name}"


class TestOperationalSchemaDDL:
    """Validate the operational PostgreSQL DDL for risk alerts."""

    def test_risk_alerts_table_syntax(self) -> None:
        """Verify table creation statements, columns, and constraints."""
        ddl = (TF_CLOUDSQL_DIR / "init_schema.sql").read_text(encoding="utf-8")
        assert "CREATE TABLE IF NOT EXISTS risk_alerts" in ddl
        assert "CREATE TABLE IF NOT EXISTS portfolio_limits" in ddl
        assert "severity            VARCHAR(16)" in ddl
        assert "CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'))" in ddl
        assert "status              VARCHAR(32)" in ddl
        assert "payload_json        JSONB" in ddl

    def test_risk_alerts_indices_defined(self) -> None:
        """Ensure operational indices exist for querying by portfolio, ticker, and severity."""
        ddl = (TF_CLOUDSQL_DIR / "init_schema.sql").read_text(encoding="utf-8")
        assert "idx_risk_alerts_portfolio_time" in ddl
        assert "idx_risk_alerts_ticker" in ddl
        assert "idx_risk_alerts_severity_status" in ddl
        assert "idx_risk_alerts_payload_gin" in ddl
