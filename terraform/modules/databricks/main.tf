/**
 * Trading Risk Engine - Databricks Workspace & Delta Lake Module
 *
 * Implements:
 * - Databricks Lakehouse Autoscaling Cluster with Delta Lake extensions
 * - Unity Catalog external locations mapping to GCS Bronze, Silver, and Gold buckets
 * - Catalog and Bronze schema definitions with GCS storage roots
 * - Production Databricks Bronze ingestion scheduled job
 * - Cost-optimized GCP Spot worker instances with automated idle termination
 */

locals {
  cluster_name = "tre-databricks-bronze-${var.environment}"
  job_name     = "tre-bronze-ingestion-${var.environment}"
  common_tags = {
    project     = "trading-risk-engine"
    environment = var.environment
    managed_by  = "terraform"
    tier        = "bronze"
  }
}

# -----------------------------------------------------------------------------
# 1. DATABRICKS LAKEHOUSE CLUSTER CONFIGURATION
# -----------------------------------------------------------------------------
resource "databricks_cluster" "bronze_lakehouse_cluster" {
  cluster_name            = local.cluster_name
  spark_version           = var.spark_version
  node_type_id            = var.cluster_node_type
  driver_node_type_id     = var.cluster_driver_node_type
  autotermination_minutes = var.autotermination_minutes

  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }

  gcp_attributes {
    use_preemptible_executors = true
    availability              = "SPOT_WITH_FALLBACK_GCP"
  }

  spark_conf = {
    # Delta Lake Spark Extensions & Catalog Configuration
    "spark.sql.extensions"                                            = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog"                                 = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "spark.databricks.delta.optimizeWrite.enabled"                    = "true"
    "spark.databricks.delta.autoCompact.enabled"                      = "true"
    "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled" = "true"
    "spark.sql.streaming.schemaInference"                             = "false"

    # Google Cloud Storage (GCS) Hadoop Connector Settings
    "spark.hadoop.fs.gs.impl"                                         = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    "spark.hadoop.fs.AbstractFileSystem.gs.impl"                      = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    "spark.hadoop.fs.gs.auth.service.account.enable"                  = "true"
  }

  spark_env_vars = {
    "ENVIRONMENT" = var.environment
    "GCP_PROJECT" = var.project_id
    "GCP_REGION"  = var.region
  }

  custom_tags = local.common_tags
}

# -----------------------------------------------------------------------------
# 2. UNITY CATALOG GCS STORAGE CREDENTIAL & EXTERNAL LOCATIONS
# -----------------------------------------------------------------------------
resource "databricks_storage_credential" "gcs_credential" {
  name    = "cred-gcs-lakehouse-${var.environment}"
  comment = "Google Cloud Service Account credential for GCS Medallion Data Lake access"

  gcp_service_account_key {
    email = "sa-composer-${var.environment}@${var.project_id}.iam.gserviceaccount.com"
  }
}

resource "databricks_external_location" "bronze_external_location" {
  name            = "ext-loc-bronze-${var.environment}"
  url             = "${var.gcs_bronze_bucket_url}/delta"
  credential_name = databricks_storage_credential.gcs_credential.name
  comment         = "Unity Catalog external storage location for Bronze Raw Delta Lake tables"
}

resource "databricks_external_location" "silver_external_location" {
  name            = "ext-loc-silver-${var.environment}"
  url             = "${var.gcs_silver_bucket_url}/delta"
  credential_name = databricks_storage_credential.gcs_credential.name
  comment         = "Unity Catalog external storage location for Silver Cleansed Delta Lake tables"
}

resource "databricks_external_location" "gold_external_location" {
  name            = "ext-loc-gold-${var.environment}"
  url             = "${var.gcs_gold_bucket_url}/delta"
  credential_name = databricks_storage_credential.gcs_credential.name
  comment         = "Unity Catalog external storage location for Gold Curated VaR Delta Lake tables"
}

# -----------------------------------------------------------------------------
# 3. UNITY CATALOG & BRONZE SCHEMA
# -----------------------------------------------------------------------------
resource "databricks_catalog" "lakehouse_catalog" {
  name    = var.catalog_name
  comment = "Analytical Lakehouse Catalog for Trading Risk Engine Medallion architecture"
  properties = {
    "environment" = var.environment
    "managed_by"  = "terraform"
  }
}

resource "databricks_schema" "bronze_schema" {
  catalog_name = databricks_catalog.lakehouse_catalog.name
  name         = "bronze"
  comment      = "Bronze raw ingestion Delta Lake schema: ticks, orders, and rolling VWAP"
  storage_root = "${var.gcs_bronze_bucket_url}/delta/bronze"
  properties = {
    "tier"        = "bronze"
    "environment" = var.environment
  }

  depends_on = [
    databricks_external_location.bronze_external_location
  ]
}

resource "databricks_schema" "silver_schema" {
  catalog_name = databricks_catalog.lakehouse_catalog.name
  name         = "silver"
  comment      = "Silver cleansed and deduplicated Delta Lake schema: ticks, orders, and rolling VWAP"
  storage_root = "${var.gcs_silver_bucket_url}/delta/silver"
  properties = {
    "tier"        = "silver"
    "environment" = var.environment
  }

  depends_on = [
    databricks_external_location.silver_external_location
  ]
}

resource "databricks_schema" "gold_schema" {
  catalog_name = databricks_catalog.lakehouse_catalog.name
  name         = "gold"
  comment      = "Gold curated analytical Delta Lake schema: portfolio Value-at-Risk (VaR) and risk feature store"
  storage_root = "${var.gcs_gold_bucket_url}/delta/gold"
  properties = {
    "tier"        = "gold"
    "environment" = var.environment
  }

  depends_on = [
    databricks_external_location.gold_external_location
  ]
}

# -----------------------------------------------------------------------------
# 4. DATABRICKS BRONZE INGESTION JOB DEFINITION
# -----------------------------------------------------------------------------
resource "databricks_job" "bronze_ingestion_job" {
  name = local.job_name

  existing_cluster_id = databricks_cluster.bronze_lakehouse_cluster.id

  task {
    task_key = "ingest_raw_ticks_and_orders_to_bronze"

    notebook_task {
      notebook_path = "/Workspace/trading-risk/notebooks/01_bronze_ingestion_runner"
      base_parameters = {
        environment        = var.environment
        gcs_bronze_bucket  = var.gcs_bronze_bucket_name
        catalog_name       = databricks_catalog.lakehouse_catalog.name
        schema_name        = databricks_schema.bronze_schema.name
        run_optimize       = "true"
        run_vacuum         = "true"
      }
    }
  }

  max_concurrent_runs = 1

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# 5. DATABRICKS SILVER CLEANSING JOB DEFINITION
# -----------------------------------------------------------------------------
resource "databricks_job" "silver_cleansing_job" {
  name = "tre-silver-cleansing-${var.environment}"

  existing_cluster_id = databricks_cluster.bronze_lakehouse_cluster.id

  task {
    task_key = "cleanse_enforce_and_deduplicate_to_silver"

    notebook_task {
      notebook_path = "/Workspace/trading-risk/notebooks/02_silver_cleansing_runner"
      base_parameters = {
        environment        = var.environment
        gcs_silver_bucket  = var.gcs_silver_bucket_name
        catalog_name       = databricks_catalog.lakehouse_catalog.name
        bronze_schema_name = databricks_schema.bronze_schema.name
        silver_schema_name = databricks_schema.silver_schema.name
        run_optimize       = "true"
        run_vacuum         = "true"
      }
    }
  }

  max_concurrent_runs = 1

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# 6. DATABRICKS GOLD VALUE-AT-RISK (VaR) & FEATURE STORE JOB DEFINITION
# -----------------------------------------------------------------------------
resource "databricks_job" "gold_var_job" {
  name = "tre-gold-var-feature-store-${var.environment}"

  existing_cluster_id = databricks_cluster.bronze_lakehouse_cluster.id

  task {
    task_key = "compute_portfolio_var_and_feature_store"

    notebook_task {
      notebook_path = "/Workspace/trading-risk/notebooks/03_gold_var_feature_store_runner"
      base_parameters = {
        environment             = var.environment
        gcs_gold_bucket         = var.gcs_gold_bucket_name
        catalog_name            = databricks_catalog.lakehouse_catalog.name
        silver_schema_name      = databricks_schema.silver_schema.name
        gold_schema_name        = databricks_schema.gold_schema.name
        confidence_levels       = "0.95,0.99"
        monte_carlo_simulations = "10000"
        run_optimize            = "true"
        run_vacuum              = "true"
      }
    }
  }

  max_concurrent_runs = 1

  tags = local.common_tags
}
