/**
 * Trading Risk Engine - Enterprise Cloud Storage (GCS) Module
 *
 * Implements:
 * - Data Lake Buckets: Bronze/Raw, Silver, Gold, and Dataflow Temp/Staging
 * - Uniform Bucket-Level Access enforced on all buckets
 * - Object Versioning enabled for regulatory auditability
 * - Automated Lifecycle Management policies (Standard -> Nearline -> Coldline -> Expiration)
 * - Incomplete multipart uploads aborted after 7 days
 * - Strict project-level labeling
 */

locals {
  bucket_prefix = "tre-${var.project_id}-${var.environment}"
  common_labels = {
    project     = "trading-risk-engine"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# -----------------------------------------------------------------------------
# 1. RAW / BRONZE DATA LAKE BUCKET
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "bronze_raw" {
  name                        = "${local.bucket_prefix}-bronze-raw"
  location                    = var.region
  project                     = var.project_id
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = var.raw_archive_retention_days
    }
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = var.raw_archive_retention_days * 3
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = var.raw_archive_expiration_days
    }
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 7
    }
  }

  labels = merge(local.common_labels, { tier = "bronze" })
}

# -----------------------------------------------------------------------------
# 2. SILVER (CLEANSED & NORMALIZED) DATA LAKE BUCKET
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "silver_processed" {
  name                        = "${local.bucket_prefix}-silver-processed"
  location                    = var.region
  project                     = var.project_id
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 90
    }
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 7
    }
  }

  labels = merge(local.common_labels, { tier = "silver" })
}

# -----------------------------------------------------------------------------
# 3. GOLD (ANALYTICS & RISK METRICS) DATA LAKE BUCKET
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "gold_curated" {
  name                        = "${local.bucket_prefix}-gold-curated"
  location                    = var.region
  project                     = var.project_id
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 7
    }
  }

  labels = merge(local.common_labels, { tier = "gold" })
}

# -----------------------------------------------------------------------------
# 4. DATAFLOW / SPARK STAGING & TEMP ARTIFACTS BUCKET
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "dataflow_staging" {
  name                        = "${local.bucket_prefix}-dataflow-staging"
  location                    = var.region
  project                     = var.project_id
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 3
    }
  }

  labels = merge(local.common_labels, { tier = "ephemeral" })
}
