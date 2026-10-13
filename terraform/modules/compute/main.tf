/**
 * Trading Risk Engine - Ingestion Worker Compute Engine Module
 *
 * Implements:
 * - Compute Engine VM instance isolated in the private Ingestion subnet
 * - Zero public IPv4 exposure (egress strictly via Cloud NAT, ingress via IAP SSH)
 * - Shielded VM security profile (Secure Boot, vTPM, Integrity Monitoring)
 * - Least-privilege IAM service account attachment
 * - Automated systemd & Docker provisioning via startup script
 */

resource "google_compute_instance" "ingestion_worker" {
  name         = var.instance_name != "" ? var.instance_name : "vm-ingestion-worker-${var.environment}"
  machine_type = var.machine_type
  zone         = var.zone
  project      = var.project_id
  description  = "High-throughput market data ingestion worker running containerized daemon"

  tags = var.network_tags

  boot_disk {
    auto_delete = true
    mode        = "READ_WRITE"

    initialize_params {
      image = var.boot_disk_image
      size  = var.boot_disk_size_gb
      type  = var.boot_disk_type
      labels = {
        environment = var.environment
        managed_by  = "terraform"
      }
    }
  }

  network_interface {
    subnetwork = var.subnetwork_id
    # Zero external public IP: No access_config block, strictly private egress via Cloud NAT
  }

  service_account {
    email  = var.service_account_email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
  }

  metadata = {
    enable-oslogin             = "TRUE"
    serial-port-logging-enable = "true"
    startup-script = templatefile("${path.module}/scripts/startup.sh", {
      environment            = var.environment
      project_id             = var.project_id
      pubsub_topic_id        = var.pubsub_topic_id
      worker_container_image = var.worker_container_image
      tickers                = join(",", var.market_feed_tickers)
    })
  }

  labels = merge(
    {
      environment = var.environment
      role        = "market-data-ingestion"
      managed_by  = "terraform"
    },
    var.labels
  )

  lifecycle {
    ignore_changes = [
      metadata["ssh-keys"],
    ]
  }
}
