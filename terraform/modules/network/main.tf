/**
 * Trading Risk Engine - Enterprise VPC Network Module
 *
 * Implements:
 * - Custom mode VPC network
 * - Subnets: Ingestion, Streaming (Dataflow/Dataproc), and Data Services
 * - Private Google Access enabled on all subnets
 * - Cloud Router and Cloud NAT for secure outbound-only internet connectivity
 * - VPC Flow Logs configured for operational visibility
 * - Private Service Access (PSA) global IP range reservation for Cloud SQL
 * - Strict ingress/egress firewall rules including IAP SSH access
 */

# -----------------------------------------------------------------------------
# VPC NETWORK DEFINITION
# -----------------------------------------------------------------------------
resource "google_compute_network" "vpc" {
  name                            = "${var.network_name}-${var.environment}"
  auto_create_subnetworks         = false
  routing_mode                    = "GLOBAL"
  delete_default_routes_on_create = false
  project                         = var.project_id
  description                     = "Enterprise VPC for Trading Risk Engine in ${var.environment}"
}

# -----------------------------------------------------------------------------
# SUBNETS
# -----------------------------------------------------------------------------
resource "google_compute_subnetwork" "sb_ingestion" {
  name                     = "sb-ingestion-${var.environment}"
  ip_cidr_range            = var.ingestion_subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
  project                  = var.project_id

  dynamic "log_config" {
    for_each = var.enable_flow_logs ? [1] : []
    content {
      aggregation_interval = "INTERVAL_5_SEC"
      flow_sampling        = 0.5
      metadata             = "INCLUDE_ALL_METADATA"
    }
  }

  description = "Subnet hosting Ingestion VMs, Kafka brokers, and feed connectors"
}

resource "google_compute_subnetwork" "sb_streaming" {
  name                     = "sb-streaming-${var.environment}"
  ip_cidr_range            = var.streaming_subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
  project                  = var.project_id

  dynamic "log_config" {
    for_each = var.enable_flow_logs ? [1] : []
    content {
      aggregation_interval = "INTERVAL_5_SEC"
      flow_sampling        = 0.5
      metadata             = "INCLUDE_ALL_METADATA"
    }
  }

  description = "Subnet hosting Cloud Dataflow Beam workers and Dataproc Spark workers"
}

resource "google_compute_subnetwork" "sb_data_services" {
  name                     = "sb-data-services-${var.environment}"
  ip_cidr_range            = var.data_services_subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
  project                  = var.project_id

  dynamic "log_config" {
    for_each = var.enable_flow_logs ? [1] : []
    content {
      aggregation_interval = "INTERVAL_5_SEC"
      flow_sampling        = 0.5
      metadata             = "INCLUDE_ALL_METADATA"
    }
  }

  description = "Subnet hosting Cloud SQL peering and Composer GKE pods"
}

# -----------------------------------------------------------------------------
# CLOUD ROUTER & CLOUD NAT (OUTBOUND EGRESS ONLY)
# -----------------------------------------------------------------------------
resource "google_compute_router" "router" {
  name    = "cr-trading-risk-${var.environment}"
  region  = var.region
  network = google_compute_network.vpc.id
  project = var.project_id

  description = "Cloud Router managing NAT egress for trading risk engine"
}

resource "google_compute_router_nat" "nat" {
  name                               = "nat-trading-risk-${var.environment}"
  router                             = google_compute_router.router.name
  region                             = var.region
  project                            = var.project_id
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# -----------------------------------------------------------------------------
# PRIVATE SERVICE ACCESS (RESERVED IP RANGE FOR CLOUD SQL & SERVICES)
# -----------------------------------------------------------------------------
resource "google_compute_global_address" "private_service_access_ip" {
  name          = "psa-ip-range-${var.environment}"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.vpc.id
  project       = var.project_id
  description   = "Reserved internal IP block for GCP managed private services peering (Cloud SQL)"
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_service_access_ip.name]
}

# -----------------------------------------------------------------------------
# FIREWALL RULES
# -----------------------------------------------------------------------------
resource "google_compute_firewall" "allow_internal" {
  name        = "fw-allow-internal-${var.environment}"
  network     = google_compute_network.vpc.name
  project     = var.project_id
  description = "Allow internal communication across RFC 1918 subnets in VPC"

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  source_ranges = [
    var.ingestion_subnet_cidr,
    var.streaming_subnet_cidr,
    var.data_services_subnet_cidr,
  ]
}

resource "google_compute_firewall" "allow_iap_ssh" {
  name        = "fw-allow-iap-ssh-${var.environment}"
  network     = google_compute_network.vpc.name
  project     = var.project_id
  description = "Allow administrative SSH ingress exclusively via Google Cloud Identity-Aware Proxy (IAP)"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  # Official Google Cloud IAP IP range
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["bastion-access", "ingestion-worker"]
}

resource "google_compute_firewall" "allow_dataflow_internal" {
  name        = "fw-allow-dataflow-internal-${var.environment}"
  network     = google_compute_network.vpc.name
  project     = var.project_id
  description = "Allow internal Dataflow communication between worker VMs on ports 12345-12346"

  allow {
    protocol = "tcp"
    ports    = ["12345", "12346"]
  }

  source_ranges = [var.streaming_subnet_cidr]
  target_tags   = ["dataflow"]
}
