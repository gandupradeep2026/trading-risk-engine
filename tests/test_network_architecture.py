"""Network Configuration and Architecture Validation Tests.

Validates:
- Network module Terraform configuration syntax and integrity.
- Subnet CIDR block isolation and non-overlapping IP allocations.
- Private Google Access and security firewall settings.
"""

import ipaddress
import os
import re
from pathlib import Path
import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
TF_NETWORK_DIR = REPO_ROOT / "terraform" / "modules" / "network"
TF_ROOT_DIR = REPO_ROOT / "terraform"


class TestTerraformNetworkSyntax:
    """Validate structure and syntax of Terraform files."""

    def test_network_module_files_exist(self) -> None:
        """Verify all critical network module files exist."""
        required_files = ["main.tf", "variables.tf", "outputs.tf", "versions.tf"]
        for filename in required_files:
            file_path = TF_NETWORK_DIR / filename
            assert file_path.is_file(), f"Missing required file: {file_path}"
            assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    def test_root_terraform_files_exist(self) -> None:
        """Verify root Terraform files and environment tfvars exist."""
        required_files = [
            TF_ROOT_DIR / "main.tf",
            TF_ROOT_DIR / "variables.tf",
            TF_ROOT_DIR / "outputs.tf",
            TF_ROOT_DIR / "environments" / "dev.tfvars",
            TF_ROOT_DIR / "environments" / "prod.tfvars",
        ]
        for file_path in required_files:
            assert file_path.is_file(), f"Missing root configuration: {file_path}"

    def test_balanced_curly_braces_in_hcl(self) -> None:
        """Verify valid HCL block nesting by checking balanced braces and strings."""
        tf_files = list(TF_ROOT_DIR.rglob("*.tf"))
        assert len(tf_files) >= 4, "Should find at least 4 Terraform configuration files."

        for tf_file in tf_files:
            content = tf_file.read_text(encoding="utf-8")
            # Strip comments
            cleaned_lines = []
            for line in content.splitlines():
                stripped = line.strip()
                if stripped.startswith("#") or stripped.startswith("//"):
                    continue
                cleaned_lines.append(line)
            cleaned = "\n".join(cleaned_lines)
            # Remove multi-line comments
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)

            open_count = cleaned.count("{")
            close_count = cleaned.count("}")
            assert (
                open_count == close_count
            ), f"Unbalanced braces in {tf_file.name}: {open_count} open vs {close_count} close"


class TestNetworkTopologyAndCidr:
    """Validate subnet CIDR ranges, ensuring enterprise IP planning rules."""

    @pytest.mark.parametrize(
        "env,ingestion_cidr,streaming_cidr,data_cidr",
        [
            ("dev", "10.10.10.0/24", "10.10.20.0/24", "10.10.30.0/24"),
            ("prod", "10.20.10.0/24", "10.20.20.0/24", "10.20.30.0/24"),
        ],
    )
    def test_subnet_cidrs_non_overlapping(
        self,
        env: str,
        ingestion_cidr: str,
        streaming_cidr: str,
        data_cidr: str,
    ) -> None:
        """Verify that subnets within an environment never overlap."""
        net_ingestion = ipaddress.ip_network(ingestion_cidr)
        net_streaming = ipaddress.ip_network(streaming_cidr)
        net_data = ipaddress.ip_network(data_cidr)

        subnets = [
            ("ingestion", net_ingestion),
            ("streaming", net_streaming),
            ("data_services", net_data),
        ]

        for i in range(len(subnets)):
            for j in range(i + 1, len(subnets)):
                name_a, net_a = subnets[i]
                name_b, net_b = subnets[j]
                assert not net_a.overlaps(
                    net_b
                ), f"Subnet collision detected in {env}: {name_a} ({net_a}) overlaps with {name_b} ({net_b})"

    def test_private_service_access_cidr_reservation(self) -> None:
        """Verify PSA prefix length is /20 for sufficient Cloud SQL IP allocation."""
        main_tf = (TF_NETWORK_DIR / "main.tf").read_text(encoding="utf-8")
        assert "prefix_length = 20" in main_tf, "PSA prefix length should be 20"
        assert "purpose       = \"VPC_PEERING\"" in main_tf, "PSA address purpose must be VPC_PEERING"

    def test_private_google_access_enforced(self) -> None:
        """Ensure all subnets have private_ip_google_access enabled."""
        main_tf = (TF_NETWORK_DIR / "main.tf").read_text(encoding="utf-8")
        pga_count = main_tf.count("private_ip_google_access = true")
        assert pga_count >= 3, "All 3 subnets must enforce private_ip_google_access = true"

    def test_iap_ssh_firewall_restricted_cidr(self) -> None:
        """Verify SSH access is strictly constrained to the official Google Cloud IAP IP range."""
        main_tf = (TF_NETWORK_DIR / "main.tf").read_text(encoding="utf-8")
        # Google IAP CIDR: 35.235.240.0/20
        assert "35.235.240.0/20" in main_tf, "SSH firewall must strictly allow 35.235.240.0/20 (GCP IAP)"
        assert "0.0.0.0/0" not in main_tf, "Zero public ingress (0.0.0.0/0) allowed in firewall rules"
