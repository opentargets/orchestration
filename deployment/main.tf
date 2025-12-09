terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.28.0"
    }
  }
  backend "gcs" {
    bucket = "open-targets-ops"
    prefix = "terraform/airflow-dev"
  }
}

provider "google" {
  region  = "europe-west1"
  project = "open-targets-eu-dev"
  zone    = "europe-west1-d"
}

# the branch to checkout in the orchestration repo in the vm
variable "orchestration_git_branch" {
  description = "Orchestration repo branch to checkout"
  type        = string
  default     = "dev"
}

resource "random_string" "up_airflow_dev_vm" {
  length  = 4
  upper   = false
  special = false
}

data "external" "user" {
  program = ["bash", "-c", "echo '{\"user\":\"'$(whoami)'\"}'"]
}

resource "google_compute_instance" "up_airflow_dev_vm" {
  name         = "up-airflow-dev-${random_string.up_airflow_dev_vm.result}"
  machine_type = "n1-standard-32"
  metadata = {
    orchestration_git_branch = var.orchestration_git_branch
  }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      type  = "pd-ssd"
      size  = "50"
    }
  }

  network_interface {
    network = "default"
    access_config {
      // ephemeral public ip
    }
  }

  metadata_startup_script = file("startup_machine.sh")
  service_account {
    email  = "up-airflow-dev@open-targets-eu-dev.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  labels = {
    "tool"        = "orchestrator"
    "environment" = "dev"
    "team"        = "open-targets"
    "subteam"     = "data"
    "creator"     = "${data.external.user.result.user}"
  }

  lifecycle {
    ignore_changes = [
      labels["creator"],                    # we don't want to change the label if other users run the script
      metadata["orchestration_git_branch"], # we don't change the vm just when git repo branches change
    ]
  }


}

output "up_airflow_dev_vm" {
  value = google_compute_instance.up_airflow_dev_vm.name
}
