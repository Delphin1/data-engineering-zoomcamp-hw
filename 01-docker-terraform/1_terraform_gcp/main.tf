terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {
  # credentials = "./keys/my-gcp.json"
  project = "dataengineeringzoomcamp2025"
  region  = "europe-central2"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "dataengineeringzoomcamp2025-terra-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}