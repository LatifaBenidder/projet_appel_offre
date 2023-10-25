terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file("C:\\Users\\Admin\\Desktop\\projet_appel_offre\\starry-sunup-401714-b6b33aff875e.json")

  project = "starry-sunup-401714"
  region  = "us-central1"
  zone    = "us-central1-c"
}

# Create new storage bucket in the US multi-region
# with standard storage

resource "google_storage_bucket" "static" {
 name          = "input__data"
 location      = "US"
 storage_class = "STANDARD"

 uniform_bucket_level_access = true
}

resource "google_service_account_iam_member" "custom_service_account" {
  provider = google-beta
  service_account_id = ""
  role = "roles/composer.ServiceAgentV2Ext"
  member = ""
}

resource "google_composer_environment" "test" {
  name   = "mycomposer"
  region = "us-central1"

  config {
    software_config {
      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "True"
      }

      pypi_packages = {
        numpy = ""
        pandas = ""
        datetime = ""
        bs4 = ""
        requests = ""
      }
    }
  }
}




