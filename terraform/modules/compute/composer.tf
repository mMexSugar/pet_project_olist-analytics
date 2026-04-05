data "google_project" "project" {}

resource "google_service_account" "composer_sa" {
  account_id   = "composer-worker-sa"
  display_name = "Service Account for Cloud Composer"
}

resource "google_project_iam_member" "composer_worker_role" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_agent_v2_ext" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

resource "google_composer_environment" "olist_composer" {
  name   = "olist-orchestrator"
  region = var.region

  config {
    node_config {
      service_account = google_service_account.composer_sa.email
    }
    software_config {
      image_version = "composer-2.16.9-airflow-2.10.5"
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.8
        storage_gb = 1
        count      = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.8
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.8
        storage_gb = 1
      }
    }
  }

  depends_on = [
    google_project_iam_member.composer_worker_role,
    google_project_iam_member.composer_agent_v2_ext
    ]
}