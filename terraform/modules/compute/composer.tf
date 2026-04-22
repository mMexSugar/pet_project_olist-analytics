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

resource "google_project_iam_member" "composer_dataflow_developer" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "dataflow_worker_storage" {
  project = var.project_id
  role    = "roles/storage.objectUser"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "composer_storage_user" {
  project = var.project_id
  role    = "roles/storage.objectUser"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_service_account_iam_member" "composer_sa_user_self" {
  service_account_id = google_service_account.composer_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_artifact_registry_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_bigquery_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
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

      pypi_packages = { }
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