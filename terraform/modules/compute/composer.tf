resource "google_composer_environment" "olist_composer" {
  name   = "olist-orchestrator"
  region = var.region

  config {
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
}