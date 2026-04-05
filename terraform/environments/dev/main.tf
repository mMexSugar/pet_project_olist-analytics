provider "google" {
  project = var.project_id
  region  = var.region
}

moved {
  from = google_storage_bucket.data_lake_buckets
  to   = module.storage.google_storage_bucket.data_lake_buckets
}
module "storage" {
  source     = "../../modules/storage"
  project_id = var.project_id
  region     = var.region
}

module "compute" {
  source     = "../../modules/compute"
  project_id = var.project_id
  region     = var.region
}