provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_lake_buckets" {
  for_each      = toset(["landing", "bronze", "silver", "gold"])
  name          = "${var.project_id}-${each.key}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}