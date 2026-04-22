resource "google_bigquery_dataset" "olist_bronze" {
  dataset_id                  = "olist_bronze"
  friendly_name               = "Olist Bronze Layer"
  description                 = "Dataset for external tables pointing to Parquet files in GCS"
  location                    = var.region
  project                     = var.project_id

  delete_contents_on_destroy = true 
}