terraform {
  backend "gcs" {
    bucket  = "olist-analytics-tfstate"
    prefix  = "terraform/state"
  }
}