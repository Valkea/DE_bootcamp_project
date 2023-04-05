locals {
  data_lake_bucket = "eco2mix-de-project-bucket"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "compelling-moon-382321"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  # type = string
  # default = "de_project_staging"
  type = list
  default = ["de_project_staging", "de_project_development", "de_project_production"]
}
