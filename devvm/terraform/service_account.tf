resource "google_service_account" "devvm_sa" {
  account_id   = "devvm-sa"
  display_name = "DevVM Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "devvm_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.devvm_sa.email}"
}

resource "google_project_iam_member" "devvm_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.devvm_sa.email}"
}

