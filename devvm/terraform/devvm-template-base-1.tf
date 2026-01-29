resource "google_compute_instance_template" "devvm_template_base" {
  confidential_instance_config {
    enable_confidential_compute = false
  }

  disk {
    auto_delete       = true
    boot              = true
    device_name       = "devvm-template.base-1"
    disk_size_gb      = 70
    disk_type         = "pd-ssd"
    mode              = "READ_WRITE"
    source_image      = var.vm_template_source_image
    type              = "PERSISTENT"
    resource_policies = []
  }

  labels = {
    devvm = "true"
  }

  machine_type = var.vm_template_instance_type

  metadata = {
    enable-oslogin = "true"
    cr_branch      = "main"
  }

  name = "devvm-template-base-1"

  network_interface {
    access_config {
      network_tier = "PREMIUM"
    }

    network    = google_compute_network.devvm_vpc.self_link
    stack_type = "IPV4_ONLY"
  }

  project = var.project_id

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    provisioning_model  = "STANDARD"
  }

  service_account {
    email = google_service_account.devvm_sa.email
    scopes = [
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/trace.append"
    ]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_vtpm                 = true
  }
}
