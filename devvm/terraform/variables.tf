variable "vm_template_instance_type" {
  # VM template is used for testing/development only, unrelated to devvm.sh script.
  description = "Default instance type used by VM template."
  default     = "e2-standard-8"
}

variable "vm_template_source_image" {
  # VM template is used for testing/development only, unrelated to devvm.sh script.
  description = "Default distro image used by VM template."
  default     = "projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20251021"
}

variable "project_id" {
  description = "project id"
}

variable "region" {
  description = "region"
}

variable "credentials_file" {
  description = "Path to the GCP credentials JSON file"
  type        = string
  default     = null
}
