variable "instance_type" {
  description = "Default instance type used during creation of the image"
  type        = string
}

variable "project_id" {
  description = "GCP project id"
  type        = string
}

variable "zone" {
  description = "GCE zone to deploy temporary VM to"
  type        = string
}

variable "image_family" {
  description = "Image family to use as basis"
  type        = string
  default     = "ubuntu-2404-lts-amd64"
}

variable "image_tag" {
  description = "string that uniquely identifies image that is going to be created"
  type        = string
  default     = "localbuild"
}

# The problem is that os-login uses the SA email from instance's metadata which
# is not what we feed it to through $GOOGLE_APPLICATION_CREDENTIALS env.
variable "account_file" {
  description = "service account to use when using os-login"
  type        = string
  default     = null
}
