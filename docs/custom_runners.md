# Custom GitLab Runner Configuration

This document describes how to set up and configure custom GitLab runners for our CI/CD pipelines that interact with Azure and AWS S3 storage.

## Overview

Our CI/CD pipelines use custom GitLab runners located in regions close to the storage buckets:

- **Azure**: Located in `useast` region
- **AWS S3**: Located in `us-east-1` region

## Access Credentials

Access credentials for the runner VMs are stored in 1Password:

- AWS VM: Entry `container-registry-cirunner-aws`
- Azure VM: Entry `container-registry-cirunner-azure`

## Runner Setup Instructions

Setting up a custom runner follows the standard GitLab Runner installation process with the following specific requirements:

### Hardware Requirements

- At least 8 vCPU
- Minimum 32GiB RAM
- 60GiB general purpose storage

### Configuration Requirements

- Use Docker executor in host network mode
- Set concurrency level to 6 jobs
- Disable running untagged jobs
- Apply the appropriate tag:
  - For AWS S3: `s3-managed_identity_auth`
  - For Azure: `azure-managed_identity_auth`
- For Azure VM only: Assign managed identity access with "Data Contributor" level to the blob storage account used for testing

## Sample Configuration

Below is a sample configuration file (`/etc/gitlab-runner/config.toml`):

```toml
# limited by the amount of RAM we have - 1 test is <=3GiB of RAM peak
concurrent = 7
check_interval = 0
connection_max_age = "15m0s"
shutdown_timeout = 0

[session_server]
  session_timeout = 1800

[[runners]]
  name = "container-registry-cirunner"
  url = "https://gitlab.com"
  id = 45220409
  token = "glrt-foobar"
  token_obtained_at = 2025-01-10T09:28:29Z
  token_expires_at = 0001-01-01T00:00:00Z
  executor = "docker"
  [runners.custom_build_dir]
  [runners.cache]
    MaxUploadedArchiveSize = 0
    [runners.cache.s3]
    [runners.cache.gcs]
    [runners.cache.azure]
  [runners.docker]
    tls_verify = false
    image = "golang:latest"
    privileged = false
    disable_entrypoint_overwrite = false
    oom_kill_disable = false
    disable_cache = false
    volumes = ["/cache"]
    shm_size = 0
    network_mtu = 0
    network_mode = "host"
```
