---
title: Google Cloud Storage driver
description: Explains how to use the Google Cloud Storage driver
keywords: registry, service, driver, images, storage, gcs, google
---

An implementation of the `storagedriver.StorageDriver` interface which uses [Google Cloud Storage](https://cloud.google.com/storage) for object storage.

## Authentication Methods

The GCS storage driver supports multiple authentication methods:

### Service Account Key

This method uses a service account key file to authenticate with Google Cloud Storage:

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `keyfile` | yes | Path to the service account key file in JSON format. The service account must have appropriate permissions to access the storage bucket. |

### Application Default Credentials

When no `keyfile` is specified, the driver will use Application Default Credentials (ADC). This includes:

- Service account attached to the GCE instance
- Credentials from `gcloud auth application-default login`
- Credentials from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable

### Service Account Credentials in Configuration

Instead of using a key file, you can embed the service account credentials directly in the configuration:

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `credentials` | no | Inline service account credentials. See the credentials subsection below for details. |

#### `credentials`

When using inline credentials, provide the following parameters:

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `type` | yes | Must be `service_account` |
| `project_id` | yes | The Google Cloud project ID |
| `private_key_id` | yes | The private key ID from the service account |
| `private_key` | yes | The private key from the service account (PEM format) |
| `client_email` | yes | The service account email address |
| `client_id` | yes | The numeric client ID |
| `auth_uri` | yes | The authentication URI (typically `https://accounts.google.com/o/oauth2/auth`) |
| `token_uri` | yes | The token URI (typically `https://oauth2.googleapis.com/token`) |
| `auth_provider_x509_cert_url` | yes | The auth provider certificate URL |
| `client_x509_cert_url` | yes | The client certificate URL |

## Configuration Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `bucket` | yes | The name of the GCS bucket to store registry data in. |
| `keyfile` | no | Path to the service account key file. If not provided, Application Default Credentials will be used. |
| `credentials` | no | Inline service account credentials as an alternative to `keyfile`. |
| `rootdirectory` | no | The root directory tree in which all registry files are stored. Defaults to an empty string (bucket root). |
| `chunksize` | no | The chunk size for uploading large blobs. Must be a positive multiple of 256KB. Default and minimum is 5242880 (5MB). |
| `useragent` | no | The user agent string to use when making requests to GCS. Defaults to `container-registry`. |
| `debug_log` | no | If set to true, enables debug logging for GCS API interactions. Defaults to false. |

## Chunk Size Configuration

The `chunksize` parameter controls the size of chunks used for uploading large objects to GCS. This affects performance and memory usage:

- Must be a positive multiple of 256KB (262144 bytes)
- Default value is 5MB (5242880 bytes)
- Larger chunk sizes may improve throughput for large files but increase memory usage
- Smaller chunk sizes reduce memory usage but may decrease throughput

Example: To use 10MB chunks, set `chunksize: 10485760`

## Debug Logging

When `debug_log` is set to true, the driver will output detailed information about its interactions with the GCS API, which can be helpful for troubleshooting:

```yaml
storage:
  gcs:
    bucket: my-registry-bucket
    debug_log: true
```

## Required Permissions

The service account or credentials used must have the following GCS permissions on the specified bucket:

- `storage.objects.create`
- `storage.objects.delete`
- `storage.objects.get`
- `storage.objects.list`
- `storage.objects.update`

These permissions are included in the `roles/storage.objectAdmin` IAM role.

## Example Configuration

### Using a service account key file

```yaml
storage:
  gcs:
    bucket: my-registry-bucket
    keyfile: /path/to/serviceaccount.json
    rootdirectory: /registry
```

### Using Application Default Credentials

```yaml
storage:
  gcs:
    bucket: my-registry-bucket
    rootdirectory: /registry
```

### Using inline credentials

```yaml
storage:
  gcs:
    bucket: my-registry-bucket
    credentials:
      type: service_account
      project_id: my-project-id
      private_key_id: key-id
      private_key: |
        -----BEGIN RSA PRIVATE KEY-----
        ...
        -----END RSA PRIVATE KEY-----
      client_email: my-service-account@my-project-id.iam.gserviceaccount.com
      client_id: "123456789"
      auth_uri: https://accounts.google.com/o/oauth2/auth
      token_uri: https://oauth2.googleapis.com/token
      auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
      client_x509_cert_url: https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40my-project-id.iam.gserviceaccount.com
```
