---
title: Microsoft Azure storage driver
description: Explains how to use the Azure storage drivers
keywords: registry, service, driver, images, storage, azure
---

An implementation of the `storagedriver.StorageDriver` interface which uses [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/) for object storage.

## Authentication Methods

The Azure storage driver supports three authentication methods:

### Shared Key

This method uses an account name and account key to authenticate with Azure storage. The following configuration parameters are required:

| Parameter    | Required | Description                                                              |
|:-------------|:---------|:-------------------------------------------------------------------------|
| `credentialstype` | no      | Must be set to `shared_key` to use this authentication method. If not specified, defaults to `shared_key`.   |
| `accountname`     | yes     | Name of the Azure Storage Account.                                    |
| `accountkey`      | yes     | Primary or Secondary Key for the Storage Account, base64 encoded.     |

## Other Parameters

| Parameter    | Required | Description                                            |
|:-------------|:---------|:-------------------------------------------------------|
| `realm`      | no       | Domain name suffix for the Storage Service API endpoint. For example realm for "Azure in China" would be `core.chinacloudapi.cn` and realm for "Azure Government" would be `core.usgovcloudapi.net`. By default, this is `core.windows.net`.                        |
| `serviceurl` | no       | Explicit URL for the blob service endpoint. If not provided, it is derived from `accountname` and `realm`. |
| `container`  | yes      | Name of the Azure root storage container in which all registry data is stored. Must comply with [Azure container naming rules](https://learn.microsoft.com/en-gb/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata). |
| `rootdirectory` | no    | Virtual directory prefix under which all registry data will be stored within the container. Must end with a `/`. Defaults to the container root. |
| `legacyrootprefix` | no | Use legacy registry layout, with a leading `/` before the root directory prefix. Defaults to `false`. |
| `trimlegacyrootprefix` | no | Trim the legacy registry root prefix. Defaults to `false`. Cannot be used together with `legacyrootprefix`. | 

## Debug Logging

The Azure storage driver can log detailed debugging information about its interactions with the Azure Storage API.
This is enabled using the following parameters:

| Parameter    | Required | Description                                            |
|:-------------|:---------|:-------------------------------------------------------|
| `debuglog`   | no       | Set to `true` to enable debug logging. Defaults to `false`. |
| `debuglogevents` | no  | Comma-separated list of API events to log. Possible values: `request`, `response`, `responseError`, `retry`, `longRunningOperation`. If not specified, all events are logged when `debuglog` is enabled. |

The logging functionality uses the `azcore` package from the Azure SDK for Go under the hood, specifically the [`SetListener` and `SetEvents` functions](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/internal/log).
Alternative way to enable logging is setting `AZURE_SDK_GO_LOGGING` environment variable to `all`.
It will cause Azure SDK itself to print its debugging information to STDERR.

## Connection Pooling 

The Azure storage driver uses [pooling for `move` operation](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-copy-async-go#about-copying-blobs-with-asynchronous-scheduling) to enhance performance and reliability.
The behavior of the pool can be tuned using the following parameters:

| Parameter    | Required | Description                                            |
|:-------------|:---------|:-------------------------------------------------------|
| `apipoolinitialinterval` | no | The initial delay before retrying an operation that failed. On subsequent retries, this delay increases exponentially up to the `apipoolmaxinterval` value. Defaults to 100ms. |
| `apipoolmaxinterval`     | no | The maximum delay between retry attempts for a failed operation. Defaults to 1s. |  
| `apipoolmaxelapsedtime` | no | The maximum total time that an operation is allowed to take, including the initial attempt and any retries. If this time elapses before the operation succeeds, no more retries are attempted. Defaults to 5s. |

All parameters accept time duration in the format used by Go's [time\.ParseDuration](https://pkg.go.dev/time#ParseDuration) package.
The retry delays increase exponentially with some randomization between attempts in order to avoid overwhelming the storage service with many simultaneous retries.
For example, with the default values, the first retry will happen after roughly 500ms, the second after 1s, the third after 2s, and so on up to a maximum of 15s, until a total of 5 minutes has elapsed.
