---
title: Microsoft Azure storage driver version 1
description: Explains how to use the Azure storage drivers
keywords: registry, service, driver, images, storage, azure
---

The older implementation of the `storagedriver.StorageDriver` interface which uses [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/) for object storage.

## Authentication Methods

The Azure storage driver supports shared key authentication method:
This method uses an account name and account key to authenticate with Azure storage.
The following configuration parameters are required:

| Parameter    | Required | Description                                                              |
|:-------------|:---------|:-------------------------------------------------------------------------|
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
