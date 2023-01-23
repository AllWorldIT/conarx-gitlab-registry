# GitLab Container Registry HTTP API V1

This document is the specification for the new GitLab Container Registry API.

This is not intended to replace the [Docker Registry HTTP API V2](https://docs.docker.com/registry/spec/api/), superseded by the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md), which clients use to upload, download and delete images. That will continue to be maintained and available at `/v2/` (documented [here](../docs/spec/api.md)).

This new API intends to provide additional functionality not covered by the `/v2/` to support the development of new features tailored explicitly for GitLab the product. This API requires the new metadata database and will not be implemented for filesystem metadata.

Please note that this is not the [Container Registry API](https://docs.gitlab.com/ee/api/container_registry.html) of GitLab Rails. This is the API of the Container Registry application. Therefore, while most of the *functionality* described here is expected to surface in the former, parity between the two is not a requirement. Similarly, and although we adhere to the same design principles, the *form* of this API is dictated by the features and constraints of the Container Registry, not by those of GitLab Rails.

## Contents

[TOC]

## Overview

### Versioning

The current `/v2/` and other `/vN/` prefixes are reserved for implementing the OCI Distribution Spec. Therefore, this API uses an independent `/gitlab/v1/` prefix for isolation and versioning purposes.

### Operations

A list of methods and URIs are covered in the table below:

| Method   | Path                                       | Description                                                                        |
|----------|--------------------------------------------|------------------------------------------------------------------------------------|
| `GET`    | `/gitlab/v1/`                              | Check that the registry implements this API specification.                         |
| `GET`    | `/gitlab/v1/repositories/<path>/`          | Obtain details about the repository identified by `path`.                          |
| `PUT`    | `/gitlab/v1/import/<path>/`                | Move the repository identified by `path` from filesystem metadata to the database. |
| `GET`    | `/gitlab/v1/import/<path>/`                | Query import status of the repository identified by `path`.                        |
| `DELETE` | `/gitlab/v1/import/<path>/`                | Cancel import of the repository identified by `path`.                              |
| `GET`    | `/gitlab/v1/repositories/<path>/tags/list/` | Obtain the list of tags for the repository identified by `path`.                   |
| `GET`    | `/gitlab/v1/repository-paths/<path>/repositories/list/` | Obtain the list of of repositories under a base repository path identified by `path`.         |

By design, any feature that incurs additional processing time, such as query parameters that allow obtaining additional data, is opt-*in*.

### Authentication

The same authentication mechanism is shared by this and the `/v2/` API. Therefore, clients must obtain a JWT token from the GitLab API using the `/jwt/auth` endpoint.

Considering the above, and unless stated otherwise, all `HEAD` and `GET` requests require a token with `pull` permissions for the target repository(ies), `POST`, `PUT`, and `PATCH` requests require  `push` permissions, and `DELETE` requests require `delete` permissions.

Please refer to the original [documentation](https://docs.docker.com/registry/spec/auth/) from Docker for more details about authentication.

### Strict Slash

We enforce a strict slash policy for all endpoints on this API. This means that all paths must end with a forward slash `/`. A `301 Moved Permanently` response will be issued to redirect the client if the request is sent without the trailing slash. The need to maintain the strict slash policy wil be reviewed on [gitlab-org/container-registry#562](https://gitlab.com/gitlab-org/container-registry/-/issues/562).

## Compliance check

Check if the registry implements the specification described in this document.

### Request

```shell
GET /gitlab/v1/
```

#### Example

```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/
```

### Response

#### Header

| Status Code        | Reason                                                                                                           |
| ------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `200 OK`           | The registry implements this API specification.                                                                  |
| `401 Unauthorized` | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |
| `404 Not Found`    | The registry implements this API specification, but it is unavailable because the metadata database is disabled. |
| Others             | The registry does not implement this API specification.                                                          |

#### Example

```shell
HTTP/1.1 200 OK
Content-Length: 0
Date: Thu, 25 Nov 2021 16:08:59 GMT
```

## Get repository details

Obtain details about a repository.

### Request

```shell
GET /gitlab/v1/repositories/<path>/
```

| Attribute | Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-----------|--------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`    | String | Yes      |         | The full path of the target repository. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies.                                                                                                                                                                                                              |
| `size`    | String | No       |         | If the deduplicated size of the repository should be calculated and included in the response.<br />May be set to `self` or `self_with_descendants`. If set to `self`, the returned value is the deduplicated size of the `path` repository. If set to `self_with_descendants`, the returned value is the deduplicated size of the target repository and any others within. An auth token with `pull` permissions for name `<path>/*` is required for the latter. |

#### Example

```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/repositories/gitlab-org/build/cng/gitlab-container-registry?size=self"
```

### Response

#### Header

| Status Code        | Reason                                                       |
| ------------------ | ------------------------------------------------------------ |
| `200 OK`           | The repository was found. The response body includes the requested details. |
| `401 Unauthorized` | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |
| `400 Bad Request`    | The value of the `size` query parameter is invalid.                                |
| `404 Not Found`    | The repository was not found.                                |

#### Body

| Key              | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Type   | Format                              | Condition                                                   |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|-------------------------------------|-------------------------------------------------------------|
| `name`           | The repository name. This is the last segment of the repository path.                                                                                                                                                                                                                                                                                                                                                                                                                             | String |                                     |                                                             |
| `path`           | The repository path.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | String |                                     |                                                             |
| `size_bytes`     | The deduplicated size of the repository (and its descendants, if requested and applicable). See `size_precision` for more details.                                                                                                                                                                                                                                                                                                                                                                | Number | Bytes                               | Only present if the request query parameter `size` was set. |
| `size_precision` | The precision of `size_bytes`. Can be one of `default` or `untagged`. If `default`, the returned size is the sum of all _unique_ image layers _referenced_ by at least one tagged manifest, either directly or indirectly (through a tagged manifest list/index). If `untagged`, any unreferenced layers are also accounted for. The latter is used as fallback in case the former fails due to temporary performance issues (see https://gitlab.com/gitlab-org/container-registry/-/issues/853). | String |                                     | Only present if the request query parameter `size` was set. |
| `created_at`     | The timestamp at which the repository was created.                                                                                                                                                                                                                                                                                                                                                                                                                                                | String | ISO 8601 with millisecond precision |                                                             |
| `updated_at`     | The timestamp at which the repository details were last updated.                                                                                                                                                                                                                                                                                                                                                                                                                                  | String | ISO 8601 with millisecond precision | Only present if updated at least once.                      |

## List Repository Tags

Obtain detailed list of tags for a repository. This extends the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#api) tag listing operation by providing additional
information about each tag and not just their name.

### Request

```shell
GET /gitlab/v1/repositories/<path>/tags/list/
```

| Attribute | Type   | Required | Default | Description                                                                                                                                                                                                                                         |
|-----------|--------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`    | String | Yes      |         | The full path of the target repository. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies. |
| `last`    | String | No       |         | Query parameter used as marker for pagination. Set this to the tag name lexicographically after which (exclusive) you want the requested page to start. The value of this query parameter must be a valid tag name. More precisely, it must respect the `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}` pattern as defined in the OCI Distribution spec [here](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests). Otherwise, an `INVALID_QUERY_PARAMETER_VALUE` error is returned. |
| `n`       | String | No       | 100     | Query parameter used as limit for pagination. Defaults to 100. Must be a positive integer between `1` and `1000` (inclusive). If the value is not a valid integer, the `INVALID_QUERY_PARAMETER_TYPE` error is returned. If the value is a valid integer but is out of the rage then an `INVALID_QUERY_PARAMETER_VALUE` error is returned. |

#### Pagination

The response is marker-based paginated, using marker (`last`) and limit (`n`) query parameters to paginate through tags.
The default page size is 100, and it can be optionally increased to a maximum of 1000.

In case more tags exist beyond those included in each response, the response `Link` header will contain the URL for the
next page, encoded as specified in [RFC5988](https://tools.ietf.org/html/rfc5988). If the header is not present, the
client can assume that all tags have been retrieved already.

As an example, consider a repository named `app` with four tags: `a`, `b`, `c` and `d`. To start retrieving the details
of these tags with a page size of `2`, the `n` query parameter should be set to `2`:

```text
?n=2
```

As result, the response will include the details of tags `a` and `b`, as they are lexicographically the first and second
tag in the repository. The `Link` header would then be filled and point to the second page:

```http
200 OK
Content-Type: application/json
Link: <https://registry.gitlab.com/gitlab/v1/repositories/app/tags/list/?n=2&last=b>; rel="next"
```

Note that `last` is set to `b`, as that was the last tag included in the first page. Invoking this URL will therefore
give us the second page with tags `c` and `d`. As there are no additional tags to receive, the response will not include
a `Link` header this time.

#### Example

```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/repositories/gitlab-org/build/cng/gitlab-container-registry/tags/list/?n=20&last=0.0.1"
```

### Response

#### Header

| Status Code        | Reason                                                                                                           |
|--------------------|------------------------------------------------------------------------------------------------------------------|
| `200 OK`           | The repository was found. The response body includes the requested details.                                      |
| `400 Bad Request`  | The value for the `n` and/or `last` pagination query parameters are invalid.                                     |
| `401 Unauthorized` | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |
| `404 Not Found`    | The repository was not found.                                                                                    |

#### Body

The response body is an array of objects (one per tag, if any) with the following attributes:

| Key          | Value                                            | Type   | Format                              | Condition                                                                                                |
|--------------|--------------------------------------------------|--------|-------------------------------------|----------------------------------------------------------------------------------------------------------|
| `name`       | The tag name.                                    | String |                                     |                                                                                                          |
| `digest`     | The digest of the tagged manifest.               | String |                                     |                                                                                                          |
| `media_type` | The media type of the tagged manifest.           | String |                                     |                                                                                                          |
| `size_bytes` | The size of the tagged image.                    | Number | Bytes                               |                                                                                                          |
| `created_at` | The timestamp at which the tag was created.      | String | ISO 8601 with millisecond precision |                                                                                                          |
| `updated_at` | The timestamp at which the tag was last updated. | String | ISO 8601 with millisecond precision | Only present if updated at least once. An update happens when a tag is switched to a different manifest. |

The tag objects are sorted lexicographically by tag name to enable marker-based pagination.

#### Example

```json
[
  {
    "name": "0.1.0",
    "digest": "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b",
    "media_type": "application/vnd.oci.image.manifest.v1+json",
    "size_bytes": 286734237,
    "created_at": "2022-06-07T12:10:12.412+00:00"
  },
  {
    "name": "latest",
    "digest": "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b",
    "media_type": "application/vnd.oci.image.manifest.v1+json",
    "size_bytes": 286734237,
    "created_at": "2022-06-07T12:11:13.633+00:00",
    "updated_at": "2022-06-07T14:37:49.251+00:00"
  }
]
```

## List Sub Repositories

Obtain a list of repositories (that have at least 1 tag) under a repository base path. If the supplied base path also corresponds to a repository with at least 1 tag it will also be returned.

### Request

```shell
GET /gitlab/v1/repository-paths/<path>/repositories/list/
```

| Attribute | Type   | Required | Default | Description                                                                                                                                                                                                                                         |
|-----------|--------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`    | String | Yes      |         | The full path of the target repository base path. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies. |
| `last`    | String | No       |         | Query parameter used as marker for pagination. Set this to the path lexicographically after which (exclusive) you want the requested page to start. The value of this query parameter must be a valid path name. More precisely, it must respect the `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}` pattern as defined in the OCI Distribution spec [here](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests). Otherwise, an `INVALID_QUERY_PARAMETER_VALUE` error is returned. |
| `n`       | String | No       | 100     | Query parameter used as limit for pagination. Defaults to 100. Must be a positive integer between `1` and `1000` (inclusive). If the value is not a valid integer, the `INVALID_QUERY_PARAMETER_TYPE` error is returned. If the value is a valid integer but is out of the rage then an `INVALID_QUERY_PARAMETER_VALUE` error is returned. |

#### Pagination

The response is marker-based paginated, using marker (`last`) and limit (`n`) query parameters to paginate through repository paths.
The default page size is 100, and it can be optionally increased to a maximum of 1000.

In case more repository paths exist beyond those included in each response, the response `Link` header will contain the URL for the
next page, encoded as specified in [RFC5988](https://tools.ietf.org/html/rfc5988). If the header is not present, the
client can assume that all tags have been retrieved already.

As an example, consider a repository named `app` with four sub repos: `app/a`, `app/b` and `app/c`. To start retrieving the list of
sub repositories with a page size of `2`, the `n` query parameter should be set to `2`:

```text
?n=2
```

As result, the response will include the repository paths of `app` and `app/a`, as they are lexicographically the first and second
repository (with at least 1 tag) in the path. The `Link` header would then be filled and point to the second page:

```http
200 OK
Content-Type: application/json
Link: <https://registry.gitlab.com/gitlab/v1/repository-paths/app/repositories/list/?n=2&last=app%2Fb>; rel="next"
```

Note that `last` is set to `app/a`, as that was the last path included in the first page. Invoking this URL will therefore
give us the second page with repositories `app/b` and `app/c`. As there are no additional repositories to receive, the response will not include
a `Link` header this time.

#### Example

```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/repository-paths/gitlab-org/build/cng/repositories/list/?n=2&last=cng/container-registry"
```

### Response

#### Header

| Status Code        | Reason                                                                                                           |
|--------------------|------------------------------------------------------------------------------------------------------------------|
| `200 OK`           | The response body includes the requested details or is empty if the repository does not exist or if there are no repositories with at least one tag under the base path provided in the request.                                      |
| `400 Bad Request`  | The value for the `n` and/or `last` pagination query parameters are invalid.                                     |
| `401 Unauthorized` | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |

#### Body

The response body is an array of objects (one per repository) with the following attributes:

| Key          | Value                                            | Type   | Format                              | Condition                                                                                                |
|--------------|--------------------------------------------------|--------|-------------------------------------|----------------------------------------------------------------------------------------------------------|
| `name`       | The repository name.                             | String |                                     |                                                                                                          |
| `path`       | The repository path.                             | String |                                     |                                                                                                          |
| `created_at`     | The timestamp at which the repository was created.                                                                                                                                                                                                                                                                                                                                                                                                                                                | String | ISO 8601 with millisecond precision |                                                             |
| `updated_at`     | The timestamp at which the repository details were last updated.                                                                                                                                                                                                                                                                                                                                                                                                                                  | String | ISO 8601 with millisecond precision | Only present if updated at least once.                      |

The repository objects are sorted lexicographically by repository path name to enable marker-based pagination.

#### Example

```json
[
  {
    "name": "docker-alpine",
    "path": "gitlab-org/build/cng/docker-alpine",
    "created_at": "2022-06-07T12:11:13.633+00:00",
    "updated_at": "2022-06-07T14:37:49.251+00:00"

  },
  {
    "name": "git-base",
    "path": "gitlab-org/build/cng/git-base",
    "created_at": "2022-06-07T12:11:13.633+00:00",
    "updated_at": "2022-06-07T14:37:49.251+00:00"

  }
]
```

### Codes

The error codes encountered via this API are enumerated in the following table.

|Code|Message|Description|
|----|-------|-----------|
`INVALID_QUERY_PARAMETER_VALUE` | `the value of a query parameter is invalid` | The value of a request query parameter is invalid. The error detail identifies the concerning parameter and the list of possible values.
`INVALID_QUERY_PARAMETER_TYPE` | `the value of a query parameter is of an invalid type` | The value of a request query parameter is of an invalid type. The error detail identifies the concerning parameter and the list of possible types.

## Import Repository

Move a single repository from filesystem metadata to the database.

Imports are processed asynchronously, the registry will send a notification via
an HTTP request once the import has finished.

Incoming writes to this repository during the import process will follow the old
code path, and will cause the import process to be cancelled.

### Request

```shell
PUT /gitlab/v1/import/<path>/
```

| Attribute     | Type    | Required | Default   | Description                                                  |
| ------------- | ------- | -------- | --------- | ------------------------------------------------------------ |
| `path`        | String  | Yes      |           | The full path of the target repository. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies. |
| `import_type` | Bool    | Yes      |           | When `import_type=pre`, only import manifests and their associated blobs, without importing tags. Once the pre import is complete, performing a final import with `import_type=final` should take far less time, reducing the amount of time required during which writes will be blocked. |

#### Example

```shell
curl -X PUT --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/import/gitlab-org/build/cng/gitlab-container-registry/?import_type=pre"
```

#### Authentication

This endpoint requires an auth token with the `registry` resource type, name set to `import`, and the `*` action. Sample:

```json
{
  "access": [
    {
      "actions": [
        "*"
      ],
      "name": "import",
      "type": "registry"
    }
  ],
  // ...
}
```

### Response
#### Header

| Status Code               | Reason                                                       |
| ------------------------- | ------------------------------------------------------------ |
| `200 OK`                  | The repository was already present on the database and does not need to be imported. This repository may have been previously migrated or native to the database. |
| `202 Accepted`            | The import or pre import was successfully started. |
| `401 Unauthorized`        | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |
| `400 Bad Request`         | The `import_type` query parameter was not set or its value is invalid. |
| `404 Not Found`           | The repository was not found. |
| `409 Conflict`            | The repository is already being imported. |
| `424 Failed Dependency`   | The repository failed to pre import. This error only affects the import request when `import_type=final`, when `import_type=pre` the pre import will be retried.|
| `424 Failed Dependency`   | The repository has not been pre imported yet. This error only affects the import request when `import_type=final` and no preceding `import_type=pre` import has been started.|
| `425 Too Early`           | The Repository is currently being pre imported. |
| `429 Too Many Requests`   | The registry is already running the maximum configured import jobs. |

### Import Notification

Once an import completes, the registry will send a synchronous notification in the form of an HTTP `PUT` request to the endpoint configured in [`migration`](../docs/configuration.md#migration). This notification includes the status of the import and any relevant details about it (such as the reason for a failure).

#### Request
##### Body

| Key          | Value                                                                  | Type   |
| ------------ | ---------------------------------------------------------------------- | ------ |
| `name`       | The repository name. This is the last segment of the repository path.  | String |
| `path`       | The repository path.                                                   | String |
| `status`     | The status of the completed import.                                    | String |
| `detail`     | A detailed explanation of the status. In case an error occurred, this field will contain the reason. | string |

##### Possible Statuses

| Value | Meaning |
| ----- | ------- |
| `import_complete`  | The import was completed successfully. |
| `pre_import_complete` | The pre-import completed successfully. |
| `import_failed` | The import has failed. |
| `pre_import_failed`    | The pre-import has failed. |
| `import_canceled` | The import was canceled. |
| `pre_import_canceled` | The pre-import was canceled. |

##### Examples

###### Success
```json
{
  "name": "gitlab-container-registry",
  "path": "gitlab-org/build/cng/gitlab-container-registry",
  "status": "import_complete",
  "detail": "final import completed successfully"
}
```

###### Error
```json
{
  "name": "gitlab-container-registry",
  "path": "gitlab-org/build/cng/gitlab-container-registry",
  "status": "import_failed",
  "detail": "importing tags: reading tags: write tcp 172.0.0.1:1234->172.0.0.1:4321: write: broken pipe"
}
```

## Get Repository Import Status

Query import status of a repository.

### Request

```shell
GET /gitlab/v1/import/<path>/
```

| Attribute     | Type    | Required | Default   | Description                                                  |
| ------------- | ------- | -------- | --------- | ------------------------------------------------------------ |
| `path`        | String  | Yes      |           | The full path of the target repository. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies. |

#### Authentication

This endpoint requires an auth token with the `registry` resource type, name set to `import`, and the `*` action.

#### Example

```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/import/gitlab-org/build/cng/gitlab-container-registry/"
```

### Response

#### Header

| Status Code        | Reason                                                       |
| ------------------ | ------------------------------------------------------------ |
| `200 OK`           | The repository was found. The response body includes the requested details. |
| `401 Unauthorized` | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |
| `404 Not Found`    | The repository was not found.                                |

#### Body

| Key          | Value                                                        | Type   | 
| ------------ | ------------------------------------------------------------ | ------ | 
| `name`       | The repository name. This is the last segment of the repository path. | String |                                     |                                                              |
| `path`       | The repository path.                                         | String |                                     |                                                              |
| `status`     | The status of the import.                                    | String |
| `detail`     | A detailed explanation of the status. | String |

##### Possible Statuses

| Value | Meaning |
| ----- | ------- |
| `native`  | This repository was originally created on the new platform. No import occurred. |
| `import_in_progress`  | The import is in progress. |
| `import_complete`  | The import was completed successfully. |
| `import_failed` | The import has failed. |
| `pre_import_in_progress`  | The pre-import is in progress. |
| `pre_import_complete` | The pre-import completed successfully. |
| `pre_import_failed`    | The pre-import has failed. |
| `import_canceled` | The import was canceled. |
| `pre_import_canceled` | The pre-import was canceled. |

#### Example

```json
{
  "name": "gitlab-container-registry",
  "path": "gitlab-org/build/cng/gitlab-container-registry",
  "status": "import_in_progress",
  "detail": "import in progress"
}
```

## Cancel Repository Import

Cancel ongoing repository (pre)import.

### Request

```shell
DELETE /gitlab/v1/import/<path>/
```

| Attribute     | Type    | Required | Default   | Description                                                  |
| ------------- | ------- | -------- | --------- | ------------------------------------------------------------ |
| `path`        | String  | Yes      |           | The full path of the target repository. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies. |
| `force`       | Bool    | No       |           | When `force=true`, any non-native migrated repository will be marked as `import_canceled` regardless of the previous migration status. |

#### Authentication

This endpoint requires an auth token with the `registry` resource type, name set to `import`, and the `*` action.

#### Example

```shell
curl -X DELETE --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/import/gitlab-org/build/cng/gitlab-container-registry/"
```

### Response

#### Header

| Status Code        | Reason                                                       |
| ------------------ | ------------------------------------------------------------ |
| `202 Accepted`     | The repository was found and the ongoing (pre)import will be canceled. An async notification will be sent once the cancelation occurs. |
| `400 Bad Request`  | The repository was found, but there is no ongoing (pre)import to cancel.  |
| `401 Unauthorized` | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again. |
| `404 Not Found`    | The repository was not found.                                |

#### Body

The response body is only included in `400 Bad Request` responses, as a way to inform the client about the current import status of the repository.

| Key          | Value                                                        | Type   | 
| ------------ | ------------------------------------------------------------ | ------ | 
| `name`       | The repository name. This is the last segment of the repository path. | String |                                     |                                                              |
| `path`       | The repository path.                                         | String |                                     |                                                              |
| `status`     | The status of the import.                                    | String |
| `detail`     | A detailed explanation of the status. | String |

##### Possible Statuses

| Value | Meaning |
| ----- | ------- |
| `native`  | This repository was originally created on the new platform. No import occurred. |
| `import_complete`  | The import was completed successfully. |
| `import_failed` | The import has failed. |
| `pre_import_complete` | The pre-import completed successfully. |
| `pre_import_failed`    | The pre-import has failed. |
| `import_canceled` | The import was canceled. |
| `pre_import_canceled` | The pre-import was canceled. |

#### Example

```json
{
  "name": "gitlab-container-registry",
  "path": "gitlab-org/build/cng/gitlab-container-registry",
  "status": "import_canceled",
  "detail": "import canceled"
}
```

## Errors

In case of an error, the response body payload (if any) follows the format defined in the
[OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes), which is the
same format found on the [V2 API](../docs/spec/api.md#errors):

```json
{
    "errors": [
        {
            "code": "<error identifier, see below>",
            "message": "<message describing condition>",
            "detail": "<unstructured>"
        },
        ...
    ]
}
```

### Codes

The error codes encountered via this API are enumerated in the following table. For consistency, whenever possible,
error codes described in the
[OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes) are reused.

|Code|Message|Description|
|----|-------|-----------|
`NAME_INVALID` | `invalid repository name` | Invalid repository name encountered either during manifest validation or any API operation.
`NAME_UNKNOWN` | `repository name not known to registry` | This is returned if the name used during an operation is unknown to the registry.
`UNAUTHORIZED` | `authentication required` | The access controller was unable to authenticate the client. Often this will be accompanied by a Www-Authenticate HTTP response header indicating how to authenticate.
`INVALID_QUERY_PARAMETER_VALUE` | `the value of a query parameter is invalid` | The value of a request query parameter is invalid. The error detail identifies the concerning parameter and the list of possible values.
`INVALID_QUERY_PARAMETER_TYPE` | `the value of a query parameter is of an invalid type` | The value of a request query parameter is of an invalid type. The error detail identifies the concerning parameter and the list of possible types.

## Changes

### 2023-01-05

- Add sub repositories list endpoint.

### 2023-01-04

- Add new "size precision" attribute to the "get repository details" response.

### 2022-06-29

- Add new error code used when query parameter values have the wrong data type.

### 2022-06-07

- Add repository tags list endpoint.

### 2022-03-03

- Add cancel repository import operation.

### 2022-01-26

- Add get repository import status endpoint.
- Consolidate statuses across the "get repository import status" endpoint and the sync import notifications. 

### 2022-01-13

- Add errors section.

### 2021-12-17

- Added import repository operation.

### 2021-11-26

- Added compliance check operation.
- Added get repository details operation.
