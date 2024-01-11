# GitLab Container Registry HTTP API V1

This document is the specification for the new GitLab Container Registry API.

This is not intended to replace the [Docker Registry HTTP API V2](https://docs.docker.com/registry/spec/api/), superseded by the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md), which clients use to upload, download and delete images. That will continue to be maintained and available at `/v2/` (documented [here](../docker/v2/api.md)).

This new API intends to provide additional functionality not covered by the `/v2/` to support the development of new features tailored explicitly for GitLab the product. This API requires the new metadata database and will not be implemented for filesystem metadata.

Please note that this is not the [Container Registry API](https://docs.gitlab.com/ee/api/container_registry.html) of GitLab Rails. This is the API of the Container Registry application. Therefore, while most of the *functionality* described here is expected to surface in the former, parity between the two is not a requirement. Similarly, and although we adhere to the same design principles, the *form* of this API is dictated by the features and constraints of the Container Registry, not by those of GitLab Rails.

## Contents

[TOC]

## Overview

### Versioning

The current `/v2/` and other `/vN/` prefixes are reserved for implementing the OCI Distribution Spec. Therefore, this API uses an independent `/gitlab/v1/` prefix for isolation and versioning purposes.

### Operations

A list of methods and URIs are covered in the table below:

| Method   | Path                                                    | Description                                                                                     |
|----------|---------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `GET`    | `/gitlab/v1/`                                           | Check that the registry implements this API specification.                                      |
| `GET`    | `/gitlab/v1/repositories/<path>/`                       | Obtain details about the repository identified by `path`.                                       |
| `PATCH`  | `/gitlab/v1/repositories/<path>/`                       | Rename a repository base `path` (i.e a GitLab project path) and all sub repositories under it.  |
| `GET`    | `/gitlab/v1/repositories/<path>/tags/list/`             | Obtain the list of tags for the repository identified by `path`.                                |
| `GET`    | `/gitlab/v1/repository-paths/<path>/repositories/list/` | Obtain the list of repositories under a base repository path identified by `path`.              |

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

| Attribute  | Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`     | String | Yes      |         | The full path of the target repository. Equivalent to the `name` parameter in the `/v2/` API, described in the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md). The same pattern validation applies.                                                                                                                                                                                                                                                                                                         |
| `before`   | String | No       |         | Query parameter used as marker for pagination. Set this to the tag name lexicographically _before_ which (exclusive) you want the requested page to start. The value of this query parameter must be a valid tag name. More precisely, it must respect the `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}` pattern as defined in the OCI Distribution spec [here](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests). Otherwise, an `INVALID_QUERY_PARAMETER_VALUE` error is returned. Cannot be used in conjunction with `last`. |
| `last`     | String | No       |         | Query parameter used as marker for pagination. Set this to the tag name lexicographically after which (exclusive) you want the requested page to start. The value of this query parameter must be a valid tag name. More precisely, it must respect the `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}` pattern as defined in the OCI Distribution spec [here](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests). Otherwise, an `INVALID_QUERY_PARAMETER_VALUE` error is returned.                                               |
| `n`        | String | No       | 100     | Query parameter used as limit for pagination. Defaults to 100. Must be a positive integer between `1` and `1000` (inclusive). If the value is not a valid integer, the `INVALID_QUERY_PARAMETER_TYPE` error is returned. If the value is a valid integer but is out of the rage then an `INVALID_QUERY_PARAMETER_VALUE` error is returned.                                                                                                                                                                                                                  |
| `name`     | String | No       |         | Tag name filter. If set, tags are filtered using a partial match against its value. Does not support regular expressions. Only lowercase and uppercase letters, digits, underscores, periods, and hyphen characters are allowed. Maximum of 128 characters. It must respect the `[a-zA-Z0-9._-]{1,128}` pattern. If the value is not valid, the `INVALID_QUERY_PARAMETER_VALUE` error is returned.                                                                                                                                                          |
| `sort`     | String | No       | "name"  | Sort tags by field in ascending or descending order. Prefix field with the `-` sign to sort in descending order according to the [JSON API spec](https://jsonapi.org/format/#fetching-sorting).                                                                                                                                                                                                                                                                                                                                                             |

#### Pagination

The response is marker-based paginated, using a marker (`last` or `before`, which are mutually exclusive) and limit (`n`) query parameters to paginate through tags.
The default page size is 100, and it can be optionally increased to a maximum of 1000.

In case more tags exist beyond those included in each response, the response `Link` header will contain the URL for the
next page or previous page (depending on if `last` or `before` was used), encoded as specified in [RFC5988](https://tools.ietf.org/html/rfc5988). If the header is not present, the client can assume that all tags have been retrieved already.

As an example, consider a repository named `app` with six tags: `a`, `b`, `c`, `d`, `e` and `f`. To start retrieving the details
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
give us the second page with tags `c` and `d`. 

Requesting the tags list from the `Link` header will return the list of tags _and_ the `Link` header with the `next` and `previous`
URLs:

```http
200 OK
Content-Type: application/json
Link: <https://registry.gitlab.com/gitlab/v1/repositories/mygroup/myproject/tags/list/?n=2&before=c>; rel="previous", <https://registry.gitlab.com/gitlab/v1/repositories/mygroup/myproject/tags/list/?n=2&last=d>; rel="next"
```

Note that the `Link` header includes `before=c` with `rel=previous` and `last=d` with `rel=next` query parameters.
Requesting the last page `https://registry.gitlab.com/gitlab/v1/repositories/mygroup/myproject/tags/list/?n=2&last=d`
should reply with tags `e` and `f`. As there are no additional tags to receive, the response will not include
a `Link` header this time.

See [sorting by published date](#sorting-by-published-date) for a special use case.

#### Examples

##### `last` marker

```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/repositories/gitlab-org/build/cng/gitlab-container-registry/tags/list/?n=20&last=0.0.1"
```

##### `before` marker

Similar to the `last` marker when this query parameter is used, the list of tags will contain up to `n` tags
from `before` the requested tag (exclusive).


```shell
curl --header "Authorization: Bearer <token>" "https://registry.gitlab.com/gitlab/v1/repositories/gitlab-org/build/cng/gitlab-container-registry/tags/list/?n=20&before=0.21.0"
```

Assuming there are 20 tags from `before=0.21.0` the response will include all 20 tags, for example ["0.1.0", "0.2.0",...,"0.20.0"].

#### Sorting

The endpoint can take a query parameter `sort` with the field value to sort by. Currently only `name` is supported.
It uses a simplified version of the [JSON API spec](https://jsonapi.org/format/#fetching-sorting) and only supports
a single field (`?sort=name,created_at` is not supported at this time). It also supports the hyphen-minus symbol
for sorting in descending order by prefixing the field with a `-` sign.
For example, to sort by name in descending order use `?sort=-name`.

Tags are returned in lexicographical order as specified by the `sort` query parameter.
If used in combination with the `before` or `last` query parameter, the tags are first filtered by these values
and then sorted in the requested order.

##### Sorting by published date

You can specify the sort parameter as `?sort=published_at` to request the list of tags by the published date, that is,
the latest time when the tag as created or updated.

For pagination purposes, when used in conjunction with the `last` and `before` query parameters, the values must be base64
encoded with the following format `base64(TIMESTAMP|TAG_NAME)`, where `TIMESTAMP` is a string in ISO 8061 format with 
microsecond precision, followed by the separator character `|` and finishing with the tag name. For example, for 
the timestamp `2023-02-01T00:00:01.000000Z` and tag name `latest`, the encoded value will be `MjAyMy0wMi0wMVQwMDowMDowMS4wMDAwMDBafGxhdGVzdAo=`

```shell
echo "2023-02-01T00:00:01.000000Z|latest" | base64
MjAyMy0wMi0wMVQwMDowMDowMS4wMDAwMDBafGxhdGVzdAo=

echo "MjAyMy0wMi0wMVQwMDowMDowMS4wMDAwMDBafGxhdGVzdAo=" | base64 -d
2023-02-01T00:00:01.000000Z|latest
```

For the example above, the request URL would look like:

```shell
/gitlab/vi/repositories/<path>/tags/list/?sort=published_at&last=MjAyMy0wMi0wMVQwMDowMDowMS4wMDBafGxhdGVzdAo=
```

The `Link` header will return and encoded value in the `last` or `before` query parameters of the `next` and `previous` 
URLs.

When two or more tags have the same published at date, they will be ordered by name in ascending or descending order.

##### Sort examples with pagination

Sorting by name given a list of tags `["a", "b", "c", "d", "e", "f"]`:

| n | before | last | sort  | Expected Result                  |
|---|--------|------|-------|----------------------------------|
|   |        |      | name  | `["a", "b", "c", "d", "e", "f"]` |
|   |        |      | -name | `["f", "e", "d", "c", "b", "a"]` |
| 3 |        |      | name  | `["a", "b", "c"]`                |
| 3 |        |      | -name | `["f", "e", "d"]`                |
|   | "c"    |      | name  | `["a", "b"]`                     |
|   | "c"    |      | -name | `["f", "e", "d"]`                |
| 2 | "c"    |      | name  | `["a", "b"]`                     |
| 2 | "d"    |      | -name | `["f", "e"]`                     |
|   |        | "c"  | name  | `["d", "e", "f"]`                |
|   |        | "c"  | -name | `["b", "a"]`                     |
| 2 |        | "b"  | name  | `["c", "d"]`                     |
| 2 |        | "e"  | -name | `["d", "c"]`                     |

Sorting by published_at given a list of tags with the following values:

| name   | created_at               | updated_at               | published_at             |
|--------|--------------------------|--------------------------|--------------------------|
| older  | 2023-01-01T00:00:01.000Z | NULL                     | 2023-01-01T00:00:01.000Z |
| old    | 2023-02-01T00:00:01.000Z | 2023-03-01T00:00:01.000Z | 2023-03-01T00:00:01.000Z |
| latest | 2023-03-01T00:00:01.000Z | 2023-05-01T00:00:01.000Z | 2023-05-01T00:00:01.000Z |
| new    | 2023-04-01T00:00:01.000Z | NULL                     | 2023-04-01T00:00:01.000Z |
| newer  | 2023-05-01T00:00:01.000Z | NULL                     | 2023-05-01T00:00:01.000Z |

Expected parameters and responses

| n | before                                  | last                                    | sort              | Expected Result                              |
|---|-----------------------------------------|-----------------------------------------|-------------------|----------------------------------------------|
|   |                                         |                                         | published_at      | `["older", old", "new", "latest", "newer"]`  |
|   |                                         |                                         | -published_at     | `["newer", "latest", "new", "old", "older"]` |
| 2 |                                         |                                         | published_at      | `["older", "old"]`                           |
| 2 |                                         |                                         | -published_at     | `["newer", "latest"]`                        |
| 2 | `base64(2023-04-01T00:00:01.000Z\|new)` |                                         | published_at      | `["older", "old"]`                           |
| 2 | `base64(2023-02-01T00:00:01.000Z\|old)` |                                         | -published_at     | `["newer", "latest"]`                        |
| 2 |                                         | `base64(2023-02-01T00:00:01.000Z\|old)` | published_at      | `["new", "latest"]`                          |
| 2 |                                         | `base64(2023-02-01T00:00:01.000Z\|new)` | -published_at     | `["old", "older"]`                           |

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

| Key             | Value                                            | Type   | Format                               | Condition                                                                                                |
|-----------------|--------------------------------------------------|--------|--------------------------------------|----------------------------------------------------------------------------------------------------------|
| `name`          | The tag name.                                    | String |                                      |                                                                                                          |
| `digest`        | The digest of the tagged manifest.               | String |                                      |                                                                                                          |
| `config_digest` | The configuration digest of the tagged image.    | String |                                      | Only present if image has an associated configuration.                                                   |
| `media_type`    | The media type of the tagged manifest.           | String |                                      |                                                                                                          |
| `size_bytes`    | The size of the tagged image.                    | Number | Bytes                                |                                                                                                          |
| `created_at`    | The timestamp at which the tag was created.      | String | ISO 8601 with millisecond precision  |                                                                                                          |
| `updated_at`    | The timestamp at which the tag was last updated. | String | ISO 8601 with millisecond precision  | Only present if updated at least once. An update happens when a tag is switched to a different manifest. |
| `published_at`   | The latest timestamp when the tag was published. | String | ISO 8601 with millisecond precision  | Must match the latest value of either `created_at` or `updated_at`.                                      |

The tag objects are sorted lexicographically by tag name to enable marker-based pagination.

#### Example

```json
[
  {
    "name": "0.1.0",
    "digest": "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b",
    "config_digest": "sha256:66b1132a0173910b01ee3a15ef4e69583bbf2f7f1e4462c99efbe1b9ab5bf808",
    "media_type": "application/vnd.oci.image.manifest.v1+json",
    "size_bytes": 286734237,
    "created_at": "2022-06-07T12:10:12.412+00:00"
  },
  {
    "name": "latest",
    "digest": "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b",
    "config_digest": "sha256:0c4c8e302e7a074a8a1c2600cd1af07505843adb2c026ea822f46d3b5a98dd1f",
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
| `404 Not Found`    | The namespace associated with the repository was not found.                                                                                    |

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

## Rename Base Repository

Rename a repository's base path (i.e a path corresponding to a GitLab project path) and all sub repositories under it. 

For more information, see the in-depth [flow diagram `rename operation`](../../rename-base-repository-request-flow.md).

### Request

```shell
PATCH /gitlab/v1/repositories/<path>/
```

| Attribute | Type   | Required | Default | Description                                                                                                                             |
|-----------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------|
| `path`    | String  | Yes      |         | This is the GitLab project path of the repository to be renamed (it is equivalent to the base repository of a project, if one exists). |
| `dry_run` | Boolean | No       | `false` | When set to `true` this option will only; validate that a rename is indeed possible and (if possible) will acquire a rename lease on the suggested `name` in the request body for a fixed amount of time (60 seconds) without blocking writes to the existing base repository path or executing the rename.<br>When set to `false` (default) this option will; acquire a rename lease on the suggested `name` in the request body, prevent further writes to the existing base repository path for the duration of the rename operation and execute the rename operation on the necessary repositories on the spot. |

#### Body

The request body is an object with the following attributes:

| Key          | Value                                                                   | Type   | Format                                                    | Condition                                                                                                          |
|--------------|-------------------------------------------------------------------------|--------|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `name`       | The new name of the base repository (i.e. the new GitLab project name). | String |  `[a-z0-9]+([.-][a-z0-9]+)*(/[a-z0-9]+([.-][a-z0-9]+)*)` & `[a-z0-9]+(?:(?:(?:[._]|__|[-]*)[a-z0-9]+)+)?` | Must match the [GitLab project name formatting requirements](https://docs.gitlab.com/ee/user/project/) as well as the registry [`v2` OCI distribution spec requirements for repository names](https://github.com/distribution/distribution/blob/main/docs/spec/api.md#overview). |

#### Authentication

The authentication token must have `full-access` to the base-repository (project-path) that is referenced in the request `path` parameter. 

For example, a rename request with `path` parameter `gitlab-org/build/cng` must have `pull` & `push` scopes on the project-path - `gitlab-org/build/cng` and `pull` scope on the project's sub-paths - `gitlab-org/build/cng/*`. 

The request authentication token must also contain access claims with the following attributes:

| Key | Description| Format | Condition | Example |
|-----|------------|--------|-----------|---------|
| `meta.project_path` | The path of the existing project to be renamed. | String | Must match the `path` parameter in the API request path. | `gitlab-org/build/cng` |

#### Example

```shell
curl  --header "Authorization: Bearer <token>" -X PATCH https://registry.gitlab.com/gitlab/v1/repositories/gitlab-org/build/cng/ \
   -H 'Content-Type: application/json' \
   -d '{"name": "new-cng"}'  
```

### Response

#### Header

| Status Code                | Reason                                                                                                                                                                                                                                                                                                 |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `202 Accepted`             | The new name was successfully leased to the `path` in the request. The response body contains an object with the `ttl` indicating the time left before the lease is released. This is returned only for successful requests with query parameter `dry_run` set to `true`. |
| `204 No Content`           | The requested `path` was successfully renamed to the suggested new name. This is returned only for successful requests with query parameter `dry_run` set to `false` (default).                                                                                                                              |
| `400 Bad Request`          | An invalid `path` parameter, request body or token claim was provided.                                                                                                                                                                                                                    |
| `401 Unauthorized`         | The client should take action based on the contents of the `WWW-Authenticate` header and try the endpoint again.                                                                                                                                                                                       |
| `404 Not Found`            | The namespace associated with the repository was not found or the rename operation is not implemented.                                                                                                                                                                                     |
| `409 Conflict`             | The proposed base repository `name` in the body is already taken.                                                                                                                                                                                                                                      |
| `422 Unprocessable Entity` | The base repository `path` contains too many sub-repositories for the operation to be executed.                                                                                                                                                                                                        |

#### Body

The response body is only returned for requests with the query parameter `dry_run` set to `true`. In the situation where the response body is returned; it is an object with the following attributes:

| Key          | Value                                                                                  | Type         | Format                              | Condition                                                                                                |
|--------------|----------------------------------------------------------------------------------------|--------------|-------------------------------------|----------------------------------------------------------------------------------------------------------|
| `ttl`        | The UTC timestamp after which the leased/reserved target name is released. | String      |   UTC                       | Request must have the query parameter `dry_run` set to `true`.                       |

#### Example

```json
{
    "ttl": "2009-11-10T23:00:00.005Z"
}
```

### Codes

The error codes encountered via this API are enumerated in the following table.

|Code                              | Message                                                                                                               | Description                                                                                                                                                                        |
|--------------------------------- |-----------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `INVALID_QUERY_PARAMETER_VALUE`  | `the value of a query parameter is invalid`                                                                           | The value of a request query parameter is invalid. The error detail identifies the concerning parameter and the list of possible values.                                           |
| `INVALID_QUERY_PARAMETER_TYPE`   | `the value of a query parameter is of an invalid type`                                                                | The value of a request query parameter is of an invalid type. The error detail identifies the concerning parameter and the list of possible types.                                 |
| `INVALID_BODY_PARAMETER_TYPE`    | `a value of the request body parameter is of an invalid type`                                                         | The value of a request body parameter is of an invalid type. The error detail identifies the concerning parameter and the list of possible types.                                  |
| `INVALID_JSON_BODY`              | `the body of the request is an invalid JSON`                                                                          | The body of a request is an invalid JSON.                                                                                                                                          |
| `NAME_UNKNOWN`                   | `the repository namespace segment of the base repository path is not known to registry`                               | The namespace used in the base repository path is unknown to the registry.                                                                                                         |
| `UNKNOWN_PROJECT_PATH_CLAIM`     | `the value of meta.project_path token claim is not found`                                                             | The value of any `access[x].meta.project_path` token claim is not found in the received token.                                                                                     |
| `MISMATCH_PROJECT_PATH_CLAIM`    | `the value of meta.project_path token claim is not assigned to the repository that operation is attempting to rename` | The value of any `access[x].meta.project_path` token claim is not assigned to the repository that operation is attempting to rename.                                               |
| `RENAME_CONFLICT`                | `the base repository name is already taken`                                                                           | The name requested (as the new name) for a base repository `path` is already in use within the registry.                                                                           |
| `EXCEEDS_LIMITS`                 | `the base repository requested path contains too many sub-repositories for the operation to be executed`              | The base-repository to be used for the operation contains too many sub-repositories. The error detail identifies the maximum amount of sub-repositories the operation can service. |
| `NOT_IMPLEMENTED`                | `the requested operation is not available`                                                                            | The operation is not available. The error detail identifies the reason why the operation is not implemented/available.                                                             |

## Errors

In case of an error, the response body payload (if any) follows the format defined in the
[OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes), which is the
same format found on the [V2 API](../docker/v2/api.md#errors):

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

### 2023-07-17

- Add support to sort the response from the List Repository Tags endpoint by descending order.

### 2023-06-15

- Add docs for renaming all repositories associated with a GitLab project.
- Add support for backwards pagination for the List Repository Tags endpoint.

### 2023-05-10

- Add support for a tag name filter in List Repository Tags.

### 2023-04-24

- Add config digest to List Repository Tags response.

### 2023-04-20

- Removed routes used for the GitLab.com online migration.

### 2023-03-22

- Add 404 status to repositories list endpoint.

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
