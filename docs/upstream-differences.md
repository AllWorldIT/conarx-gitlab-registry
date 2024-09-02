# Differences From Upstream

Here is the list of the most significant differences when compared with the upstream
[Docker Distribution Registry](https://github.com/docker-archive/docker-registry),
now [CNCF Distribution](https://github.com/distribution/distribution).

## Configuration

### S3 Storage Driver

#### Additional parameters

`pathstyle`

When set to `true`, the driver will use path style routes.
When not set, the driver will default to virtual path style routes, unless
`regionendpoint` is set. In which case, the driver will use path style routes.
When explicitly set to `false`, the driver will continue to default to virtual
host style routes, even when the `regionendpoint` parameter is set.

`parallelwalk`

When this feature flag is set to `true`, the driver will run certain operations,
most notably garbage collection, using multiple concurrent goroutines. This
feature will improve the performance of garbage collection, but will
increase the memory and CPU usage of this command as compared to the default,
particularly when the `--delete-untagged` (`-m`) option is specified.

`maxrequestspersecond`

This parameter determines the maximum number of requests that
the driver will make to the configured S3 bucket per second. Defaults to `350`
with a maximum value of `3500` which corresponds to the [current rate limits of S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html)
`0` is a special value which disables rate limiting. This is not recommended
for use in production environments, as exceeding your request budget will result
in errors from the Amazon S3 service.

`maxretries`

The maximum number of times the driver will attempt to retry failed requests.
Set to `0` to disable retries entirely.

### Azure Storage Driver

#### Additional parameters

`rootdirectory`

This parameter specifies the root directory in which all registry files are
stored. Defaults to the empty string (bucket root).

`trimlegacyrootprefix`

Originally, the Azure driver would write to `//` as the root directory, also
appearing in some places as `/<no-name>/` within the Azure UI. This legacy
behavior must be preserved to support older deployments using this driver.
Set to `false` to build root paths with an extra leading slash (i.e `//`).
Defaults to the `true` to remove legacy root prefix. It is recommended to use
`legacyrootprefix` to control this behavior.

`legacyrootprefix`

This parameter is the recommended configuration (as opposed to `trimlegacyrootprefix`) to be used to preserve
the Azure driver legacy behavior of using  `//` (appearing in some places as `/<no-name>/` within the Azure UI)
as the root directory. When `legacyrootprefix` is set to `true` the Azure driver uses the legacy Azure root directory.
When this parameter is specified together with `trimlegacyrootprefix` the registry will fail to start if the parameters conflict
( i.e `trimlegacyrootprefix: true` and `legacyrootprefix: true` or `legacyrootprefix: false` and `trimlegacyrootprefix: true`).

### GCS Storage Driver

#### Additional parameters

`parallelwalk`

When this feature flag is set to `true`, the driver will run certain operations,
most notably garbage collection, using multiple concurrent goroutines. This
feature will improve the performance of garbage collection, but will
increase the memory and CPU usage of this command as compared to the default.

## Garbage Collection

### Invalid Link Files

If a bad link file (e.g. 0B in size or invalid checksum) is found during the
*mark* stage, instead of stopping the garbage collector (standard behavior)
it will log a warning message and ignore it, letting the process continue.
Blobs related with invalid link files will be automatically swept away in the
*sweep* stage if those blobs are not associated with another valid link file.

See [Cleanup Invalid Link Files](cleanup-invalid-link-files.md) for a guide on
how to detect and clean these files based on the garbage collector output log.

### Estimating Freed Storage

Garbage collection now estimates the amount of storage that will be freed.
It's possible to estimate freed storage without setting the registry to
read-only mode by doing a garbage collection dry run; however, this will affect
the accuracy of the estimate. Without the registry being read-only, blobs may be
re-referenced, which would lead to an overestimate. Blobs might be
dereferenced, leading to an underestimate.

### Debug Server

A pprof debug server can be used to collect profiling information on a
garbage collection run by providing an `address:port` to the command via
the `--debug-server` (`--s`) flag. Usage information for this server can be
found in [the documentation for pprof](https://golang.org/pkg/net/http/pprof/).

## API

### Tag Delete

A new route, `DELETE /v2/<name>/tags/reference/<reference>`, was added to the
API, enabling the deletion of tags by name.

### Broken link files when fetching a manifest by tag

When fetching a manifest by tag, through `GET /v2/<name>/manifests/<tag>`, if
the manifest link file in
`/docker/registry/v2/repositories/<name>/_manifests/tags/<tag>/current/link` is
empty or corrupted, instead of returning a `500 Internal Server Error` response
like the upstream implementation, it returns a `404 Not Found` response with a
`MANIFEST_UNKNOWN` error in the body.

If for some reason a tag manifest link file is broken, in practice, it's as if
it didn't exist at all, thus the `404 Not Found`. Re-pushing the tag will fix
the broken link file.

### Custom Headers on `GET /v2/`

The following headers were added to the response of `GET /v2/` requests:

- `Gitlab-Container-Registry-Version`: The semantic version of the GitLab
  Container Registry (e.g. `2.9.0-gitlab`). This is set during build time (in
  `version.Version`).
- `Gitlab-Container-Registry-Features`: A comma separated list of supported
  features/extensions that are not part of the Docker Distribution spec (e.g.
  `tag_delete,...`). Its value (hardcoded in `version.ExtFeatures`) should be
  updated whenever a custom feature is added/deprecated.
- `Gitlab-Container-Registry-Database-Enabled`: A boolean indicating whether
  the metadata database is enabled in the registry configuration.

This is necessary to detect whether a registry is the GitLab Container Registry
and which extra features it supports.
