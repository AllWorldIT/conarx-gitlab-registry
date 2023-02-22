# GitLab Container Registry

## Guides

### Development

- [Development Environment Setup](development-environment-setup.md)
- [Local Integration Testing](storage-driver-integration-testing-guide.md)
- [Offline Garbage Collection Testing](garbage-collection-testing-guide.md)
- [Database Development Guidelines](database-dev-guidelines.md)
- [Database Migrations](database-migrations.md)

### Technical Documentation

- [Metadata Import](database-import-tool.md)
- [Push/pull Request Flow](push-pull-request-flow.md)
- [Authentication Request Flow](auth-request-flow.md)
- [Online Garbage Collection](db/online-garbage-collection.md)
- [HTTP API Queries](db/http-api-queries.md)

### Troubleshooting

- [Cleanup Invalid Link Files](cleanup-invalid-link-files.md)

## Differences From Upstream

### Configuration

#### S3 Storage Driver

##### Additional parameters

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
with a maximum value of `3500` which corresponds to the current rate limits of
S3: https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html
`0` is a special value which disables rate limiting. This is not recommended
for use in production environments, as exceeding your request budget will result
in errors from the Amazon S3 service.

`maxretries`

The maximum number of times the driver will attempt to retry failed requests.
Set to `0` to disable retries entirely.

#### Azure Storage Driver

##### Additional parameters

`rootdirectory`

This parameter specifies the root directory in which all registry files are
stored. Defaults to the empty string (bucket root).

`trimlegacyrootprefix`

Originally, the Azure driver would write to `//` as the root directory, also
appearing in some places as `/<no-name>/` within the Azure UI. This legacy
behavior must be preserved to support older deployments using this driver.
Set to `true` to build root paths without an extra leading slash.

#### GCS Storage Driver

##### Additional parameters

`parallelwalk`

When this feature flag is set to `true`, the driver will run certain operations,
most notably garbage collection, using multiple concurrent goroutines. This
feature will improve the performance of garbage collection, but will
increase the memory and CPU usage of this command as compared to the default.

### Garbage Collection

#### Invalid Link Files

If a bad link file (e.g. 0B in size or invalid checksum) is found during the
*mark* stage, instead of stopping the garbage collector (standard behaviour)
it will log a warning message and ignore it, letting the process continue.
Blobs related with invalid link files will be automatically swept away in the
*sweep* stage if those blobs are not associated with another valid link file.

See [Cleanup Invalid Link Files](cleanup-invalid-link-files.md) for a guide on
how to detect and clean these files based on the garbage collector output log.

#### Estimating Freed Storage

Garbage collection now estimates the amount of storage that will be freed.
It's possible to estimate freed storage without setting the registry to
read-only mode by doing a garbage collection dry run; however, this will affect
the accuracy of the estimate. Without the registry being read-only, blobs may be
re-referenced, which would lead to an overestimate. Blobs might be
dereferenced, leading to an underestimate.

#### Debug Server

A pprof debug server can be used to collect profiling information on a
garbage collection run by providing an `address:port` to the command via
the `--debug-server` (`--s`) flag. Usage information for this server can be
found in the documentation for pprof: https://golang.org/pkg/net/http/pprof/

### API

#### Tag Delete

A new route, `DELETE /v2/<name>/tags/reference/<reference>`, was added to the
API, enabling the deletion of tags by name.

#### Broken link files when fetching a manifest by tag

When fetching a manifest by tag, through `GET /v2/<name>/manifests/<tag>`, if
the manifest link file in 
`/docker/registry/v2/repositories/<name>/_manifests/tags/<tag>/current/link` is
empty or corrupted, instead of returning a `500 Internal Server Error` response
like the upstream implementation, it returns a `404 Not Found` response with a 
`MANIFEST_UNKNOWN` error in the body.

If for some reason a tag manifest link file is broken, in practice, it's as if
it didn't exist at all, thus the `404 Not Found`. Re-pushing the tag will fix
the broken link file.

#### Custom Headers on `GET /v2/`

Two new headers were added to the response of `GET /v2/` requests:

* `Gitlab-Container-Registry-Version`: The semantic version of the GitLab
Container Registry (e.g. `2.9.0-gitlab`). This is set during build time (in
`version.Version`).
* `Gitlab-Container-Registry-Features`: A comma separated list of supported
features/extensions that are not part of the Docker Distribution spec (e.g.
`tag_delete,...`). Its value (hardcoded in `version.ExtFeatures`) should be
updated whenever a custom feature is added/deprecated.

This is necessary to detect whether a registry is the GitLab Container Registry
and which extra features it supports.

## Releases

We use [semantic-release](https://semantic-release.gitbook.io/semantic-release/)
to generate changelog entries, release commits and new git tags. A new release
is created by the project maintainers, using the `make release` command,
invoked from their local development machine. A `make release-dry-run` command
is available to anyone and allows previewing the next release.

**Note:** If you are a maintainer and this is the first time you are generating a
release, you must invoke the `make dev-tools` command to install the required
dependencies. This requires having [Node.js](https://nodejs.org/en/) and
[npm](https://docs.npmjs.com/cli/) installed locally.

Once a new tag is pushed to this repository, a CI pipeline is created 
([sample](https://gitlab.com/gitlab-org/container-registry/-/pipelines/713632199)).
Within the `release` stage, there are several ordered jobs that Maintainers
are responsible for triggering. These jobs are responsible for releasing in several GitLab
projects and their sequence is described in the [Release Plan](https://gitlab.com/gitlab-org/container-registry/-/blob/master/.gitlab/issue_templates/Release%20Plan.md) issue template. A new issue based on the the same 
template is automatically created as part of the CI pipeline with title `Release Version vX.Y.Z-gitlab`.

## Contributing

### Commit Messages

Commit messages must:

- Be formatted following the
[Conventional Commits 1.0](https://www.conventionalcommits.org/en/v1.0.0/)
specification;

- Be all lower case, except for acronyms and source code identifiers;

- Have the affected package full path in the scope portion, whenever applicable;

- Include the module name and the target version if upgrading or adding a
dependency;

- End with ` (<issue reference>)` if the commit is fixing an issue.

Please see [this](https://github.com/angular/angular/blob/8ce1ac603a9936784225c28f5526226d208eeb27/CONTRIBUTING.md) for additional information about the supported commit types.

#### Examples

```
build: upgrade cloud.google.com/go/storage to v1.16.0
```

```
fix(handlers): handle manifest not found errors gracefully (#12345)
```

```
perf(registry/storage/driver/gcs): improve blob upload performance
```

### Golang Version Support

Please see the [Supporting multiple Go versions](https://docs.gitlab.com/ee/development/go_guide/go_upgrade.html#supporting-multiple-go-versions).

Support for individual versions is ensured via the `.gitlab-ci.yml` file in the
root of this repository. If you modify this file to add additional jobs, please
ensure that those jobs run against all supported versions.
