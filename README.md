# GitLab Container Registry

## Conarx Extended Storage

This README is kept in sync with the upstream GitLab Container Registry project.
The [Conarx Extended Storage README](./docs-ces/README.md) can be found under the
[docs-ces](./docs-ces/) directory.

## GitLab

**Quick Links**:
[**Roadmap**](https://about.gitlab.com/handbook/engineering/development/ops/package/#roadmap) |
[Want to Contribute?](#contributing) |

## Historical Background

The GitLab Container Registry originated as a fork of the
[Docker Distribution Registry](https://github.com/docker-archive/docker-registry),
now [CNCF Distribution](https://github.com/distribution/distribution), both
distributed under [Apache License Version 2.0](LICENSE).

The first GitLab change on top of the upstream implementation was
[efe421fd](https://gitlab.com/gitlab-org/container-registry/-/commit/efe421fde5bfb1312c98863edee25856ba5fd204),
and since then we have diverged enough to the point where we decided
to detach from upstream and proceed on our own path.

Since then, we have implemented and released several major performance
improvements and bug fixes. For a list of changes, please see
[differences from upstream](docs/upstream-differences.md).
These changes culminated on a new architecture based on a relational metadata
database and the original goal, enabling
[online garbage collection](docs/spec/gitlab/online-garbage-collection.md).

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.
