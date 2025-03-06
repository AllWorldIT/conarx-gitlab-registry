# Redis rate limiter

This package contains a modified copy of the
[redis_rate](https://github.com/go-redis/redis_rate) package
which allows for keys to be prefixed with a given name.

This is done to comply with the GitLab registry
[Redis dev guidelines](./../../docs/redis_dev_guidelines.md)
key formats.

A [pull request](https://github.com/go-redis/redis_rate/pull/111)
has been submitted to the upstream repository. We will replace
this package with the upstream version if/when it is accepted.

See the [internal issue](https://gitlab.com/gitlab-org/container-registry/-/issues/1527)
for extra context.
