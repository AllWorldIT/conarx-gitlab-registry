# Redis Development Guidelines

## Key Format

All keys are prefixed with `registry:` to provide isolation in case the Redis instance is shared with other
applications. Additionally, a second prefix is added to denote the application component that the data is related to.
The component prefix can be one of `api`, `db`, `gc`, or `storage`. This identifies whether the data was generated
within the scope of the HTTP API, the
metadata database, the garbage collector or the storage backend, respectively.

Additionally, in order to guarantee optimal forward compatibility with Redis Cluster, we ensure these keys
are CROSSSLOT compatible. See the Redis Cluster specification
[here](https://redis.io/docs/reference/cluster-spec/#hash-tags) and the corresponding GitLab documentation
[here](https://docs.gitlab.com/ee/development/redis.html#multi-key-commands) for additional information on this subject.

### Suffixes

We use [hash tags](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
when storing keys for a given repository. This ensures that operations for that repository
are done on the same Redis server.

Sometimes, a key may need to refer to sub-categories, for example for certain operations.
To add sub-categories, we use suffixes that are added after the hash key inside the brackets `{}`.
For example, for push/pull counters we would use the following keys:

- `registry:api:{repository:<namespace path>:<path hash>}:pull`
- `registry:api:{repository:<namespace path>:<path hash>}:push`

### Cache Data

#### Repository Objects

For cached data, starting with repository objects from the metadata database, we follow the
`registry:db:{repository:<namespace path>:<path hash>}` naming convention.

We use the repository path as unique identifier for repository objects as that's what we have during lookups. However,
because paths can be really long, we use the hex portion of their SHA256 hash, which limits the length to 64 characters.

As an additional safety measure, we always prefix the hash hex with the top-level namespace name in plaintext. This
ensures that in case of a path hash collision, we never leak data about a repository outside the same top-level
namespace. Nevertheless, after obtaining a repository object from Redis, we should always check that its decoded path
matches the one that we were looking for.

## Value Format

The key values in Redis are strings in various formats. In this section we describe what is the format for each type of
data that we currently store in Redis.

### Cache Data

#### Repository Objects

Repository object values are
[`Repository`](https://gitlab.com/gitlab-org/container-registry/-/blob/7ec72eccb53bd2dfd75ce3da1e96f7dcef434918/registry/datastore/models/models.go#L32)
structs encoded in [MessagePack](https://msgpack.org/). An average value has ~250 bytes in size.
