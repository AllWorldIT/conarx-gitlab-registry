# Rate limiting

**Note: This feature is currently a work-in-progress and not readily available.**

GitLab Container Registry implements application side rate-limiting
backed by Redis. This document specifies the behavior of such limiting.

[TOC]

## Algorithm

The documentation has been updated to explain how the new algorithm works - instead of thinking
about a "leaky bucket" that drains tokens, it now tracks when the next valid request should arrive.
This approach is more accurate and doesn't require background processes to refill tokens.

The registry implements rate limiting using a custom Lua script-based
implementation of the
[Generic Cell Rate Algorithm (GCRA)](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm),
also known as the `leaky bucket` algorithm.

GCRA is a timestamp-based rate limiting algorithm that tracks the
"theoretical arrival time" (TAT) for the next compliant request rather
than physically counting tokens. This approach provides several advantages:

1. **Memory efficiency**: Only stores a single timestamp per rate limit key instead of maintaining token counts
1. **Atomicity**: The Lua script ensures atomic execution of all rate limiting operations in Redis
1. **Precision**: Uses high-precision timestamps to accurately track request intervals
1. **No background processes**: Unlike traditional leaky buckets, GCRA doesn't require background token dripping processes

The algorithm works by:

1. Calculating a TAT (theoretical arrival time) for each request based on the configured rate
1. Comparing the current request time against the stored TAT
1. If the request arrives before the TAT, it's rejected (rate limited)
1. If the request is compliant, the TAT is updated for the next request
1. Burst capacity is handled by allowing the TAT to fall behind the current time by a configurable amount

## Redis

The registry requires a rate-limiter Redis instance to be configured, see the
[rate-limiter](../../configuration.md#ratelimiter) configuration section for available
connection settings.

### Redis Cluster Support

The rate limiting implementation supports Redis Cluster deployments with the following considerations:

**Key Distribution**: Rate limiting keys are distributed across cluster shards based on Redis's hash slot algorithm
(CRC16 mod 16384). This ensures even distribution of rate limiting data across the cluster.

**Lua Script Execution**: The GCRA Lua script executes atomically on the specific shard containing the rate limiting
key. Since each IP-based rate limit uses its own key, rate limiting operations for different IPs will distribute across
different shards, preventing any single shard from becoming a bottleneck.

**No Cross-Shard Dependencies**: Each rate limiting key is self-contained with its own TAT value, eliminating the need
for cross-shard operations or global state synchronization.

### Key Format

- Each client (e.g., IP) has its own Redis key.
- Example key for IP `1.2.3.4`: `registry:api:{rate-limit:ip:1.2.3.4}`
- The use of hash tags (`{...}`) ensures that all keys related to this IP hash to the same Redis Cluster slot, avoiding cross-slot errors.

Each key is a **Redis hash** containing two fields:

| Field        | Type   | Description                                   |
|--------------|--------|-----------------------------------------------|
| `tokens`     | string | Current number of available tokens (float stored as string) |
| `last_refill`| string | Unix timestamp (seconds) of the last bucket update |

Example contents:

```yaml
tokens: "6.5"
last_refill: "1720001234"
```

## Limit types

The registry implements:

1. [IP-based](#ip-based)

### IP-based

Each request has a remote IP address that will be used to generate a key that will
be stored in Redis and will be used to calculate the available limit of that key.
The [`RemoteIP`](../../../context/http.go#L78) function will determine the IP address of
the originating request. The IP address will be obtained in the following order:

1. The second value of the `X-Forwarded-For` header if it exists. For example, if
`X-Forwarded-For: 192.168.1.1,142.250.70.174`, then the IP address will be `142.250.70.174`.
1. The value of `X-Real-Ip` if present and is a valid IP address.
1. The value of `http.Request.RemoteAddr` as calculated by the Go `net/http` package.

It is important to determine the correct IP address as the registry may be behind a
reverse proxy or load balancer. The access logs of the registry show the `remote_ip`
field as the IP address of the originating request, this will be the value of the IP
that will be used to generate the Redis key.

The Redis key will follow the [guidelines key format](../../redis-dev-guidelines.md#key-format).

```go
    key := `registry:api:{rate-limit:ip:` + ip + `}`
```

### Enabling rate limiters

To enable the rate limiters, the following configuration must be set:

```yaml
redis:
  ratelimiter:
    enabled: true
  # specific rate-limiting configuration will be added along with 
  # https://gitlab.com/gitlab-org/container-registry/-/issues/1224
```

## Rate limiter example

For a per minute rate period, with a rate of 60 and burst of 100,
we will allow a given IP to have a burst of up to 100 requests in 1m,
which is equivalent to the maximum burst capacity. The theoretical arrival time
will advance at a rate of 1 second per request (60 requests per 60 seconds).
In practice it would look something like this:

- 0 requests at T0
- Burst of 100 requests at T1 will be allowed (TAT advances to T1 + 100 seconds)
- Request number 101 arrives at T2
- If T2 < TAT - burst_duration, the request will be denied
- If T2 >= TAT - burst_duration, the request will be allowed and TAT will be updated

The burst capacity allows for sudden spikes in traffic while maintaining the overall rate limit over time.

## Response code

When a request is denied by the limiter, the registry will immediately reply with
a status code `429 Too Many Requests` and the sample payload:

- IP address limiter

  ```json
  {
     "errors": [
         {
             "code": "TOOMANYREQUESTS",
             "message": "too many requests",
             "detail": {
                 "limiter": "ip",
                 "entity": "172.16.123.1"
             }
         }
     ]
  }
  ```

## Redis Clustering FAQ

### Does rate limiting traffic hit the same shard?

No. Rate limiting keys are distributed across Redis Cluster shards using Redis's standard hash slot mechanism.
Each IP address generates a unique key that maps to a specific hash slot, and hash slots are distributed across
shards. This means:

- Different IP addresses will likely map to different shards
- No single shard becomes a bottleneck for rate limiting operations
- The cluster topology is preserved and utilized effectively

### How does this handle high cardinality?

The GCRA implementation is designed for high cardinality scenarios:

- **Minimal memory footprint**: Each rate limiting key stores only a single timestamp value (the TAT)
- **Automatic cleanup**: Keys can be configured with TTL values to automatically expire unused rate limits
- **Distributed storage**: Keys are distributed across cluster shards based on their hash, spreading the load

### Are there limits on Redis hash fields per key?

This implementation doesn't use Redis hashes with multiple fields per key. Each rate limiting entity
(IP address) gets its own individual Redis key containing a single timestamp value. This approach:

- Avoids Redis's hash field limits
- Supports per-key TTL for automatic cleanup
- Enables better distribution across cluster shards
- Simplifies the Lua script implementation
