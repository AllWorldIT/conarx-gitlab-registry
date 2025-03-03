# Rate limiting

**Note: This feature is currently a work-in-progress and not readily available.**

GitLab Container Registry implements application side rate-limiting
backed by Redis. This document specifies the behavior of such limiting.

[TOC]

## Algorithm

The registry uses the official [redis_rate](https://github.com/go-redis/redis_rate) package to
implement rate-limiting as middleware for every request received. The `redis_rate`
package implements a [Generic cell rate algorithm (GCRA)](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm),
also known as `leaky bucket`.

The leaky bucket name is an analogy that helps understand how requests are counted. In short:

1. A bucket is full of "tokens".
1. When a request is received, a token is taken out of the bucket (leak).
1. If the bucket is empty, a request will be denied.
1. The bucket is refilled at a given rate.
1. Bursts are configurable to allow for sudden spikes in traffic.

## Redis

The registry requires a rate-limiter Redis instance to be configured, see the
[rate-limiter](../../configuration.md#ratelimiter) configuration section for available
connection settings.

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
which is equivalent to the size of the bucket (maximum number of requests). The bucket will
fill at a rate of 60 tokens per minute. In practice it would look something like this:

- 0 requests at T0
- Burst of 100 requests at T1 will be allowed
- Request number 101 arrives at T2
- The bucket refills at a rate of 60 tokens per minute, or one token every 1s
- If T2 = now() - t1 < 1s, the request will be denied

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
