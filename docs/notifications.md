# Event Notifications Metrics

When enabled, the registry emits [webhook notifications](../docs/configuration.md#notifications) when certain events occur, such as pushing or pulling manifests and blobs from the registry.

## Overview

The notifications subsystem tracks various metrics to help monitor the health and performance of webhook endpoints.
These metrics provide insights into event processing, delivery success rates, and system performance.

## Available Metrics

### Event Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_events_total` | Counter | `type`, `action`, `artifact`, `endpoint` | Total number of events processed by the notification system |

**Label Values:**

<!-- markdownlint-disable MD044 -->
- `type`: Event processing stage
  - `Events` - Initial event ingress into the system
  - `Successes` - Successfully delivered events
  - `Failures` - Failed event deliveries
- `action`: Registry action that triggered the event (e.g., `push`, `pull`, `mount`, `delete`)
- `artifact`: Type of registry artifact
  - `manifest` - Container image manifest
  - `blob` - Binary blob/layer
  - `tag` - Container image tag
- `endpoint`: Name of the configured webhook endpoint

### Queue Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_pending` | Gauge | `endpoint` | Current number of events pending in the queue waiting to be sent |

This gauge increases when events are added to the queue and decreases when they are successfully sent or dropped.

**Label Values:**

- `endpoint`: Name of the configured webhook endpoint

## Delivery Metrics

### Event Delivery Outcomes

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_delivery_total` | Counter | `endpoint`, `delivery_type` | Total number of events by final delivery outcome after all retry attempts |

**Label Values:**

- `endpoint`: Name of the configured webhook endpoint
- `delivery_type`: Final outcome of the event delivery
  - `delivered` - Event successfully sent to endpoint (may have required retries)
  - `lost` - Event dropped after exhausting all retry attempts

This metric helps track the reliability of your notification endpoints. A high number of lost events indicates endpoint issues that need investigation.

## Performance Metrics

### Retry Distribution

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_retries_count` | Histogram | `endpoint` | Distribution of retry attempts needed before successful delivery or failure |

**Histogram Buckets:** 0, 1, 2, 3, 5, 10, 15, 20, 30, 50 retries

**Label Values:**

- `endpoint`: Name of the configured webhook endpoint

This histogram shows how many retries were needed for each event. Events that succeed on the first attempt (0 retries) indicate healthy endpoints, while higher retry counts suggest intermittent issues.

### HTTP Request Latency

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_http_latency` | Histogram | `endpoint` | Distribution of HTTP request duration when sending notifications to endpoints (seconds) |

**Histogram Buckets:** 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000 seconds

**Label Values:**

- `endpoint`: Name of the configured webhook endpoint

This metric tracks only the HTTP request time, helping identify slow endpoints or network issues. High latencies can cause queue backups and should be investigated.

### Total Event Processing Latency

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_total_latency` | Histogram | `endpoint` | Distribution of total time from event creation to successful delivery (seconds) |

**Histogram Buckets:** 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000 seconds

**Label Values:**

- `endpoint`: Name of the configured webhook endpoint

This end-to-end latency includes:

- Queue wait time
- HTTP request time
- Any retry delays

High total latencies with low HTTP latencies indicate queue congestion. This metric is calculated from the event timestamp to delivery completion.

### HTTP Status Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_status_total` | Counter | `code`, `endpoint` | Total number of HTTP response status codes received from endpoints |

**Label Values:**

- `code`: HTTP status code and text (e.g., "200 OK", "404 Not Found", "500 Internal Server Error")
- `endpoint`: Name of the configured webhook endpoint

### Error Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_errors_total` | Counter | `endpoint` | Total number of events that failed due to internal errors (not HTTP failures) |

This metric tracks non-HTTP errors such as:

- Network connectivity issues
- Timeout errors
- Malformed event data
- Transport-level errors

**Label Values:**

- `endpoint`: Name of the configured webhook endpoint

## Implementation Details

### Endpoint Metrics Structure

Each endpoint maintains its own `safeMetrics` instance that tracks:

- `pending`: Current queue depth
- `events`: Total events ingressed
- `successes`: Successfully delivered events
- `failures`: Failed deliveries
- `errors`: Internal errors
- `statuses`: HTTP status code distribution

### ExpVar Integration

In addition to Prometheus metrics, the system also exposes metrics via expvar at the `/debug/vars` endpoint under `registry.notifications.endpoints`.
This provides a JSON representation of all endpoint metrics including:

- Endpoint name and URL
- Endpoint configuration
- Current metric values
