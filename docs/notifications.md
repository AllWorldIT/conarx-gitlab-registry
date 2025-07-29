# Event Notifications Metrics

When enabled, the registry emits [webhook notifications](../docs/configuration.md#notifications) when certain events occur, such as pushing or pulling manifests and blobs from the registry.

## Overview

The notifications subsystem tracks various metrics to help monitor the health and performance of webhook endpoints.
These metrics provide insights into event processing, delivery success rates, and system performance.

## Available Metrics

### Event Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_events` | Counter | `type`, `action`, `artifact`, `endpoint` | Total number of events processed by the notification system |

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

### HTTP Status Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_status` | Counter | `code`, `endpoint` | Total number of HTTP response status codes received from endpoints |

**Label Values:**

- `code`: HTTP status code and text (e.g., "200 OK", "404 Not Found", "500 Internal Server Error")
- `endpoint`: Name of the configured webhook endpoint

### Error Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `registry_notifications_errors` | Counter | `endpoint` | Total number of events that failed due to internal errors (not HTTP failures) |

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
