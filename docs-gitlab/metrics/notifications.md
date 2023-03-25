# Event notifications metrics

When enabled, the registry emits [webhook notifications](./../docs/configuration.md#notifications)
when certain events occur, for example when pushing a manifest to the registry.

## Metrics available

The following metrics are available:

| Metric                                  | Type    | Since | Description                                            | Labels                        |
|-----------------------------------------|---------|-------|--------------------------------------------------------|-------------------------------|
| `registry_notifications_events_total`   | Counter | -     | The total number of events                             | `type`, `action`, `artifact`  |
| `registry_notifications_pending_total`  | Gauge   | -     | Pending events available to be sent                    |                               |
| `registry_notifications_status_total`   | Counter | -     | The total number of notification response status codes | `code`                        |
