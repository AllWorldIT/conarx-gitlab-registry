# API Endpoint Development Guidelines

## Geo

[Geo](https://docs.gitlab.com/ee/administration/geo/) makes use of the registry
[notifications](spec/gitlab/notifications.md) to sync data between the
primary and secondary site. Therefore, when adding new endpoints care must be
taken such that the Geo team is capable of supporting this new endpoint. The
following workflow ensures that new endpoints will not cause instability for
users of Geo.

### Rails Authentication

GitLab Rails is the [authentication service](https://distribution.github.io/distribution/spec/auth/token/)
for the container registry. We can use this to ensure that new endpoints are not
available for Geo users prematurely. This can be accomplished by denying
authentication based on the value of `Gitlab::Geo.geo_database_configured?` 

### Notifications

Create a [notification](spec/gitlab/notifications.md) for the new endpoint.

### Open an Issue for the Geo Team

Open an issue for the Geo team in the Rails project with the `~group::geo` label,
so that they can schedule the work for supporting this endpoint. This issue
should include:

- a short description of the endpoint
- a link to our documentation
- and examples of the notification that would be emitted by this endpoint
