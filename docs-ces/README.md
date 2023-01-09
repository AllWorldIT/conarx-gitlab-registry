# Conarx Extended Storage

The purpose of this project and why we forked GitLab's container-registry project is to fill the gap where GitLab refuse to accept
patches or fix support for S3 backend storage systems that differ slightly in implementation to AWS S3.


# Commercial Support

Commercial support is available from [Conarx](https://conarx.tech).


# What do we do?

* We will import new GitLab container-registry release tags and provide patched versions of these.
* GitLab images will be generated including these fixed binaries [here](https://gitlab.conarx.tech/containers/gitlab).
* We will strive to accept bugfix patches and contributions to improve S3 support.


# Working S3 Storage Platforms

The below S3 storage platforms did not work correct, had bugs or unexpected behavior when used with GitLab as a container registry
backend.

## Minio (replicated/with versioning)

Minio differs slightly to AWS S3 in the way that it treats versioned objects with delete markers and paths leading up to the
object.

AWS S3 will not return any leading path components should all objects under those components have delete markers.

Minio will return CommonPrefixes for each component path even if all objects under those components have delete markers.

