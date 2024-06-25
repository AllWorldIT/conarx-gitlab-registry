# Unsupported Digest

This test fixture simulates a registry with a repository in which digests use
unsupported algorithms that will trigger the `digest.UnsupportedDigest`
error. This can be used to cause errors in order to test the
importer error path.

## Fixture Creation

This fixture was created by copying the a-simple repository and relevant blobs
from the happy path text fixture and modifying the digests' algorithm
to be unsupported.
