# Lockfile database in use

This test fixture simulates a registry that is already using the metadata database
with an existing lockfile `database-in-use`. However, this registry has no data
as it only uses the lockfiles to check if the app can start or not.


## Fixture Creation

This fixture was created copying the `happy-path` registry and removing all blobs
and manifests from the corresponding directories.
