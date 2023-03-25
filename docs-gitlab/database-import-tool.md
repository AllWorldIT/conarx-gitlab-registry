# Importing Existing Data to the Database

The database import utility enables a registry which was previously using object
storage to manage its metadata to use the metadata database while preserving
the images and tags previously pushed to the registry.

## The Import Command

This command can be accessed via the registry binary and takes the following
form.

```bash
./registry database import [flags] path/to/config.yml
```

### Options

#### All Repositories

The `--all-repositories` option will import all repository metadata. With this
flag, it is required to switch the registry to read-only mode. Start with a
`pre-import` to limit the amount of read-only time required as much of the
import work can be handled by the pre-import phase which has no read-only
requirement. Alias: `--step-two`

#### Common Blobs

The `--common-blobs` option instructs the tool to import all blob metadata from
common blob storage not already present in the metadata database. This command
does not import any repository scope metadata and is intended to be used as the
final step of a three step import. Running this command ensures that any
unreferenced blobs are visible to the online garbage collector, allowing them
to be removed from object storage. Alias: `--step-three`

#### Require Empty Database

The `--require-empty-database` option allows the user to enable a safety check
which prevents the import command from running on a database which already
contains some information. This option is useful for relatively small registries
where it is possible to import all registry data in one single period of
read-only mode or downtime. Larger registries will likely need to break up the
import process over multiple sessions.

#### Dangling Manifests

The `--dangling-manifests` option instructs the import to import all manifests
without confirmation that this information is reachable from a tagged image.

#### Dry Run

The `--dry-run` option will perform a full import without committing any changes
to the database. This option is useful for testing and debugging purposes and
for smaller registries were the runtime of the import process is not
prohibitively long. Additionally, for even larger registries this option can
be ran while the registry is in full operation, although this could impact the
performance of the registry and the import may not capture any images which
are added while the dry run is in progress.

#### Repository

The `--repository` option allow the user to pass the path to a particular
repository to be imported. This option enables the user to import a subset of
repositories via repeated calls to the import command, passing in a new
repository path each time.

Note: The `--dangling-blobs` option is ignored when this option is specified.

#### Blob Transfer Destination
The `--blob-transfer-destination` option allows the user to copy imported blobs to
another storage location. This option is only available for GCS and filesystem
drivers. For GCS, the name of the new bucket should be passed to this flag. For
the filesystem driver, this should be the new root directory. In both cases,
the configured storage driver must have read and write access to the new storage.

#### Pre Import
The `--pre-import` option will only import immutable registry data. When running
with this flag, it is not necessary to switch the registry to read-only mode.
This, in conjunction with a normal import command ran afterward, should enable
administrators to limit the amount of time a repository must be read-only, as
much of the import work can be handled by the pre-import phase. Alias: `--step-one`

While it is not necessary to switch the registry to read-only mode,
administrators should take care not to use the [blob delete API
endpoint](https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/api.md#delete-blob)
during the pre-import phase. This endpoint is not used by any of the Docker
client commands. If a blob is deleted after one of its associated manifests was
pre-imported, the import step would import the manifest with the deleted blob
still linked.

Since tags are mutable data, all objects imported during the pre import step
are subject to online garbage collection, and therefore it is important to
ensure that the subsequent import step is completed within the configured
garbage collector workers
[`reviewafter`](https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/configuration.md#gc)
delay or garbage collection is not enabled until after the import step finishes.

#### Row Count

The `--row-count` option allows logging the row count of relevant database tables on (pre)import completion.

## Prerequisites

### Create Database

Please make sure to create a `registry_metadata` (naming suggestion) database in your
PostgreSQL instance before running the import command.

#### Example

```text
psql -h localhost -U postgres -w -c "CREATE DATABASE registry_metadata;"
```

### Configuration

The configuration passed to the import command should be based on the
configuration of the registry that you are importing. Particularly important
is that the `storage` section is configured the same way so that the import
command has access to the data used by the registry you wish to import.

The following sections discuss configuration options that are relevant to the
import process, directly or indirectly. These section assumes that you are
starting with a working and appropriate configuration for an existing registry
which has not yet been imported.

#### Read-Only Mode

Enabling read-only mode allows the maximum access possible to the registry while
the import is in progress. This setting allows users to pull images, but will
prevent any new pushes. Without this, it's possible that the import utility
would not import data related to pushes which happen after the start of the
import.

```
maintenance:
  readonly:
    enabled: false
```

Once the configuration is updated, you should restart the registry service for
read-only mode to take effect.

#### Database

This is an example database configuration section which the registry which will
use to store the data picked up by the import and will serve as the source of
metadata for the registry after the import is complete. Please substitute these
example values with ones the ones that are applicable to your environment.

```
database:
  enabled:  true
  host:     "localhost"
  port:     8080
  user:     "postgres"
  password: "secret"
  dbname:   "registry_metadata"
  sslmode:  "disable"
```

Note: If you wish to continue reading from the registry during the import, you
will need make a copy of this configuration and pass it to the import command
with `enabled` set to `true`, while the running registry will need to have
`enabled` set to `false` to prevent it from attempting to read from the database
before it is fully populated.

### Import

Once you have prepared the registry for import and have prepared a
configuration file containing the database connection information, you are
ready to run the import command:

Navigate to the environment where your registry binary is located. You will need
to locate the registry binary the run the import command. For this example, we
will assume the registry binary is the in current working directory:

```bash
./registry database import [flags] config.yml
```

### Restarting Registry Services with the Database

Once the import has successfully completed, you will need to add the database
section that was added in the `config-copy.yml` to the registry configuration
and disable read-only mode. Once this is done, you will need to restart the
registry for the new configuration to take effect.
