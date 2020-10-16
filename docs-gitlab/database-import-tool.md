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

#### Continue Import

The `--continue-import` option allows the user to bypass the safety check which
prevent the import command from running on a database which already contains
some information. This option is useful for very large registries where it is
not feasible to import all registry data in one single period of read-only mode
or downtime. Or for when only a subset of repositories should be imported.

This also possible to use this option in conjunction with a registry which is
configured to use the database and is in read/write mode and taking new
requests. In this way, the importer will catch up previously pushed images and
blobs without downtime. This relies on filesystem mirroring which is a
temporary feature for testing that will be removed in a future version.

#### Dangling Blobs

The `--dangling-blobs` option instructs the tool to import all blob metadata
without confirmation that this information is reachable from a tagged image
or that the blob is linked to any repository.

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
repository to be imported. This option can be combined with the
`--continue-import` to import a subset of repositories via repeated calls to
the import command, passing in a new repository path each time.

Note: The `--dangling-blobs` option is ignored when this option is specified.

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