Follow this guide to start using the metadata database with the container registry.


**Warning:** The metadata database is a
[beta](https://docs.gitlab.com/ee/policy/experiment-beta-support.html#beta)
feature available starting at GitLab version 16.4.
**Carefully review the documentation before enabling the registry database in production!**
If you encounter a problem with either the import or normal operation of the
registry, please open an issue [here](https://gitlab.com/gitlab-org/container-registry/-/issues).

Please review the [feedback issue](https://gitlab.com/gitlab-org/gitlab/-/issues/423459)
before using this feature.

The metadata database enables you make use of many new features, such as
[online garbage collection](./db/online-garbage-collection.md) and increases the
efficiency of many registry operations.

[TOC]


## Things to Consider Before Migration

- Once you enable the database, you **must** continue to use it. The database is
now the sole source of the registry metadata, so disabling it after this point
will cause the registry to lose visibility to all images written to it while
the database was active.
- **Never** run offline garbage collection at any point after the
[import step](#import-exisiting-data) has been completed. That command is not compatible with registries using
the database and it will delete good data. 
   - Check that you have not used a service, such as cron, to automate offline garbage collection!
## Prepare the Database Instance

If you only wish to experiment, follow [this document](./database-local-setup.md)
for local environment setup with the metadata database enabled, stopping when
the guide indicates that you are ready to run migrations.

You may also choose to use a more permanent database instance if you wish to use
a registry with the database long term. You should be able to follow the guide
as normal, substituting in the values that are relevant to your database
instance. Specific recommendations and instructions for this are out of the 
scope of this guide.

## Update Registry Configuration

Before proceeding, we will now add the information the registry needs to
connect to the database to its configuration, without actually enabling the
database.
The tooling needed to support the database and import existing data read from
the same configuration file as the registry.
Therefore, in the course of running database migrations and importing, we can
ensure that the database is reachable via the values supplied in the configuration.

Locate your registry configuration, for omnibus this is
`/var/opt/gitlab/registry/config.yml`.

Add the following to your configuration as a top-level stanza, filling in the
placeholder information with the values specific to the database that was
prepared above.
Please note that `enabled: false` is **crucial**, as it will prevent the registry
from trying to use the database before we've completed all of the steps outlined
in this guide.

```yaml
database:
  enabled:  false
  host:     127.0.0.1
  port:     5432
  user:     "registry"
  password: "apassword"
  dbname:   "registry_dev"
  sslmode:  "disable"
```

A complete, but minimal, configuration file using the database can be seen at
[config/database-filesystem.yml](../config/database-filesystem.yml)


## Run Database Migrations

Locate your registry binary, for omnibus installs this is found in
`/opt/gitlab/embedded/bin/registry`.
This binary not only runs the main registry process, but also contains the
toolchain for interacting with the database.
We will use this binary throughout this guide.

For all following examples, we'll use `registry` and `config.yml` for brevity,
rather than typing full paths. Please substitute the values that are
appropriate for your environment.

Run the following command to apply all pending migrations to the registry database:

```
$ registry database migrate up config.yml
```

You will also need to run this command each time before upgrading to a newer
version of the registry.
The registry will fail to start if the database is enabled and there are pending migrations.

For a complete treatment of migrations, please see the
[database-migrations](./database-migrations.yml) guide.

## Enabling the Database

### Fresh Install

For a new registry without any existing images, we can now enable the database:

```yaml
database:
  enabled: true
```

You may now start the registry service and begin using the database!

### Existing Registry

For an existing registry, we must first import all existing metadata to the
database.
To do this, we'll continue to use the registry binary and configuration from
earlier in the guide.

#### Make a Backup

Before attempting import, make a backup of registry storage.
This backup can be used to restore the registry to a known good state if you
encounter issues after enabling the database and running the registry.
Be sure to disable the database in the registry configuration before restoring
object storage.

If you encounter failures during the import step, you **do not** need to restore
to backup, simply keep the registry database disabled in the configuration.
#### Import existing data

Choose an option to import your existing registry data into the database.

##### Option 1: One Step Import

This is the simplest way to import a registry and is recommended unless you
have strict limits on the amount of downtime on your registry instance. Once the
import command has exited successfully, the registry will be ready to use the database.

First, place the registry in read-only mode or stop the service. This ensures
that the data sent to the importer is consistent. Failure to do this will result
in data loss and may even result in inconsistent data.

You may place the registry in read-only mode via the configuration and restarting
the registry service:

```yaml
database:
  enabled: false
storage:
  filesystem:
    rootdirectory: "/<path>/<to>/<dir>"
  maintenance:
    readonly:
      enabled: true
```

Next, run the following command:

```bash
$ registry database import config.yml
```

If the command completed successfully, the registry is now fully imported. We
can now enable the database, and turn off read-only mode in the configuration
and start the registry service:

```yaml
database:
  enabled: true
storage:
  filesystem:
    rootdirectory: "/<path>/<to>/<dir>"
  maintenance:
    readonly:
      enabled: false
```

You are now fully migrated to the metadata database!

##### Option 2: Three Step Import

This procedure is a little more complicated, but allows for the minimal amount
of downtime possible.

###### Step One

First, run the following command:

```bash
$ registry database import config.yml --step-one
```

For larger instances, this command may take some time to complete, but you may
continue to use the registry as normal. Once this command completes
successfully, we can move to the next step.

Please note: You should try to schedule the following step as soon as practical
after this one to reduce the amount of downtime required. Ideally, within one
week from when this command completes. The more new data that is written to
the registry between steps one and two, the more time step two will require.

###### Step Two

Place the registry in read-only mode or stop the service. This ensures that the
data sent to the importer is consistent. Failure to do this will result in data
loss and may even result in inconsistent data.

You may place the registry in read-only mode via the configuration and restarting
the registry service:

```yaml
database:
  enabled: false
storage:
  filesystem:
    rootdirectory: "/<path>/<to>/<dir>"
  maintenance:
    readonly:
      enabled: true
```

Next, run the following command:

```bash
$ registry database import config.yml --step-two
```

If the command completed successfully, the registry is now fully imported. We
can now enable the database, turn off read-only mode in the configuration, and
start the registry service:

```yaml
database:
  enabled: true
storage:
  filesystem:
    rootdirectory: "/<path>/<to>/<dir>"
  maintenance:
    readonly:
      enabled: false
```

You are now able to use the metadata database for all operations!

###### Step Three

Even though the registry is now fully using the database for its metadata. It
does not yet have access to any potentially unused layer blobs. We'll need one
final step to enable the online garbage collector to remove these old blobs:

```bash
$ registry database import config.yml --step-three
```

Once that command exists successfully, the registry is now fully migrated to the database!
