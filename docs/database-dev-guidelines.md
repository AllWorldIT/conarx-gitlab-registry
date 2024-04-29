# Database Development Guidelines

The registry database development adheres to the same [rules and
principles](https://docs.gitlab.com/ee/development/database/) defined for the
GitLab database. We only detail what is specific to the registry in this document.

## Generic Interfaces

Although we're using the standard library
[`database/sql`](https://golang.org/pkg/database/sql/), we should strive for
maintaining our internal service interfaces generic and decoupled from the
underlying database client/driver. For this reason, we should wrap standard
types (like `sql.DB`), and their methods (only the ones we rely on) if they
return anything but an `error`. This will guarantee a low effort in case we want
to swap the client/driver at some point.

## ER Model

The database ER model should be updated when doing a schema change. We're using
[pgModeler](https://github.com/pgmodeler/pgmodeler) for this (`.dbm` extension).
The source files can be found at `docs-gitlab/db`.

## SQL Formatting

Long, complex or multi-line SQL statements must be formatted with
[pgFormatter](https://github.com/darold/pgFormatter), using the default settings.
There are plugins for several editors/IDEs and there is also an online version at
[sqlformat.darold.net](http://sqlformat.darold.net/).

## Schema DDL Script

Whenever a schema migration is added, you must regenerate the DDL script at `registry/datastore/migrations/structure.sql`. This guarantees that we always have a coherent DDL script. This is useful to check the current state of the full database schema (and easily validate changes) as well as to bootstrap a database without having to compile the registry binary.

Make sure to run `make db-structure-dump` to update the DDL script whenever you change the database schema. This will dump the current schema from your local registry database with `pg_dump` and format it with [pgFormatter](https://github.com/darold/pgFormatter) for consistency.

## Creating New Tables

New tables introduced to the registry database, which may potentially undergo data migration in the future, must include an auto-incrementing integer `id` column. This requirement ensures compatibility with the registry's [Batched Background Migration](./spec/gitlab/database-background-migrations.md) process, which depend on the sequential ordering of the `id` column to determine migration batches.

## Testing

### Golden Files

Some database operations generate a considerable amount of data on the database.
In some cases it's not practical or maintainable to define structs for all
expected rows and then compare them one by one.

When the only thing we need to assert is that the database data ended up in a
given state, we can use golden files, which should contain a pre-validated
snapshot/dump (in JSON format for easy readability) of a given table.

Therefore, instead of comparing item by item, we can save the expected database
table content to a file named `<table>.golden` within
`<package>/testdata/golden/<test function name>`, and then compare these against
an actual JSON dump in the test functions.

#### Test Flags

To facilitate the development process, two flags for the `go test` command were
added:

- `update`: Updates existing golden files with a new expected value. For
  example, if we change a column name, it's impractical to update the golden
  files manually. With this command the golden files are automatically updated
  with a fresh dump that reflects the new column name.

- `create`: Create missing golden files, followed by `update`. In case we add
  new tables, new golden files need to be created for them. Instead of creating
  them manually, we can use this flag and they will be automatically created and
  updated with the current table content.

These flags are defined in `registry.datastore.datastore_test`. The caller
(running test) is responsible for passing the value of these flags to the
`registry.datastore.testutil.CompareWithGoldenFile(tb testing.TB, path string,
actual []byte, create, update bool)` helper function. Whenever this function is
called, it'll attempt to find the golden file at `path`, read it and then
compare its contents against `actual`. If these don't match, the test will fail
and a diff will be presented in the test output.

#### Example

Please have a look at the integration tests for `registry.datastore.Importer`.
We'll use this test as example.

##### Create

If we wanted to test a new table `foo` in `TestImporter_Import`, we would do:

1. Run the `go test` command with the `create` flag:

```
go test -v -tags=integration github.com/docker/distribution/registry/datastore -create
```

After doing so, a new
`registry/datastore/testdata/golden/TestImporter_Import/foo.golden` golden file
would be created with the contents of the `foo` table during the test. We could
then modify this file to match what should be the correct/expected state of the
`foo` table for our test.

Alternatively, we could also manually create the golden file in the path
mentioned above and skip the `create` flag.

##### Update

If for example we changed the name of column `a` in the `foo` table to `b`,
instead of manually updating the corresponding golden file we could simply rerun
the test with the `update` flag:

```
go test -v -tags=integration github.com/docker/distribution/registry/datastore -update
```

Doing so, the golden file would be updated with the current state of the `foo`
table at the time of running the test (which would already have the column `a`
renamed to `b`).

Alternatively, we could update the golden file manually as well. This update
flag is useful when whe changed something on our database and we're sure that
the new output is the correct one, so we can simply use it to refresh all golden
files to match the new expectation.
