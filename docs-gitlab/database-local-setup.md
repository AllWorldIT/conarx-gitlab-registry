# Metadata database local development setup

Follow this guide to enable the Registry with the metadata database configured.


## macOS and Linux

Requirements:

- Standalone development environment setup (see **NOTE** below) 
- A container runtime, see [Docker Desktop alternatives](./development-environment-setup.md#alternatives-to-docker-desktop)
- The `docker` CLI

**NOTE**: if you have an instance of the GDK running, you can use its postgres
server to run your local registry. You might find this slightly more performant
when running multiple tests. Check the
[running the registry against the GDK postgres instance](#running-the-registry-against-the-gdk-postgres-instance) section.

### Run postgresql

There are several options to install and run PostgreSQL
and the instructions can be found in the [Postgres website](https://www.postgresql.org/download/).
For simplicity, this guide will focus on using a container runtime approach.

To run PostgreSQL as a container, follow these instructions:

1. Open a new terminal window
2. Create a new directory to store the data

   ```shell
   cd ~
   mkdir -p postgres-registry/data
   ```

3. Run PostgreSQL 12 (the current minimum required version) as a container

   ```shell
   docker run --name postgres-registry -d \
     --restart=always -p 5432:5432 \
     -e POSTGRES_USER=registry \
     -e POSTGRES_PASSWORD=apassword \
     -e POSTGRES_DB=registry_dev \
     -v $PWD/postgres-registry/data:/var/lib/postgres/data \
     postgres:12-alpine
   ```

4. Verify postgres is running by checking the logs:

   ```shell
   docker logs postgres-registry
   
   ...
   2022-10-18 04:18:09.106 UTC [1] LOG:  database system is ready to accept connections
   ```

5. Connect to the database using a client, for example `psql` via a container:

**NOTE**: if you are using a virtual machine to run the `docker daemon` like `colima`,
you will need to obtain the container's IP or the host's IP address before connecting
to the database using this method. The command below gets the container IP and uses
it as the `PGHOST` environment variable.

```shell
docker run -it --rm \
  -e PGHOST=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres-registry) \
  -e PGPASSWORD=apassword postgres:12-alpine psql -U registry -d registry_dev
```

### Registry database and migrations

When we created the `postgres-registry` database container, we specified the
`POSTGRES_DB=registry_dev` environment variable. This will create that database
and will be used as default.
You should now be able to verify the database exists and you will be ready
to run the [database migrations](./database-migrations.md).

1. Connect to the database as shown in the last step of the previous section.
2. Verify that the `registry_dev` database exists (you should already be connected by default), for example type
`\l` in the psql session:
   
   ```shell
   registry_dev=# \l
                                     List of databases
        Name     |  Owner   | Encoding |  Collate   |   Ctype    |   Access privileges
   --------------+----------+----------+------------+------------+-----------------------
    postgres     | registry | UTF8     | en_US.utf8 | en_US.utf8 |
    registry_dev | registry | UTF8     | en_US.utf8 | en_US.utf8 |
    template0    | registry | UTF8     | en_US.utf8 | en_US.utf8 | =c/registry          +
                 |          |          |            |            | registry=CTc/registry
    template1    | registry | UTF8     | en_US.utf8 | en_US.utf8 | =c/registry          +
                 |          |          |            |            | registry=CTc/registry
   (4 rows)
   ```

3. The database exists but there are currently no tables, you can verify this by typing `\d`

   ```shell
   registry_dev=# \d
   Did not find any relations.
   ```

You are ready to run the registry migrations!

#### Migrations

1. Open a separate terminal and `cd` into your local copy of the container registry codebase
2. Update your local `config.yml` file for the registry and add the following section

```yaml
database:
  enabled:  true
  host:     127.0.0.1
  port:     5432
  user:     "registry"
  password: "apassword"
  dbname:   "registry_dev"
  sslmode:  "disable"
```

or use `cp config/database-filesystem.yml config.yml` to work with the pre-configured one.

**NOTE**: we use the host's localhost here. If your registry can't connect to
the database, try using the host's IP address instead (e.g. 192.168.1.100) 

3. Compile the `bin/registry` binary

```shell
make bin/registry
```

4. Run the following command which should apply all database migrations:

```shell
./bin/registry database migrate up /path/to/your/config.yml
```

**NOTE**: Replace the `/path/to/your/config.yml` file with the full path of where your file exists.

You should see all the migrations being applied. Something like this should be expected at the end:

```shell
...
20221222120826_post_add_layers_simplified_usage_index_batch_6
OK: applied 127 migrations in 4.501s
```

5. Verify the migrations have been applied to the correct database and that the tables are empty.
Go back to the terminal where you connected to the `registry_dev` database and type `\d`
to see all the existing tables:

   ```shell
   registry_dev=# \d
                              List of relations
    Schema |              Name              |       Type        |  Owner
   --------+--------------------------------+-------------------+----------
    public | blobs                          | partitioned table | registry
    public | gc_blob_review_queue           | table             | registry
    public | gc_blobs_configurations        | partitioned table | registry
    public | gc_blobs_configurations_id_seq | sequence          | registry
    public | gc_blobs_layers                | partitioned table | registry
    public | gc_blobs_layers_id_seq         | sequence          | registry
    public | gc_manifest_review_queue       | table             | registry
    public | gc_review_after_defaults       | table             | registry
    public | gc_tmp_blobs_manifests         | table             | registry
    public | layers                         | partitioned table | registry
    public | layers_id_seq                  | sequence          | registry
    public | manifest_references            | partitioned table | registry
    public | manifest_references_id_seq     | sequence          | registry
    public | manifests                      | partitioned table | registry
    public | manifests_id_seq               | sequence          | registry
    public | media_types                    | table             | registry
    public | media_types_id_seq             | sequence          | registry
    public | repositories                   | table             | registry
    public | repositories_id_seq            | sequence          | registry
    public | repository_blobs               | partitioned table | registry
    public | repository_blobs_id_seq        | sequence          | registry
    public | schema_migrations              | table             | registry
    public | tags                           | partitioned table | registry
    public | tags_id_seq                    | sequence          | registry
    public | top_level_namespaces           | table             | registry
    public | top_level_namespaces_id_seq    | sequence          | registry
   (26 rows)
   ```

6. Perform a test query on any table, for example:

   ```sql
   registry_dev=# select count(*) from repositories;
    count
   -------
        0
   (1 row)
   ```

7. Optional. You can verify which migration was last applied by querying the `schema_migrations` table

```sql
registry_dev=# select * from schema_migrations order by applied_at desc limit 1;
                          id                           |          applied_at
-------------------------------------------------------+------------------------------
 20220803114849_update_gc_track_deleted_layers_trigger | 2022-10-18 04:51:47.55385+00
(1 row)
```

You can verify that the ID of the last migration matches the output of step 4.

### Run the registry with the database enabled

Now that the migrations are done, you can start the registry with the same configuration
you used to run the migrations.

1. Run the registry using `./bin/registry serve /path/to/your/config.yml`. Then check the logs for something similar to this:

```shell
INFO[0000] storage backend redirection enabled           go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e service=registry version=v3.65.1-gitlab-11-g44ce3d88.m
WARN[0000] the metadata database is an experimental feature, please do not enable it in production  go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e service=registry version=v3.65.1-gitlab-11-g44ce3d88.m
INFO[0000] Starting upload purge in 55m0s                go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e service=registry version=v3.65.1-gitlab-11-g44ce3d88.m
INFO[0000] starting online GC agent                      component=registry.gc.Agent go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e jitter_s=16 service=registry version=v3.65.1-gitlab-11-g44ce3d88.m worker=registry.gc.worker.ManifestWorker
INFO[0000] starting health checker                       address=":5001" go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e path=/debug/health version=v3.65.1-gitlab-11-g44ce3d88.m
INFO[0000] listening on [::]:5000                        go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e service=registry version=v3.65.1-gitlab-11-g44ce3d88.m
INFO[0000] starting online GC agent                      component=registry.gc.Agent go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e jitter_s=11 service=registry version=v3.65.1-gitlab-11-g44ce3d88.m worker=registry.gc.worker.BlobWorker
INFO[0000] starting Prometheus listener                  address=":5001" go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e path=/metrics version=v3.65.1-gitlab-11-g44ce3d88.m
INFO[0000] starting pprof listener                       address=":5001" go_version=go1.19.5 instance_id=b440332c-e835-45cf-9510-64f63cb2807e path=/debug/pprof/ version=v3.65.1-gitlab-11-g44ce3d88.m
```

2. Open a new terminal and push a new image to the current registry:

```shell
docker pull alpine
docker tag alpine localhost:5000/root/registry-tests/alpine:latest
docker push localhost:5000/root/registry-tests/alpine:latest
```

3. Verify the repository has been created in the database. To do so go to the `psql` terminal
and query the following tables:

```psql
registry_dev=# select * from repositories where path = 'root/registry-tests/alpine';
 id | top_level_namespace_id | parent_id |          created_at          | updated_at |  name  |            path            | migration_status | deleted_at | migration_error
----+------------------------+-----------+------------------------------+------------+--------+----------------------------+------------------+------------+-----------------
  2 |                      1 |           | 2022-10-18 05:03:26.14314+00 |            | alpine | root/registry-tests/alpine | native           |            |
(1 row)

registry_dev-# select r.name as repo_name, r.path as repo_path, r.created_at, t.name as tag_name, tln.name as namespace from repositories r join tags t on t.repository_id = r.id join top_level_namespaces tln on tln.id = r.top_level_namespace_id ;
 repo_name |          repo_path          |          created_at          | tag_name  | namespace
-----------+-----------------------------+------------------------------+-----------+-----------
 alpine    | root/registry-tests/alpine  | 2022-10-18 05:03:26.14314+00 | latest    | root

(1 row)
```

4. You can also verify that the API is running properly by making a request to the
[get repository details API](./api.md#get-repository-details)

```shell
$ curl http://localhost:5000/gitlab/v1/repositories/root/registry-tests/alpine/

{"name":"alpine","path":"root/registry-tests/alpine","created_at":"2022-10-18T05:03:26.143Z"}
```

### Integration tests

You can run the integration tests locally too. However, you will need to use
environment variables to make it work. And you will need to create an extra `registry_test` database
that can be safely wiped after the tests run.

1. Connect to the database using `psql` and create a `registry_test` database:

```sql
registry_dev=# create database registry_test;
CREATE DATABASE
```

2. Create a test file `test.env` with the following environment variables:

```dotenv
export REGISTRY_DATABASE_ENABLED=true
export REGISTRY_DATABASE_PORT=5432
export REGISTRY_DATABASE_HOST=127.0.0.1
export REGISTRY_DATABASE_USER=registry
export REGISTRY_DATABASE_PASSWORD=apassword
export REGISTRY_DATABASE_SSLMODE=disable
```

3. Source the environment variables. Please note that environment variables take precedence over the corresponding attributes in the registry configuration file used to execute the `registry` binary. You can consider using a tool to automate the process of loading and unloading variables (such as [direnv](https://direnv.net/)) or configure isolated test commands on your editor/IDE of choice. 

```shell
cd /path/to/container/registry
source /path/to/test.env
```

4. Run some integration tests with the metadata database enabled:

```shell
go run gotest.tools/gotestsum@v1.8.2 --format testname -- ./registry/handlers  -timeout 25m -run "TestAPIConformance" --tags api_conformance_test,integration
```

The command above is equivalent to the [job `database:api-conformance`](https://gitlab.com/gitlab-org/container-registry/-/blob/ef704fd1c07be20061e677a3cca624f6e24d4c91/.gitlab-ci.yml#L337) 
that we run in the [CI pipelines](https://gitlab.com/gitlab-org/container-registry/-/jobs/3186379779).

There are other database-related test suites you may need to run. Look for jobs prefixed with `database:` in the project `gitlab-ci.yml` file.

## Running the registry against the GDK postgres instance

If you have an instance of the GDK running and postgres has been setup,
you can connect to it, create a new database and use it with the registry:

1. Go to the GDK directory, connect to the database

```shell
cd $GDK
gdk psql
psql (12.10)
Type "help" for help.
```

2. Create the `registry_dev` database and connect to it

```shell
gitlabhq_development=# create database registry_dev;
gitlabhq_development=# \c registry_dev;
You are now connected to database "registry_dev" as user "jaime".
registry_dev=#
```

3. Update your `config.yml` file with the GDK settings:

```yaml
database:
  enabled:  true
  host:     "/path/to/gdk/postgresql/"
  port:     5432
  dbname:   "registry_dev"
  sslmode:  "disable"
```

You should be able to [run the migrations](#migrations) and continue from there.
