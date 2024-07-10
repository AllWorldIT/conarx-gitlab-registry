# Metadata database local development setup

Follow this guide to enable the Registry with the metadata database configured.


## macOS and Linux

Requirements:

- Standalone development environment setup (see **NOTE** below) 
- A container runtime, see [Docker Desktop alternatives](development-environment-setup.md#alternatives-to-docker-desktop)
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

3. Run PostgreSQL 13 (the current minimum required version) as a container

   ```shell
   docker run --name postgres-registry -d \
     --restart=always -p 5432:5432 \
     -e POSTGRES_USER=registry \
     -e POSTGRES_PASSWORD=apassword \
     -e POSTGRES_DB=registry_dev \
     -v $PWD/postgres-registry/data:/var/lib/postgres/data \
     postgres:13-alpine
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
  -e PGPASSWORD=apassword postgres:13-alpine psql -U registry -d registry_dev
```

### Registry database and migrations

When we created the `postgres-registry` database container, we specified the
`POSTGRES_DB=registry_dev` environment variable. This will create that database
and will be used as default.
You should now be able to verify the database exists and you will be ready
to run the [database migrations](database-migrations.md).

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
[get repository details API](spec/gitlab/api.md#get-repository-details)

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

## Load Balancing

> This feature is a work in progress. See [epic 8591](https://gitlab.com/groups/gitlab-org/-/epics/8591).

The easiest path to set up load balancing locally is to rely on GDK, which includes support for PostgreSQL replication,
PgBouncer and Consul.

You have two options, using a fixed hosts list or service discovery.

### Fixed Hosts

This option does not rely on service discovery to find the PostgreSQL hosts. Instead, you need to provide a fixed list
of hosts that should be used as read replicas.

1. Set up GDK following the setup guide [here](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/doc/howto/database_load_balancing.md); 

1. Run `gdk psql` to get into a `psql` console and then create the registry database:
   ```sql
   CREATE DATABASE registry;
   ```

1. Configure the registry:
   ```yaml
   log:
     level: debug
     formatter: text
   database:
     enabled: true
     host: /<full path to gdk root>/postgresql/
     port: 5432
     user: <your local username>
     password: gitlab
     dbname: registry
     sslmode: disable
     loadbalancing:
       enabled: true
       hosts:
         - /<full path to gdk root>/postgresql-replica/
   ```
   
   You can optionally add the primary host to `loadbalancing.hosts` to make it part of the read-only pool.

3. Tail PostgreSQL logs in a separate window:
   ```shell
   gdk tail postgresql*
   ```

4. Run the registry. You should see something like this:
   ```text
   INFO[0000] Connect                                       database=registry duration_ms=3 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84101 port=5432 version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Connect                                       database=registry duration_ms=5 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql-replica/ instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84102 port=5432 version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Connect                                       database=registry duration_ms=2 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84104 port=5432 version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Query                                         args="[]" commandTag="CREATE TABLE" database=registry duration_ms=0 go_version=go1.21.5 instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84104 sql="create table if not exists \"schema_migrations\" (\"id\" text not null primary key, \"applied_at\" timestamp with time zone) ;" version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Connect                                       database=registry duration_ms=4 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84105 port=5432 version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Query                                         args="[map[16:1 17:1 20:1 21:1 23:1 26:1 28:1 29:1 700:1 701:1 1082:1 1114:1 1184:1]]" commandTag="SELECT 162" database=registry duration_ms=5 go_version=go1.21.5 instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84105 sql="SELECT * FROM \"schema_migrations\" ORDER BY \"id\" ASC" version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Connect                                       database=registry duration_ms=1 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84106 port=5432 version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Query                                         args="[]" commandTag="CREATE TABLE" database=registry duration_ms=0 go_version=go1.21.5 instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84106 sql="create table if not exists \"schema_migrations\" (\"id\" text not null primary key, \"applied_at\" timestamp with time zone) ;" version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Connect                                       database=registry duration_ms=1 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84107 port=5432 version=v4.5.0-gitlab-20-gbac9b1549.m
   INFO[0000] Query                                         args="[map[16:1 17:1 20:1 21:1 23:1 26:1 28:1 29:1 700:1 701:1 1082:1 1114:1 1184:1]]" commandTag="SELECT 162" database=registry duration_ms=0 go_version=go1.21.5 instance_id=0d128114-c0ed-44be-8bec-e3a40f2b1fb1 pid=84107 sql="SELECT * FROM \"schema_migrations\" ORDER BY \"id\" ASC" version=v4.5.0-gitlab-20-gbac9b1549.m
   ```

   Note that database log entries contain a `host` key/value pair that tells us the host that each operation is
targeting. We can see that the registry connects to both primary and replica hosts.

5. In the PostgreSQL logs you should see something like this:
   ```sql
   2024-06-27_13:56:57.69718 postgresql            : 2024-06-27 14:56:57.697 WEST [16895] LOG:  statement: -- ping
   2024-06-27_13:56:57.71803 postgresql-replica    : 2024-06-27 14:56:57.716 WEST [16896] LOG:  statement: -- ping
   2024-06-27_13:56:57.75166 postgresql            : 2024-06-27 14:56:57.751 WEST [16920] LOG:  statement: create table if not exists "schema_migrations" ("id" text not null primary key, "applied_at" timestamp with time zone) ;
   2024-06-27_13:56:57.75564 postgresql            : 2024-06-27 14:56:57.755 WEST [16922] LOG:  statement: SELECT * FROM "schema_migrations" ORDER BY "id" ASC
   2024-06-27_13:56:57.77475 postgresql            : 2024-06-27 14:56:57.773 WEST [16927] LOG:  statement: create table if not exists "schema_migrations" ("id" text not null primary key, "applied_at" timestamp with time zone) ;
   2024-06-27_13:56:57.78049 postgresql            : 2024-06-27 14:56:57.779 WEST [16931] LOG:  statement: SELECT * FROM "schema_migrations" ORDER BY "id" ASC
   ```

### Service Discovery

This option allows automatic discovery of PostgreSQL hosts using DNS lookup queries and SRV records. See the related
[specification](./spec/gitlab/database-load-balancing.md?ref_type=heads#service-discovery) for more details.

1. Set up GDK following the setup guide [here](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/doc/howto/database_load_balancing_with_service_discovery.md);

1. Lookup the addresses behind the default replicas SRV record in Consul to confirm that service discovery is working:
   ```shell
   dig +short @127.0.0.1 -p 8600 replica.pgbouncer.service.consul -t SRV
   ```
   You should see something similar to the following:
   ```text
   1 1 6434 7f000001.addr.dc1.consul.
   1 1 6435 7f000001.addr.dc1.consul.
   1 1 6432 7f000001.addr.dc1.consul.
   1 1 6433 7f000001.addr.dc1.consul.
   ```

1. Run `gdk psql` to get into a `psql` console and then create the registry database:
   ```sql
   CREATE DATABASE registry;
   ```

1. Update the GDK PgBouncer configuration template at `support/templates/pgbouncer/pgbouncer-replica.ini.erb` to add an
entry for the registry database under the `[databases]` section:
   ```text
   registry = host=<%= host %> dbname=registry user=<%= config.__whoami %>
   ```

1. Reconfigure and restart your GDK:
   ```shell
   gdk reconfigure
   gdk restart
   ```

1. Open a `psql` session in one of the replica hosts to confirm that the setup is ready:
   ```shell
   PGPASSWORD=gitlab psql -h 127.0.0.1 -p 6434 -U <your local username> -d registry
   ```

1. Configure the registry:
   ```yaml
   log:
     level: debug
     formatter: text
   database:
     enabled: true
     host: /Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/
     port: 5432
     user: <your local username>
     password: gitlab
     dbname: registry
     sslmode: disable
     loadbalancing:
       enabled: true
       nameserver: localhost
       port: 8600
       record: replica.pgbouncer.service.consul
   ```

3. Tail PostgreSQL logs in a separate window:
   ```shell
   gdk tail postgresql*
   ```

4. Run the registry. You should see something like this:
   ```text
   INFO[0000] enabling database load balancing with service discovery  go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 nameserver=localhost port=8600 record=replica.pgbouncer.service.consul version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=9 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16048 port=5432 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=0 go_version=go1.21.5 host=127.0.0.1 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=776127547 port=6435 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=2 go_version=go1.21.5 host=127.0.0.1 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=837580158 port=6433 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=1 go_version=go1.21.5 host=127.0.0.1 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=806750629 port=6434 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=0 go_version=go1.21.5 host=127.0.0.1 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=890723796 port=6432 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=2 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16049 port=5432 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Query                                         args="[]" commandTag="CREATE TABLE" database=registry duration_ms=0 go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16049 sql="create table if not exists \"schema_migrations\" (\"id\" text not null primary key, \"applied_at\" timestamp with time zone) ;" version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=1 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16050 port=5432 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Query                                         args="[map[16:1 17:1 20:1 21:1 23:1 26:1 28:1 29:1 700:1 701:1 1082:1 1114:1 1184:1]]" commandTag="SELECT 162" database=registry duration_ms=0 go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16050 sql="SELECT * FROM \"schema_migrations\" ORDER BY \"id\" ASC" version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=2 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16051 port=5432 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Query                                         args="[]" commandTag="CREATE TABLE" database=registry duration_ms=0 go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16051 sql="create table if not exists \"schema_migrations\" (\"id\" text not null primary key, \"applied_at\" timestamp with time zone) ;" version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Connect                                       database=registry duration_ms=2 go_version=go1.21.5 host=/Users/jpereira/Developer/gitlab.com/gitlab-org/gdk/postgresql/ instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16052 port=5432 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] Query                                         args="[map[16:1 17:1 20:1 21:1 23:1 26:1 28:1 29:1 700:1 701:1 1082:1 1114:1 1184:1]]" commandTag="SELECT 162" database=registry duration_ms=0 go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 pid=16052 sql="SELECT * FROM \"schema_migrations\" ORDER BY \"id\" ASC" version=v4.6.0-gitlab-9-gd3cdf3193.m
   DEBU[0000] filesystem.Stat("/docker/registry/lockfiles/database-in-use")  go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 trace_duration="38.583µs" trace_file=/Users/jpereira/Developer/gitlab.com/gitlab-org/container-registry/registry/storage/driver/base/base.go trace_func="github.com/docker/distribution/registry/storage/driver/base.(*Base).Stat" trace_id=9c4c46c5-0b5e-4aed-b67f-c683835b8002 trace_line=155 version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] starting health checker                       address=":5001" go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 path=/debug/health version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] starting Prometheus listener                  address=":5001" go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 path=/metrics version=v4.6.0-gitlab-9-gd3cdf3193.m
   INFO[0000] listening on 172.16.123.1:5000                go_version=go1.21.5 instance_id=044536cc-e998-469f-880b-52e62fd6d535 version=v4.6.0-gitlab-9-gd3cdf3193.m
   ```

   Note that database log entries contain a `host` key/value pair that tells us the host that each operation is
   targeting. We can see that the registry connects to all hosts.

5. In the PostgreSQL logs you should see something like this:
   ```sql
   2024-07-02_15:15:59.87681 postgresql            : 2024-07-02 16:15:59.876 WEST [16048] LOG:  statement: -- ping
   2024-07-02_15:15:59.89020 postgresql-replica-2  : 2024-07-02 16:15:59.890 WEST [2438] LOG:  statement: select 1
   2024-07-02_15:15:59.89081 postgresql-replica-2  : 2024-07-02 16:15:59.890 WEST [2438] LOG:  statement: -- ping
   2024-07-02_15:15:59.89635 postgresql-replica    : 2024-07-02 16:15:59.894 WEST [2436] LOG:  statement: select 1
   2024-07-02_15:15:59.90377 postgresql-replica    : 2024-07-02 16:15:59.901 WEST [2436] LOG:  statement: -- ping
   2024-07-02_15:15:59.90807 postgresql-replica-2  : 2024-07-02 16:15:59.907 WEST [93615] LOG:  statement: select 1
   2024-07-02_15:15:59.90879 postgresql-replica-2  : 2024-07-02 16:15:59.908 WEST [93615] LOG:  statement: -- ping
   2024-07-02_15:15:59.91026 postgresql-replica    : 2024-07-02 16:15:59.910 WEST [2437] LOG:  statement: select 1
   2024-07-02_15:15:59.91049 postgresql-replica    : 2024-07-02 16:15:59.910 WEST [2437] LOG:  statement: -- ping
   2024-07-02_15:15:59.91307 postgresql            : 2024-07-02 16:15:59.912 WEST [16049] LOG:  statement: create table if not exists "schema_migrations" ("id" text not null primary key, "applied_at" timestamp with time zone) ;
   2024-07-02_15:15:59.91542 postgresql            : 2024-07-02 16:15:59.915 WEST [16050] LOG:  statement: SELECT * FROM "schema_migrations" ORDER BY "id" ASC
   2024-07-02_15:15:59.91822 postgresql            : 2024-07-02 16:15:59.918 WEST [16051] LOG:  statement: create table if not exists "schema_migrations" ("id" text not null primary key, "applied_at" timestamp with time zone) ;
   2024-07-02_15:15:59.92081 postgresql            : 2024-07-02 16:15:59.920 WEST [16052] LOG:  statement: SELECT * FROM "schema_migrations" ORDER BY "id" ASC
   ```
