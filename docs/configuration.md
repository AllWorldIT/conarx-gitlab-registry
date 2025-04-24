---
title: "Configuring a registry"
description: "Explains how to configure a registry"
keywords: registry, on-prem, images, tags, repository, distribution, configuration
---

The Registry configuration is based on a YAML file, detailed below. While it
comes with sane default values out of the box, you should review it exhaustively
before moving your systems to production.

## Override specific configuration options

In a typical setup where you run your Registry from the official image, you can
specify a configuration variable from the environment by passing `-e` arguments
to your `docker run` stanza or from within a Dockerfile using the `ENV`
instruction.

To override a configuration option, create an environment variable named
`REGISTRY_variable` where `variable` is the name of the configuration option
and the `_` (underscore) represents indention levels. For example, you can
configure the `rootdirectory` of the `filesystem` storage backend:

```none
storage:
  filesystem:
    rootdirectory: /var/lib/registry
```

To override this value, set an environment variable like this:

```none
REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY=/somewhere
```

This variable overrides the `/var/lib/registry` value to the `/somewhere`
directory.

> **Note**: Create a base configuration file with environment variables that can
> be configured to tweak individual values. Overriding configuration sections
> with environment variables is not recommended.

## Overriding the entire configuration file

If the default configuration is not a sound basis for your usage, or if you are
having issues overriding keys from the environment, you can specify an alternate
YAML configuration file by mounting it as a volume in the container.

Typically, create a new configuration file from scratch,named `config.yml`, then
specify it in the `docker run` command:

```shell
$ docker run -d -p 5000:5000 --restart=always --name registry \
             -v `pwd`/config.yml:/etc/docker/registry/config.yml \
             registry:2
```

Use this
[example YAML file](https://github.com/docker/distribution/blob/master/cmd/registry/config-example.yml)
as a starting point.

## List of configuration options

These are all configuration options for the registry. Some options in the list
are mutually exclusive. Read the detailed reference information about each
option before finalizing your configuration.

```none
version: 0.1
log:
  accesslog:
    disabled: true
    formatter: json
  level: debug
  formatter: text
  output: stderr
  fields:
    service: registry
    environment: staging
loglevel: debug # deprecated: use "log"
storage:
  filesystem:
    rootdirectory: /var/lib/registry
    maxthreads: 100
  azure:
    accountname: accountname
    accountkey: base64encodedaccountkey
    realm: realm
    serviceurl: serviceurl
    container: containername
    rootdirectory: /azure/virtual/container
    legacyrootprefix: false
    trimlegacyrootprefix: false
  azure_v2:
    credentials_type: shared_key
    accountname: accountname
    accountkey: base64encodedaccountkey
    realm: realm
    serviceurl: serviceurl
    container: containername
    rootdirectory: /azure/virtual/container
    legacyrootprefix: false
    trimlegacyrootprefix: false
    api_pool_initial_interval: 500ms
    api_pool_max_interval: 15s
    api_pool_max_elapsed_time: 5m
    debug_log: false
  gcs:
    bucket: bucketname
    keyfile: /path/to/keyfile
    credentials:
      type: service_account
      project_id: project_id_string
      private_key_id: private_key_id_string
      private_key: private_key_string
      client_email: client@example.com
      client_id: client_id_string
      auth_uri: http://example.com/auth_uri
      token_uri: http://example.com/token_uri
      auth_provider_x509_cert_url: http://example.com/provider_cert_url
      client_x509_cert_url: http://example.com/client_cert_url
    rootdirectory: /gcs/object/name/prefix
    chunksize: 5242880
  s3:
    accesskey: awsaccesskey
    secretkey: awssecretkey
    region: us-west-1
    regionendpoint: http://myobjects.local
    bucket: bucketname
    encrypt: true
    keyid: mykeyid
    secure: true
    v4auth: true
    chunksize: 5242880
    multipartcopychunksize: 33554432
    multipartcopymaxconcurrency: 100
    multipartcopythresholdsize: 33554432
    rootdirectory: /s3/object/name/prefix
    loglevel: logdebug
    maxretries: 10
    objectownership: false
  s3_v2:
    accesskey: awsaccesskey
    secretkey: awssecretkey
    region: us-west-1
    regionendpoint: http://myobjects.local
    bucket: bucketname
    encrypt: true
    keyid: mykeyid
    secure: true
    chunksize: 5242880
    multipartcopychunksize: 33554432
    multipartcopymaxconcurrency: 100
    multipartcopythresholdsize: 33554432
    rootdirectory: /s3/object/name/prefix
    loglevel: logdebug
    maxretries: 10
    objectownership: false
  inmemory:  # This driver takes no parameters
  delete:
    enabled: false
  redirect:
    disable: false
    expirydelay: 20m
  cache:
    blobdescriptor: redis
  maintenance:
    uploadpurging:
      enabled: true
      age: 168h
      interval: 24h
      dryrun: false
    readonly:
      enabled: false
database:
  enabled: true
  host: localhost
  port: 5432
  user: postgres
  password:
  dbname: registry
  sslmode: verify-full
  sslcert: /path/to/client.crt
  sslkey: /path/to/client.key
  sslrootcert: /path/to/root.crt
  connecttimeout: 5s
  draintimeout: 2m
  preparedstatements: false
  primary: primary.record.fqdn
  pool:
    maxidle: 25
    maxopen: 25
    maxlifetime: 5m
auth:
  silly:
    realm: silly-realm
    service: silly-service
  token:
    autoredirect: true
    realm: token-realm
    service: token-service
    issuer: registry-token-issuer
    rootcertbundle: /root/certs/bundle
middleware:
  registry:
    - name: ARegistryMiddleware
      options:
        foo: bar
  repository:
    - name: ARepositoryMiddleware
      options:
        foo: bar
  storage:
    - name: cloudfront
      options:
        baseurl: https://my.cloudfronted.domain.com/
        privatekey: /path/to/pem
        keypairid: cloudfrontkeypairid
        duration: 3000s
        ipfilteredby: awsregion
        awsregion: us-east-1, use-east-2
        updatefrequency: 12h
        iprangesurl: https://ip-ranges.amazonaws.com/ip-ranges.json
  storage:
    - name: redirect
      options:
        baseurl: https://example.com/
reporting:
  sentry:
    enabled: true
    dsn: https://examplePublicKey@o0.ingest.sentry.io/0
    environment: production
profiling:
  stackdriver:
    service: registry
    serviceversion: 1.0.0
    projectid: tBXV4hFr4QJM6oGkqzhC
    keyfile: /path/to/credentials.json
http:
  addr: localhost:5000
  prefix: /my/nested/registry/
  host: https://myregistryaddress.org:5000
  secret: asecretforlocaldevelopment
  relativeurls: false
  draintimeout: 60s
  tls:
    certificate: /path/to/x509/public
    key: /path/to/x509/private
    clientcas:
      - /path/to/ca.pem
      - /path/to/another/ca.pem
    letsencrypt:
      cachefile: /path/to/cache-file
      email: emailused@letsencrypt.com
      hosts: [myregistryaddress.org]
  debug:
    addr: localhost:5001
    tls:
      enabled: true
      certificate: /path/to/x509/public
      key: /path/to/x509/private
      clientcas:
        - /path/to/ca.pem
        - /path/to/another/ca.pem
      minimumtls: tls1.2
      ciphersuites:
        - TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
        - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
    prometheus:
      enabled: true
      path: /metrics
    pprof:
      enabled: true
  headers:
    X-Content-Type-Options: [nosniff]
  http2:
    disabled: false
notifications:
  events:
    includereferences: true
  endpoints:
    - name: alistener
      disabled: false
      url: https://my.listener.com/event
      headers: <http.Header>
      timeout: 1s
      threshold: 10
      maxretries: 5
      backoff: 1s
      ignoredmediatypes:
        - application/octet-stream
      ignore:
        mediatypes:
           - application/octet-stream
        actions:
           - pull
redis:
  addr: localhost:16379,localhost:26379
  mainname: mainserver
  password: asecret
  db: 0
  dialtimeout: 10ms
  readtimeout: 10ms
  writetimeout: 10ms
  sentinelusername: my-sentinel-username
  sentinelpassword: some-sentinel-password
  tls:
    enabled: true
    insecure: true
  pool:
    size: 10
    maxlifetime: 1h
    idletimeout: 300s
  cache:
    enabled: true
    addr: localhost:16379,localhost:26379
    mainname: mainserver
    username: default
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    sentinelusername: my-sentinel-username
    sentinelpassword: some-sentinel-password
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
  ratelimiter:
    enabled: true
    addr: localhost:16379,localhost:26379
    username: registry
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    sentinelusername: my-sentinel-username
    sentinelpassword: some-sentinel-password
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
  load_balancing:
    enabled: true
    addr: localhost:16379,localhost:26379
    username: registry
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
health:
  storagedriver:
    enabled: true
    interval: 10s
    threshold: 3
  file:
    - file: /path/to/checked/file
      interval: 10s
  http:
    - uri: http://server.to.check/must/return/200
      headers:
        Authorization: [Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==]
      statuscode: 200
      timeout: 3s
      interval: 10s
      threshold: 3
  tcp:
    - addr: redis-server.domain.com:6379
      timeout: 3s
      interval: 10s
      threshold: 3
validation:
  manifests:
    referencelimit: 150
    payloadsizelimit: 64000
    urls:
      allow:
        - ^https?://([^/]+\.)*example\.com/
      deny:
        - ^https?://www\.example\.com/
```

In some instances a configuration option is **optional** but it contains child
options marked as **required**. In these cases, you can omit the parent with
all its children. However, if the parent is included, you must also include all
the children marked **required**.

<!--- start_remove The following content will be removed on remove_date: '2025/08/15' -->

WARNING:
Support for authenticating requests using Amazon S3 Signature Version 2 in the container registry is deprecated in GitLab 17.8 and is planned for removal in 18.0. Use Signature Version 4 instead. This is a breaking change. For more information, see [issue 1449](https://gitlab.com/gitlab-org/container-registry/-/issues/1449).

<!--- end_remove -->

## `version`

```none
version: 0.1
```

The `version` option is **required**. It specifies the configuration's version.
It is expected to remain a top-level field, to allow for a consistent version
check before parsing the remainder of the configuration file.

## `log`

The `log` subsection configures the behavior of the logging system. The logging
system outputs everything to stdout by default. You can adjust the granularity,
output and format with this configuration section.

```none
log:
  accesslog:
    disabled: true
    formatter: json
  level: debug
  formatter: text
  output: stderr
  fields:
    service: registry
    environment: staging
```

| Parameter   | Required | Description |
|-------------|----------|-------------|
| `level`     | no       | Sets the sensitivity of logging output. Permitted values are `error`, `warn`, `info`, `debug` and `trace`. The default is `info`. |
| `formatter` | no       | This selects the format of logging output. The format primarily affects how keyed attributes for a log line are encoded. Options are `text` and `json`. The default is `json`. |
| `output`    | no       | This sets the output destination. Options are `stdout` and `stderr`. The default is `stdout`. |
| `fields`    | no       | A map of field names to values. These are added to every log line for the context. This is useful for identifying log messages source after being mixed in other systems. |

### `accesslog`

```none
accesslog:
  disabled: true
  formatter: json
```

Within `log`, `accesslog` configures the behavior of the access logging
system. By default, the access logging system outputs to stdout in JSON format.

| Parameter   | Required | Description |
|-------------|----------|-------------|
| `disabled`  | no       | Set to `true` to disable access logging. The default is `false`. |
| `formatter` | no       | This selects the format of logging output. Options are `text` and `json`. The default is `json`. |

## `loglevel`

> **DEPRECATED:** Please use [log](#log) instead.

```none
loglevel: debug
```

Permitted values are `error`, `warn`, `info` and `debug`. The default is
`info`.

## `storage`

```none
storage:
  filesystem:
    rootdirectory: /var/lib/registry
  azure:
    accountname: accountname
    accountkey: base64encodedaccountkey
    realm: realm
    serviceurl: serviceurl
    container: containername
    rootdirectory: /azure/virtual/container
    legacyrootprefix: false
    trimlegacyrootprefix: false
  azure_v2:
    credentials_type: shared_key
    accountname: accountname
    accountkey: base64encodedaccountkey
    realm: realm
    serviceurl: serviceurl
    container: containername
    rootdirectory: /azure/virtual/container
    legacyrootprefix: false
    trimlegacyrootprefix: false
    api_pool_initial_interval: 500ms
    api_pool_max_interval: 15s
    api_pool_max_elapsed_time: 5m
    debug_log: false
  gcs:
    bucket: bucketname
    keyfile: /path/to/keyfile
    credentials:
      type: service_account
      project_id: project_id_string
      private_key_id: private_key_id_string
      private_key: private_key_string
      client_email: client@example.com
      client_id: client_id_string
      auth_uri: http://example.com/auth_uri
      token_uri: http://example.com/token_uri
      auth_provider_x509_cert_url: http://example.com/provider_cert_url
      client_x509_cert_url: http://example.com/client_cert_url
    rootdirectory: /gcs/object/name/prefix
  s3:
    accesskey: awsaccesskey
    secretkey: awssecretkey
    region: us-west-1
    regionendpoint: http://myobjects.local
    bucket: bucketname
    encrypt: true
    keyid: mykeyid
    secure: true
    v4auth: true
    chunksize: 5242880
    multipartcopychunksize: 33554432
    multipartcopymaxconcurrency: 100
    multipartcopythresholdsize: 33554432
    rootdirectory: /s3/object/name/prefix
    loglevel: logdebug
    maxretries: 10
    objectownership: false
  s3_v2:
    accesskey: awsaccesskey
    secretkey: awssecretkey
    region: us-west-1
    regionendpoint: http://myobjects.local
    bucket: bucketname
    encrypt: true
    keyid: mykeyid
    secure: true
    chunksize: 5242880
    multipartcopychunksize: 33554432
    multipartcopymaxconcurrency: 100
    multipartcopythresholdsize: 33554432
    rootdirectory: /s3/object/name/prefix
    loglevel: logdebug
    maxretries: 10
    objectownership: false
  inmemory:
  delete:
    enabled: false
  cache:
    blobdescriptor: inmemory
  maintenance:
    uploadpurging:
      enabled: true
      age: 168h
      interval: 24h
      dryrun: false
    readonly:
      enabled: false
  redirect:
    disable: false
    expirydelay: 20m
```

The `storage` option is **required** and defines which storage backend is in use.
You must configure exactly one backend.
If you configure more, the registry returns an error.
You can choose any of these backend storage drivers:

| Storage driver         | Description                                                                                                                                                                                                                                                                              |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `filesystem`           | Uses the local disk to store registry files. It is ideal for development and may be appropriate for some small-scale production applications. See the [driver's reference documentation](https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers/filesystem.md). |
| `azure`                | Uses Microsoft Azure Blob Storage ([deprecated version](https://docs.gitlab.com/update/deprecations/#azure-storage-driver-for-the-container-registry)). See the [driver's reference documentation](./storage-drivers/azure_v1.md).                                                                                                                 |
| `azure_v2`             | Improved Microsoft Azure Blob Storage. See the [driver's reference documentation](./storage-drivers/azure_v2.md).                                                                                                                 |
| `gcs`                  | Uses Google Cloud Storage. See the [driver's reference documentation](https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers/gcs.md).                                                                                                                           |
| `s3`                   | Uses Amazon Simple Storage Service (S3) ([deprecated version](https://docs.gitlab.com/update/deprecations/#s3-storage-driver-aws-sdk-v1-for-the-container-registry)). See the [driver's reference documentation](./storage-drivers/s3_v1.md).                                                                  |
| `s3_v2`                | Improved Amazon Simple Storage Service (S3) driver. See the [driver's reference documentation](./storage-drivers/s3_v2.md).                                 |

For testing only, you can use the [`inmemory` storage driver](https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers/inmemory.md).
If you would like to run a registry from volatile memory, use the [`filesystem` driver](https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers/filesystem.md) on a ramdisk.

### `maintenance`

Currently, upload purging and read-only mode are the only `maintenance`
functions available.

### `uploadpurging`

Upload purging is a background process that periodically removes orphaned files
from the upload directories of the registry. Upload purging is enabled by
default. Upload purging will be disabled if `readonly` is enabled.
To configure upload directory purging, the following parameters must
be set.

| Parameter  | Required | Description                                                                                        |
|------------|----------|----------------------------------------------------------------------------------------------------|
| `enabled`  | yes      | Set to `true` to enable upload purging. Defaults to `true`.                                        |
| `age`      | yes      | Upload directories which are older than this age will be deleted. Defaults to `168h` (1 week).      |
| `interval` | yes      | The interval between upload directory purging. Defaults to `24h`.                                  |
| `dryrun`   | yes      | Set `dryrun` to `true` to obtain a summary of what directories will be deleted. Defaults to `false`.|

> **Note**: `age` and `interval` are strings containing a number with optional
fraction and a unit suffix. Some examples: `45m`, `2h10m`, `168h`.

### `readonly`

If the `readonly` section under `maintenance` has `enabled` set to `true`,
clients will not be allowed to write to the registry, including`uploadpurging`.
This mode is useful to temporarily prevent writes to the backend storage so a
garbage collection pass can be run.
Before running garbage collection, the registry should be
restarted with readonly's `enabled` set to true. After the garbage collection
pass finishes, the registry may be restarted again, this time with `readonly`
removed from the configuration (or set to false).

### `delete`

Use the `delete` structure to enable the deletion of image blobs and manifests
by digest. It defaults to false, but it can be enabled by writing the following
on the configuration file:

```none
delete:
  enabled: true
```

### `cache`

Use the `cache` structure to enable caching of data accessed in the storage
backend. Currently, the only available cache provides fast access to layer
metadata, which uses the `blobdescriptor` field if configured.

You can set `blobdescriptor` field to `redis` or `inmemory`. If set to `redis`,a
Redis pool caches layer metadata. If set to `inmemory`, an in-memory map caches
layer metadata.

> **NOTE**: Formerly, `blobdescriptor` was known as `layerinfo`. While these
> are equivalent, `layerinfo` has been deprecated.

### `redirect`

The `redirect` subsection provides configuration for managing redirects from
content backends. This is supported and enabled by default for Azure, GCS, and
S3 backends. In certain deployment scenarios, you may decide to route
all data through the Registry, rather than redirecting to the backend. This may
be more efficient when using a backend that is not co-located or when a
registry instance is aggressively caching.

```none
redirect:
  disable: false
  expirydelay: 20m
```

| Parameter    | Required | Description                                                                                     |
|--------------|----------|-------------------------------------------------------------------------------------------------|
| `disable`    | no       | Set to `true` to disable redirects. Defaults to `false`.                                        |
| `expirydelay`| no       | An integer and unit for the expiration delay of pre-signed URLs. Defaults to `20m` (20 minutes). Please note that storage providers have different min and max allowed values for this parameter. Check your provider's documentation before setting a custom value. |

## `database`

The `database` subsection configures the PostgreSQL metadata database.

```none
database:
  enabled: true
  host: localhost
  port: 5432
  user: postgres
  password:
  dbname: registry
  sslmode: verify-full
  sslcert: /path/to/client.crt
  sslkey: /path/to/client.key
  sslrootcert: /path/to/root.crt
  connecttimeout: 5s
  draintimeout: 2m
  preparedstatements: false
  primary: primary.record.fqdn
  pool:
    maxidle: 25
    maxopen: 25
    maxlifetime: 5m
  backgroundmigrations:
    enabled: true
    jobinterval: 1m
  loadbalancing:
    enabled: true
    nameserver: localhost
    port: 8600
    record: db-replica-registry.service.consul
    replicacheckinterval: 1m  
```

| Parameter  | Required | Description                                                                                                                                                                                                                                          |
|------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enabled`  | no       | When set to `false` the metadata database is bypassed. Defaults to `false`.                                                                                                                                                                           |
| `host`     | yes      | The database server hostname.                                                                                                                                                                                                                        |
| `port`     | yes      | The database server port.                                                                                                                                                                                                                            |
| `user`     | yes      | The database username.                                                                                                                                                                                                                               |
| `password` | yes      | The database password.                                                                                                                                                                                                                               |
| `dbname`   | yes      | The database name.                                                                                                                                                                                                                                   |
| `sslmode`  | yes      | The SSL mode. Can be one of `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. See the [PostgreSQL documentation](http://www.postgresql.cn/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS) for additional information. |
| `sslcert`  | no       | The PEM encoded certificate file path. |
| `sslkey`   | no       | The PEM encoded key file path. |
| `sslrootcert`  | no       | The PEM encoded root certificate file path. |
| `connecttimeout`  | no       | Maximum time to wait for a connection. Zero or not specified means waiting indefinitely. |
| `draintimeout`    | no       | Maximum time to wait to drain all connections on shutdown. Zero or not specified means waiting indefinitely. |
| `preparedstatements`  | no       | When set to `true`, prepared statements may be used. Defaults to `false` for compatibility with PgBouncer. |
| `primary`  | no       | The database server hostname to target for schema migrations. Useful for high availability PostgreSQL deployments, where connecting to the primary server directly is required to avoid timeouts while executing long-running schema migrations. Should be set to the primary PostgreSQL server hostname. Defaults to `database.host`. |

### `pool`

```none
pool:
  maxidle: 25
  maxopen: 25
  maxlifetime: 5m
  maxidletime: 10m
```

Use these settings to configure the behavior of the database connection pool.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `maxidle` | no       | The maximum number of connections in the idle connection pool. If `maxopen` is less than `maxidle`, then `maxidle` is reduced to match the `maxopen` limit. Defaults to 0 (no idle connections).   |
| `maxopen`| no      | The maximum number of open connections to the database. If `maxopen` is less than `maxidle`, then `maxidle` is reduced to match the `maxopen` limit. Defaults to 0 (unlimited). |
| `maxlifetime`| no    | The maximum amount of time a connection may be reused. Expired connections may be closed lazily before reuse. Defaults to 0 (unlimited). |
| `maxidletime` | no | The maximum amount of time a connection may be idle. Expired connections may be closed lazily before reuse. Defaults to 0 (unlimited). |

### `backgroundmigrations`

> **_NOTE:_** Batched Background Migrations (BBM) are an experimental feature, please do not enable it in production.

The `backgroundmigrations` subsection configures Batched Background Migrations (BBM) in the registry. BBM are used for performing database data migration in batches, ensuring efficient and manageable data migrations without disrupting service availability. See the [specification](spec/gitlab/database-background-migrations.md) for a detailed explanation of how it works.

```yaml
backgroundmigrations:
  enabled: true
  maxjobretries: 3
  jobinterval: 1m
```

| Parameter       | Required | Description                                                                                                                                                |
| --------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`       | no       | When set to `true`, enables asynchronous Batched Background Migrations (BBM). Defaults to `false`.                                                         |
| `maxjobretries` | no       | The maximum number of times a job is retried before it is marked as failed in asynchronous BBM. Defaults to `0` - no retry.                                |
| `jobinterval`   | no       | The periodic duration to wait before checking for eligible BBM jobs to run and acquiring a lock on the BBM process in asynchronous mode. Defaults to `1m`. |

### `loadbalancing`

> **Note**: This is an [experimental](https://docs.gitlab.com/policy/development_stages_support/#experiment) feature and should _not_ be used in production.

This subsection allows enabling and configuring Database Load Balancing (DLB). See the corresponding [specification](spec/gitlab/database-load-balancing.md)
for more details on how it works.

```none
loadbalancing:
  enabled: true
  nameserver: localhost
  port: 8600
  record: db-replica-registry.service.consul
  replicacheckinterval: 1m
```

| Parameter              | Required | Description                                                                                                                                                              | Default          |
|------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `hosts`                | No       | A static list of hostnames to use for load balancing. Can be used as an alternative to service discovery. Ignored if `record` is set. `port` will be used for all hosts. |                  |
| `nameserver`           | No       | The nameserver to use for looking up the DNS record.                                                                                                                     | `localhost`      |
| `port`                 | No       | The port of the nameserver.                                                                                                                                              | `8600`           |
| `record`               | Yes      | The `SRV` record to look up. This option is required for service discovery to work.                                                                                      |                  |
| `replicacheckinterval` | No       | The minimum amount of time between checking the status of a replica.                                                                                                     | `1m`             |

## `auth`

```none
auth:
  silly:
    realm: silly-realm
    service: silly-service
  token:
    realm: token-realm
    service: token-service
    issuer: registry-token-issuer
    rootcertbundle: /root/certs/bundle
```

The `auth` option is **optional**. Possible auth providers include:

- [`silly`](#silly)
- [`token`](#token)
- [`none`]

You can configure only one authentication provider.

### `silly`

The `silly` authentication provider is only appropriate for development. It simply checks
for the existence of the `Authorization` header in the HTTP request. It does not
check the header's value. If the header does not exist, the `silly` auth
responds with a challenge response, echoing back the realm, service, and scope
for which access was denied.

The following values are used to configure the response:

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `realm`   | yes      | The realm in which the registry server authenticates. |
| `service` | yes      | The service being authenticated.                      |

### `token`

Token-based authentication allows you to decouple the authentication system from
the registry. It is an established authentication paradigm with a high degree of
security.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `realm`   | yes      | The realm in which the registry server authenticates. |
| `service` | yes      | The service being authenticated.                      |
| `issuer`  | yes      | The name of the token issuer. The issuer inserts this into the token so it must match the value configured for the issuer. |
| `rootcertbundle` | yes | The absolute path to the root certificate bundle. This bundle contains the public part of the certificates used to sign authentication tokens. |
| `autoredirect`   | no      | When set to `true`, `realm` will automatically be set using the Host header of the request as the domain and a path of `/auth/token/`|

## `middleware`

The `middleware` structure is **optional**. Use this option to inject middleware at
named hook points. Each middleware must implement the same interface as the
object it is wrapping. For instance, a registry middleware must implement the
`distribution.Namespace` interface, while a repository middleware must implement
`distribution.Repository`, and a storage middleware must implement
`driver.StorageDriver`.

This is an example configuration of the `cloudfront`  middleware, a storage
middleware:

```none
middleware:
  registry:
    - name: ARegistryMiddleware
      options:
        foo: bar
  repository:
    - name: ARepositoryMiddleware
      options:
        foo: bar
  storage:
    - name: cloudfront
      options:
        baseurl: https://my.cloudfronted.domain.com/
        privatekey: /path/to/pem
        keypairid: cloudfrontkeypairid
        duration: 3000s
        ipfilteredby: awsregion
        awsregion: us-east-1, use-east-2
        updatefrequency: 12h
        iprangesurl: https://ip-ranges.amazonaws.com/ip-ranges.json
```

This is an example configuration of the `googlecdn` storage middleware:

```yaml
middleware:
  storage:
    - name: googlecdn
      options:
        baseurl: https://my.googlecdn.domain.com/
        privatekey: /path/to/key
        keyname: googlecdnkeyname
        duration: 20s
        ipfilteredby: gcp
        updatefrequency: 12h
        iprangesurl: https://www.gstatic.com/ipranges/goog.json
```

Each middleware entry has `name` and `options` entries. The `name` must
correspond to the name under which the middleware registers itself. The
`options` field is a map that details custom configuration required to
initialize the middleware. It is treated as a `map[string]any`. As such,
it supports any interesting structures desired, leaving it up to the middleware
initialization function to best determine how to handle the specific
interpretation of the options.

### `storage`

#### `cloudfront`

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `baseurl` | yes      | The `SCHEME://HOST[/PATH]` at which Cloudfront is served. |
| `privatekey` | yes   | The private key for Cloudfront, provided by AWS.        |
| `keypairid` | yes    | The key pair ID provided by AWS.                         |
| `duration` | no      | An integer and unit for the duration of the Cloudfront session. Valid time units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, or `h`. For example, `3000s` is valid, but `3000 s` is not. If you do not specify a `duration` or you specify an integer without a time unit, the duration defaults to `20m` (20 minutes). |
| `ipfilteredby` | no     | A string with the following value `none`, `aws` or `awsregion`. |
| `awsregion` | no        | A comma separated string of AWS regions, only available when `ipfilteredby` is `awsregion`. For example, `us-east-1, us-west-2` |
| `updatefrequency`  | no | The frequency to update AWS IP regions, default: `12h` |
| `iprangesurl` | no      | The URL contains the AWS IP ranges information, default: `https://ip-ranges.amazonaws.com/ip-ranges.json` |

Value of `ipfilteredby` can be:

| Value       | Description                        |
|-------------|------------------------------------|
| `none`      | default, do not filter by IP       |
| `aws`       | IP from AWS goes to S3 directly    |
| `awsregion` | IP from certain AWS regions goes to S3 directly, use together with `awsregion`. |

#### `googlecdn`

Middleware to redirect blob download requests to [Google Cloud CDN](https://cloud.google.com/cdn) using
[pre-signed URLs](https://cloud.google.com/cdn/docs/using-signed-urls).

This option can only be used when `storage.redirect.disable` is `false`.

| Parameter          | Required | Description                                                                                                                                                                                                                                                                                                            |
|--------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `baseurl`          | yes      | The `SCHEME://HOST[/PATH]` where the Google CDN load balancer is listening.                                                                                                                                                                                                                                            |
| `privatekey`       | yes      | The full path to the private key file used for signing URLs.                                                                                                                                                                                                                                                           |
| `keyname`          | yes      | The name of the key used for signing URLs.                                                                                                                                                                                                                                                                             |
| `duration`         | no       | An integer and unit for the expiration delay of pre-signed URLs. Valid time units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, or `h`. For example, `3000s` is valid, but `3000 s` is not. If you do not specify a `duration` or you specify an integer without a time unit, the duration defaults to `20m` (20 minutes). |
| `ipfilteredby`     | no       | If redirections to Cloud CDN should be skipped (redirecting to GCS instead) based on an IP filter. Can be one of `none` (no filtering) or `gcp` (all known GCP IP ranges). Defaults to `none`.                                                                                                                         |
| `updatefrequency` | no       | The frequency to update the GCP IP ranges list. Only applies if `ipfilteredby` is not `none`. Defaults to `12h` (12 hours).                                                                                                                                                                                            |
| `iprangesurl`      | no       | The URL contains the GCP IP ranges information. Only applies if `ipfilteredby` is not `none`. Defaults to `https://www.gstatic.com/ipranges/goog.json`.                                                                                                                                                                |

#### `redirect`

You can use the `redirect` storage middleware to specify a custom URL to a
location of a proxy for the layer stored by the S3 storage driver.

| Parameter | Required | Description                                                                                                 |
|-----------|----------|-------------------------------------------------------------------------------------------------------------|
| `baseurl` | yes      | `SCHEME://HOST` at which layers are served. Can also contain port. For example, `https://example.com:5443`. |

## `reporting`

```yaml
reporting:
  sentry:
    enabled: true
    dsn: https://examplePublicKey@o0.ingest.sentry.io/0
    environment: production
```

The `reporting` option is **optional** and configures error and metrics
reporting tools. At the moment only one service is supported:

- [Sentry](#sentry)

A valid configuration may contain multiple.

### `sentry`

| Parameter     | Required | Description                                                                           |
|---------------|----------|---------------------------------------------------------------------------------------|
| `enabled`     | no       | Set `true` to enable error reporting with Sentry. Defaults to `false`.                |
| `dsn`         | yes      | The Sentry DSN.                                                                       |
| `environment` | no       | The Sentry [environment](https://docs.sentry.io/product/sentry-basics/environments/). |

## `profiling`

```yaml
profiling:
  stackdriver:
    service: registry
    serviceversion: 1.0.0
    projectid: tBXV4hFr4QJM6oGkqzhC
    keyfile: /path/to/credentials.json
```

The `profiling` option is **optional** and configures external profiling
services. At the moment only the Google Stackdriver Profiler is supported.

- [Stackdriver](#stackdriver)

### `stackdriver`

Please note that according to Google Cloud, the Stackdriver Profiler adds a 5%
performance overhead to processes when it is enabled ([source](https://medium.com/google-cloud/continuous-profiling-of-go-programs-96d4416af77b)).

| Parameter        | Required | Description                                                                                                                                                               |
|------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enabled`        | no       | Set `true` to enable the Stackdriver profiler.                                                                                                                            |
| `service`        | no       | The name of the service under which the profiled data will be recorded and exposed. Defaults to the value of the `GAE_SERVICE` environment variable or instance metadata.                      |
| `serviceversion` | no       | The version of the service. Defaults to the `GAE_VERSION` environment variable if that is set, or to empty string otherwise.                                              |
| `projectid`      | no       | The project ID. Defaults to the `GOOGLE_CLOUD_PROJECT` environment variable or instance metadata.                                                                                              |
| `keyfile`        | no       | Path of a private service account key file in JSON format used for [Service Account Authentication](https://cloud.google.com/storage/docs/authentication#service_accounts). The service account must have the `roles/cloudprofiler.agent` role or manually specified permissions as described [here](https://cloud.google.com/profiler/docs/iam#roles) for the agent role. Defaults to the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or instance metadata. |

See the Stackdriver Profiler [API docs](https://pkg.go.dev/cloud.google.com/go/profiler?tab=doc#Config)
for more details about configuration options.

## `http`

```none
http:
  addr: localhost:5000
  net: tcp
  prefix: /my/nested/registry/
  host: https://myregistryaddress.org:5000
  secret: asecretforlocaldevelopment
  relativeurls: false
  draintimeout: 60s
  tls:
    certificate: /path/to/x509/public
    key: /path/to/x509/private
    clientcas:
      - /path/to/ca.pem
      - /path/to/another/ca.pem
    minimumtls: tls1.2
    ciphersuites:
      - TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
      - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
    letsencrypt:
      cachefile: /path/to/cache-file
      email: emailused@letsencrypt.com
      hosts: [myregistryaddress.org]
  debug:
    addr: localhost:5001
    tls:
      certificate: /path/to/x509/public
      key: /path/to/x509/private
      clientcas:
        - /path/to/ca.pem
        - /path/to/another/ca.pem
      minimumtls: tls1.2

  headers:
    X-Content-Type-Options: [nosniff]
  http2:
    disabled: false
```

The `http` option details the configuration for the HTTP server that hosts the
registry.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `addr`    | yes      | The address for which the server should accept connections. The form depends on a network type (see the `net` option). Use `HOST:PORT` for TCP and `FILE` for a UNIX socket. |
| `net`     | no       | The network used to create a listening socket. Known networks are `unix` and `tcp`. |
| `prefix`  | no       | If the server does not run at the root path, set this to the value of the prefix. The root path is the section before `v2`. It requires both preceding and trailing slashes, such as in the example `/path/`. |
| `host`    | no       | A fully-qualified URL for an externally-reachable address for the registry. If present, it is used when creating generated URLs. Otherwise, these URLs are derived from client requests. |
| `secret`  | no       | A random piece of data used to sign state that may be stored with the client to protect against tampering. For production environments you should generate a random piece of data using a cryptographically secure random generator. If you omit the secret, the registry will automatically generate a secret when it starts. **If you are building a cluster of registries behind a load balancer, you MUST ensure the secret is the same for all registries.**|
| `relativeurls`| no    | If `true`, the registry returns relative URLs in Location headers. The client is responsible for resolving the correct URL. **This option is not compatible with Docker 1.7 and earlier.**|
| `draintimeout`| no    | Amount of time to wait for HTTP connections to drain before shutting down after registry receives SIGTERM signal|

### `tls`

The `tls` structure within `http` is **optional**. Use this to configure TLS
for the server. If you already have a web server running on
the same host as the registry, you may prefer to configure TLS on that web server
and proxy connections to the registry server.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `certificate` | yes  | Absolute path to the X.509 certificate file.           |
| `key`         | yes  | Absolute path to the X.509 private key file.           |
| `clientcas`   | no   | An array of absolute paths to X.509 CA files.          |
| `minimumtls`  | no   | Minimum TLS version allowed (tls1.2, tls1.3). Defaults to tls1.2. |
| `ciphersuites` | no   | Cipher suites allowed. Please see below for allowed values and defaults. In case TLS 1.3 version is chosen, `ciphersuites` is ignored as TLS 1.3 cipher suites are automatically enabled and do not need explicit configuration. |

Available TLS 1.2 cipher suites:

- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA
- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
- TLS_RSA_WITH_3DES_EDE_CBC_SHA
- TLS_RSA_WITH_AES_128_CBC_SHA
- TLS_RSA_WITH_AES_128_GCM_SHA256
- TLS_RSA_WITH_AES_256_CBC_SHA
- TLS_RSA_WITH_AES_256_GCM_SHA384

Default cipher suites selected for secure communication:

- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256

### `letsencrypt`

The `letsencrypt` structure within `tls` is **optional**. Use this to configure
TLS certificates provided by
[Let's Encrypt](https://letsencrypt.org/how-it-works/).

>**NOTE**: When using Let's Encrypt, ensure that the outward-facing address is
> accessible on port `443`. The registry defaults to listening on port `5000`.
> If you run the registry as a container, consider adding the flag `-p 443:5000`
> to the `docker run` command or using a similar setting in a cloud
> configuration. You should also set the `hosts` option to the list of hostnames
> that are valid for this registry to avoid trying to get certificates for random
> hostnames due to malicious clients connecting with bogus SNI hostnames. Please
> ensure that you have the `ca-certificates` package installed in order to verify
> letsencrypt certificates.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `cachefile` | yes    | Absolute path to a file where the Let's Encrypt agent can cache data. |
| `email`   | yes      | The email address used to register with Let's Encrypt. |
| `hosts`   | no       | The hostnames allowed for Let's Encrypt certificates. |

### `debug`

The `debug` option is **optional** . Use it to configure a debug server that
can be helpful in diagnosing problems. The debug endpoint can be used for
monitoring registry metrics and health, as well as profiling. Sensitive
information may be available via the debug endpoint. Please be certain that
access to the debug endpoint is locked down in a production environment.

| Parameter | Required | Description                                                                    |
|-----------|----------|--------------------------------------------------------------------------------|
| `addr`    | yes      | Specifies the `HOST:PORT` on which the debug server should accept connections. |

#### `tls`

The `tls` subsection allows configuring the monitoring service using TLS certificates.
All the TLS parameters in the parent section are also available here. The only addition is a new
`enabled` parameter to toggle using the TLS functionality for the debug server.
If `enabled` is set to true and `http.tls` is provided but `http.debug.tls` is not,
the monitoring service will inherit the TLS connection settings from the `http.tls` subsection.
Please refer to the [`tls`](#tls) documentation for details.

**Note**: `letsencrypt` is not available for the debug server.

#### `prometheus`

The `prometheus` option defines whether the Prometheus metrics is enable, as well
as the path to access the metrics.

These parameters are ignored if `debug.addr` is not set.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `enabled` | no       | Set `true` to enable the Prometheus server            |
| `path`    | no       | The path to access the metrics, `/metrics` by default |

The URL to access the metrics is `HOST:PORT/path`, where `HOST:PORT` is defined
in `addr` under `debug`.

#### `pprof`

The `pprof` section configures a pprof server, which listens at `/debug/pprof/`.

These parameters are ignored if `debug.addr` is not set.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `enabled` | no       | Set `true` to enable the pprof server                 |

The URL to access the pprof server is `HOST:PORT/debug/pprof/`, where `HOST:PORT`
is defined in `addr` under `debug`.

### `headers`

The `headers` option is **optional** . Use it to specify headers that the HTTP
server should include in responses. This can be used for security headers such
as `Strict-Transport-Security`.

The `headers` option should contain an option for each header to include, where
the parameter name is the header's name, and the parameter value a list of the
header's payload values.

Including `X-Content-Type-Options: [nosniff]` is recommended, so that browsers
will not interpret content as HTML if they are directed to load a page from the
registry. This header is included in the example configuration file.

### `http2`

The `http2` structure within `http` is **optional**. Use this to control http2
settings for the registry.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `disabled` | no      | If `true`, then `http2` support is disabled.          |

## `notifications`

```none
notifications:
  fanouttimeout: 5s
  events:
    includereferences: true
  endpoints:
    - name: alistener
      disabled: false
      url: https://my.listener.com/event
      headers: <http.Header>
      timeout: 1s
      threshold: 10
      backoff: 1s
      ignoredmediatypes:
        - application/octet-stream
      ignore:
        mediatypes:
           - application/octet-stream
        actions:
           - pull
      queuepurgetimeout: 15s
```

The notifications option is **optional**.

| Parameter | Required | Description                                                                                                                                                                                                                        |
|-----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fanouttimeout` | no | The maximum amount of time registry tries to fan out notifications in the buffer after it received SIGINT. A positive integer and an optional suffix indicating the unit of time, which may be `ns`, `us`, `ms`, `s`, `m`, or `h`. If you omit the unit of time, `ns` is used. The default value is 15 seconds.|

### `endpoints`

The `endpoints` structure contains a list of named services (URLs) that can
accept event notifications.

| Parameter | Required | Description                                                                                                                                                                                                                        |
|-----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`    | yes      | A human-readable name for the service.                                                                                                                                                                                             |
| `disabled` | no      | If `true`, notifications are disabled for the service.                                                                                                                                                                             |
| `url`     | yes      | The URL to which events should be published.                                                                                                                                                                                       |
| `headers` | yes      | A list of static headers to add to each request. Each header's name is a key beneath `headers`, and each value is a list of payloads for that header name. Values must always be lists.                                            |
| `timeout` | yes      | A value for the HTTP timeout. A positive integer and an optional suffix indicating the unit of time, which may be `ns`, `us`, `ms`, `s`, `m`, or `h`. If you omit the unit of time, `ns` is used.                                  |
| `threshold` | yes    | DEPRECATED: use maxretries instead, more details [here](https://gitlab.com/gitlab-org/container-registry/-/issues/1243). An integer specifying how long to wait before backing off a failure.                                                            |
| `maxretries` | no | An integer specifying the maximum number of times to retry sending a failed event. `threshold` is ignored when defining this field.                                                                                                |
| `backoff` | yes      | How long the system backs off before retrying after a failure. A positive integer and an optional suffix indicating the unit of time, which may be `ns`, `us`, `ms`, `s`, `m`, or `h`. If you omit the unit of time, `ns` is used. |
| `ignoredmediatypes`|no| A list of target media types to ignore. Events with these target media types are not published to the endpoint.                                                                                                                    |
| `ignore`  |no| Events with these mediatypes or actions are not published to the endpoint.                                                                                                                                                         |
| `queuepurgetimeout` | no | The maximum amount of time registry tries to sent unsent notifications in the buffer after it received SIGINT. A positive integer and an optional suffix indicating the unit of time, which may be `ns`, `us`, `ms`, `s`, `m`, or `h`. If you omit the unit of time, `ns` is used. The default is 5 seconds. The zero value is always defaulted to 5 seconds. User may set a very low value (e.g. 1ns) to simulate no-wait if desired. |

#### `ignore`

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `mediatypes`|no| A list of target media types to ignore. Events with these target media types are not published to the endpoint. |
| `actions`   |no| A list of actions to ignore. Events with these actions are not published to the endpoint. |

### `events`

The `events` structure configures the information provided in event notifications.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `includereferences` | no | If `true`, include reference information in manifest events. |

## `redis`

```yaml
redis:
  addr: localhost:16379
  username: registry
  password: asecret
  db: 0
  dialtimeout: 10ms
  readtimeout: 10ms
  writetimeout: 10ms
  tls:
    enabled: true
    insecure: true
  pool:
    size: 10
    maxlifetime: 1h
    idletimeout: 300s
  cache:
    enabled: true
    addr: localhost:16380,localhost:26381
    mainname: mainserver
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    sentinelusername: my-sentinel-username
    sentinelpassword: some-sentinel-password
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
  ratelimiter:
    enabled: true
    addr: localhost:16379,localhost:26379
    username: registry
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
  loadbalancing:
    enabled: true
    addr: localhost:16379,localhost:26379
    username: registry
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
```

Declare parameters for constructing the `redis` connections. Single instances, Redis Sentinel and Redis Cluster are supported.

For backward compatibility reasons, registry instances use the root Redis connection exclusively to cache information about
immutable blobs when `storage.cache.blobdescriptor` is set to `redis`. When using this feature, you should configure
Redis with the `allkeys-lru` eviction policy, because the registry does not set an expiration value on keys.

For other caching purposes/features, please see the new dedicated `redis.cache`, `redis.ratelimiter` and `redis.loadbalancing` subsections.

| Parameter          | Required | Description                                                                                                           |
|--------------------|----------|-----------------------------------------------------------------------------------------------------------------------|
| `addr`             | yes      | The address (host and port) of the Redis instance. For Sentinel and Cluster it should be a list of addresses separated by commas. |
| `mainname`         | no       | The main server name. Only applicable for Sentinel.                                                                   |
| `username`         | no       | A username used to authenticate to the Redis instance.                                                                |
| `password`         | no       | A password used to authenticate to the Redis instance.                                                                |
| `sentinelusername` | no       | A username used to authenticate to the Redis Sentinel instance.                                                       |
| `sentinelpassword` | no       | A password used to authenticate to the Redis Sentinel instance.                                                       |
| `db`               | no       | The name of the database to use for each connection.                                                                  |
| `dialtimeout`      | no       | The timeout for connecting to the Redis instance. Defaults to no timeout.                                             |
| `readtimeout`      | no       | The timeout for reading from the Redis instance. Defaults to no timeout.                                              |
| `writetimeout`     | no       | The timeout for writing to the Redis instance. Defaults to no timeout.                                                |

### `tls`

```yaml
tls:
  enabled: true
  insecure: true
```

Use these settings to configure TLS connections.

| Parameter  | Required | Description                                                                                      |
|------------|----------|--------------------------------------------------------------------------------------------------|
| `enabled`  | no       | Set to `true` to enable TLS. Defaults to `false`.                                                |
| `insecure` | no       | Set to `true` to disable server name verification when connecting over TLS. Defaults to `false`. |

### `pool`

```yaml
pool:
  size: 10
  maxlifetime: 1h
  idletimeout: 300s
```

Use these settings to configure the behavior of the Redis connection pool.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `size` | no       | The maximum number of socket connections. Default is 10 connections. |
| `maxlifetime`| no      | The connection age at which client retires a connection. Default is to not close aged connections. |
| `idletimeout`| no    | How long to wait before closing inactive connections. |

### `cache`

```yaml
redis:
  cache:
    enabled: true
    addr: localhost:16379,localhost:26379
    mainname: mainserver
    username: default
    password: asecret
    sentinelusername: my-sentinel-username
    sentinelpassword: some-sentinel-password
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
```

The cache subsection allows configuring a Redis connection specifically for caching purposes.

The intent is to allow using separate instances for different purposes, achieving isolation and improved performance and
availability. In case this is not a concern, it is also possible to use the same settings as those on the
top-level [`redis`](#redis) section (if any) that are exclusively used for caching blob descriptors (if enabled).

Currently, the functionality dependent on this subsection is caching repository objects from the metadata database.
Other use cases are expected to follow and will be documented here.

The registry is currently applying a non-configurable TTL of 6 hours to all cached keys. We intend to fine-tune this
value and make it configurable once the feature is considered stable.

Please read the corresponding development documentation [here](redis-dev-guidelines.md) for more details about caching in Redis, such as key and value formats.

All the Redis connection parameters in the parent section are also available here. The only addition is a new
`enabled` parameter to toggle the caching functionality without having to comment or remove the whole subsection. Please
refer to the documentation for the remaining connection parameters [`here`](#redis).

| Parameter | Required | Description                                                                   |
|-----------|----------|-------------------------------------------------------------------------------|
| `enabled` | no       | If the Redis caching functionality is enabled (boolean). Defaults to `false`. |

### `ratelimiter`

```yaml
redis:
  ratelimiter:
    enabled: true
    addr: localhost:16379,localhost:26379
    username: registry
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
```

The `ratelimiter` subsection allows configuring a Redis connection specifically for rate-limiting purposes.

This functionality is [currently in development](https://gitlab.com/groups/gitlab-org/-/epics/13237).
More functionality details will be added to this section as they become available.

All the Redis connection parameters in the parent section are also available here.
There are two new parameters available specific to the rate-limiting functionality.
Please refer to the documentation for the remaining connection parameters [`here`](#redis).

| Parameter  | Required | Description                                                                            |
|------------|----------|----------------------------------------------------------------------------------------|
| `enabled`  | no       | If the Redis rate-limiting functionality is enabled (boolean). Defaults to `false`.    |

### `loadbalancing`

> **Note**: This is an [experimental](https://docs.gitlab.com/policy/development_stages_support/#experiment) feature and should _not_ be used in production.

```yaml
redis:
  loadbalancing:
    enabled: true
    addr: localhost:16379,localhost:26379
    username: registry
    password: asecret
    db: 0
    dialtimeout: 10ms
    readtimeout: 10ms
    writetimeout: 10ms
    tls:
      enabled: true
      insecure: true
    pool:
      size: 10
      maxlifetime: 1h
      idletimeout: 300s
```

The `loadbalancing` subsection allows configuring a Redis connection specifically for database load balancing purposes.

All the Redis connection parameters in the parent section are also available here. The only addition is a new
`enabled` parameter to toggle the Redis connection without having to comment or remove the whole subsection. Please
refer to the documentation for the remaining connection parameters [here](#redis).

| Parameter | Required | Description                                                        |
|-----------|----------|--------------------------------------------------------------------|
| `enabled` | no       | If the Redis connection is enabled (boolean). Defaults to `false`. |

## `health`

```none
health:
  storagedriver:
    enabled: true
    interval: 10s
    threshold: 3
  database:
    enabled: true
    interval: 3s
    threshold: 2
    timeout: 2s
  file:
    - file: /path/to/checked/file
      interval: 10s
  http:
    - uri: http://server.to.check/must/return/200
      headers:
        Authorization: [Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==]
      statuscode: 200
      timeout: 3s
      interval: 10s
      threshold: 3
  tcp:
    - addr: redis-server.domain.com:6379
      timeout: 3s
      interval: 10s
      threshold: 3
```

The health option is **optional**, and contains preferences for a periodic
health check on the storage driver's backend storage, as well as optional
periodic checks on local files, HTTP URIs, and/or TCP servers. The results of
the health checks are available at the `/debug/health` endpoint on the debug
HTTP server if the debug HTTP server is enabled (see http section).

### `database`

The `database` structure contains options for a health check on the
configured database and replicas if any. The health check is only active
when `enabled` is set to `true`.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `enabled` | yes      | Set to `true` to enable database health checks or `false` to disable them. |
| `interval`| no       | How long to wait between repetitions of the database health check. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. Defaults to `10s` if the value is omitted. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |
| `threshold`| no      | A positive integer which represents the number of times the check must fail before the state is marked as unhealthy. If not specified, a single failure marks the state as unhealthy. |
| `timeout` | no       | How long to wait before timing out the database check. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |

### `storagedriver`

The `storagedriver` structure contains options for a health check on the
configured storage driver's backend storage. The health check is only active
when `enabled` is set to `true`.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `enabled` | yes      | Set to `true` to enable storage driver health checks or `false` to disable them. |
| `interval`| no       | How long to wait between repetitions of the storage driver health check. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. Defaults to `10s` if the value is omitted. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |
| `threshold`| no      | A positive integer which represents the number of times the check must fail before the state is marked as unhealthy. If not specified, a single failure marks the state as unhealthy. |

### `file`

The `file` structure includes a list of paths to be periodically checked for the\
existence of a file. If a file exists at the given path, the health check will
fail. You can use this mechanism to bring a registry out of rotation by creating
a file.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `file`    | yes      | The path to check for existence of a file.            |
| `interval`| no       | How long to wait before repeating the check. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. Defaults to `10s` if the value is omitted. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |

### `http`

The `http` structure includes a list of HTTP URIs to periodically check with
`HEAD` requests. If a `HEAD` request does not complete or returns an unexpected
status code, the health check will fail.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `uri`     | yes      | The URI to check.                                     |
| `headers` | no       | Static headers to add to each request. Each header's name is a key beneath `headers`, and each value is a list of payloads for that header name. Values must always be lists. |
| `statuscode` | no    | The expected status code from the HTTP URI. Defaults to `200`. |
| `timeout` | no       | How long to wait before timing out the HTTP request. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |
| `interval`| no       | How long to wait before repeating the check. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. Defaults to `10s` if the value is omitted. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |
| `threshold`| no      | The number of times the check must fail before the state is marked as unhealthy. If this field is not specified, a single failure marks the state as unhealthy. |

### `tcp`

The `tcp` structure includes a list of TCP addresses to periodically check using
TCP connection attempts. Addresses must include port numbers. If a connection
attempt fails, the health check will fail.

| Parameter | Required | Description                                           |
|-----------|----------|-------------------------------------------------------|
| `addr`    | yes      | The TCP address and port to connect to.               |
| `timeout` | no       | How long to wait before timing out the TCP connection. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |
| `interval`| no       | How long to wait between repetitions of the check. A positive integer and an optional suffix indicating the unit of time. The suffix is one of `ns`, `us`, `ms`, `s`, `m`, or `h`. Defaults to `10s` if the value is omitted. If you specify a value but omit the suffix, the value is interpreted as a number of nanoseconds. |
| `threshold`| no      | The number of times the check must fail before the state is marked as unhealthy. If this field is not specified, a single failure marks the state as unhealthy. |

## `validation`

```none
validation:
  manifests:
    referencelimit: 150
    payloadsizelimit: 64000
    urls:
      allow:
        - ^https?://([^/]+\.)*example\.com/
      deny:
        - ^https?://www\.example\.com/
```

### `disabled`

The `disabled` flag disables the other options in the `validation`
section. They are enabled by default. This option deprecates the `enabled` flag.

### `manifests`

Use the `manifests` subsection to configure validation of manifests. If
`disabled` is `false`, the validation allows nothing.

#### `referencelimit`

Limit the number of manifest references (layers, configurations, other manifests)
to the set number. `0` (default) disables limiting the number of references.

#### `payloadsizelimit`

Limit the size in bytes of a manifest payload. `0` (default) disables limiting
the manifest payload size.

#### `urls`

The `allow` and `deny` options are each a list of
[regular expressions](https://godoc.org/regexp/syntax) that restrict the URLs in
pushed manifests.

If `allow` is unset, pushing a manifest containing URLs fails.

If `allow` is set, pushing a manifest succeeds only if all URLs match
one of the `allow` regular expressions **and** one of the following holds:

1. `deny` is unset.
1. `deny` is set but no URLs within the manifest match any of the `deny` regular expressions.

## `gc`

The `gc` subsection configures online Garbage Collection (GC). See the [specification](spec/gitlab/online-garbage-collection.md) for an explanation of how it works. Please note that these configuration settings only apply to the last stage of online GC: processing blob and manifest tasks, determining eligibility for deletion and deleting from database and storage backends, if eligible.

```yaml
gc:
  disabled: false
  maxbackoff: 24h
  errorcooldownperiod: 30m
  noidlebackoff: false
  transactiontimeout: 10s
  reviewafter: 24h
  manifests:
    disabled: false
    interval: 5s
  blobs:
    disabled: false
    interval: 5s
    storagetimeout: 5s
```

| Parameter       | Required | Description                                                                                                                                                                                                                                                                                                               |
| --------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `disabled`      | no       | When set to `true`, the online GC workers are disabled. Defaults to `false`.                                                                                                                                                                                                                                                           |
| `noidlebackoff` | no       | When set to `true`, disables exponential backoffs between worker runs when there was no task to be processed. Defaults to `false`.                                                                                                                                                                                       |
| `maxbackoff`    | no       | The maximum exponential backoff duration used to sleep between worker runs when an error occurs. Also applied when there are no tasks to be processed unless `noidlebackoff` is `true`. Please note that this is not the absolute maximum, as a randomized jitter factor of up to 33% is always added. Defaults to `24h`. |
| `transactiontimeout`   | no       | The database transaction timeout for each worker run. Each worker starts a database transaction at the start. The worker run is canceled if this timeout is exceeded to avoid stalled or long-running transactions. Defaults to `10s`.                                                                                    |
| `reviewafter`   | no       | The minimum amount of time after which the garbage collector should pick up a record for review. `-1` means no wait. Defaults to `24h`. |
| `errorcooldownperiod` | no | The period of time after an error occurs that the GC workers will continue to exponentially backoff. If the worker encounters an error while cooling down, the cool down period is extended again by the configured value. This is useful to ensure that GC workers in multiple registry deployments will slow down during periods of intermittent errors. Defaults to 0 (no cooldown) by default. |

### `blobs`

The `blobs` subsection configures the blob worker.

```yaml
blobs:
  disabled: false
  interval: 5s
  storagetimeout: 5s
```

| Parameter        | Required | Description                                                                                                                                   |
| ---------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `disabled`       | no       | When set to `true`, the worker is disabled. Defaults to `false`.                                                                              |
| `interval`       | no       | The initial sleep interval between each worker run. Defaults to `5s`.                                                                                 |
| `storagetimeout` | no       | The timeout for storage operations. Used to limit the duration of requests to delete dangling blobs on the storage backend. Defaults to `5s`. |

### `manifests`

The `manifests` subsection configures the manifest worker.

```yaml
manifests:
  disabled: false
  interval: 5s
```

| Parameter  | Required | Description                                                      |
| ---------- | -------- | ---------------------------------------------------------------- |
| `disabled` | no       | When set to `true`, the worker is disabled. Defaults to `false`. |
| `interval` | no       | The initial sleep interval between each worker run. Defaults to `5s`.    |

## Example: Development configuration

You can use this simple example for local development:

```none
version: 0.1
log:
  level: debug
storage:
    filesystem:
        rootdirectory: /var/lib/registry
http:
    addr: localhost:5000
    secret: asecretforlocaldevelopment
    debug:
        addr: localhost:5001
```

This example configures the registry instance to run on port `5000`, binding to
`localhost`, with the `debug` server enabled. Registry data is stored in the
`/var/lib/registry` directory. Logging is set to `debug` mode, which is the most
verbose.

See
[`config-example.yml`](https://github.com/docker/distribution/blob/master/cmd/registry/config-example.yml)
for another simple configuration. Both examples are generally useful for local
development.

## Example: Middleware configuration

This example configures [Amazon Cloudfront](http://aws.amazon.com/cloudfront/)
as the storage middleware in a registry. Middleware allows the registry to serve
layers via a content delivery network (CDN). This reduces requests to the
storage layer.

Cloudfront requires the S3 storage driver.

This is the configuration expressed in YAML:

```none
middleware:
  storage:
  - name: cloudfront
    disabled: false
    options:
      baseurl: http://d111111abcdef8.cloudfront.net
      privatekey: /path/to/asecret.pem
      keypairid: asecret
      duration: 60s
```

See the configuration reference for [Cloudfront](#cloudfront) for more
information about configuration options.

> **Note**: Cloudfront keys exist separately from other AWS keys. See
> [the documentation on AWS credentials](http://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html)
> for more information.

## Rate limiter

The Registry can be configured to use application-side rate limiters.
It uses [Redis](#ratelimiter) in the backend to keep track of requests, and can be
used to log only or log and enforce the configured limits.

```yaml
rate_limiter:
  enabled: true
  # Limiters is a list of rate limits which may be defined in any order, although they are applied to requests in precedence order (lowest values first)  
  limiters:
    # Instance-wide global limits
    - name: global_rate_limit
      description: "Global IP rate limit"  # Maybe useful for visibility
      log_only: false     # When true, only logs violations without enforcement
      match:
        type: IP          # No value applies to all requests from a specific IP
      precedence: 10      # Low precedence, evaluated first
      limit:
        rate: 5000
        period: "minute"
        burst: 8000
      action:
        warn_threshold: 0.7
        warn_action: "log"
        hard_action: "block"
```

| Parameter  | Required | Description                                                                         |
|------------|----------|-------------------------------------------------------------------------------------|
| `enabled`  | no       | When set to `true`, the rate limiter functionality is enabled. Defaults to `false`. |
| `limiters` | no       | List of [`limiters`](#limiters) to be configured.                                   |

### Limiters

The `limiters` section defines the fine-grained configuration of the rate-limiting capability.
See the [rate-limiting](./spec/gitlab/rate-limiting.md) documentation for examples.

| Parameter               | Required | Description                                                                                                         |
|-------------------------|----------|---------------------------------------------------------------------------------------------------------------------|
| `name`                  | yes      | The name of the rate limiter.                                                                                       |
| `description`           | no       | A description for this limiter.                                                                                     |
| `log_only`              | no       | When set to `true`, the rate limiter will be enabled but not enforced. Defaults to `false`.                         |
| `match.type`            | yes      | One of [`IP`] (only IP is available right now).                                                                     |
| `precedence`            | yes      | A positive integer. The lower the value the higher precedence it will have against other limiters.                  |
| `limit.rate`            | yes      | The rate at which the limiter's capability is refilled at a given `limiter.period`.                                 |
| `limit.period`          | yes      | The period at which `limiter.rate` refills the limiter's capability. One of [`second`, `minute`, `hour`].           |
| `limit.burst`           | yes      | The maximum number of requests that the limiter allows at a given time.                                             |
| `action.warn_threshold` | no       | A percentage [0.0, 1.0] of the limiter's capacity that triggers a warning action. 0 means no action. Defaults to 0. |
| `action.warn_action`    | no       | One of [`none`, `log`]. Defaults to `none`.                                                                         |
| `action.hard_action`    | yes      | One of [`none`, `log`, `block`].                                                                                    |
