# Medium-Scale Garbage Collection Testing

This guide provides instructions on setting up a registry in order to test
garbage collection. Of particular interest is seeding the registry with data and
preserving that data so that multiple successive garbage collection runs can be
as similar as possible to each other.

## Configuration

These configurations are minimal starting points to enable garbage
collection to run smoothly, though some relevant additional configuration
will be applied when it is impactful to garbage collection.

### S3

This configuration assumes that you have access to an S3 bucket and can provide
an appropriate `accesskey` and `secretkey`. A setting of particular interest
to us is `maxrequestspersecond` which will increase or decrease the rate of
requests to S3. This setting will impact the time the mark state takes to
complete significantly.

```yaml
version: 0.1
log:
  fields:
    service: registry
storage:
  delete:
    enabled: true
  cache:
    blobdescriptor: inmemory
  s3:
    region: "us-east-1"
    bucket: "registry-bucket"
    encrypt: false
    secure: false
    maxrequestspersecond: 500
    accesskey: "<ACCESS_KEY>"
    secretkey: "<SECRET_KEY>"
  maintenance:
      uploadpurging:
          enabled: false
http:
  addr: :5000
  headers:
    X-Content-Type-Options: [nosniff]
```

## Seeding

After configuring and deploying the registry, we need to push images to it to
seed data for the garbage collection process.

We can use a Dockerfile included with this repository,
[Dockerfile-Walk](../script/dev/seed/Dockerfile-Walk) that will enable us to
generate unique layers that will not be shared across all images, without
needing to alter the contents of the Dockerfile itself.

This [seed.sh](../script/dev/seed/seed.sh) script will use the Dockerfile we created in
order to build images and push them to the registry. If you are not running the
container registry locally, you will need to replace `localhost:5000` with the
IP address of your container registry.

From inside the [script/dev/seed](../script/dev/seed) directory, you can run the
script with `./seed.sh`. By default, the script will seed three repositories
with three tags each. You can customize this by passing the number of
repositories and tags via the command line. For example, this command will seed
five repositories with seven tags: `./seed.sh 5 7`. Please note this operation
will take a considerable amount of time for even modest numbers of repositories
and tags.

## Preparing Layers for Garbage Collection

Although we have seeded our repository, all layers and manifests are referenced
and therefore we will not be able to properly test garbage collection. The
simplest way to do this is to run the script again with the same arguments, or
you may wish to only override a subset of the repositories and/or tags. This
method will only produce layers which may be deleted when the
`--delete-untagged` (`-m`) option is passed to the garbage collection command.

Removing an entire repository directly with the storage backend can be used as
a quick way to remove references to the layers uploaded to the repository,
allowing them to be garbage collected without the `--delete-untagged` option, if
they are not referenced by other repositories.

After removing the repository, you may wish to create a backup of the registry
storage in order to restore it. This enables testing garbage collection with
consistent data and is somewhat faster than repeating the seeding process.

Storage backend specific instructions on how to remove the repository and
perform backups and restores follow.

### S3

We will be working with the `s3cmd` utility to manage the S3 bucket directly.
Please ensure that you have this utility installed and configured such that you
have access to the S3 bucket your container registry is using. This utility
and installation and usage instructions can be found [here](https://s3tools.org/s3cmd)

Substitute `registry-bucket` with the name the S3 bucket you are using in all
the following commands.

#### Deleting the Remove Repository

This command will delete the `repository-1` repository and all the files within:

```shell
s3cmd rm --recursive s3://registry-bucket/docker/registry/v2/repositories/repository-1/
```

#### Backing up and Restoring the Registry

These commands employ the `sync` subcommand to avoid unnecessary copying of
files via the `--skip--existing` flag.

Backup:

```shell
s3cmd sync --recursive --skip-existing s3://registry-bucket/docker/ s3://registry-bucket/backup/registry-1/
```

Restore:

```shell
s3cmd sync --recursive --skip-existing s3://registry-bucket/backup/registry-1/registry/ s3://registry-bucket/docker/
```

## Results

Running the garbage collection command (optionally with the `--dry-run` flag)
will produce a significant amount of log entries. It might be useful to
redirect this output to a file in order to analyze the output.

```shell
docker exec -it walker /bin/registry garbage-collect --dry-run /etc/docker/registry/config.yml &> /path/to/file
```

Log entries are in a structured format which should allow for easy analysis.

All blobs, both in use and eligible for deletion, will be logged.

Entry from a blob in use (marked):

```plaintext
time="2020-01-31T17:43:02.81636132Z" level=info msg="marking manifest" digest=sha256:45d437916d5781043432f2d72608049dcf74ddbd27daa01a25fa63c8f1b9adc4 digest_type=layer go.version=go1.11.13 instance.id=1ba7d474-5018-4845-a1f6-871caebc8671 repository=ubuntu service=registry
```

Entry from a blob eligible for deletion (not marked):

```plaintext
time="2020-01-31T17:44:31.620341601Z" level=info msg="blob eligible for deletion" digest=sha256:b2214af1aed703acc22b29e146875e9cf3979f7c3a4f70e3c09c33ce174b6053 go.version=go1.11.13 instance.id=1ba7d474-5018-4845-a1f6-871caebc8671 path="/docker/registry/v2/blobs/sha256/b2/b2214af1aed703acc22b29e146875e9cf3979f7c3a4f70e3c09c33ce174b6053/data" service=registry
```

Of particular interest are the following entries, which mark the end of the
mark and sweep stages, respectively.

```plaintext
time="2020-01-31T17:44:31.61267147Z" level=info msg="mark stage complete" blobs_marked=15041 blobs_to_delete=9030 duration_s=91.933066704 go.version=go1.11.13 instance.id=1ba7d474-5018-4845-a1f6-871caebc8671 manifests_to_delete=0 service=registry
```

```plaintext
time="2020-01-31T17:44:34.851525753Z" level=info msg="sweep stage complete" duration_s=3.238775081 go.version=go1.11.13 instance.id=1ba7d474-5018-4845-a1f6-871caebc8671 service=registry
```
