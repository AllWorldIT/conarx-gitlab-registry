# Storage Driver Local Integration Testing Guide

## Using MinIO to test the S3 storage driver

This section will use a conainerized MinIO server to simulate an S3 storage
server. This can be used to test the S3 storage driver locally.

### Setting up the environment

In a terminal, set up the following environment variables:

```shell
export AWS_REGION="us-east-2"
export REGION_ENDPOINT="http://127.0.0.1:9000"
export S3_ENCRYPT="false"
export AWS_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
export S3_BUCKET="test"
 ```

Next, run the MinIO server:

```shell
docker run -d -p 9000:9000 --name s3-test-minio \
  -e "MINIO_ACCESS_KEY=$AWS_ACCESS_KEY" \
  -e "MINIO_SECRET_KEY=$AWS_SECRET_KEY" \
  minio/minio server /data
```

The following command will create the test bucket without having to install
the MinIO client locally:

```shell
docker run --network=host -t --entrypoint=/bin/sh minio/mc \
  -c "mc config host add mino $REGION_ENDPOINT $AWS_ACCESS_KEY $AWS_SECRET_KEY && \
  mc mb mino/$S3_BUCKET"
```

Now you can run the S3 integration tests against the MinIO server we created above:

```shell
go test -timeout 20m -v github.com/docker/distribution/registry/storage/driver/s3-aws -args -check.v
```

Finally, the MinIO server can be stopped once you are finished with the
integration tests:

```shell
docker stop s3-test-minio
```

### Running the Benchmark Suite

To run the benchmarks against any configured driver, run the following
command, substituting the appropriate driver:

```shell
go test -v -cpuprofile profile.out github.com/docker/distribution/registry/storage/driver/s3-aws -args -check.v -check.b
```

Afterwards, the `profile.out` file we generated above can be used to analyze
the performance of the benchmarks with `pprof`:

```shell
go tool pprof profile.out
```

If you are unfamiliar with pprof,
[Profiling Go Programs](https://blog.golang.org/profiling-go-programs)
on The Go Blog is an excellent introduction.
