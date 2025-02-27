package s3

import (
	"github.com/docker/distribution/registry/storage/driver/factory"
	v1 "github.com/docker/distribution/registry/storage/driver/s3-aws/v1"
)

func init() {
	factory.Register(v1.DriverName, new(v1.S3DriverFactory))
	factory.Register(v1.DriverNameAlt, new(v1.S3DriverFactory))
}
