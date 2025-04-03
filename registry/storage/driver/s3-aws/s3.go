package s3

import (
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	v1 "github.com/docker/distribution/registry/storage/driver/s3-aws/v1"
	v2 "github.com/docker/distribution/registry/storage/driver/s3-aws/v2"
)

func init() {
	factory.Register(common.V1DriverName, new(v1.S3DriverFactory))
	factory.Register(common.V1DriverNameAlt, new(v1.S3DriverFactory))
	factory.Register(common.V2DriverName, new(v2.S3DriverFactory))
}
