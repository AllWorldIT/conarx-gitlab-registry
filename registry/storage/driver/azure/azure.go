package azure

import (
	v1 "github.com/docker/distribution/registry/storage/driver/azure/v1"
	v2 "github.com/docker/distribution/registry/storage/driver/azure/v2"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

func init() {
	factory.Register(v1.DriverName, new(v1.AzureDriverFactory))
	factory.Register(v2.DriverName, new(v2.AzureDriverFactory))
}
