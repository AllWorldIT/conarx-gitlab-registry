package azure

import (
	v1 "github.com/docker/distribution/registry/storage/driver/azure/v1"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

func init() {
	factory.Register(v1.DriverName, new(v1.AzureDriverFactory))
}
