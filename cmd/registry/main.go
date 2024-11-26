package main

import (
	"fmt"
	"os"

	"github.com/docker/distribution/registry"
	_ "github.com/docker/distribution/registry/auth/silly"
	_ "github.com/docker/distribution/registry/auth/token"
	_ "github.com/docker/distribution/registry/storage/driver/azure"
	_ "github.com/docker/distribution/registry/storage/driver/filesystem"
	_ "github.com/docker/distribution/registry/storage/driver/gcs"
	_ "github.com/docker/distribution/registry/storage/driver/inmemory"
	_ "github.com/docker/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/docker/distribution/registry/storage/driver/middleware/googlecdn"
	_ "github.com/docker/distribution/registry/storage/driver/middleware/redirect"
	_ "github.com/docker/distribution/registry/storage/driver/s3-aws"

	"go.uber.org/automaxprocs/maxprocs"
)

func init() {
	_, _ = maxprocs.Set()
}

func main() {
	err := registry.RootCmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
