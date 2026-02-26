package main

import (
	"fmt"
	"os"

	"gitlab.com/gitlab-org/container-registry/devvm/go/cmd/devvm/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
