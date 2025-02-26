package main

import (
	"fmt"
	"os"

	"github.com/docker/distribution/cmd/internal/release-cli/cmd"
)

func main() {
	err := cmd.RootCmd.Execute()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
