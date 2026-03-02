package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os/user"

	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/misc"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/systemops"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/userops"
)

var (
	systemMode bool
	logLevel   string
	logPretty  bool
	watch      bool
)

func main() {
	if err := run(); err != nil {
		// Use the standard log package to report the final error and exit.
		log.Fatalf("Execution failed: %v", err)
	}
}

func run() error {
	// FIXME(prozlach):  Rewrite command line args handling to spf13/cobra CLI framework.
	flag.BoolVar(&systemMode, "system-mode", false, "enable system-mode operation")
	flag.StringVar(&logLevel, "log-level", "info", "log-level")
	flag.BoolVar(&watch, "watch", false, "enable continuous printing of the status messages")
	flag.BoolVar(&logPretty, "log-pretty", false, "enables pretty-formatting of logs, be journald-friendly by default")
	flag.Parse()

	logger, syncF, err := misc.LoggerWithCRLog(logLevel, logPretty)
	if err != nil {
		return err
	}
	defer syncF()

	u, err := user.Current()
	if err != nil {
		return fmt.Errorf("unable to get current user: %w", err)
	}

	if systemMode {
		if u.Username != "root" {
			return errors.New("system-mode requires running as root user")
		}
		err = systemops.Do(logger)
	} else {
		if u.Username != "dev" && u.Username != "root" {
			return errors.New("please run as either `dev` or `root` user")
		}
		err = userops.Do(logger, watch)
	}

	// The error from systemops.Do or userops.Do is returned directly.
	return err
}
