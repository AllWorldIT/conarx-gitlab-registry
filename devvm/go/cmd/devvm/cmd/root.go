package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
)

func Execute() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	shutdownTimeout := time.Duration(constants.ShutdownTimeout) * time.Second

	// Signal handling code is based on
	// https://github.com/cosmos/relayer/blob/259b1278264180a2aefc2085f1b55753849c4815/cmd/root.go
	go func() {
		<-sigCh

		cancel()

		_, _ = fmt.Fprintln(
			os.Stderr,
			"SIGINT received, attempting a clean shutdown. "+
				"Send interrupt again to force hard shutdown.",
		)

		select {
		case <-time.After(shutdownTimeout):
			panic(fmt.Sprintf("timed after %s while waiting for clean shutdown, forcing shutdown", shutdownTimeout))
		case <-sigCh:
			panic("received another SIGINT signal, forcing shutdown")
		}
	}()

	rootCmd := &cobra.Command{
		Use:   "devvm",
		Short: "devvm is a tool to manage devvms",
	}

	logLevel := new(string)
	rootCmd.PersistentFlags().StringVar(
		logLevel, "log-level", "info", "logging subsystem log level")

	rootCmd.AddCommand(newCreateCmd(logLevel))
	rootCmd.AddCommand(newListCmd(logLevel))
	rootCmd.AddCommand(newDeleteCmd(logLevel))
	rootCmd.AddCommand(newKubeconfigCmd(logLevel))

	return rootCmd.ExecuteContext(ctx)
}
