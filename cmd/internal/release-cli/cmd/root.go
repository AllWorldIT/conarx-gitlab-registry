package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	RegistryToken   string
	SlackWebhookURL string
)

var RootCmd = &cobra.Command{
	Use:           "release",
	Short:         "A CLI tool for Container Registry releases",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	cobra.OnInitialize(initConfig)
	RootCmd.PersistentFlags().StringVarP(&RegistryToken, "registry-access-token", "", "", "Registry Access Token")
	RootCmd.PersistentFlags().StringVarP(&SlackWebhookURL, "slack-webhook-url", "", "", "Slack Webhook URL")
	_ = RootCmd.MarkFlagRequired("registry-access-token")
	_ = RootCmd.MarkFlagRequired("slack-webhook-url")

	RootCmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		return fmt.Errorf("%w\n\n%s", err, c.UsageString())
	})
}
