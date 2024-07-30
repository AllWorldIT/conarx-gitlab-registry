package cmd

import "github.com/spf13/cobra"

var (
	RegistryToken   string
	SlackWebhookURL string
)

var rootCmd = &cobra.Command{
	Use:   "release",
	Short: "A CLI tool for Container Registry releases",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&RegistryToken, "registry-access-token", "", "", "Registry Access Token")
	rootCmd.PersistentFlags().StringVarP(&SlackWebhookURL, "slack-webhook-url", "", "", "Slack Webhook URL")
	rootCmd.MarkFlagRequired("registry-access-token")
	rootCmd.MarkFlagRequired("slack-webhook-url")
}
