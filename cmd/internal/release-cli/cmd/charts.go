package cmd

import (
	"log"
	"os"

	"github.com/docker/distribution/cmd/internal/release-cli/client"
	"github.com/docker/distribution/cmd/internal/release-cli/slack"
	"github.com/spf13/cobra"
)

var chartsCmd = &cobra.Command{
	Use:   "charts",
	Short: "Manage Charts release",
	Run: func(cmd *cobra.Command, args []string) {
		triggerToken, err := cmd.Flags().GetString("charts-trigger-token")
		if err != nil {
			log.Fatal(err)
		}

		accessToken, err := cmd.Flags().GetString("registry-access-token")
		if err != nil {
			log.Fatal(err)
		}

		version := os.Getenv("CI_COMMIT_TAG")
		if version == "" {
			log.Fatal("Version is empty. Aborting.")
		}

		release, err := readConfig(cmd.Use, version)
		if err != nil {
			log.Fatalf("Error reading config: %v", err)
			return
		}

		webhookUrl, err := cmd.Flags().GetString("slack-webhook-url")
		if err != nil {
			log.Fatal(err)
		}

		gitlabClient := client.NewClient(accessToken)

		pipelineURL, err := gitlabClient.SendRequestToDeps(release.ProjectID, triggerToken, release.Ref)
		if err != nil {
			errMsg := "Failed to trigger a pipeline in Charts: " + err.Error()
			err = slack.SendSlackNotification(webhookUrl, errMsg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			log.Fatalf(errMsg)
		}
		msg := "Charts trigger pipeline URL for version bump: " + pipelineURL
		err = slack.SendSlackNotification(webhookUrl, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}
		log.Println(msg)
	},
}

func init() {
	rootCmd.AddCommand(chartsCmd)

	chartsCmd.Flags().StringP("charts-trigger-token", "", "", "Trigger token for Charts")
	chartsCmd.MarkFlagRequired("charts-trigger-token")
}
