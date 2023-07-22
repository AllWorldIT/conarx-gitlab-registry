package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/docker/distribution/cmd/internal/release-cli/client"
	"github.com/docker/distribution/cmd/internal/release-cli/slack"
	"github.com/spf13/cobra"
)

var omnibusCmd = &cobra.Command{
	Use:   "omnibus",
	Short: "Manage Omnibus release",
	Run: func(cmd *cobra.Command, args []string) {
		triggerToken, err := cmd.Flags().GetString("omnibus-trigger-token")
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
			fmt.Println("Error reading config:", err)
			return
		}

		webhookUrl, err := cmd.Flags().GetString("slack-webhook-url")
		if err != nil {
			log.Fatal(err)
		}

		gitlabClient := client.NewClient(accessToken)

		pipelineURL, err := gitlabClient.SendRequestToDeps(release.ProjectID, triggerToken, release.Ref)
		if err != nil {
			errMsg := "Failed to trigger a pipeline in Omnibus: " + err.Error()
			err = slack.SendSlackNotification(webhookUrl, errMsg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			log.Fatalf(errMsg)
		}
		msg := "Omnibus trigger pipeline URL for version bump: " + pipelineURL
		err = slack.SendSlackNotification(webhookUrl, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}

		log.Println(msg)
	},
}

func init() {
	rootCmd.AddCommand(omnibusCmd)

	omnibusCmd.Flags().StringP("omnibus-trigger-token", "", "", "Trigger token for Omnibus")
	omnibusCmd.MarkFlagRequired("omnibus-trigger-token")
}
