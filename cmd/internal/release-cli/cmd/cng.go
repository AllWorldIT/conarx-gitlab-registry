package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/docker/distribution/cmd/internal/release-cli/client"
	"github.com/docker/distribution/cmd/internal/release-cli/slack"
	"github.com/spf13/cobra"
)

var cngCmd = &cobra.Command{
	Use:   "cng",
	Short: "Manage CNG release",
	Run: func(cmd *cobra.Command, args []string) {
		triggerToken, err := cmd.Flags().GetString("cng-trigger-token")
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
			msg := fmt.Sprintf("%s release: Failed to trigger CNG version bump MR: %s", version, err.Error())
			err = slack.SendSlackNotification(webhookUrl, msg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			log.Fatalf(msg)
		}
		msg := fmt.Sprintf("%s release: CNG version bump MR trigger pipeline: %s", version, pipelineURL)
		err = slack.SendSlackNotification(webhookUrl, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}

		log.Println(msg)
	},
}

func init() {
	rootCmd.AddCommand(cngCmd)

	cngCmd.Flags().StringP("cng-trigger-token", "", "", "Trigger token for CNG")
	cngCmd.MarkFlagRequired("cng-trigger-token")
}
