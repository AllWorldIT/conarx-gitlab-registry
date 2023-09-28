package cmd

import (
	"fmt"
	"log"
	"os"
	"regexp"

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

		patternStr := fmt.Sprintf(`Update gitlab-org/container-registry from .* to %s`, version)
		pattern, err := regexp.Compile(patternStr)
		if err != nil {
			log.Fatalf("Error compiling regex pattern: %v", err)
		}
		
		exists, err := gitlabClient.MergeRequestExistsByPattern(release.ProjectID, pattern)
		if err != nil {
			log.Fatalf("Error checking if MR exists: %v", err)
		}
		
		if exists {
			log.Printf("Merge Request matching pattern '%s' already exists. Aborting.", patternStr)
			return
		}
				
		pipelineURL, err := gitlabClient.SendRequestToDeps(release.ProjectID, triggerToken, release.Ref)
		if err != nil {
			msg := fmt.Sprintf("%s release: Failed to trigger Charts version bump MR: %s", version, err.Error())
			err = slack.SendSlackNotification(webhookUrl, msg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			log.Fatalf(msg)
		}
		msg := fmt.Sprintf("%s release: Charts version bump MR trigger pipeline: %s", version, pipelineURL)
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
