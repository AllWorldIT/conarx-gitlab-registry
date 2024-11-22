package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/docker/distribution/cmd/internal/release-cli/client"
	"github.com/docker/distribution/cmd/internal/release-cli/slack"
	"github.com/docker/distribution/cmd/internal/release-cli/utils"
	"github.com/spf13/cobra"
	"github.com/xanzy/go-gitlab"
)

var gdkCmd = &cobra.Command{
	Use:   "gdk",
	Short: "Manage GDK release",
	Run: func(cmd *cobra.Command, args []string) {
		version := os.Getenv("CI_COMMIT_TAG")
		if version == "" {
			log.Fatal("Version is empty. Aborting.")
		}

		accessTokenGDK, err := cmd.Flags().GetString("gdk-access-token")
		if err != nil {
			log.Fatal(err)
		}

		accessTokenRegistry, err := cmd.Flags().GetString("registry-access-token")
		if err != nil {
			log.Fatal(err)
		}

		webhookUrl, err := cmd.Flags().GetString("slack-webhook-url")
		if err != nil {
			log.Fatal(err)
		}

		labels := &gitlab.LabelOptions{
			"workflow::ready for review",
			"group::container registry",
			"devops::package",
		}

		reviewerIDs := utils.ParseReviewerIDs(os.Getenv("MR_REVIWER_IDS"))

		release, err := readConfig(cmd.Use, version)
		if err != nil {
			log.Fatalf("Error reading config: %v", err)
			return
		}

		gdkClient := client.NewClient(accessTokenGDK)
		registryClient := client.NewClient(accessTokenRegistry)

		exists, err := gdkClient.BranchExists(release.ProjectID, release.BranchName)
		if err != nil {
			log.Printf("Error checking if branch exists: %v", err)
		}

		if exists {
			log.Printf("Branch %s already exists. Aborting.", release.BranchName)
			return
		}

		branch, err := gdkClient.CreateBranch(release.ProjectID, release.BranchName, release.Ref)
		if err != nil {
			log.Fatalf("Failed to create branch: %v", err)
		}

		desc, err := registryClient.GetChangelog(version)
		if err != nil {
			log.Fatalf("Failed to get changelog: %v", err)
		}

		for i := range release.Paths {
			fileName, err := gdkClient.GetFile(release.Paths[i], release.Ref, release.ProjectID)
			if err != nil {
				log.Fatalf("Failed to get the file: %v", err)
			}

			fileChange, err := utils.UpdateFileInGDK(fileName, version)
			if err != nil {
				log.Fatalf("Failed to update file: %v", err)
			}

			_, err = gdkClient.CreateCommit(release.ProjectID, fileChange, release.Paths[i], release.CommitMessage, branch)
			if err != nil {
				log.Fatalf("Failed to create commit: %v", err)
			}
		}

		mr, err := gdkClient.CreateMergeRequest(release.ProjectID, branch, desc, release.Ref, release.MRTitle, labels, reviewerIDs)
		if err != nil {
			msg := fmt.Sprintf("%s release: Failed to create GDK version bump MR: %s", version, err.Error())
			err = slack.SendSlackNotification(webhookUrl, msg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			log.Fatal(msg)
		}

		msg := fmt.Sprintf("%s release: GDK version bump MR: %s", version, mr.WebURL)
		err = slack.SendSlackNotification(webhookUrl, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}

		log.Println(msg)
	},
}

func init() {
	rootCmd.AddCommand(gdkCmd)

	gdkCmd.Flags().StringP("gdk-access-token", "", "", "Access token for GDK")
	_ = gdkCmd.MarkFlagRequired("gdk-access-token")
}
