package cmd

import (
	"errors"
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
	RunE: func(cmd *cobra.Command, _ []string) error {
		version := os.Getenv("CI_COMMIT_TAG")
		if version == "" {
			return errors.New("version is empty, aborting")
		}

		accessTokenGDK, err := cmd.Flags().GetString("gdk-access-token")
		if err != nil {
			return fmt.Errorf("getting `gdk-access-token`: %w", err)
		}

		accessTokenRegistry, err := cmd.Flags().GetString("registry-access-token")
		if err != nil {
			return fmt.Errorf("getting flag `registry-access-token`: %w", err)
		}

		webhookURL, err := cmd.Flags().GetString("slack-webhook-url")
		if err != nil {
			return fmt.Errorf("getting flag `slack-webhook-url`: %w", err)
		}

		labels := &gitlab.LabelOptions{
			"workflow::ready for review",
			"group::container registry",
			"devops::package",
		}

		reviewerIDs := utils.ParseReviewerIDs(os.Getenv("MR_REVIWER_IDS"))

		release, err := readConfig(cmd.Use, version)
		if err != nil {
			return fmt.Errorf("error reading config: %w", err)
		}

		gdkClient, err := client.NewClient(accessTokenGDK)
		if err != nil {
			return err
		}
		registryClient, err := client.NewClient(accessTokenRegistry)
		if err != nil {
			return err
		}

		exists, err := gdkClient.BranchExists(release.ProjectID, release.BranchName)
		if err != nil {
			return fmt.Errorf("checking if branch exists: %w", err)
		}

		if exists {
			return fmt.Errorf("branch %s already exists", release.BranchName)
		}

		branch, err := gdkClient.CreateBranch(release.ProjectID, release.BranchName, release.Ref)
		if err != nil {
			return fmt.Errorf("creating branch: %w", err)
		}

		desc, err := registryClient.GetChangelog(version)
		if err != nil {
			return fmt.Errorf("getting changelog: %w", err)
		}

		for i := range release.Paths {
			fileName, err := gdkClient.GetFile(release.Paths[i], release.Ref, release.ProjectID)
			if err != nil {
				return fmt.Errorf("getting file: %w", err)
			}

			fileChange, err := utils.UpdateFileInGDK(fileName, version)
			if err != nil {
				return fmt.Errorf("updating file: %w", err)
			}

			_, err = gdkClient.CreateCommit(release.ProjectID, fileChange, release.Paths[i], release.CommitMessage, branch)
			if err != nil {
				return fmt.Errorf("creating commit: %w", err)
			}
		}

		mr, err := gdkClient.CreateMergeRequest(release.ProjectID, branch, desc, release.Ref, release.MRTitle, labels, reviewerIDs)
		if err != nil {
			errIn := fmt.Errorf("%s release: Failed to create GDK version bump MR: %w", version, err)
			errSlack := slack.SendSlackNotification(webhookURL, errIn.Error())
			if errSlack != nil {
				log.Printf("Failed to send error notification to Slack: %v", errSlack)
			}
			return errIn
		}

		msg := fmt.Sprintf("%s release: GDK version bump MR: %s", version, mr.WebURL)
		err = slack.SendSlackNotification(webhookURL, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}

		log.Println(msg)
		return nil
	},
}

func init() {
	RootCmd.AddCommand(gdkCmd)

	gdkCmd.Flags().StringP("gdk-access-token", "", "", "Access token for GDK")
	_ = gdkCmd.MarkFlagRequired("gdk-access-token")
}
