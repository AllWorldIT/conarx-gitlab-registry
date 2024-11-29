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

var stage string

var k8sCmd = &cobra.Command{
	Use:   "k8s",
	Short: "Manage K8s Workloads release",
	RunE: func(cmd *cobra.Command, _ []string) error {
		var suffix string

		version := os.Getenv("CI_COMMIT_TAG")
		if version == "" {
			return errors.New("version is empty")
		}

		reviewerIDs := utils.ParseReviewerIDs(os.Getenv("MR_REVIWER_IDS"))

		accessTokenK8s, err := cmd.Flags().GetString("k8s-access-token")
		if err != nil {
			return fmt.Errorf("fetching access token flag: %w", err)
		}

		accessTokenRegistry, err := cmd.Flags().GetString("registry-access-token")
		if err != nil {
			return fmt.Errorf("fetching registry access token: %w", err)
		}

		webhookURL, err := cmd.Flags().GetString("slack-webhook-url")
		if err != nil {
			return fmt.Errorf("fetching slack webhook url: %w", err)
		}

		labels := &gitlab.LabelOptions{
			"workflow::ready for review",
			"team::Delivery",
			"Service::Container Registry",
		}

		switch stage {
		case "gprd":
			suffix = "_gprd"
		case "gprd-cny":
			suffix = "_gprd_cny"
		case "gstg-pre":
			suffix = "_gstg_pre"
		default:
			return fmt.Errorf("unknown stage supplied: %q", stage)
		}
		newCmd := fmt.Sprintf("%s%s", cmd.Use, suffix)

		release, err := readConfig(newCmd, version)
		if err != nil {
			return fmt.Errorf("reading config: %w", err)
		}

		k8sClient, err := client.NewClient(accessTokenK8s)
		if err != nil {
			return err
		}
		registryClient, err := client.NewClient(accessTokenRegistry)
		if err != nil {
			return err
		}

		exists, err := k8sClient.BranchExists(release.ProjectID, release.BranchName)
		if err != nil {
			return fmt.Errorf("checking if branch exists: %w", err)
		}

		if exists {
			return fmt.Errorf("branch %s already exists", release.BranchName)
		}

		branch, err := k8sClient.CreateBranch(release.ProjectID, release.BranchName, release.Ref)
		if err != nil {
			return fmt.Errorf("creating branch: %w", err)
		}

		desc, err := registryClient.GetChangelog(version)
		if err != nil {
			return fmt.Errorf("getting changelog: %w", err)
		}

		for i := range release.Paths {
			fileName, err := k8sClient.GetFile(release.Paths[i], release.Ref, release.ProjectID)
			if err != nil {
				return fmt.Errorf("getting the file: %w", err)
			}

			fileChange, err := utils.UpdateFileInK8s(fileName, stage, version)
			if err != nil {
				return fmt.Errorf("updating the file: %w", err)
			}
			_, err = k8sClient.CreateCommit(release.ProjectID, fileChange, release.Paths[i], release.CommitMessage, branch)
			if err != nil {
				return fmt.Errorf("creating the commit: %w", err)
			}
		}

		mr, err := k8sClient.CreateMergeRequest(release.ProjectID, branch, desc, release.Ref, release.MRTitle, labels, reviewerIDs)
		if err != nil {
			msg := fmt.Sprintf("%s release: Failed to create K8s Workloads version bump MR (%s): %s", version, stage, err.Error())
			err = slack.SendSlackNotification(webhookURL, msg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			return errors.New(msg)
		}

		msg := fmt.Sprintf("%s release: K8s Workloads version bump MR (%s): %s", version, stage, mr.WebURL)
		err = slack.SendSlackNotification(webhookURL, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}

		log.Println(msg)
		return nil
	},
}

func init() {
	RootCmd.AddCommand(k8sCmd)

	k8sCmd.Flags().StringVarP(&stage, "stage", "s", "", "Stage in the environment")
	k8sCmd.Flags().String("k8s-access-token", "", "Access token for K8s")
	_ = k8sCmd.MarkFlagRequired("k8s-access-token")
	_ = k8sCmd.MarkFlagRequired("stage")
}
