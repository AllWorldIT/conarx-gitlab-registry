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

var stage string

var k8sCmd = &cobra.Command{
	Use:   "k8s",
	Short: "Manage K8s Workloads release",
	Run: func(cmd *cobra.Command, args []string) {
		var suffix string

		version := os.Getenv("CI_COMMIT_TAG")
		if version == "" {
			log.Fatal("Version is empty. Aborting.")
		}

		reviewerIDs := utils.ParseReviewerIDs(os.Getenv("MR_REVIWER_IDS"))

		accessTokenK8s, err := cmd.Flags().GetString("k8s-access-token")
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
			log.Fatalf("unknown stage supplied: %q", stage)
		}
		newCmd := fmt.Sprintf("%s%s", cmd.Use, suffix)

		release, err := readConfig(newCmd, version)
		if err != nil {
			log.Fatalf("Error reading config: %v", err)
			return
		}

		k8sClient := client.NewClient(accessTokenK8s)
		registryClient := client.NewClient(accessTokenRegistry)

		exists, err := k8sClient.BranchExists(release.ProjectID, release.BranchName)
		if err != nil {
			log.Printf("Error checking if branch exists: %v", err)
		}

		if exists {
			log.Printf("Branch %s already exists. Aborting.", release.BranchName)
			return
		}

		branch, err := k8sClient.CreateBranch(release.ProjectID, release.BranchName, release.Ref)
		if err != nil {
			log.Fatalf("Failed to create branch: %v", err)
		}

		desc, err := registryClient.GetChangelog(version)
		if err != nil {
			log.Fatalf("Failed to get changelog: %v", err)
		}

		for i := range release.Paths {
			fileName, err := k8sClient.GetFile(release.Paths[i], release.Ref, release.ProjectID)
			if err != nil {
				log.Fatalf("Failed to get the file: %v", err)
			}

			fileChange, err := utils.UpdateFileInK8s(fileName, stage, version)
			if err != nil {
				log.Fatalf("Failed to update file: %v", err)
			}
			_, err = k8sClient.CreateCommit(release.ProjectID, fileChange, release.Paths[i], release.CommitMessage, branch)
			if err != nil {
				log.Fatalf("Failed to create commit: %v", err)
			}
		}

		mr, err := k8sClient.CreateMergeRequest(release.ProjectID, branch, desc, release.Ref, release.MRTitle, labels, reviewerIDs)
		if err != nil {
			msg := fmt.Sprintf("%s release: Failed to create K8s Workloads version bump MR (%s): %s", version, stage, err.Error())
			err = slack.SendSlackNotification(webhookUrl, msg)
			if err != nil {
				log.Printf("Failed to send error notification to Slack: %v", err)
			}
			log.Fatal(msg)
		}

		msg := fmt.Sprintf("%s release: K8s Workloads version bump MR (%s): %s", version, stage, mr.WebURL)
		err = slack.SendSlackNotification(webhookUrl, msg)
		if err != nil {
			log.Printf("Failed to send notification to Slack: %v", err)
		}

		log.Println(msg)
	},
}

func init() {
	rootCmd.AddCommand(k8sCmd)

	k8sCmd.Flags().StringVarP(&stage, "stage", "s", "", "Stage in the environment")
	k8sCmd.Flags().String("k8s-access-token", "", "Access token for K8s")
	k8sCmd.MarkFlagRequired("k8s-access-token")
	k8sCmd.MarkFlagRequired("stage")
}
