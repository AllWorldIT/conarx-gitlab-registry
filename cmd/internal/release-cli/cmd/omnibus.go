package cmd

import (
	"fmt"
	"log"

	"github.com/docker/distribution/cmd/internal/release-cli/client"
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

		release, err := readConfig(cmd.Use)
		if err != nil {
			fmt.Println("Error reading config:", err)
			return
		}

		gitlabClient := client.NewClient(accessToken)

		err = gitlabClient.SendRequestToDeps(release.ProjectID, triggerToken, release.Ref)
		if err != nil {
			log.Fatalf("Failed to trigger a pipeline in Omnibus: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(omnibusCmd)

	omnibusCmd.Flags().StringP("omnibus-trigger-token", "", "", "Trigger token for Omnibus")
	omnibusCmd.MarkFlagRequired("omnibus-trigger-token")
}
