package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	pkgcmd "gitlab.com/gitlab-org/container-registry/devvm/go/pkg/cmd"
	"golang.org/x/term"
)

// Parts of the code were inspired/based on https://github.com/openclarity/vmclarity

func newCreateCmd(logLevel *string) *cobra.Command {
	var zone string
	var registryBranch string
	var gitLabBranch string
	var customSSHKey string
	var imageName string
	var licensePath string
	var omnibus bool
	var eeVersion bool
	var noDeploy bool

	createCmd := &cobra.Command{
		Use:     "create <devvm-name>",
		Aliases: []string{"launch", "start"},
		Short:   "Launch a devvm",
		Long: `Launch a new devvm instance.

If an SSH key is provided with --ssh-key-path, the command will:
1. First try to parse the key without a passphrase (for unencrypted keys)
2. If the key is encrypted, check if it's available in the SSH agent
3. If not found in the SSH agent, prompt for the passphrase

This means if your key is already loaded in an SSH agent, you won't need to enter
the passphrase manually.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate that license is provided when EE version is enabled
			if eeVersion && licensePath == "" {
				return fmt.Errorf("--license is mandatory when --ee-version is specified")
			}

			deploymentType := "charts"
			if omnibus {
				deploymentType = "omnibus"
			}

			// Validate branch options based on deployment type
			if omnibus {
				if cmd.Flags().Changed("registry-branch") {
					return fmt.Errorf("--registry-branch is not yet supported for omnibus deployment")
				}
				if cmd.Flags().Changed("gitlab-branch") {
					return fmt.Errorf("--gitlab-branch is not yet supported for omnibus deployment")
				}
			} else if cmd.Flags().Changed("gitlab-branch") {
				return fmt.Errorf("--gitlab-branch is not yet supported for charts deployment")
			}

			return pkgcmd.CreateVM(
				cmd.Context(),
				args[0],
				zone,
				registryBranch,
				gitLabBranch,
				*logLevel,
				term.IsTerminal(int(os.Stdout.Fd())),
				customSSHKey,
				imageName,
				licensePath,
				deploymentType,
				eeVersion,
				noDeploy,
			)
		},
	}

	createCmd.Flags().BoolVar(
		&omnibus, "omnibus",
		false, "use omnibus deployment instead of charts (default: charts)",
	)
	createCmd.Flags().StringVarP(
		&imageName, "image-name", "i",
		"devvm-current", "devvm base image to use for launching the VM")
	createCmd.Flags().StringVarP(
		&zone, "zone", "z",
		"europe-west1-b", "GCE zone to launch VM in")
	createCmd.Flags().StringVarP(
		&registryBranch, "registry-branch", "r",
		"master", "GOB branch to use to launch VM",
	)
	createCmd.Flags().StringVarP(
		&gitLabBranch, "gitlab-branch", "g",
		"master", "GitLab branch to use to launch VM",
	)
	createCmd.Flags().StringVarP(
		&customSSHKey, "ssh-key-path", "k",
		"", "SSH key to use to launch devvm (uses SSH agent if key is encrypted and agent is available)",
	)
	createCmd.Flags().StringVarP(
		&licensePath, "license", "l",
		"", "path to GitLab EE license file to upload to the devvm",
	)
	createCmd.Flags().BoolVar(
		&eeVersion, "ee",
		false, "enable GitLab EE version (requires --license)",
	)
	createCmd.Flags().BoolVar(
		&noDeploy, "no-deploy",
		false, "skip GitLab deployment",
	)

	return createCmd
}
