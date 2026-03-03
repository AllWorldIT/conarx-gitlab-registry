package cmd

import (
	"context"
	"fmt"
	"os"

	compute "cloud.google.com/go/compute/apiv1"
	"github.com/spf13/cobra"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/misc"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/provisioning"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
)

func newKubeconfigCmd(logLevel *string) *cobra.Command {
	var (
		zone       string
		sshKeyPath string
		outputFile string
		logPretty  bool
	)

	cmd := &cobra.Command{
		Use:   "kubeconfig <devvm-name>",
		Short: "Fetch kubeconfig from a running devvm",
		Long: `Fetch the kubeconfig file from a running devvm instance.

This command requires a custom SSH key that was used when creating the devvm.
The kubeconfig can be printed to stdout or saved to a file.

The --zone flag is optional if there is only one instance with the given name.
If multiple instances with the same name exist in different zones, you must specify the zone.

If an SSH agent is running and contains the key, the passphrase will not be required.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fetchKubeconfig(
				cmd.Context(),
				args[0], // instanceName from positional argument
				zone,
				sshKeyPath,
				outputFile,
				*logLevel,
				logPretty,
			)
		},
	}

	cmd.Flags().StringVarP(&zone, "zone", "z", "", "GCE zone where the devvm is running (optional if only one instance with the name exists)")
	cmd.Flags().StringVarP(&sshKeyPath, "ssh-key", "k", "", "Path to the SSH private key used to create the devvm (required)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file path (if not specified, prints to stdout)")
	cmd.Flags().BoolVar(&logPretty, "log-pretty", false, "Enable pretty-formatting of logs")

	_ = cmd.MarkFlagRequired("ssh-key")

	return cmd
}

func fetchKubeconfig(
	ctx context.Context,
	instanceName, zone, sshKeyPath, outputFile, logLevel string,
	logPretty bool,
) error {
	logger, syncF, err := misc.Logger(logLevel, logPretty)
	if err != nil {
		return fmt.Errorf("unable to instantiate logger: %w", err)
	}
	defer syncF()

	// Get SSH signer using the shared function (tries agent first, then falls back to passphrase)
	signer, _, err := provisioning.ReadSSHKeysWithAgent(sshKeyPath)
	if err != nil {
		return err
	}
	logger.Infof("SSH key ready from %s", sshKeyPath)

	// Get VM info to find public IP
	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create instance client: %w", err)
	}
	defer instancesClient.Close()

	// Resolve zone if not provided (same pattern as delete subcommand)
	resolvedZone, err := misc.ResolveInstanceZone(ctx, instancesClient, instanceName, zone)
	if err != nil {
		return fmt.Errorf("resolving instance zone: %w", err)
	}
	if zone == "" {
		logger.Infof("Auto-detected zone: %s", resolvedZone)
	}

	publicIP, err := provisioning.GetVMPublicIP(ctx, instanceName, constants.GCEProject, resolvedZone, instancesClient)
	if err != nil {
		return err
	}

	logger.Infof("Devvm public IP: %s", publicIP)

	sshConn, sftpClient, err := provisioning.EstablishSSHWithSFTP(ctx, signer, publicIP)
	if err != nil {
		return err
	}
	defer sshConn.Close()
	defer sftpClient.Close()

	// Fetch status data
	statusData, err := provisioning.FetchStatusData(sftpClient)
	if err != nil {
		return fmt.Errorf("unable to fetch status data from devvm: %w", err)
	}

	// Find kubeconfig in status metadata
	kubeconfig := findKubeconfigInStatus(statusData)
	if kubeconfig == "" {
		return fmt.Errorf("kubeconfig not found in devvm status - the cluster may not be ready yet")
	}

	// Output kubeconfig
	if outputFile == "" {
		_, _ = fmt.Print(kubeconfig)
	} else {
		err = os.WriteFile(outputFile, []byte(kubeconfig), 0o600)
		if err != nil {
			return fmt.Errorf("unable to write kubeconfig to %s: %w", outputFile, err)
		}
		logger.Infof("Kubeconfig written to %s", outputFile)
	}

	return nil
}

func findKubeconfigInStatus(statusData *status.StatusData) string {
	for _, ps := range statusData.PartialStatuses {
		if ps.Metadata != nil {
			if kubeconfig, ok := ps.Metadata["kubeconfig"]; ok {
				return kubeconfig
			}
		}
	}
	return ""
}
