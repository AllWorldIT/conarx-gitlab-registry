package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/misc"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/provisioning"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"golang.org/x/crypto/ssh"
)

func CreateVM(
	ctx context.Context,
	instanceName, zone, registryBranch, gitLabBranch, logLevel string,
	logPretty bool,
	customSSHKey, imageName, licensePath, deploymentType string,
	eeVersion, noDeploy bool,
) error {
	// Validate deployment type
	if deploymentType != "charts" && deploymentType != "omnibus" {
		return fmt.Errorf("invalid deployment type %q: must be 'charts' or 'omnibus'", deploymentType)
	}

	logger, syncF, err := misc.LoggerWithCRLog(logLevel, logPretty)
	if err != nil {
		return err
	}
	defer syncF()

	region, err := provisioning.GetGCERegion(zone)
	if err != nil {
		return fmt.Errorf("unable to determine region basing on gce zone: %w", err)
	}

	var signer ssh.Signer
	var pubKey string
	// nolint: revive // nesting reflects business logic
	if customSSHKey == "" {
		signer, pubKey, err = provisioning.GenerateECDSAKeys(256)
		if err != nil {
			return fmt.Errorf("unable to generate ssh key: %w", err)
		}
		logger.Infow("using ephemeral SSH key")
	} else {
		// Use SSH agent-aware key reading
		signer, pubKey, err = provisioning.ReadSSHKeysWithAgent(customSSHKey)
		if err != nil {
			return err
		}
		logger.Infof("SSH key ready from %s", customSSHKey)
	}

	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create instance client: %w", err)
	}
	defer instancesClient.Close()

	created, err := provisioning.EnsureVM(
		ctx, instanceName, constants.GCEProject, zone, pubKey,
		region, registryBranch, gitLabBranch, instancesClient,
		imageName, deploymentType, eeVersion, noDeploy,
	)
	if err != nil {
		return fmt.Errorf("ensuring VM is running failed: %w", err)
	}
	logger.Infow("Instance ensured", "created", created)

	// If VM already exists and no custom SSH key was provided, we cannot
	// re-generate the ephemeral key that was used during initial creation
	if !created && customSSHKey == "" {
		return fmt.Errorf("VM %q already exists but no custom SSH key provided; cannot regenerate the ephemeral key used during initial creation", instanceName)
	}

	publicIP, err := provisioning.GetVMPublicIP(ctx, instanceName, constants.GCEProject, zone, instancesClient)
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

	// Upload license file if provided
	if licensePath != "" {
		logger.Infof("Uploading license file from %s", licensePath)
		err = provisioning.UploadLicenseFile(sftpClient, licensePath)
		if err != nil {
			return fmt.Errorf("unable to upload license file: %w", err)
		}
		logger.Info("License file uploaded successfully")
	}

	// It takes some time for booter to configure all operations, hence we
	// retry with a time limit:
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	timeout := 25 * time.Minute
	to := time.NewTimer(timeout)
	defer to.Stop()

	var sd *status.StatusData
	var lastMsgs []string
LOOP:
	for {
		select {
		case <-ctx.Done():
			return errors.New("context canceled while waiting for booter to set up VM")
		case <-to.C:
			return fmt.Errorf("timed out after %s while waiting for booter to set up devvm", timeout.String())
		case <-ticker.C:
			sd, err = provisioning.FetchStatusData(sftpClient)
			if err != nil {
				// nolint: revive // nesting reflects business logic
				if errors.Is(err, os.ErrNotExist) {
					logger.Infof("Booter has not started yet, status files does not exist")
					continue
				}
				return fmt.Errorf("unable to fetch status data from devvm: %w", err)
			}
			allDone, msgs := sd.StatusSummary()
			if allDone {
				break LOOP
			}
			if !misc.StringSlicesEqual(lastMsgs, msgs) {
				logger.Info("Booter is executing:")
				for _, msg := range msgs {
					logger.Infof(" - %s", msg)
				}
				lastMsgs = msgs
			}
		}
	}

	logger.Info("Booter finished initialization of the devvm")

	logger.Info("All done!")
	return nil
}
